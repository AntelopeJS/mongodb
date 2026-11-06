/**
 * Checks that a query on a schema and the index builds of that schema's
 * registrations never race (see SKILL.md). Drives the real
 * `Schemas.register`, `RunQuery` and `InitializeSchema` of `src/`,
 * compiled with this folder by `check.mjs`, against the in-memory database
 * of `fake-mongo.ts`.
 * fast-check draws the scenarios and `explore.ts` runs each under every
 * order of its steps. Exits 0 when every rule matches the record in
 * SKILL.md, 1 when a rule changed, and 2 when the check could not run.
 */
import { parseArgs } from "node:util";
import fc from "fast-check";
import type { SchemaDefinition } from "@antelopejs/interface-database/schema";

import { Disconnect, InitializeSchema } from "../../../src/connection";
import { RunQuery } from "../../../src/implementations/database/query";
import { Schemas } from "../../../src/implementations/database/schema";
import type { QueryStage } from "../../../src/implementations/database/utils";
import { SchemaInitialized } from "../../../src/schema-initialization";
import { exploreOrders, type Schedule } from "./explore";
import { actor, FakeDatabase } from "./fake-mongo";
import {
  type Query,
  type QueryKind,
  RULES,
  type Run,
  type Scenario,
  scenarioArb,
  TABLE,
} from "./rules";
import { connectFake, server } from "./server";

const EXIT = { ok: 0, changed: 1, tool: 2 };
const BOUNDS = {
  ci: { scenarios: 1000, orders: 5000 },
  full: { scenarios: 3000, orders: 20000 },
};
const DEFAULT_SEED = 969;

/** Rules the code breaks today; SKILL.md records each one. */
const FINDINGS: ReadonlySet<string> = new Set<keyof typeof RULES>([
  "SupersededQueryWaits",
  "InFlightReadSeesDeclaredIndexes",
]);

interface Options {
  scenarios: number;
  orders: number;
  seed: number;
}

interface Exploration {
  counterexamples: Map<string, string>;
  scenarios: number;
  orders: number;
  cut: number;
}

let runCount = 0;
/** Set once the rules are judged, so a crash before that exits as "could not run". */
let judged = false;

async function barrierState(schemaId: string): Promise<string> {
  const unsettled = "still pending";
  return Promise.race([
    SchemaInitialized(schemaId).then(
      () => "open",
      (error: unknown) => `rejected (${String(error)})`,
    ),
    new Promise<string>((resolve) => setImmediate(() => resolve(unsettled))),
  ]);
}

/** The stages `RunQuery` receives for each kind of query, as the interface builds them. */
function queryStages(kind: QueryKind, schemaId: string): QueryStage[] {
  const schema: QueryStage = { stage: "schema", options: { id: schemaId }, args: [] };
  if (kind === "listInstances") {
    return [schema, { stage: "listInstances", args: [] }];
  }
  return [
    schema,
    { stage: "instance", options: { id: "tenant" }, args: [] },
    { stage: "table", options: { id: TABLE }, args: [] },
  ];
}

function issueQuery(run: Run, database: FakeDatabase, kind: QueryKind): void {
  const query: Query = {
    name: `query ${run.queries.length + 1}`,
    issuedAfter: run.declarations.length,
    passedBarrier: false,
    superseded: false,
    inFlight: false,
    outcome: "waiting",
  };
  run.queries.push(query);
  database.trace.push(`${query.name}: issued, RunQuery ${kind}`);
  SchemaInitialized(run.schemaId).then(
    () => {
      query.passedBarrier = true;
    },
    () => undefined,
  );
  void actor.run(query.name, () =>
    RunQuery(queryStages(kind, run.schemaId)).then(
      () => {
        const read = database.reads.get(query.name);
        Object.assign(query, read ? { outcome: "read", read } : { outcome: "answered without a read" });
      },
      (error: unknown) =>
        Object.assign(query, {
          outcome: `rejected (${error instanceof Error ? error.message : String(error)})`,
        }),
    ),
  );
}

function register(
    run: Run,
  database: FakeDatabase,
  declaration: SchemaDefinition,
): void {
  run.declarations.push(declaration);
  database.registrations = run.declarations.length;
  const name = `build ${run.declarations.length}`;
  database.trace.push(`register ${name}: ${JSON.stringify(declaration)}`);
  for (const query of run.queries.filter((each) => each.outcome === "waiting")) {
    query.superseded ||= !query.passedBarrier;
    query.inFlight ||= query.passedBarrier;
  }
  run.latest = declaration;
  actor.run(name, () => Schemas.register(run.schemaId, declaration));
}

/** Runs `scenario` in the order `schedule` picks and returns what it left. */
async function runScenario(
    schedule: Schedule,
  scenario: Scenario,
): Promise<Run> {
  runCount += 1;
  const database = new FakeDatabase();
  server.database = database;
  const run: Run = {
    schemaId: `s${runCount}`,
    earlier: scenario.earlier,
    latest: scenario.first,
    declarations: [],
    queries: [],
    barrier: "",
    settled: new Map(),
    trace: [],
  };
  if (scenario.earlier) {
    const earlier = scenario.earlier;
    await actor.run("earlier build", () =>
      InitializeSchema(run.schemaId, earlier),
    );
    database.trace.length = 0;
  }
  database.step = (_label, apply) => schedule.step().then(apply);
  const issue = (at: number): void => {
    const action = scenario.later[at];
    if (!action) {
      return;
    }
    void schedule.step().then(() => {
      if ("register" in action) {
        register(run, database, action.register);
      } else {
        issueQuery(run, database, action.query);
      }
      issue(at + 1);
    });
  };
  register(run, database, scenario.first);
  issue(0);
  await schedule.drain();
  run.barrier = await barrierState(run.schemaId);
  run.settled = database.snapshot();
  run.trace = [...database.trace];
  Schemas.unregister(run.schemaId);
  return run;
}

/** The run's steps in order, with collections named by their table. */
function describeRun(run: Run, problems: string[], picks: number[]): string {
  return [
    `picks [${picks.join(",")}]`,
    ...(run.earlier ? [`earlier build: ${JSON.stringify(run.earlier)}`] : []),
    ...run.trace,
    ...problems.map((problem) => `=> ${problem}`),
  ]
    .map((line) => `  ${line.replaceAll(`${run.schemaId}__`, "")}`)
    .join("\n");
}

/**
 * Runs `options.scenarios` scenarios, each under every order of its steps
 * up to `options.orders`, and keeps for each rule its shortest
 * counterexample. It stops after the first scenario that breaks a rule
 * outside `FINDINGS`, since that run fails whatever comes next.
 */
async function explore(options: Options): Promise<Exploration> {
  const exploration: Exploration = {
    counterexamples: new Map(),
    scenarios: 0,
    orders: 0,
    cut: 0,
  };
  const shortest = new Map<string, number>();
  const scenarios = fc.sample(scenarioArb, {
    numRuns: options.scenarios,
    seed: options.seed,
  });
  for (const scenario of scenarios) {
    const explored = await exploreOrders(async (schedule) => {
      const run = await runScenario(schedule, scenario);
      for (const [rule, judge] of Object.entries(RULES)) {
        const problems = judge(run);
        const length = run.trace.length;
        if (problems.length > 0 && length < (shortest.get(rule) ?? Infinity)) {
          shortest.set(rule, length);
          const picks = schedule.choices.map((choice) => choice.pick);
          exploration.counterexamples.set(rule, describeRun(run, problems, picks));
        }
      }
    }, options.orders);
    exploration.orders += explored.orders;
    exploration.cut += explored.exhausted ? 0 : 1;
    exploration.scenarios += 1;
    if ([...exploration.counterexamples.keys()].some((rule) => !FINDINGS.has(rule))) {
      return exploration;
    }
  }
  return exploration;
}

function positiveInteger(flag: string, given: string | undefined, fallback: number): number {
  if (given === undefined) {
    return fallback;
  }
  const value = Number(given);
  if (!/^\d+$/.test(given) || !Number.isSafeInteger(value) || value < 1) {
    throw new Error(`${flag} takes a positive integer, got ${given}`);
  }
  return value;
}

function parseOptions(): Options {
  const { values } = parseArgs({
    options: {
      full: { type: "boolean", default: false },
      scenarios: { type: "string" },
      orders: { type: "string" },
      seed: { type: "string" },
    },
  });
  const bounds = values.full ? BOUNDS.full : BOUNDS.ci;
  return {
    scenarios: positiveInteger("--scenarios", values.scenarios, bounds.scenarios),
    orders: positiveInteger("--orders", values.orders, bounds.orders),
    seed: positiveInteger("--seed", values.seed, DEFAULT_SEED),
  };
}

async function main(): Promise<number> {
  const options = parseOptions();
  await connectFake();
  const { counterexamples, scenarios, orders, cut } = await explore(options);
  await Disconnect();
  const complete = scenarios === options.scenarios;
  const holding: string[] = [];
  const unjudged: string[] = [];
  let changed = false;
  for (const rule of Object.keys(RULES)) {
    const counterexample = counterexamples.get(rule);
    const isFinding = FINDINGS.has(rule);
    if (counterexample === undefined && !complete) {
      unjudged.push(rule);
    } else if (counterexample === undefined) {
      changed ||= isFinding;
      if (isFinding) {
        console.log(`HOLDS ${rule}`);
      } else {
        holding.push(rule);
      }
    } else {
      changed ||= !isFinding;
      console.log(`${isFinding ? "finding" : "BROKEN"} ${rule}\n${counterexample}`);
    }
  }
  const bounds = `${scenarios} of ${options.scenarios} scenarios, seed ${options.seed}: ${orders} orders, ${cut} scenarios cut at ${options.orders} orders`;
  if (holding.length > 0) {
    console.log(`holds ${holding.join(", ")} (${bounds})`);
  }
  if (unjudged.length > 0) {
    console.log(`not judged ${unjudged.join(", ")} (stopped at the first broken rule, ${bounds})`);
  }
  judged = true;
  return changed ? EXIT.changed : EXIT.ok;
}

process.on("exit", (code) => {
  if (code !== EXIT.ok && !judged) {
    process.exitCode = EXIT.tool;
  }
});

main().then(
  (code) => process.exit(code),
  (error: unknown) => {
    console.error(error);
    process.exit(EXIT.tool);
  },
);
