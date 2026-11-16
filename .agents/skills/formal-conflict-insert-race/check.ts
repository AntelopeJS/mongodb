/**
 * Checks that conflict inserts racing on one `_id` all succeed and apply
 * (see SKILL.md). Drives the real `RunQuery` and `UpsertAll` of `src/`,
 * compiled with this folder by `check.mjs`, against the in-memory collection
 * of `fake-collection.ts`. fast-check draws the scenarios and the explorer of
 * `formal-query-index-rebuild` runs each under every order of its steps, up
 * to a bound. Exits 0 when every rule matches the record in SKILL.md, 1 when
 * a rule changed, and 2 when the check could not run.
 */
import { parseArgs } from "node:util";
import fc from "fast-check";

import { Disconnect } from "../../../src/connection";
import { RunQuery } from "../../../src/implementations/database/query";
import type { QueryStage } from "../../../src/implementations/database/utils";
import { exploreOrders, type Schedule } from "../formal-query-index-rebuild/explore";
import { actor, FOREIGN, HeldId, OWN, Store } from "./fake-collection";
import { describeError, type Outcome, RULES, type Run, type Scenario, scenarioArb } from "./rules";
import { connectFake, server } from "./server";

const EXIT = { ok: 0, changed: 1, tool: 2 };
const BOUNDS = {
  ci: { scenarios: 200, orders: 3000 },
  full: { scenarios: 400, orders: 8000 },
};
const DEFAULT_SEED = 135;
const MAX_STEPS = 200;
const SCHEMA = "formal";
const TABLE = "rows";

/** Rules the code breaks today; SKILL.md records each one. */
const FINDINGS: ReadonlySet<string> = new Set<keyof typeof RULES>(["DuplicateOnlyForForeignIdUnderDeletes"]);

/** Set once the rules are judged, so a crash before that exits as "could not run". */
let judged = false;

function insertStages(scenario: Scenario, writer: string, ids: string[]): QueryStage[] {
  return [
    { stage: "schema", options: { id: SCHEMA }, args: [] },
    { stage: "instance", options: { id: OWN }, args: [] },
    { stage: "table", options: { id: TABLE }, args: [] },
    {
      stage: "insert",
      options: { conflict: scenario.mode },
      args: [ids.map((_id) => ({ _id, [writer]: true }))],
    },
  ];
}

/** Runs `scenario` in the order `schedule` picks and returns what it left. */
async function runScenario(schedule: Schedule, scenario: Scenario): Promise<Run> {
  const store = new Store();
  server.store = store;
  for (const id of scenario.seeded) {
    store.rows.push({ _id: id, _instance: OWN, seed: true });
  }
  let steps = 0;
  store.step = async (apply) => {
    await schedule.step();
    steps += 1;
    if (steps > MAX_STEPS) {
      throw new Error(`more than ${MAX_STEPS} steps: a writer does not settle`);
    }
    return apply();
  };
  const outcomes: Outcome[] = scenario.writers.map((ids, at) => ({
    writer: `w${at + 1}`,
    ids,
    state: "waiting",
  }));
  const actors: Promise<void>[] = outcomes.map((outcome) =>
    actor.run(outcome.writer, () =>
      RunQuery(insertStages(scenario, outcome.writer, outcome.ids)).then(
        () => {
          outcome.state = "resolved";
        },
        (error: unknown) => {
          Object.assign(
            outcome,
            { state: "rejected" },
            describeError(error instanceof Error ? error : new Error(String(error))),
          );
        },
      ),
    ),
  );
  actors.push(
    (async () => {
      for (const id of scenario.deletes) {
        await schedule.step();
        store.remove(id, OWN);
        store.trace.push(`deleter: removes ${id}`);
      }
    })(),
    (async () => {
      for (const id of scenario.foreign) {
        await schedule.step();
        try {
          store.insert({ _id: id, _instance: FOREIGN, foreign: true });
          store.events.push({ kind: "foreign", id });
          store.trace.push(`other instance: inserts ${id}`);
        } catch (error) {
          if (!(error instanceof HeldId)) {
            throw error;
          }
          store.trace.push(`other instance: inserts ${id}, refused: the _id is held`);
        }
      }
    })(),
  );
  await schedule.drain();
  return { scenario, store, outcomes };
}

function describeRun(run: Run, problems: string[], picks: number[]): string {
  const { scenario } = run;
  return [
    `picks [${picks.join(",")}]`,
    `${scenario.mode}; ${scenario.writers.map((ids, at) => `w${at + 1} inserts ${ids.join(",")}`).join("; ")}; rows held at the start: ${scenario.seeded.join(",") || "none"}`,
    ...run.store.trace,
    ...run.outcomes.map(
      (outcome) => `${outcome.writer}: ${outcome.state}${outcome.detail ? ` (${outcome.detail})` : ""}`,
    ),
    ...problems.map((problem) => `=> ${problem}`),
  ]
    .map((line) => `  ${line}`)
    .join("\n");
}

interface Options {
  scenarios: number;
  orders: number;
  seed: number;
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

interface Exploration {
  counterexamples: Map<string, { length: number; text: string }>;
  orders: number;
  cut: number;
}

/** Explores every drawn scenario and keeps, per rule, its shortest counterexample. */
async function explore(options: Options): Promise<Exploration> {
  const scenarios = fc.sample(scenarioArb, { numRuns: options.scenarios, seed: options.seed });
  const counterexamples: Exploration["counterexamples"] = new Map();
  let orders = 0;
  let cut = 0;
  await connectFake();
  for (const scenario of scenarios) {
    const explored = await exploreOrders(async (schedule) => {
      const run = await runScenario(schedule, scenario);
      for (const [rule, judge] of Object.entries(RULES)) {
        const problems = judge(run);
        const length = run.store.trace.length;
        if (problems.length > 0 && length < (counterexamples.get(rule)?.length ?? Infinity)) {
          const picks = schedule.choices.map((choice) => choice.pick);
          counterexamples.set(rule, { length, text: describeRun(run, problems, picks) });
        }
      }
    }, options.orders);
    orders += explored.orders;
    cut += explored.exhausted ? 0 : 1;
    if ([...counterexamples.keys()].some((rule) => !FINDINGS.has(rule))) {
      break;
    }
  }
  await Disconnect();
  return { counterexamples, orders, cut };
}

async function main(): Promise<number> {
  const options = parseOptions();
  const { counterexamples, orders, cut } = await explore(options);
  const broken = [...counterexamples.keys()].filter((rule) => !FINDINGS.has(rule));
  const holding: string[] = [];
  let changed = broken.length > 0;
  for (const rule of Object.keys(RULES)) {
    const found = counterexamples.get(rule);
    const isFinding = FINDINGS.has(rule);
    if (found) {
      console.log(`${isFinding ? "finding" : "BROKEN"} ${rule}\n${found.text}`);
    } else if (isFinding) {
      changed = true;
      console.log(`HOLDS ${rule}`);
    } else if (broken.length === 0) {
      holding.push(rule);
    }
  }
  const bounds = `${options.scenarios} scenarios, seed ${options.seed}: ${orders} orders, ${cut} scenarios cut at ${options.orders} orders`;
  if (holding.length > 0) {
    console.log(`holds ${holding.join(", ")} (${bounds})`);
  }
  judged = true;
  return changed ? EXIT.changed : EXIT.ok;
}

process.on("exit", () => {
  if (!judged) {
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
