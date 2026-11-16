/**
 * Checks that closing a query cursor at any step of its life leaves no
 * server cursor open and sends no read after the close (see SKILL.md).
 * Drives the real `ReadCursor` and `CloseCursor` of `src/`, compiled with
 * this folder by `check.mjs`, against the fake server and driver cursor of
 * `fake-driver.ts`. The explorer of `formal-query-index-rebuild` runs each
 * scenario under every order of its steps, up to a bound. Exits 0 when every
 * rule matches the record in SKILL.md, 1 when a rule changed, and 2 when the
 * check could not run.
 */
import { parseArgs } from "node:util";

import { Disconnect } from "../../../src/connection";
import { CloseCursor, ReadCursor } from "../../../src/implementations/database/query";
import type { QueryStage } from "../../../src/implementations/database/utils";
import { exploreOrders, type Schedule } from "../formal-query-index-rebuild/explore";
import { Server } from "./fake-driver";
import { type Call, type Limits, RULES, type Run, type Scenario, scenarios } from "./rules";
import { connectFake, world } from "./server";

const EXIT = { ok: 0, changed: 1, tool: 2 };
const BOUNDS = {
  ci: { limits: { items: 3, changeItems: 2, reads: 3 }, orders: 20000 },
  full: { limits: { items: 5, changeItems: 3, reads: 4 }, orders: 200000 },
};
const MAX_STEPS = 200;
let requests = 7300;

/** Rules the code breaks today; SKILL.md records each one. */
const FINDINGS: ReadonlySet<string> = new Set<keyof typeof RULES>();

/** Set once the rules are judged, so a crash before that exits as "could not run". */
let judged = false;

function readStages(scenario: Scenario): QueryStage[] {
  const stages: QueryStage[] = [
    { stage: "schema", options: { id: "formal" }, args: [] },
    { stage: "instance", options: { id: "tenant" }, args: [] },
    { stage: "table", options: { id: "rows" }, args: [] },
  ];
  return scenario.kind === "changes" ? [...stages, { stage: "changes", args: [] }] : stages;
}

/** Waits for `pending` and records on `call` how it settled. */
async function settle(call: Call, pending: Promise<unknown>): Promise<void> {
  try {
    await pending;
    call.state = "resolved";
  } catch (error) {
    call.state = "rejected";
    call.detail = error instanceof Error ? error.message : String(error);
  }
}

/** Runs `scenario` in the order `schedule` picks and returns what it left. */
async function runScenario(schedule: Schedule, scenario: Scenario): Promise<Run> {
  requests += 1;
  const request = requests;
  const server = new Server(scenario);
  world.server = server;
  let steps = 0;
  server.step = async (apply) => {
    await schedule.step();
    steps += 1;
    if (steps > MAX_STEPS) {
      throw new Error(`more than ${MAX_STEPS} steps: a call does not settle`);
    }
    return apply();
  };
  const calls: Call[] = [];
  const stages = readStages(scenario);
  const consumer = (async () => {
    for (let read = 1; read <= scenario.reads && !server.closeCalled; read += 1) {
      const call: Call = { name: `read ${read}`, state: "waiting" };
      calls.push(call);
      const reading = ReadCursor(request, stages);
      await settle(call, reading);
      const batch = call.state === "resolved" ? await reading : [];
      if (batch.length === 0) {
        return;
      }
    }
  })();
  const closer = (async () => {
    for (let close = 1; close <= scenario.closes; close += 1) {
      const call: Call = { name: `close ${close}`, state: "waiting" };
      calls.push(call);
      await schedule.step();
      server.closeCalled = true;
      server.trace.push(`caller: close ${close} is called`);
      await settle(call, CloseCursor(request));
    }
  })();
  const events = (async () => {
    for (let event = 0; event < (scenario.kind === "changes" ? scenario.items : 0); event += 1) {
      await schedule.step();
      server.event();
    }
  })();
  void consumer;
  void closer;
  void events;
  await schedule.drain();
  return { scenario, server, calls };
}

function describeRun(run: Run, problems: string[], picks: number[]): string {
  const { scenario } = run;
  const source =
    scenario.kind === "table"
      ? `table of ${scenario.items} rows, batches of ${scenario.batchSize}`
      : `change stream, ${scenario.items} events arrive`;
  return [
    `picks [${picks.join(",")}]`,
    `${source}; up to ${scenario.reads} reads, ${scenario.closes} closes`,
    ...run.server.trace,
    ...run.calls.map((call) => `${call.name}: ${call.state}${call.detail ? ` (${call.detail})` : ""}`),
    ...problems.map((problem) => `=> ${problem}`),
  ]
    .map((line) => `  ${line}`)
    .join("\n");
}

interface Options {
  limits: Limits;
  orders: number;
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
      orders: { type: "string" },
    },
  });
  const bounds = values.full ? BOUNDS.full : BOUNDS.ci;
  return {
    limits: bounds.limits,
    orders: positiveInteger("--orders", values.orders, bounds.orders),
  };
}

interface Exploration {
  counterexamples: Map<string, { length: number; text: string }>;
  scenarios: number;
  orders: number;
  cut: number;
}

/** Explores every scenario and keeps, per rule, its shortest counterexample. */
async function explore(options: Options): Promise<Exploration> {
  const all = scenarios(options.limits);
  const counterexamples: Exploration["counterexamples"] = new Map();
  let orders = 0;
  let cut = 0;
  await connectFake();
  for (const scenario of all) {
    const explored = await exploreOrders(async (schedule) => {
      const run = await runScenario(schedule, scenario);
      for (const [rule, judge] of Object.entries(RULES)) {
        const problems = judge(run);
        const length = run.server.trace.length;
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
  return { counterexamples, scenarios: all.length, orders, cut };
}

async function main(): Promise<number> {
  const options = parseOptions();
  const { counterexamples, scenarios: total, orders, cut } = await explore(options);
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
  const bounds = `${total} scenarios: ${orders} orders, ${cut} scenarios cut at ${options.orders} orders`;
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
