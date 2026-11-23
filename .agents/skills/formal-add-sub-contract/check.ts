import { parseArgs } from "node:util";
import { MongoMemoryServer } from "mongodb-memory-server-core";
import { ValueProxy } from "@antelopejs/interface-database";
import type { StagedObject } from "@antelopejs/interface-database/common";
import { Schema } from "@antelopejs/interface-database/schema";

import { SetOptions } from "../../../src/config";
import { Connect, Disconnect, GetDatabase } from "../../../src/connection";
import { RunQuery } from "../../../src/implementations/database/query";
import { type Operation, reference, type Type, TYPES, type Value } from "./reference";
import { draw } from "./values";

const EXIT = { ok: 0, changed: 1, tool: 2 };
const BOUNDS = { ci: 2, full: 8 };
const DEFAULT_SEED = 119;
const MONGODB_VERSION = "8.0.8";
const DATABASE = "formal";
const SCHEMA = "formal";
const INSTANCE = "tenant";
const TABLE = "rows";
const RECEIVER_KINDS = ["field", "constant", "let"] as const;
const OPERAND_KINDS = ["field", "constant"] as const;
const SHOWN = 4;

interface Row {
  _id: string;
  recv: Value;
  operand: Value;
}

/** The document the case stores, which the query reads. */
interface StoredRow extends Row {
  _instance: string;
}

/** What the `map` of a case answers: one row holding its `answer`. */
interface Answers extends Array<{ answer: Value }> {
  0: { answer: Value };
}

function isAnswers<Rows>(rows: Rows): rows is Rows & Answers {
  return Array.isArray(rows) && rows.length === 1;
}

interface Case {
  operation: Operation;
  receiverKind: (typeof RECEIVER_KINDS)[number];
  receiverType: Type;
  operandKind: (typeof OPERAND_KINDS)[number];
  operandType: Type;
  receiver: Value;
  operand: Value;
}

/** `add` and `sub` of a proxy with the operands the contract types forbid too, as an untyped caller sends them. */
interface Arithmetic {
  add(operand: Value | StagedObject): StagedObject;
  sub(operand: Value | StagedObject): StagedObject;
}

/** Rules the code breaks today; SKILL.md records each one. */
const FINDINGS: ReadonlySet<string> = new Set<(typeof RULES)[number]>([
  "OutsideContractNeverFailsPipeline",
]);

const RULES = ["AnswersContract", "NeverFailsPipeline", "OutsideContractNeverFailsPipeline"] as const;

const SIGNALS = [
  ["SIGINT", 130],
  ["SIGTERM", 143],
  ["SIGHUP", 129],
] as const;

/** The mongod this run started, so a signal can stop it. */
let running: MongoMemoryServer | undefined;

/** Set once the rules are judged, so a crash before that exits as "could not run". */
let judged = false;

const schema = new Schema<{ rows: Row }>(SCHEMA, { [TABLE]: { fields: {}, indexes: {} } });

function describeValue(value: Value): string {
  return value instanceof Date ? `Date(${value.getTime()})` : JSON.stringify(value);
}

function describeCase(each: Case): string {
  const receiver = `${each.receiverKind} ${each.receiverType} ${describeValue(each.receiver)}`;
  const operand = `${each.operandKind} ${each.operandType} ${describeValue(each.operand)}`;
  return `${each.operation}: receiver ${receiver}, operand ${operand}`;
}

/** The stages of the query that applies the operation to the case's receiver and operand. */
function buildStages(each: Case) {
  return schema
    .instance(INSTANCE)
    .table(TABLE)
    .map((row) => {
      const field = row.key("recv");
      const receiver = {
        field: () => field,
        constant: () => ValueProxy.constant(each.receiver),
        let: () => field.default(each.receiver),
      }[each.receiverKind]();
      const operand = each.operandKind === "field" ? row.key("operand") : each.operand;
      const arithmetic: Arithmetic = receiver;
      return { answer: arithmetic[each.operation](operand) };
    })
    .build();
}

/** What the case answers: the value, or the failure the server or the translation raised. */
async function run(each: Case): Promise<{ value: Value } | { failure: string }> {
  const database = await GetDatabase();
  await database
    .collection<StoredRow>(`${SCHEMA}__${TABLE}`)
    .replaceOne(
      { _id: "row" },
      { _instance: INSTANCE, recv: each.receiver, operand: each.operand },
      { upsert: true },
    );
  try {
    const rows = await RunQuery(buildStages(each));
    if (!isAnswers(rows)) {
      return { failure: `the query answered ${JSON.stringify(rows)}` };
    }
    return { value: rows[0].answer };
  } catch (error) {
    return { failure: error instanceof Error ? error.message : String(error) };
  }
}

function equal(left: Value | undefined, right: Value | undefined): boolean {
  return describeValue(left ?? null) === describeValue(right ?? null) && (left === undefined) === (right === undefined);
}

function pairs(valuesPerType: number, seed: number): [Case["receiverType"], Case["operandType"], Value, Value][] {
  const values = Object.fromEntries(TYPES.map((type) => [type, draw(type, valuesPerType, seed)]));
  return TYPES.flatMap((receiverType) =>
    TYPES.flatMap((operandType) =>
      values[receiverType].map((receiver, at): [Type, Type, Value, Value] => [
        receiverType,
        operandType,
        receiver,
        values[operandType][at],
      ]),
    ),
  );
}

/** Every operation on every receiver kind and operand kind, over each pair of drawn types. */
function cases(valuesPerType: number, seed: number): Case[] {
  const chosen = pairs(valuesPerType, seed);
  return (["add", "sub"] as const).flatMap((operation) =>
    RECEIVER_KINDS.flatMap((receiverKind) =>
      OPERAND_KINDS.flatMap((operandKind) =>
        chosen.map(([receiverType, operandType, receiver, operand]) => ({
          operation,
          receiverKind,
          receiverType,
          operandKind,
          operandType,
          receiver,
          operand,
        })),
      ),
    ),
  );
}

function judge(each: Case, outcome: Awaited<ReturnType<typeof run>>): [rule: string, problem: string] | undefined {
  const expected = reference(each.operation, each.receiver, each.operand);
  if (expected === undefined) {
    return "failure" in outcome
      ? ["OutsideContractNeverFailsPipeline", `${describeCase(each)} fails the pipeline: ${outcome.failure}`]
      : undefined;
  }
  if ("failure" in outcome) {
    return ["NeverFailsPipeline", `${describeCase(each)} fails the pipeline: ${outcome.failure}`];
  }
  return equal(outcome.value, expected)
    ? undefined
    : ["AnswersContract", `${describeCase(each)} answers ${describeValue(outcome.value)}, the contract says ${describeValue(expected)}`];
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

/**
 * Checks that `add` and `sub` answer what the interface-database contract
 * says over every receiver kind, receiver type and operand type, and never
 * fail the pipeline (see SKILL.md). Builds each query with the real
 * `ValueProxy` of interface-database, runs it through the real `RunQuery` of
 * `src/` against a mongod, and compares the answer with `reference.ts`.
 * fast-check draws the values with a fixed seed. Exits 0 when every rule
 * matches the record in SKILL.md, 1 when a rule changed, and 2 when the
 * check could not run.
 */
async function main(): Promise<number> {
  const { values } = parseArgs({
    options: {
      full: { type: "boolean", default: false },
      values: { type: "string" },
      seed: { type: "string" },
    },
  });
  const valuesPerType = positiveInteger("--values", values.values, values.full ? BOUNDS.full : BOUNDS.ci);
  const seed = positiveInteger("--seed", values.seed, DEFAULT_SEED);
  const server = await MongoMemoryServer.create({ binary: { version: MONGODB_VERSION } });
  running = server;
  const problems = new Map<string, string[]>();
  const all = cases(valuesPerType, seed);
  try {
    SetOptions({ url: server.getUri(), database: DATABASE });
    await Connect(server.getUri(), DATABASE);
    for (const each of all) {
      const found = judge(each, await run(each));
      if (found) {
        problems.set(found[0], [...(problems.get(found[0]) ?? []), found[1]]);
      }
    }
  } finally {
    await Disconnect();
    await server.stop();
  }
  let changed = false;
  const holding: string[] = [];
  for (const rule of RULES) {
    const found = problems.get(rule) ?? [];
    const isFinding = FINDINGS.has(rule);
    if (found.length > 0) {
      changed ||= !isFinding;
      const shown = found.slice(0, SHOWN).map((line) => `  => ${line}`);
      const more = found.length > SHOWN ? [`  ... and ${found.length - SHOWN} more`] : [];
      console.log(`${isFinding ? "finding" : "BROKEN"} ${rule}: ${found.length} of ${all.length} cases\n${[...shown, ...more].join("\n")}`);
    } else if (isFinding) {
      changed = true;
      console.log(`HOLDS ${rule}`);
    } else {
      holding.push(rule);
    }
  }
  if (holding.length > 0) {
    console.log(`holds ${holding.join(", ")} (${all.length} cases, seed ${seed}, ${valuesPerType} values per type)`);
  }
  judged = true;
  return changed ? EXIT.changed : EXIT.ok;
}

for (const [signal, code] of SIGNALS) {
  process.on(signal, () => {
    void (running?.stop() ?? Promise.resolve()).finally(() => process.exit(code));
  });
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
