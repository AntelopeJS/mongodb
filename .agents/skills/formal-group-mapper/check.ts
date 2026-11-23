import { parseArgs } from "node:util";
import { MongoMemoryServer } from "mongodb-memory-server-core";
import type { Datum, Stream, ValueProxy } from "@antelopejs/interface-database";
import { Schema, type SchemaDefinition } from "@antelopejs/interface-database/schema";

import { SetOptions } from "../../../src/config";
import { Connect, Disconnect, GetDatabase } from "../../../src/connection";
import { RunQuery } from "../../../src/implementations/database/query";
import { Schemas } from "../../../src/implementations/database/schema";
import { AllowSchemaInitializations, DrainSchemaInitializations } from "../../../src/schema-initialization";
import {
  type Answer,
  type Comparison,
  emptiesGroup,
  type Field,
  filterOf,
  type Filter,
  groupRows,
  isUnfiltered,
  type Reduction,
  reference,
  type Row,
} from "./reference";
import { type Drawn, draw } from "./values";

const EXIT = { ok: 0, changed: 1, tool: 2 };
const BOUNDS = { ci: 200, full: 2000 };
const DEFAULT_SEED = 3422;
const TOLERANCE = 1e-9;
const MONGODB_VERSION = "8.0.8";
const DATABASE = "formal";
const SCHEMA = "formal";
const INSTANCE = "tenant";
const TABLE = "rows";
const SHOWN = 4;

/** The document the case stores, which the query reads. */
interface StoredRow extends Row {
  _instance: string;
}

/** What a result field holds: the group key, or the answer of a field of the mapper. */
type Cell = Answer | string;

/** One row of the query's answer, by field name. */
type GroupAnswer = Record<string, Cell>;

/** What a field of the mapper builds on the stream of a group. */
type Reduced = Datum<number> | Stream<number> | ValueProxy<number>;

const RULES = ["AnswersReference", "NeverFailsPipeline"] as const;

/** Rules the code breaks today; SKILL.md records each one. */
const FINDINGS: ReadonlySet<string> = new Set<(typeof RULES)[number]>([]);

const SIGNALS = [
  ["SIGINT", 130],
  ["SIGTERM", 143],
  ["SIGHUP", 129],
] as const;

/** The mongod this run started, so a signal can stop it. */
let running: MongoMemoryServer | undefined;

/** Set once the rules are judged, so a crash before that exits as "could not run". */
let judged = false;

const DEFINITION: SchemaDefinition = { [TABLE]: { fields: {}, indexes: {} } };

const schema = new Schema<{ rows: Row }>(SCHEMA, DEFINITION);

const COMPARE: Record<Comparison, (value: ValueProxy<number>, threshold: number) => ValueProxy<boolean>> = {
  gt: (value, threshold) => value.gt(threshold),
  lt: (value, threshold) => value.lt(threshold),
  eq: (value, threshold) => value.eq(threshold),
  ne: (value, threshold) => value.ne(threshold),
};

const REDUCE: Record<Reduction, (rows: Stream<Row>) => Reduced> = {
  count: (rows) => rows.count(),
  sum: (rows) => rows.sum("n"),
  avg: (rows) => rows.avg("n"),
  min: (rows) => rows.min("n"),
  max: (rows) => rows.max("n"),
  distinct: (rows) => rows.distinct("n"),
};

function keep(rows: Stream<Row>, filter: Filter): Stream<Row> {
  return rows.filter((row) => COMPARE[filter.comparison](row.key("n"), filter.threshold));
}

/** The value of one field of the mapper, from the stream of the rows of a group. */
function build(rows: Stream<Row>, field: Field): Reduced {
  switch (field.kind) {
    case "reduce":
      return REDUCE[field.reduction](rows);
    case "filterReduce":
      return REDUCE[field.reduction](keep(rows, field.filter));
    case "filterMapCount":
      return keep(rows, field.filter)
        .map((row) => row.key("n"))
        .count();
    case "orderFirst":
      return rows.orderBy("t", field.direction).nth(0).key("t");
  }
}

function fieldName(at: number): string {
  return `f${at}`;
}

/** The stages of the query that groups the table by `bucket` and maps each group with the drawn fields. */
function buildStages(fields: Field[]) {
  return schema
    .instance(INSTANCE)
    .table(TABLE)
    .group("bucket", (rows, bucket) => ({
      bucket,
      ...Object.fromEntries(fields.map((field, at) => [fieldName(at), build(rows, field)])),
    }))
    .build();
}

function reduction(each: Reduction): string {
  return each === "count" ? "count()" : `${each}(n)`;
}

function describeField(field: Field): string {
  switch (field.kind) {
    case "reduce":
      return reduction(field.reduction);
    case "filterReduce":
      return `filter(n ${field.filter.comparison} ${field.filter.threshold}).${reduction(field.reduction)}`;
    case "filterMapCount":
      return `filter(n ${field.filter.comparison} ${field.filter.threshold}).map(n).count()`;
    case "orderFirst":
      return `orderBy(t, ${field.direction}).nth(0).t`;
  }
}

function describeCase(each: Drawn): string {
  const rows = each.rows.map((row) => `${row.bucket}:n=${row.n},t=${row.t}`).join(" ");
  const fields = each.fields.map((field, at) => `${fieldName(at)}=${describeField(field)}`).join(", ");
  return `rows [${rows}], mapper {${fields}}`;
}

function isNumber<Item>(value: Item): value is Item & number {
  return typeof value === "number";
}

function isString<Item>(value: Item): value is Item & string {
  return typeof value === "string";
}

function isObject<Item>(value: Item): value is Item & object {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNumbers<Item>(value: Item): value is Item & number[] {
  return Array.isArray(value) && value.every(isNumber);
}

/** Whether a value is a group key or the answer of a field, checked down to each number of a list. */
function isCell<Item>(value: Item): value is Item & Cell {
  return value === null || isNumber(value) || isString(value) || isNumbers(value);
}

function isGroup<Item>(row: Item): row is Item & GroupAnswer {
  return isObject(row) && Object.values(row).every(isCell);
}

/** Whether the answer of the query is a list of rows of cells. */
function areGroups<Rows>(rows: Rows): rows is Rows & GroupAnswer[] {
  return Array.isArray(rows) && rows.every(isGroup);
}

/** What the case answers: the groups, or the failure the server or the translation raised. */
async function run(each: Drawn): Promise<{ groups: GroupAnswer[] } | { failure: string }> {
  const database = await GetDatabase();
  const collection = database.collection<StoredRow>(`${SCHEMA}__${TABLE}`);
  await collection.deleteMany({});
  await collection.insertMany(each.rows.map((row) => ({ ...row, _instance: INSTANCE })));
  try {
    const groups = await RunQuery(buildStages(each.fields));
    return areGroups(groups) ? { groups } : { failure: `the query answered ${JSON.stringify(groups)}` };
  } catch (error) {
    return { failure: error instanceof Error ? error.message : String(error) };
  }
}

function sameAnswer(expected: Answer, actual: Cell | undefined): boolean {
  if (Array.isArray(expected)) {
    return Array.isArray(actual) && JSON.stringify([...actual].sort((a, b) => a - b)) === JSON.stringify(expected);
  }
  if (expected === null || !isNumber(actual)) {
    return expected === actual;
  }
  return Math.abs(expected - actual) <= TOLERANCE;
}

/** The first way the groups the query answered differ from the reference, or undefined when they agree. */
function firstDifference(each: Drawn, groups: GroupAnswer[]): string | undefined {
  const expected = reference(each.rows, each.fields);
  const answered = new Map<Cell | undefined, GroupAnswer[]>();
  for (const group of groups) {
    answered.set(group.bucket, [...(answered.get(group.bucket) ?? []), group]);
  }
  for (const bucket of answered.keys()) {
    if (!isString(bucket) || !expected.has(bucket)) {
      return `answers a group ${JSON.stringify(bucket)} that has no row`;
    }
  }
  for (const [bucket, answers] of expected) {
    const found = answered.get(bucket) ?? [];
    if (found.length !== 1) {
      return `answers ${found.length} rows for group ${bucket}, the contract says 1`;
    }
    for (const [at, field] of each.fields.entries()) {
      const actual = found[0][fieldName(at)];
      if (!sameAnswer(answers[at], actual)) {
        return `group ${bucket}, ${fieldName(at)}=${describeField(field)} answers ${JSON.stringify(actual)}, the contract says ${JSON.stringify(answers[at])}`;
      }
    }
  }
  return undefined;
}

function judge(each: Drawn, outcome: Awaited<ReturnType<typeof run>>): [rule: string, problem: string] | undefined {
  if ("failure" in outcome) {
    return ["NeverFailsPipeline", `${describeCase(each)} fails the pipeline: ${outcome.failure}`];
  }
  const difference = firstDifference(each, outcome.groups);
  return difference ? ["AnswersReference", `${describeCase(each)} ${difference}`] : undefined;
}

/**
 * Throws when the drawn cases leave out what the rules need: a mapper with a
 * filtered and an unfiltered reduction, a group with no row passing a filter,
 * and an `orderBy` field beside a filtered one.
 */
function requireCoverage(all: Drawn[]): void {
  const mixed = all.some(({ fields }) => fields.some((field) => filterOf(field)) && fields.some(isUnfiltered));
  const emptied = all.some(({ rows, fields }) =>
    fields.some((field) => [...groupRows(rows).values()].some((group) => emptiesGroup(field, group))),
  );
  const ordered = all.some(({ fields }) => fields.some((field) => field.kind === "orderFirst") && fields.some((field) => filterOf(field)));
  if (!mixed || !emptied || !ordered) {
    throw new Error("the drawn cases do not cover a mixed mapper, an emptied group and an ordered field: raise --cases");
  }
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
 * Checks that `group(key, mapper)` answers what the interface-database
 * contract says when a field of the mapper filters, orders or picks rows
 * before it reduces them, and never fails the pipeline (see SKILL.md).
 * Builds each query with the real `Stream` of interface-database, runs it
 * through the real `RunQuery` of `src/` against a mongod, and compares the
 * answer with `reference.ts`. fast-check draws the tables and the mappers
 * with a fixed seed. Exits 0 when every rule matches the record in SKILL.md,
 * 1 when a rule changed, and 2 when the check could not run.
 */
async function main(): Promise<number> {
  const { values } = parseArgs({
    options: {
      full: { type: "boolean", default: false },
      cases: { type: "string" },
      seed: { type: "string" },
    },
  });
  const count = positiveInteger("--cases", values.cases, values.full ? BOUNDS.full : BOUNDS.ci);
  const seed = positiveInteger("--seed", values.seed, DEFAULT_SEED);
  const all = draw(count, seed);
  requireCoverage(all);
  const server = await MongoMemoryServer.create({ binary: { version: MONGODB_VERSION } });
  running = server;
  const problems = new Map<string, string[]>();
  try {
    SetOptions({ url: server.getUri(), database: DATABASE });
    await Connect(server.getUri(), DATABASE);
    AllowSchemaInitializations();
    Schemas.register(SCHEMA, DEFINITION);
    for (const each of all) {
      const found = judge(each, await run(each));
      if (found) {
        problems.set(found[0], [...(problems.get(found[0]) ?? []), found[1]]);
      }
    }
  } finally {
    await DrainSchemaInitializations();
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
    console.log(`holds ${holding.join(", ")} (${all.length} cases, seed ${seed})`);
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
