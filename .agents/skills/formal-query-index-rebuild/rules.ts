/**
 * The scenarios the check draws and the rules it judges each run by. A
 * scenario is the indexes a former release left (`earlier`), a first
 * registration, then up to three later registrations of the same schema
 * and queries on it, in order.
 */
import fc from "fast-check";
import type {
  IndexDefinition,
  SchemaDefinition,
} from "@antelopejs/interface-database/schema";

import { keyOf, type Read, type Snapshot } from "./fake-mongo";

/** The one table of every scenario. */
export const TABLE = "users";
const INSTANCE_FIELD = "_instance";
const MAX_LATER_ACTIONS = 3;

/** A query through `RunQuery`: a read of the table, or the lifecycle stage listing instances. */
export type QueryKind = "read" | "listInstances";

/** A registration of the schema, or a query on it. */
export type Action = { register: SchemaDefinition } | { query: QueryKind };

/** What the explorer runs under every order. */
export interface Scenario {
  earlier: SchemaDefinition | undefined;
  first: SchemaDefinition;
  later: Action[];
}

/** A query on the schema and what became of it. */
export interface Query {
  name: string;
  /** How many registrations came before the query was issued. */
  issuedAfter: number;
  /** Whether the readiness promise the query awaits had resolved. */
  passedBarrier: boolean;
  /** The schema was registered again while the query waited on the barrier. */
  superseded: boolean;
  /** The schema was registered again after the query passed the barrier, before it settled. */
  inFlight: boolean;
  outcome: string;
  read?: Read;
}

/** What one run of a scenario under one order left. */
export interface Run {
  schemaId: string;
  earlier: SchemaDefinition | undefined;
  latest: SchemaDefinition;
  /** Every registration of the run, in order. */
  declarations: SchemaDefinition[];
  queries: Query[];
  barrier: string;
  settled: Snapshot;
  trace: string[];
}

interface WantedIndex {
  key: string[];
  unique: boolean;
}

/** The physical indexes `InitializeSchema` is meant to leave for a table declaration. */
function wantedIndexes(indexes: Record<string, IndexDefinition>): WantedIndex[] {
  const declared = Object.entries(indexes);
  if (declared.length === 0) {
    return [{ key: [INSTANCE_FIELD], unique: false }];
  }
  return declared.flatMap(([indexId, index]) => {
    const fields = index.fields ?? [indexId];
    const instancePrefixed = {
      key: [INSTANCE_FIELD, ...fields],
      unique: index.unique === true,
    };
    return index.crossInstance
      ? [instancePrefixed, { key: fields, unique: false }]
      : [instancePrefixed];
  });
}

function sameIndex(left: WantedIndex, right: WantedIndex): boolean {
  return keyOf(left.key) === keyOf(right.key) && left.unique === right.unique;
}

/**
 * What a query expecting the `wanted` indexes on `collection` would get
 * wrong from `indexes`: a wanted index that is missing, or another index on
 * its key that enforces the other uniqueness.
 */
function missing(indexes: Snapshot, collection: string, wanted: WantedIndex[]): string[] {
  const present = indexes.get(collection);
  if (!present) {
    return [`${collection}: no collection`];
  }
  return wanted.flatMap((index) => {
    const sameKey = present.filter((each) => keyOf(each.key) === keyOf(index.key));
    const kind = `{${keyOf(index.key)}}${index.unique ? " unique" : ""}`;
    if (!sameKey.some((each) => each.unique === index.unique)) {
      return [`${collection}: no index ${kind}`];
    }
    const stale = sameKey.find((each) => each.unique !== index.unique);
    return stale ? [`${collection}: ${stale.name} conflicts with ${kind}`] : [];
  });
}

/** What `indexes` gets wrong for a query on `declaration`. */
function mismatches(indexes: Snapshot, declaration: SchemaDefinition, schemaId: string): string[] {
  return Object.entries(declaration).flatMap(([tableId, table]) =>
    missing(indexes, `${schemaId}__${tableId}`, wantedIndexes(table.indexes)),
  );
}

/** The declarations made between a query's issue and its read. */
function declarationsSeen(query: Query, run: Run, read: Read): SchemaDefinition[] {
  return run.declarations.slice(query.issuedAfter - 1, read.registrations);
}

/**
 * What an in-flight read gets wrong about the indexes every declaration
 * between its issue and its read agrees on: those no build had a reason to
 * drop.
 */
function unchangedProblems(query: Query, run: Run): string[] {
  const { read } = query;
  if (!read) {
    return [];
  }
  const [first, ...others] = declarationsSeen(query, run, read);
  return Object.entries(first ?? {}).flatMap(([tableId, table]) => {
    const unchanged = wantedIndexes(table.indexes).filter((index) =>
      others.every((other) =>
        wantedIndexes(other[tableId]?.indexes ?? {}).some((each) => sameIndex(each, index)),
      ),
    );
    return missing(read.indexes, `${run.schemaId}__${tableId}`, unchanged).map(
      (problem) => `${query.name} read lost an index no declaration changed: ${problem}`,
    );
  });
}

/**
 * What is wrong with a query's read: the indexes it saw must be complete
 * for one declaration made between its issue and its read. A mix of two
 * declarations, or one half built, matches none.
 */
function readProblems(query: Query, run: Run): string[] {
  const { read } = query;
  if (!read) {
    return [];
  }
  const candidates = declarationsSeen(query, run, read);
  const misses = candidates.map((declaration) =>
    mismatches(read.indexes, declaration, run.schemaId),
  );
  if (misses.some((problems) => problems.length === 0)) {
    return [];
  }
  const builds = `build ${query.issuedAfter}${read.registrations > query.issuedAfter ? `-${read.registrations}` : ""}`;
  return (misses.at(-1) ?? []).map(
    (problem) => `${query.name} read matches no declaration of ${builds}: ${problem}`,
  );
}

/** Each rule lists what a run did wrong; an empty list means it held. */
export const RULES = {
  QuerySettles: (run) =>
    run.queries
      .filter((query) => query.outcome === "waiting")
      .map((query) => `${query.name} never settled`),
  QueryNeverFails: (run) =>
    run.queries
      .filter((query) => !query.superseded && !query.inFlight && query.outcome !== "read")
      .map((query) => `${query.name} ${query.outcome}`),
  SupersededQueryWaits: (run) =>
    run.queries
      .filter((query) => query.superseded && query.outcome.startsWith("rejected"))
      .map((query) => `${query.name} ${query.outcome}`),
  ReadSeesDeclaredIndexes: (run) =>
    run.queries.filter((query) => !query.inFlight).flatMap((query) => readProblems(query, run)),
  InFlightReadKeepsUnchangedIndexes: (run) =>
    run.queries.filter((query) => query.inFlight).flatMap((query) => unchangedProblems(query, run)),
  InFlightReadSeesDeclaredIndexes: (run) =>
    run.queries.filter((query) => query.inFlight).flatMap((query) => readProblems(query, run)),
  SettledMatchesLatest: (run) => [
    ...(run.barrier === "open" ? [] : [`the barrier is ${run.barrier}`]),
    ...mismatches(run.settled, run.latest, run.schemaId).map(
      (problem) => `settled ${problem}`,
    ),
  ],
} satisfies Record<string, (run: Run) => string[]>;

const optionsArb = fc.record({ unique: fc.boolean(), crossInstance: fc.boolean() });

/**
 * Two index slots. The first indexes `email` under the name `email` or
 * `mail`, so a later registration can rename an index over the same key;
 * the second is `byOwner`, on `owner` and `email`. Either slot may be empty.
 */
const declarationArb: fc.Arbitrary<SchemaDefinition> = fc
  .tuple(
    fc.option(fc.tuple(fc.constantFrom("email", "mail"), optionsArb), { nil: undefined }),
    fc.option(optionsArb, { nil: undefined }),
  )
  .map(([emailSlot, byOwner]) => {
    const indexes: Record<string, IndexDefinition> = {};
    if (emailSlot) {
      const [name, options] = emailSlot;
      indexes[name] = { ...options, fields: ["email"] };
    }
    if (byOwner) {
      indexes.byOwner = { ...byOwner, fields: ["owner", "email"] };
    }
    return { [TABLE]: { fields: {}, indexes } };
  });

const actionArb: fc.Arbitrary<Action> = fc.oneof(
  fc.record({ register: declarationArb }),
  fc.record({ query: fc.constantFrom<QueryKind>("read", "listInstances") }),
);

/** Scenarios: an optional earlier layout, a first registration, then up to three actions. */
export const scenarioArb: fc.Arbitrary<Scenario> = fc.record({
  earlier: fc.option(declarationArb, { nil: undefined }),
  first: declarationArb,
  later: fc.array(actionArb, { minLength: 1, maxLength: MAX_LATER_ACTIONS }),
});
