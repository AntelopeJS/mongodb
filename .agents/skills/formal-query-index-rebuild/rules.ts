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

import { keyOf, type Snapshot } from "./fake-mongo";

const TABLE = "users";
const INSTANCE_FIELD = "_instance";
const INDEX_POOL = {
  email: undefined,
  byOwner: ["owner", "email"],
} satisfies Record<string, string[] | undefined>;
const MAX_LATER_ACTIONS = 3;

/** A registration of the schema, or a query on it. */
export type Action = { register: SchemaDefinition } | { query: true };

/** What the explorer runs under every order. */
export interface Scenario {
  earlier: SchemaDefinition | undefined;
  first: SchemaDefinition;
  later: Action[];
}

/** A query on the schema and what became of it. */
export interface Query {
  name: string;
  declaration: SchemaDefinition;
  superseded: boolean;
  outcome: string;
  read?: Snapshot;
}

/** What one run of a scenario under one order left. */
export interface Run {
  schemaId: string;
  earlier: SchemaDefinition | undefined;
  latest: SchemaDefinition;
  builds: number;
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

/**
 * What a query on `declaration` would get wrong from `indexes`: a wanted
 * index that is missing, or another index on its key that enforces the
 * other uniqueness.
 */
function mismatches(
  indexes: Snapshot,
  declaration: SchemaDefinition,
  schemaId: string,
): string[] {
  return Object.entries(declaration).flatMap(([tableId, table]) => {
    const collection = `${schemaId}__${tableId}`;
    const present = indexes.get(collection);
    if (!present) {
      return [`${collection}: no collection`];
    }
    return wantedIndexes(table.indexes).flatMap((wanted) => {
      const sameKey = present.filter(
        (index) => keyOf(index.key) === keyOf(wanted.key),
      );
      const kind = `{${keyOf(wanted.key)}}${wanted.unique ? " unique" : ""}`;
      if (!sameKey.some((index) => index.unique === wanted.unique)) {
        return [`${collection}: no index ${kind}`];
      }
      const stale = sameKey.find((index) => index.unique !== wanted.unique);
      return stale
        ? [`${collection}: ${stale.name} conflicts with ${kind}`]
        : [];
    });
  });
}

/** Each rule lists what a run did wrong; an empty list means it held. */
export const RULES = {
  QueryNeverFails: (run) =>
    run.queries
      .filter((query) => !query.superseded && query.outcome !== "read")
      .map((query) => `${query.name} ${query.outcome}`),
  SupersededQueryWaits: (run) =>
    run.queries
      .filter((query) => query.superseded && query.outcome !== "read")
      .map((query) => `${query.name} ${query.outcome}`),
  ReadSeesDeclaredIndexes: (run) =>
    run.queries.flatMap((query) =>
      query.read && !query.superseded
        ? mismatches(query.read, query.declaration, run.schemaId).map(
            (problem) => `${query.name} read ${problem}`,
          )
        : [],
    ),
  SettledMatchesLatest: (run) => [
    ...(run.barrier === "open" ? [] : [`the barrier is ${run.barrier}`]),
    ...mismatches(run.settled, run.latest, run.schemaId).map(
      (problem) => `settled ${problem}`,
    ),
  ],
} satisfies Record<string, (run: Run) => string[]>;

const indexArb = fc.option(
  fc.record({ unique: fc.boolean(), crossInstance: fc.boolean() }),
  { nil: undefined },
);

const declarationArb: fc.Arbitrary<SchemaDefinition> = fc
  .tuple(indexArb, indexArb)
  .map((choices) => {
    const indexes = Object.entries(INDEX_POOL).flatMap(
      ([indexId, fields], at): [string, IndexDefinition][] => {
        const choice = choices[at];
        return choice ? [[indexId, fields ? { ...choice, fields } : choice]] : [];
      },
    );
    return { [TABLE]: { fields: {}, indexes: Object.fromEntries(indexes) } };
  });

const actionArb: fc.Arbitrary<Action> = fc.oneof(
  fc.record({ register: declarationArb }),
  fc.constant<Action>({ query: true }),
);

/** Scenarios: an optional earlier layout, a first registration, then up to three actions. */
export const scenarioArb: fc.Arbitrary<Scenario> = fc.record({
  earlier: fc.option(declarationArb, { nil: undefined }),
  first: declarationArb,
  later: fc.array(actionArb, { minLength: 1, maxLength: MAX_LATER_ACTIONS }),
});
