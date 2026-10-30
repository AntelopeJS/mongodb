import type { Document } from "mongodb";
import type { CROSS_INSTANCE } from "@antelopejs/interface-database/schema";
import type { QueryStage } from "@antelopejs/interface-database/common";

import { GetExternalCollection } from "./schema";
import { IsConstantValue } from "./expression";
import type { ExternalCollection, KeyClass } from "./external-descriptor";
import {
  type BsonValue,
  KeyExpression,
  KeyMatch,
  KeyValues,
  NativeFilter,
} from "./external-read";
import {
  ExternalFeedRefusal,
  KeylessRefusal,
  RefuseNamedInstance,
} from "./external-refusals";

type StageArgument = QueryStage["args"][number];

const ID_FIELD = "_id";
const NOTHING: Document = { _id: { $in: [] } };
const KEY_ELEMENT = "key";

/**
 * The stages that run on an external collection's raw documents when they come before every
 * other: each decides for itself whether it needs the normalized row first.
 */
const RAW_STAGES = new Set([
  "get",
  "getAll",
  "between",
  "filter",
  "orderBy",
  "slice",
  "nth",
  "count",
  "sum",
  "avg",
  "min",
  "max",
  "insert",
  "update",
  "replace",
  "delete",
]);

export function RunsOnRawDocuments(stage: QueryStage): boolean {
  return RAW_STAGES.has(stage.stage);
}

/** The collection an external read addresses, refusing a named instance and a feed. */
export function ExternalTarget(
  schemaId: string,
  tableName: string,
  instanceId: string | typeof CROSS_INSTANCE | undefined,
  isChangeStream: boolean,
): ExternalCollection {
  RefuseNamedInstance(tableName, instanceId);
  if (isChangeStream) {
    throw ExternalFeedRefusal(tableName);
  }
  return GetExternalCollection(schemaId, tableName);
}

function RequireKey(external: ExternalCollection): KeyClass {
  if (!external.key) {
    throw KeylessRefusal(external);
  }
  return external.key;
}

/** The `$match` of `get`: a constant key parsed once, a runtime one converted on the server. */
export function KeyGetMatch(
  external: ExternalCollection,
  key: StageArgument,
  decoded: BsonValue,
): Document {
  const keyClass = RequireKey(external);
  return IsConstantValue(key)
    ? KeyMatch(keyClass, [key])
    : { $expr: { $eq: ["$_id", KeyExpression(keyClass, decoded)] } };
}

/**
 * The native `$match` of `getAll` over constant keys of a single field, each converted the way
 * the field reads; undefined when the field cannot be compared natively.
 */
export function KeysFilter(
  external: ExternalCollection,
  field: string,
  keys: StageArgument,
): Document | undefined {
  if (!IsConstantValue(keys)) {
    return undefined;
  }
  const values: BsonValue[] = Array.isArray(keys) ? keys : [keys];
  if (field === ID_FIELD) {
    return KeyMatch(RequireKey(external), values);
  }
  const filters = values.map((value) =>
    NativeFilter(external, { $eq: [`$$ROOT.${field}`, value] }),
  );
  if (!filters.every((filter) => filter !== undefined)) {
    return undefined;
  }
  return filters.length > 0 ? { $or: filters } : NOTHING;
}

/**
 * The native `$match` of `between` on `_id` with constant bounds; undefined for any other range,
 * which compares natively only when the field reads unchanged.
 */
export function KeyRange(
  external: ExternalCollection,
  field: string,
  low: StageArgument,
  high: StageArgument,
): Document | undefined {
  if (field !== ID_FIELD || !IsConstantValue(low) || !IsConstantValue(high)) {
    return undefined;
  }
  const keyClass = RequireKey(external);
  const [lowKey] = KeyValues(keyClass, [low]);
  const [highKey] = KeyValues(keyClass, [high]);
  return lowKey === undefined || highKey === undefined
    ? NOTHING
    : { _id: { $gte: lowKey, $lt: highKey } };
}

/** A reduction over a declared number field's native values, converted once at the end. */
export function NumericReduction(
  field: string,
  operator: string,
  wrapped: string,
): Document[] {
  const numeric = { $cond: [{ $isNumber: `$${field}` }, `$${field}`, null] };
  return [
    { $group: { _id: null, [wrapped]: { [operator]: numeric } } },
    { $set: { [wrapped]: { $toDouble: `$${wrapped}` } } },
  ];
}

/**
 * The values a lookup's local value matches on the other side, as an array `$lookup` matches any
 * element of: the local value converted to the key type class for `_id`, else the value itself and,
 * when the other field holds ObjectIds, its ObjectId form. A local array matches each element.
 * A value that converts to nothing is dropped, since a null candidate would match every document
 * missing the field.
 */
export function LookupCandidates(
  other: ExternalCollection,
  otherKey: string,
  local: string,
): Document {
  const key = otherKey === ID_FIELD ? other.key : undefined;
  const holdsObjectIds =
    other.fields[otherKey]?.nativeTypes.includes("objectId") ?? false;
  const convert = (value: string): BsonValue[] => {
    if (key) {
      return [KeyExpression(key, value)];
    }
    return holdsObjectIds ? [value, KeyExpression("objectId", value)] : [value];
  };
  const element = `$$${KEY_ELEMENT}`;
  const candidates = {
    $cond: [
      { $isArray: local },
      {
        $reduce: {
          input: {
            $map: { input: local, as: KEY_ELEMENT, in: convert(element) },
          },
          initialValue: [],
          in: { $concatArrays: ["$$value", "$$this"] },
        },
      },
      convert(local),
    ],
  };
  return {
    $filter: {
      input: candidates,
      as: KEY_ELEMENT,
      cond: { $ne: [element, null] },
    },
  };
}

/**
 * Moves the predicate of the join just compiled after the other side's stages: on an external
 * collection those normalize its rows, which are what the predicate compares.
 */
export function PredicateAfterRows(pipeline: Document[], from: number) {
  const lookup = pipeline.slice(from).find((stage) => "$lookup" in stage);
  const pipelineOfLookup: unknown = lookup?.$lookup.pipeline;
  const stages: unknown[] = Array.isArray(pipelineOfLookup)
    ? pipelineOfLookup
    : [];
  const [predicate, ...rows] = stages;
  if (lookup && predicate) {
    lookup.$lookup.pipeline = [...rows, predicate];
  }
}
