import assert from "node:assert";
import { MongoServerError } from "mongodb";
import { Logging } from "@antelopejs/interface-core/logging";
import { DuplicateKeyError } from "@antelopejs/interface-database";
import type { IndexDefinition } from "@antelopejs/interface-database/schema";

import { FindIndex } from "./schema";
import { INSTANCE_INDEX_SUFFIX } from "./utils";

const PRIMARY_KEY = "_id";
const SINGLE_FIELD = 1;
const DUPLICATE_KEY_CODE = 11000;
/** MongoDB's name for the primary key index of every collection. */
const PRIMARY_KEY_INDEX = "_id_";
/** Where MongoDB names the refused index in an E11000 message. */
const DUPLICATE_INDEX_PATTERN = / index: (\S+) dup key/;

const warnedIndexes = new WeakSet<IndexDefinition>();

export interface ResolvedIndex {
  schemaId: string;
  tableName: string;
  indexId: string | undefined;
  fields: string[];
  definition?: IndexDefinition;
}

function isPrimaryKey(
  indexId: string | undefined,
): indexId is typeof PRIMARY_KEY | undefined {
  return indexId === undefined || indexId === PRIMARY_KEY;
}

export function ResolveIndex(
  schemaId: string,
  tableName: string,
  indexId: string | undefined,
): ResolvedIndex {
  if (isPrimaryKey(indexId)) {
    return { schemaId, tableName, indexId, fields: [PRIMARY_KEY] };
  }
  const definition = FindIndex(schemaId, tableName, indexId);
  const fields = definition?.fields ?? [indexId];
  return { schemaId, tableName, indexId, fields, definition };
}

export function SingleIndexField(index: ResolvedIndex): string {
  assert(
    index.fields.length === SINGLE_FIELD,
    `Index "${index.indexId}" spans ${index.fields.length} fields, but this operation needs a single-field index`,
  );
  return index.fields[0];
}

export function WarnIfNotCrossInstance(index: ResolvedIndex) {
  const { definition } = index;
  if (!definition || definition.crossInstance) {
    return;
  }
  if (warnedIndexes.has(definition)) {
    return;
  }
  warnedIndexes.add(definition);
  Logging.Warn(
    `Cross-instance query on "${index.schemaId}.${index.tableName}" uses index "${index.indexId}", which is not declared crossInstance: it may be slow. Set crossInstance: true on this index to keep cross-instance queries fast.`,
  );
}

/** The declared index a physical index of the collection serves. */
function declaredIndexId(physicalName: string): string {
  if (physicalName === PRIMARY_KEY_INDEX) {
    return PRIMARY_KEY;
  }
  return physicalName.endsWith(INSTANCE_INDEX_SUFFIX)
    ? physicalName.slice(0, -INSTANCE_INDEX_SUFFIX.length)
    : physicalName;
}

/**
 * The typed error of a write MongoDB refused for a duplicate key (E11000),
 * naming the declared index rather than its physical, instance-prefixed one;
 * any other error as it was. A batch reports its first refused row.
 */
export function TypedWriteError(
  schemaId: string,
  tableName: string,
  error: unknown,
): unknown {
  if (
    !(error instanceof MongoServerError) ||
    error.code !== DUPLICATE_KEY_CODE
  ) {
    return error;
  }
  const physicalName = DUPLICATE_INDEX_PATTERN.exec(error.message)?.[1];
  if (!physicalName) {
    return error;
  }
  const index = ResolveIndex(
    schemaId,
    tableName,
    declaredIndexId(physicalName),
  );
  return new DuplicateKeyError(index.indexId ?? PRIMARY_KEY, index.fields);
}
