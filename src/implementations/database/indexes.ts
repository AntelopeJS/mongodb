import assert from "node:assert";
import { Logging } from "@antelopejs/interface-core/logging";
import type { IndexDefinition } from "@antelopejs/interface-database/schema";

import { FindIndex } from "./schema";

const PRIMARY_KEY = "_id";
const SINGLE_FIELD = 1;

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
