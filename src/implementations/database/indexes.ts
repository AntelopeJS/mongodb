import assert from "node:assert";
import { Logging } from "@antelopejs/interface-core/logging";

import { GetIndex, GetTable } from "./schema";

const PRIMARY_KEY = "_id";
const SINGLE_FIELD = 1;

const warnedIndexes = new Set<string>();

function isPrimaryKey(
  indexId: string | undefined,
): indexId is typeof PRIMARY_KEY | undefined {
  return indexId === undefined || indexId === PRIMARY_KEY;
}

export function ResolveIndexFields(
  schemaId: string,
  tableName: string,
  indexId: string | undefined,
): string[] {
  if (isPrimaryKey(indexId)) {
    return [PRIMARY_KEY];
  }
  return GetIndex(schemaId, tableName, indexId).fields ?? [indexId];
}

export function ResolveSingleIndexField(
  schemaId: string,
  tableName: string,
  indexId: string | undefined,
): string {
  const fields = ResolveIndexFields(schemaId, tableName, indexId);
  assert(
    fields.length === SINGLE_FIELD,
    `Index "${indexId}" spans ${fields.length} fields, but this operation needs a single-field index`,
  );
  return fields[0];
}

export function WarnIfNotCrossInstance(
  schemaId: string,
  tableName: string,
  indexId: string | undefined,
) {
  if (isPrimaryKey(indexId)) {
    return;
  }
  const index = GetTable(schemaId, tableName).indexes[indexId];
  if (!index || index.crossInstance) {
    return;
  }
  const warningKey = JSON.stringify([schemaId, tableName, indexId]);
  if (warnedIndexes.has(warningKey)) {
    return;
  }
  warnedIndexes.add(warningKey);
  Logging.Warn(
    `Cross-instance query on "${schemaId}.${tableName}" uses index "${indexId}", which is not declared crossInstance: it may be slow. Set crossInstance: true on this index to keep cross-instance queries fast.`,
  );
}
