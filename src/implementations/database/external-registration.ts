import type { SchemaDefinition } from "@antelopejs/interface-database/schema";
import { Logging } from "@antelopejs/interface-core/logging";

import { GetDatabase } from "../../connection";
import { ProbeCollection } from "./external-probe";
import {
  DescribeCollection,
  type ExternalCollection,
} from "./external-descriptor";

export type { ExternalCollection } from "./external-descriptor";

/**
 * Describes every table of an external schema from the collection of the same name, and creates,
 * alters and drops nothing. A table the database cannot honour fails the whole registration with
 * a message naming it; warnings and notices are logged once per registration.
 */
export async function RegisterExternalSchema(
  schemaId: string,
  schema: SchemaDefinition,
  signal: AbortSignal,
): Promise<Record<string, ExternalCollection>> {
  const db = await GetDatabase();
  const collections: Record<string, ExternalCollection> = {};
  for (const [tableId, table] of Object.entries(schema)) {
    signal.throwIfAborted();
    const facts = await ProbeCollection(db, `${schemaId}.${tableId}`, tableId);
    const described = DescribeCollection({ schemaId, tableId, table }, facts);
    described.warnings.forEach((warning) => Logging.Warn(warning));
    described.notices.forEach((notice) => Logging.Info(notice));
    collections[tableId] = described.collection;
  }
  return collections;
}
