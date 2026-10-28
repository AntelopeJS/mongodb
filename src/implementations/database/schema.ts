import assert from "node:assert";
import type {
  IndexDefinition,
  SchemaDefinition,
} from "@antelopejs/interface-database/schema";

import { IsExternal } from "../../config";
import { InitializeSchema } from "../../connection";
import {
  type ExternalCollection,
  RegisterExternalSchema,
} from "./external-registration";
import {
  CancelSchemaInitialization,
  StartSchemaInitialization,
} from "../../schema-initialization";

const existingSchemas = new Map<string, SchemaDefinition>();
const externalSchemas = new Map<string, Record<string, ExternalCollection>>();

async function InitializeExternalSchema(
  schemaId: string,
  schema: SchemaDefinition,
  signal: AbortSignal,
) {
  externalSchemas.set(
    schemaId,
    await RegisterExternalSchema(schemaId, schema, signal),
  );
}

export const Schemas = {
  register(schemaId: string, schema: SchemaDefinition) {
    const didStart = StartSchemaInitialization(schemaId, (signal) =>
      IsExternal()
        ? InitializeExternalSchema(schemaId, schema, signal)
        : InitializeSchema(schemaId, schema, signal),
    );
    if (didStart) {
      existingSchemas.set(schemaId, schema);
    }
  },
  unregister(schemaId: string) {
    CancelSchemaInitialization(schemaId);
    existingSchemas.delete(schemaId);
    externalSchemas.delete(schemaId);
  },
};

/** The collection an external table reads, as its registration described it. */
export function GetExternalCollection(
  schemaId: string,
  tableId: string,
): ExternalCollection {
  const collection = externalSchemas.get(schemaId)?.[tableId];
  assert(collection, `Table '${schemaId}.${tableId}' is not registered`);
  return collection;
}

export function GetSchema(schemaId: string) {
  const definition = existingSchemas.get(schemaId);
  assert(definition);
  return definition;
}

export function GetTable(schemaId: string, tableId: string) {
  const schema = GetSchema(schemaId);
  assert(tableId in schema);
  return schema[tableId];
}

export function GetTableNames(schemaId: string): string[] {
  return Object.keys(GetSchema(schemaId));
}

export function FindIndex(
  schemaId: string,
  tableId: string,
  indexId: string,
): IndexDefinition | undefined {
  return existingSchemas.get(schemaId)?.[tableId]?.indexes[indexId];
}

export function GetIndex(
  schemaId: string,
  tableId: string,
  indexId: string,
  onlyIndex?: boolean,
) {
  const table = GetTable(schemaId, tableId);
  if (indexId in table.indexes) {
    return table.indexes[indexId];
  } else {
    assert(!onlyIndex);
    return { fields: [indexId] };
  }
}
