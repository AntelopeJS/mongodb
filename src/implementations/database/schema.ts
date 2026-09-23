import assert from "node:assert";
import type { SchemaDefinition } from "@antelopejs/interface-database/schema";

import { InitializeSchema } from "../../connection";
import {
  CancelSchemaInitialization,
  StartSchemaInitialization,
} from "../../schema-initialization";

const existingSchemas = new Map<string, SchemaDefinition>();

export const Schemas = {
  register(schemaId: string, schema: SchemaDefinition) {
    const didStart = StartSchemaInitialization(schemaId, () =>
      InitializeSchema(schemaId, schema),
    );
    if (didStart) {
      existingSchemas.set(schemaId, schema);
    }
  },
  unregister(schemaId: string) {
    CancelSchemaInitialization(schemaId);
    existingSchemas.delete(schemaId);
  },
};

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
