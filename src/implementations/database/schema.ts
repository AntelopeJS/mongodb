import assert from "node:assert";
import type { SchemaDefinition } from "@antelopejs/interface-database/schema";
import { InitializeSchema } from "../../connection";
import { StartSchemaInitialization } from "../../schema-initialization";

const existingSchemas: Record<string, SchemaDefinition> = {};

export const Schemas = {
  register(schemaId: string, schema: SchemaDefinition) {
    const didStart = StartSchemaInitialization(async () => {
      try {
        await InitializeSchema(schemaId, schema);
      } catch (error) {
        if (existingSchemas[schemaId] === schema) {
          delete existingSchemas[schemaId];
        }
        throw error;
      }
    });
    if (didStart) {
      existingSchemas[schemaId] = schema;
    }
  },
  unregister(schemaId: string) {
    delete existingSchemas[schemaId];
  },
};

export function GetSchema(schemaId: string) {
  assert(schemaId in existingSchemas);
  return existingSchemas[schemaId];
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
