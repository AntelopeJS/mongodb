import assert from "node:assert";
import type { SchemaDefinition } from "@antelopejs/interface-database/schema";
import { InitializeSchema } from "../../connection";
import { StartSchemaInitialization } from "../../schema-initialization";

interface SchemaRegistration {
  definition: SchemaDefinition;
  generation: symbol;
}

const existingSchemas = new Map<string, SchemaRegistration>();

export const Schemas = {
  register(schemaId: string, schema: SchemaDefinition) {
    const generation = Symbol(schemaId);
    const didStart = StartSchemaInitialization(async () => {
      try {
        await InitializeSchema(schemaId, schema);
      } catch (error) {
        if (existingSchemas.get(schemaId)?.generation === generation) {
          existingSchemas.delete(schemaId);
        }
        throw error;
      }
    });
    if (didStart) {
      existingSchemas.set(schemaId, { definition: schema, generation });
    }
  },
  unregister(schemaId: string) {
    existingSchemas.delete(schemaId);
  },
};

export function GetSchema(schemaId: string) {
  const registration = existingSchemas.get(schemaId);
  assert(registration);
  return registration.definition;
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
