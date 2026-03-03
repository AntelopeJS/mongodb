import { SchemaDefinition, SchemaOptions } from '@ajs.local/database/beta/schema';
import { CreateRowLevelDatabase, CreateSchemaInstance, DestroySchemaInstance } from '../../../connection';
import assert from 'assert';

export const existingSchemas: Record<string, { definition: SchemaDefinition; options: SchemaOptions }> = {};
export const existingInstances: Record<string, Set<string>> = {};

// TODO: watch changes to db to update existingInstances

export const Schemas = {
  async register(schemaId: string, schema: SchemaDefinition, options: SchemaOptions) {
    existingSchemas[schemaId] = { definition: schema, options };
    if (!existingInstances[schemaId]) {
      existingInstances[schemaId] = new Set<string>();
    }
    if (!options.rowLevel) {
      return;
    }
    await CreateRowLevelDatabase(schemaId, schema);
  },
  unregister(schemaId: string) {
    delete existingSchemas[schemaId];
    delete existingInstances[schemaId];
  },
};

export function IsRowLevel(schemaId: string): boolean {
  return existingSchemas[schemaId]?.options?.rowLevel === true;
}

export function GetSchema(schemaId: string) {
  assert(schemaId in existingSchemas);
  return existingSchemas[schemaId].definition;
}

export function GetTable(schemaId: string, tableId: string) {
  const schema = GetSchema(schemaId);
  assert(tableId in schema);
  return schema[tableId];
}

export function GetIndex(schemaId: string, tableId: string, indexId: string, onlyIndex?: boolean) {
  const table = GetTable(schemaId, tableId);
  if (indexId in table.indexes) {
    return table.indexes[indexId];
  } else {
    assert(!onlyIndex);
    return { fields: [indexId] };
  }
}

export function IsValidInstance(schemaId: string, instanceId: string | undefined) {
  assert(schemaId in existingInstances);
  if (IsRowLevel(schemaId)) {
    if (instanceId === undefined) {
      throw new Error(`Row-level schema '${schemaId}' requires a tenant ID`);
    }
    return true;
  }
  const instances = existingInstances[schemaId];
  return instances.has(instanceId ?? '');
}

export async function CreateInstance(schemaId: string, instanceId: string | undefined) {
  if (IsRowLevel(schemaId)) {
    return;
  }
  const schema = GetSchema(schemaId);
  const instances = existingInstances[schemaId];
  instances.add(instanceId ?? '');
  await CreateSchemaInstance(schemaId, instanceId, schema);
}

export async function DestroyInstance(schemaId: string, instanceId: string | undefined) {
  if (IsRowLevel(schemaId)) {
    return;
  }
  assert(schemaId in existingInstances);
  const instances = existingInstances[schemaId];
  const key = instanceId ?? '';
  if (instances.has(key)) {
    instances.delete(key);
  }
  await DestroySchemaInstance(schemaId, instanceId);
}
