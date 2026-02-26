import { SchemaDefinition } from '@ajs.local/database/beta/schema';
import { CreateSchemaInstance, CreateTables, DestroySchemaInstance } from '../../../connection';
import { assert } from 'console';

export const existingSchemas: Record<string, SchemaDefinition> = {};
export const existingInstances: Record<string, Set<string>> = {};

// TODO: watch changes to db to update existingInstances

export const Schemas = {
  async register(schemaId: string, schema: SchemaDefinition) {
    existingSchemas[schemaId] = schema;
    existingInstances[schemaId] = new Set<string>();
    const instances = await CreateTables(schemaId, schema);
    for (const instance of instances) {
      existingInstances[schemaId].add(instance);
    }
  },
  unregister(schemaId: string) {
    delete existingSchemas[schemaId];
    delete existingInstances[schemaId];
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

export function GetIndex(schemaId: string, tableId: string, indexId: string, onlyIndex?: boolean) {
  const table = GetTable(schemaId, tableId);
  if (indexId in table.indexes) {
    return table.indexes[indexId];
  } else {
    assert(!onlyIndex);
    return { fields: [indexId] };
  }
}

export function IsValidInstance(schemaId: string, instanceId: string) {
  assert(schemaId in existingInstances);
  const instances = existingInstances[schemaId];
  return instances.has(instanceId);
}

export async function CreateInstance(schemaId: string, instanceId: string) {
  const schema = GetSchema(schemaId);
  const instances = existingInstances[schemaId];
  instances.add(instanceId);
  await CreateSchemaInstance(schemaId, instanceId, schema);
}

export async function DestroyInstance(schemaId: string, instanceId: string) {
  assert(schemaId in existingInstances);
  const instances = existingInstances[schemaId];
  if (instances.has(instanceId)) {
    instances.delete(instanceId);
  }
  await DestroySchemaInstance(schemaId, instanceId);
}
