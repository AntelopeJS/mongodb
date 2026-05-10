import assert from "node:assert";
import type {
  SchemaDefinition,
  SchemaOptions,
} from "@antelopejs/interface-database/schema";
import { InitializeSchemaInPhysicalStore } from "../../connection";

const existingSchemas: Record<
  string,
  { definition: SchemaDefinition; options: SchemaOptions }
> = {};

const collectionOwnership = new Map<string, string>();

function ownershipKey(physicalStore: string, tableName: string): string {
  return `${physicalStore}\0${tableName}`;
}

function claimOwnership(
  physicalStore: string,
  tableName: string,
  schemaId: string,
) {
  const key = ownershipKey(physicalStore, tableName);
  const owner = collectionOwnership.get(key);
  if (owner && owner !== schemaId) {
    throw new Error(
      `Collection '${tableName}' in physical store '${physicalStore}' is already declared by schema '${owner}', cannot redeclare in '${schemaId}'`,
    );
  }
  collectionOwnership.set(key, schemaId);
}

function releaseOwnership(
  physicalStore: string,
  tableNames: Iterable<string>,
  schemaId: string,
) {
  for (const tableName of tableNames) {
    const key = ownershipKey(physicalStore, tableName);
    if (collectionOwnership.get(key) === schemaId) {
      collectionOwnership.delete(key);
    }
  }
}

function rollbackRegistration(
  schemaId: string,
  physicalStore: string,
  claimedTables: string[],
) {
  releaseOwnership(physicalStore, claimedTables, schemaId);
  delete existingSchemas[schemaId];
}

function releasePreviousClaims(schemaId: string) {
  const previous = existingSchemas[schemaId];
  if (!previous) return;
  const previousStore = previous.options.physicalStore ?? schemaId;
  releaseOwnership(previousStore, Object.keys(previous.definition), schemaId);
}

export const Schemas = {
  async register(
    schemaId: string,
    schema: SchemaDefinition,
    options: SchemaOptions,
  ) {
    const physicalStore = options.physicalStore ?? schemaId;
    releasePreviousClaims(schemaId);
    existingSchemas[schemaId] = { definition: schema, options };
    const claimed: string[] = [];
    try {
      for (const tableName of Object.keys(schema)) {
        claimOwnership(physicalStore, tableName, schemaId);
        claimed.push(tableName);
      }
      await InitializeSchemaInPhysicalStore(physicalStore, schema);
    } catch (err) {
      rollbackRegistration(schemaId, physicalStore, claimed);
      throw err;
    }
  },
  unregister(schemaId: string) {
    const entry = existingSchemas[schemaId];
    if (entry) {
      const physicalStore = entry.options.physicalStore ?? schemaId;
      releaseOwnership(physicalStore, Object.keys(entry.definition), schemaId);
    }
    delete existingSchemas[schemaId];
  },
};

export function GetPhysicalStore(schemaId: string): string {
  assert(schemaId in existingSchemas, `Unknown schema '${schemaId}'`);
  return existingSchemas[schemaId].options.physicalStore ?? schemaId;
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

export function IsTenantScoped(schemaId: string, tableId: string): boolean {
  return GetTable(schemaId, tableId).tenantScoped === true;
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
