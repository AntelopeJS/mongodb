import { internal } from "@antelopejs/interface-mongodb";
import {
  type Collection,
  type Db,
  MongoClient,
  type MongoClientOptions,
} from "mongodb";
import { TENANT_ID_FIELD } from "./implementations/database/utils";

const TENANT_ID_INDEX = "tenant_id";

export async function Connect(url: string, options?: MongoClientOptions) {
  await Disconnect();
  const mongoClient = await MongoClient.connect(url, options);
  internal.connected = true;
  internal.SetClient(mongoClient);
}

export async function Disconnect() {
  if (internal.connected) {
    await internal.client.then((client) => client.close());
    internal.connected = false;
    internal.UnsetClient();
  }
}

export async function GetCollection(
  database: string,
  collection: string,
): Promise<Collection> {
  return internal.client.then((client) =>
    client.db(database).collection(collection),
  );
}

export async function GetDatabase(database: string): Promise<Db> {
  return internal.client.then((client) => client.db(database));
}

export async function ListDatabases(): Promise<{ name: string }[]> {
  return internal.client
    .then((client) =>
      client.db("admin").command({ listDatabases: 1, nameOnly: true }),
    )
    .then((result) => result.databases);
}

export interface IndexDefinition {
  fields?: string[];
}

export interface TableDefinition {
  indexes: Record<string, IndexDefinition>;
  tenantScoped?: boolean;
}

export interface SchemaDefinition {
  [tableName: string]: TableDefinition;
}

async function ensureCollection(
  db: Db,
  tableId: string,
  existingCollections: Set<string>,
) {
  if (!existingCollections.has(tableId)) {
    await db.createCollection(tableId);
  }
}

async function syncSecondaryIndexes(
  collection: Collection,
  table: TableDefinition,
) {
  const existingIndexes = await collection.indexes();
  const indexesByName = Object.fromEntries(
    existingIndexes.map((index) => [index.name, index]),
  );
  const indexesByFields = Object.fromEntries(
    existingIndexes.map((index) => [Object.keys(index.key).join(","), index]),
  );
  for (const [indexId, index] of Object.entries(table.indexes)) {
    const fields = index.fields ?? [indexId];
    const fieldsKey = fields.join(",");
    const existingByName = indexesByName[indexId];
    const existingByFields = indexesByFields[fieldsKey];

    if (existingByName) {
      const existingKeys = Object.keys(existingByName.key);
      const nameFieldsMatch =
        existingKeys.length === fields.length &&
        fields.every((field, i) => existingKeys[i] === field);
      if (nameFieldsMatch) {
        continue;
      }
      await collection.dropIndex(indexId);
    } else if (existingByFields) {
      continue;
    }
    await collection.createIndex(fields, { name: indexId });
  }
}

async function ensureTenantIndex(collection: Collection) {
  const existingIndexes = await collection.indexes();
  const hasTenantIndex = existingIndexes.some(
    (index) =>
      index.name === TENANT_ID_INDEX ||
      (Object.keys(index.key).length === 1 && index.key[TENANT_ID_FIELD]),
  );
  if (!hasTenantIndex) {
    await collection.createIndex([TENANT_ID_FIELD], { name: TENANT_ID_INDEX });
  }
}

export async function InitializeSchemaInPhysicalStore(
  physicalStore: string,
  schema: SchemaDefinition,
) {
  const db = await GetDatabase(physicalStore);
  const existingCollections = new Set(
    (await db.listCollections().toArray()).map((collection) => collection.name),
  );
  for (const [tableId, table] of Object.entries(schema)) {
    await ensureCollection(db, tableId, existingCollections);
    const collection = db.collection(tableId);
    await syncSecondaryIndexes(collection, table);
    if (table.tenantScoped) {
      await ensureTenantIndex(collection);
    }
  }
}
