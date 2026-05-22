import { internal } from "@antelopejs/interface-mongodb";
import {
  type Collection,
  type Db,
  MongoClient,
  type MongoClientOptions,
  MongoServerError,
} from "mongodb";
import {
  BOOKKEEPING_COLLECTION,
  collectionName,
  INSTANCE_FIELD,
} from "./implementations/database/utils";

const INSTANCE_INDEX = "_instance";
const BOOKKEEPING_INDEX = "schemaId_instanceId";
const NAMESPACE_EXISTS_CODE = 48;
const COLLECTION_OPTIONS = {
  changeStreamPreAndPostImages: { enabled: true },
} as const;

let configuredDatabase: string | undefined;

export async function Connect(
  url: string,
  database: string,
  options?: MongoClientOptions,
) {
  await Disconnect();
  const mongoClient = await MongoClient.connect(url, options);
  internal.connected = true;
  internal.SetClient(mongoClient);
  configuredDatabase = database;
}

export async function Disconnect() {
  if (internal.connected) {
    await internal.client.then((client) => client.close());
    internal.connected = false;
    internal.UnsetClient();
  }
  configuredDatabase = undefined;
}

export function GetConfiguredDatabaseName(): string {
  if (!configuredDatabase) {
    throw new Error(
      "MongoDB adapter is not connected: call construct({ url, database }) first",
    );
  }
  return configuredDatabase;
}

export async function GetCollection(collection: string): Promise<Collection> {
  const dbName = GetConfiguredDatabaseName();
  return internal.client.then((client) =>
    client.db(dbName).collection(collection),
  );
}

export async function GetDatabase(): Promise<Db> {
  const dbName = GetConfiguredDatabaseName();
  return internal.client.then((client) => client.db(dbName));
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
}

export interface SchemaDefinition {
  [tableName: string]: TableDefinition;
}

function isNamespaceExistsError(err: unknown): boolean {
  return err instanceof MongoServerError && err.code === NAMESPACE_EXISTS_CODE;
}

async function ensureCollection(
  db: Db,
  collectionId: string,
  existingCollections: Set<string>,
) {
  if (existingCollections.has(collectionId)) {
    return;
  }
  try {
    await db.createCollection(collectionId, COLLECTION_OPTIONS);
  } catch (err) {
    if (!isNamespaceExistsError(err)) throw err;
    await db.command({ collMod: collectionId, ...COLLECTION_OPTIONS });
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

async function ensureInstanceIndex(collection: Collection) {
  const existingIndexes = await collection.indexes();
  const hasInstanceIndex = existingIndexes.some(
    (index) =>
      index.name === INSTANCE_INDEX ||
      (Object.keys(index.key).length === 1 && index.key[INSTANCE_FIELD]),
  );
  if (!hasInstanceIndex) {
    await collection.createIndex([INSTANCE_FIELD], { name: INSTANCE_INDEX });
  }
}

export async function InitializeSchema(
  schemaId: string,
  schema: SchemaDefinition,
) {
  const db = await GetDatabase();
  const existingCollections = new Set(
    (await db.listCollections().toArray()).map((collection) => collection.name),
  );
  for (const [tableId, table] of Object.entries(schema)) {
    const mongoCollection = collectionName(schemaId, tableId);
    await ensureCollection(db, mongoCollection, existingCollections);
    const collection = db.collection(mongoCollection);
    await syncSecondaryIndexes(collection, table);
    await ensureInstanceIndex(collection);
  }
}

export async function EnsureBookkeepingCollection() {
  const db = await GetDatabase();
  const existing = new Set(
    (await db.listCollections().toArray()).map((c) => c.name),
  );
  if (!existing.has(BOOKKEEPING_COLLECTION)) {
    await db.createCollection(BOOKKEEPING_COLLECTION);
  }
  const collection = db.collection(BOOKKEEPING_COLLECTION);
  const indexes = await collection.indexes();
  const hasIndex = indexes.some((idx) => idx.name === BOOKKEEPING_INDEX);
  if (!hasIndex) {
    await collection.createIndex(
      { schemaId: 1, instanceId: 1 },
      { name: BOOKKEEPING_INDEX, unique: true },
    );
  }
}
