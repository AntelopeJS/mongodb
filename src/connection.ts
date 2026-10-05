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
  instanceIndexName,
} from "./implementations/database/utils";

const INSTANCE_INDEX = "_instance";
const BOOKKEEPING_INDEX = "schemaId_instanceId";
const NAMESPACE_EXISTS_CODE = 48;
const INDEX_NOT_FOUND_CODE = 27;
const INDEX_OPTIONS_CONFLICT_CODE = 85;
const COLLECTION_OPTIONS = {
  changeStreamPreAndPostImages: { enabled: true },
} as const;

let configuredDatabase: string | undefined;
let atomicClient: MongoClient | undefined;

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
  atomicClient = new MongoClient(url, {
    ...options,
    retryWrites: false,
    retryReads: false,
  });
}

export async function Disconnect() {
  await atomicClient?.close();
  atomicClient = undefined;
  if (internal.connected) {
    await internal.client.then((client) => client.close());
    internal.connected = false;
    void internal.UnsetClient();
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

export function GetAtomicCollection(collection: string): Collection {
  const dbName = GetConfiguredDatabaseName();
  if (!atomicClient) {
    throw new Error("MongoDB atomic mutation client is not configured");
  }
  return atomicClient.db(dbName).collection(collection);
}

async function GetDatabase(): Promise<Db> {
  const dbName = GetConfiguredDatabaseName();
  return internal.client.then((client) => client.db(dbName));
}

interface IndexDefinition {
  fields?: string[];
  crossInstance?: boolean;
}

export interface TableDefinition {
  indexes: Record<string, IndexDefinition>;
}

export interface SchemaDefinition {
  [tableName: string]: TableDefinition;
}

interface PhysicalIndex {
  name: string;
  fields: string[];
}

type ExistingIndex = Awaited<ReturnType<Collection["indexes"]>>[number];

interface ExistingIndexes {
  byName: Record<string, ExistingIndex>;
  byFields: Record<string, ExistingIndex>;
}

function isServerError(err: unknown, code: number): boolean {
  return err instanceof MongoServerError && err.code === code;
}

function isNamespaceExistsError(err: unknown): boolean {
  return isServerError(err, NAMESPACE_EXISTS_CODE);
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

function physicalIndexes(
  indexId: string,
  index: IndexDefinition,
): PhysicalIndex[] {
  const fields = index.fields ?? [indexId];
  const instancePrefixed = {
    name: instanceIndexName(indexId),
    fields: [INSTANCE_FIELD, ...fields],
  };
  if (!index.crossInstance) {
    return [instancePrefixed];
  }
  return [instancePrefixed, { name: indexId, fields }];
}

function fieldsKey(fields: string[]): string {
  return fields.join(",");
}

async function listExistingIndexes(
  collection: Collection,
): Promise<ExistingIndexes> {
  const existingIndexes = await collection.indexes();
  return {
    byName: Object.fromEntries(
      existingIndexes.map((index) => [index.name, index]),
    ),
    byFields: Object.fromEntries(
      existingIndexes.map((index) => [
        fieldsKey(Object.keys(index.key)),
        index,
      ]),
    ),
  };
}

async function dropIndexIfPresent(collection: Collection, name: string) {
  try {
    await collection.dropIndex(name);
  } catch (err) {
    if (!isServerError(err, INDEX_NOT_FOUND_CODE)) throw err;
  }
}

async function createIndexIfAbsent(
  collection: Collection,
  wanted: PhysicalIndex,
) {
  try {
    await collection.createIndex(wanted.fields, { name: wanted.name });
  } catch (err) {
    // Error 85 means an index on the same keys already exists under another name (concurrent
    // initialization, or another declared index with these fields): it serves the same queries.
    if (!isServerError(err, INDEX_OPTIONS_CONFLICT_CODE)) throw err;
  }
}

async function ensurePhysicalIndex(
  collection: Collection,
  wanted: PhysicalIndex,
  existing: ExistingIndexes,
) {
  const existingByName = existing.byName[wanted.name];
  if (existingByName) {
    if (
      fieldsKey(Object.keys(existingByName.key)) === fieldsKey(wanted.fields)
    ) {
      return;
    }
    await dropIndexIfPresent(collection, wanted.name);
  } else if (existing.byFields[fieldsKey(wanted.fields)]) {
    return;
  }
  await createIndexIfAbsent(collection, wanted);
}

async function syncSecondaryIndexes(
  collection: Collection,
  table: TableDefinition,
  signal?: AbortSignal,
) {
  const existing = await listExistingIndexes(collection);
  const wantedIndexes = Object.entries(table.indexes).flatMap(
    ([indexId, index]) => physicalIndexes(indexId, index),
  );
  for (const wanted of wantedIndexes) {
    signal?.throwIfAborted();
    await ensurePhysicalIndex(collection, wanted, existing);
  }
}

async function ensureInstanceIndex(
  collection: Collection,
  table: TableDefinition,
) {
  const hasDeclaredIndexes = Object.keys(table.indexes).length > 0;
  if (hasDeclaredIndexes) {
    return;
  }
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
  signal?: AbortSignal,
) {
  const db = await GetDatabase();
  const existingCollections = new Set(
    (await db.listCollections().toArray()).map((collection) => collection.name),
  );
  for (const [tableId, table] of Object.entries(schema)) {
    signal?.throwIfAborted();
    const mongoCollection = collectionName(schemaId, tableId);
    await ensureCollection(db, mongoCollection, existingCollections);
    const collection = db.collection(mongoCollection);
    await syncSecondaryIndexes(collection, table, signal);
    signal?.throwIfAborted();
    await ensureInstanceIndex(collection, table);
  }
}

export async function EnsureBookkeepingCollection() {
  const db = await GetDatabase();
  const existing = new Set(
    (await db.listCollections().toArray()).map((c) => c.name),
  );
  if (!existing.has(BOOKKEEPING_COLLECTION)) {
    try {
      await db.createCollection(BOOKKEEPING_COLLECTION);
    } catch (err) {
      if (!isNamespaceExistsError(err)) throw err;
    }
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
