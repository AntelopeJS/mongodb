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
import { IsExternal } from "./config";

const INSTANCE_INDEX = "_instance";
const BOOKKEEPING_INDEX = "schemaId_instanceId";
const NAMESPACE_EXISTS_CODE = 48;
const INDEX_NOT_FOUND_CODE = 27;
const INDEX_OPTIONS_CONFLICT_CODE = 85;
const TABLE_CONCURRENCY = 8;
const COLLECTION_OPTIONS = {
  changeStreamPreAndPostImages: { enabled: true },
} as const;

let configuredDatabase: string | undefined;
let atomicDatabase: Db | undefined;
let connectedClient: MongoClient | undefined;
let resolveDatabase: (db: Db) => void = () => undefined;
let databasePromise = PendingDatabase();

function PendingDatabase(): Promise<Db> {
  return new Promise((resolve) => {
    resolveDatabase = resolve;
  });
}

export async function Connect(
  url: string,
  database: string,
  options?: MongoClientOptions,
) {
  await Disconnect();
  const mongoClient = await MongoClient.connect(url, options);
  connectedClient = mongoClient;
  configuredDatabase = database;
  resolveDatabase(mongoClient.db(database));
  if (IsExternal()) {
    return;
  }
  internal.connected = true;
  internal.SetClient(mongoClient);
  atomicDatabase = new MongoClient(url, {
    ...options,
    retryWrites: false,
    retryReads: false,
  }).db(database);
}

export async function Disconnect() {
  await atomicDatabase?.client.close();
  atomicDatabase = undefined;
  await connectedClient?.close();
  if (connectedClient) {
    databasePromise = PendingDatabase();
  }
  connectedClient = undefined;
  if (internal.connected) {
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
  return (await GetDatabase()).collection(collection);
}

export function GetAtomicCollection(collection: string): Collection {
  GetConfiguredDatabaseName();
  if (!atomicDatabase) {
    throw new Error("MongoDB atomic mutation client is not configured");
  }
  return atomicDatabase.collection(collection);
}

/** The database this instance serves, once connected. */
export async function GetDatabase(): Promise<Db> {
  GetConfiguredDatabaseName();
  return databasePromise;
}

interface IndexDefinition {
  fields?: string[];
  crossInstance?: boolean;
  unique?: boolean;
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
  isUnique: boolean;
}

type ExistingIndex = Awaited<ReturnType<Collection["indexes"]>>[number];

interface ExistingIndexes {
  all: ExistingIndex[];
  byName: Record<string, ExistingIndex>;
  byFields: Record<string, ExistingIndex>;
}

function isServerError(err: unknown, code: number): err is MongoServerError {
  return err instanceof MongoServerError && err.code === code;
}

function isNamespaceExistsError(err: unknown): err is MongoServerError {
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

/**
 * Uniqueness holds per instance, so only the instance-prefixed index enforces
 * it; the cross-instance one would refuse a key another instance holds.
 */
function physicalIndexes(
  indexId: string,
  index: IndexDefinition,
): PhysicalIndex[] {
  const fields = index.fields ?? [indexId];
  const instancePrefixed = {
    name: instanceIndexName(indexId),
    fields: [INSTANCE_FIELD, ...fields],
    isUnique: index.unique === true,
  };
  if (!index.crossInstance) {
    return [instancePrefixed];
  }
  return [instancePrefixed, { name: indexId, fields, isUnique: false }];
}

function fieldsKey(fields: string[]): string {
  return fields.join(",");
}

async function listExistingIndexes(
  collection: Collection,
): Promise<ExistingIndexes> {
  const existingIndexes = await collection.indexes();
  return {
    all: existingIndexes,
    byName: Object.fromEntries(
      existingIndexes.map((index): [string, ExistingIndex] => [
        String(index.name),
        index,
      ]),
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

/**
 * Error 85 means an index on the same keys already exists under another name
 * (concurrent initialization, or another declared index with these fields): it
 * serves the same queries.
 */
async function createIndexIfAbsent(
  collection: Collection,
  wanted: PhysicalIndex,
) {
  try {
    await collection.createIndex(wanted.fields, {
      name: wanted.name,
      unique: wanted.isUnique,
    });
  } catch (err) {
    if (!isServerError(err, INDEX_OPTIONS_CONFLICT_CODE)) throw err;
  }
}

function servesWanted(index: ExistingIndex, wanted: PhysicalIndex): boolean {
  return (
    fieldsKey(Object.keys(index.key)) === fieldsKey(wanted.fields) &&
    (index.unique === true) === wanted.isUnique
  );
}

/**
 * MongoDB keeps one index per key pattern: another index on the wanted fields
 * is dropped for the one with the declared uniqueness. Resolves whether the
 * wanted index still has to be created.
 */
async function dropStaleIndexes(
  collection: Collection,
  wanted: PhysicalIndex,
  existing: ExistingIndexes,
): Promise<boolean> {
  const existingByName = existing.byName[wanted.name];
  if (existingByName) {
    if (servesWanted(existingByName, wanted)) {
      return false;
    }
    await dropIndexIfPresent(collection, wanted.name);
  }
  const existingByFields = existing.byFields[fieldsKey(wanted.fields)];
  if (existingByFields && existingByFields.name !== wanted.name) {
    if (servesWanted(existingByFields, wanted)) {
      return false;
    }
    await dropIndexIfPresent(collection, existingByFields.name!);
  }
  return true;
}

/**
 * Creates the indexes in one round trip. The server refuses the whole batch
 * when one index fails, so a failed batch creates them one by one: an error-85
 * index is tolerated, and the indexes before a failing one stay built.
 */
async function createIndexesTogether(
  collection: Collection,
  wantedIndexes: PhysicalIndex[],
  signal?: AbortSignal,
) {
  if (wantedIndexes.length === 0) {
    return;
  }
  signal?.throwIfAborted();
  try {
    await collection.createIndexes(
      wantedIndexes.map((wanted) => ({
        key: new Map(wanted.fields.map((field) => [field, 1 as const])),
        name: wanted.name,
        unique: wanted.isUnique,
      })),
    );
  } catch (err) {
    if (!(err instanceof MongoServerError)) throw err;
    for (const wanted of wantedIndexes) {
      signal?.throwIfAborted();
      await createIndexIfAbsent(collection, wanted);
    }
  }
}

async function syncSecondaryIndexes(
  collection: Collection,
  table: TableDefinition,
  existing: ExistingIndexes,
  signal?: AbortSignal,
) {
  const wantedIndexes = Object.entries(table.indexes).flatMap(
    ([indexId, index]) => physicalIndexes(indexId, index),
  );
  const missing: PhysicalIndex[] = [];
  for (const wanted of wantedIndexes) {
    signal?.throwIfAborted();
    if (await dropStaleIndexes(collection, wanted, existing)) {
      missing.push(wanted);
    }
  }
  await createIndexesTogether(collection, missing, signal);
}

async function ensureInstanceIndex(
  collection: Collection,
  existing: ExistingIndexes,
) {
  const hasInstanceIndex = existing.all.some(
    (index) =>
      index.name === INSTANCE_INDEX ||
      (Object.keys(index.key).length === 1 && index.key[INSTANCE_FIELD]),
  );
  if (!hasInstanceIndex) {
    await collection.createIndex([INSTANCE_FIELD], { name: INSTANCE_INDEX });
  }
}

async function initializeTable(
  db: Db,
  mongoCollection: string,
  table: TableDefinition,
  existingCollections: Set<string>,
  signal?: AbortSignal,
) {
  signal?.throwIfAborted();
  await ensureCollection(db, mongoCollection, existingCollections);
  const collection = db.collection(mongoCollection);
  const existing = await listExistingIndexes(collection);
  signal?.throwIfAborted();
  if (Object.keys(table.indexes).length > 0) {
    await syncSecondaryIndexes(collection, table, existing, signal);
    return;
  }
  await ensureInstanceIndex(collection, existing);
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
  const tables = Object.entries(schema)[Symbol.iterator]();
  const failures: unknown[] = [];
  const work = async () => {
    for (const [tableId, table] of tables) {
      if (failures.length > 0) return;
      await initializeTable(
        db,
        collectionName(schemaId, tableId),
        table,
        existingCollections,
        signal,
      ).catch((err: unknown) => {
        failures.push(err);
      });
    }
  };
  await Promise.all(Array.from({ length: TABLE_CONCURRENCY }, work));
  if (failures.length > 0) throw failures[0];
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
