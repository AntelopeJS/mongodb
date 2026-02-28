import { Collection, MongoClient, MongoClientOptions, Db } from 'mongodb';
import { internal } from '@ajs.local/mongodb/beta';

export function buildDatabaseName(schemaId: string, instanceId: string | undefined): string {
  return instanceId !== undefined ? `${schemaId}-${instanceId}` : schemaId;
}

export async function Connect(url: string, options?: MongoClientOptions) {
  Disconnect();
  const mongoClient = await MongoClient.connect(url, options);
  internal.connected = true;
  internal.SetClient(mongoClient);
}

export async function Disconnect() {
  if (internal.connected) {
    await internal.client.then((client) => client.close());
    internal.UnsetClient();
  }
}

export async function GetCollection(database: string, collection: string): Promise<Collection> {
  return internal.client.then((client) => client.db(database).collection(collection));
}

export async function GetDatabase(database: string): Promise<Db> {
  return internal.client.then((client) => client.db(database));
}

export async function ListDatabases(): Promise<{ name: string }[]> {
  return internal.client
    .then((client) => client.db('admin').command({ listDatabases: 1, nameOnly: true }))
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

async function InitializeDatabase(db: Db, schema: SchemaDefinition, rowLevel?: boolean) {
  const existingCollections = new Set((await db.listCollections().toArray()).map((collection) => collection.name));
  for (const [tableId, table] of Object.entries(schema)) {
    if (!existingCollections.has(tableId)) {
      await db.createCollection(tableId);
    }
    const indexes = Object.fromEntries(
      (await db.collection(tableId).indexes()).map((index) => [index.name ?? index.key[0], index]),
    );
    for (const [indexId, index] of Object.entries(table.indexes)) {
      const fields = index.fields ?? [indexId];
      if (indexId in indexes) {
        const oldIndex = indexes[indexId];
        if (Object.keys(oldIndex.key).length === fields.length && fields.every((field) => oldIndex.key[field])) {
          continue;
        }
      }
      await db.collection(tableId).createIndex(fields, { name: indexId });
    }
    if (rowLevel && !('tenant_id' in indexes)) {
      await db.collection(tableId).createIndex(['tenant_id'], { name: 'tenant_id' });
    }
  }
}

export async function CreateTables(schemaId: string, schema: SchemaDefinition, rowLevel?: boolean): Promise<string[]> {
  const databases = await ListDatabases();
  const instances = [];
  // database-level instances
  for (const { name } of databases) {
    if (name.startsWith(schemaId + '-')) {
      instances.push(name.substring(schemaId.length + 1));
      const db = await GetDatabase(name);
      await InitializeDatabase(db, schema);
    }
  }
  // global instance (no instanceId)
  for (const { name } of databases) {
    if (name === schemaId) {
      instances.push('');
      const db = await GetDatabase(name);
      await InitializeDatabase(db, schema, rowLevel);
      break;
    }
  }
  return instances;
}

export async function CreateSchemaInstance(schemaId: string, instanceId: string | undefined, schema: SchemaDefinition) {
  const dbName = buildDatabaseName(schemaId, instanceId);
  const db = await GetDatabase(dbName);
  await InitializeDatabase(db, schema);
}

export async function DestroySchemaInstance(schemaId: string, instanceId: string | undefined) {
  const dbName = buildDatabaseName(schemaId, instanceId);
  const db = await GetDatabase(dbName);
  await db.dropDatabase();
}
