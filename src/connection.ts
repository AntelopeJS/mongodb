import { Collection, MongoClient, MongoClientOptions, Db } from 'mongodb';
import { internal } from '@ajs.local/mongodb/beta';

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

export async function ListDatabases(): Promise<{name: string}[]> {
  return internal.client
    .then((client) => client.db('admin').command({ listDatabases: 1, nameOnly: true }))
    .then((result) => result.databases);
}
