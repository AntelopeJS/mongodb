import assert from 'node:assert/strict';
import { afterEach, describe, it } from 'node:test';
import type { Collection, Db, MongoClient } from 'mongodb';
import { MongoClient as MongoClientClass } from 'mongodb';
import { Connect, Disconnect, GetCollection, GetDatabase, ListDatabases } from '../../../connection';
import { GetClient, internal as mongodbInternal } from '../../../interfaces/mongodb/beta';

type MongoClientConnectType = typeof MongoClientClass.connect;

const mutableMongoClient = MongoClientClass as unknown as { connect: MongoClientConnectType };
const originalConnect = MongoClientClass.connect;

afterEach(async () => {
  mutableMongoClient.connect = originalConnect;
  await Disconnect();
  mongodbInternal.connected = false;
  mongodbInternal.UnsetClient();
});

describe('mongodb interface', () => {
  it('connects and exposes the active client through GetClient', async () => {
    let closeCallCount = 0;
    const fakeCollection = { name: 'users' } as unknown as Collection;
    const fakeDatabase = {
      collection: (name: string) => {
        assert.equal(name, 'users');
        return fakeCollection;
      },
      command: async (command: { listDatabases: number; nameOnly: boolean }) => {
        assert.deepEqual(command, { listDatabases: 1, nameOnly: true });
        return { databases: [{ name: 'app' }, { name: 'logs' }] };
      },
    } as unknown as Db;

    const fakeClient = {
      db: (database: string) => {
        assert.ok(['app', 'admin'].includes(database));
        return fakeDatabase;
      },
      close: async () => {
        closeCallCount += 1;
      },
    } as unknown as MongoClient;

    mutableMongoClient.connect = async () => fakeClient;

    await Connect('mongodb://localhost:27017');

    const currentClient = await GetClient();
    assert.equal(currentClient, fakeClient);
    assert.equal(mongodbInternal.connected, true);

    const resolvedDatabase = await GetDatabase('app');
    const resolvedCollection = await GetCollection('app', 'users');
    const databaseList = await ListDatabases();

    assert.equal(resolvedDatabase, fakeDatabase);
    assert.equal(resolvedCollection, fakeCollection);
    assert.deepEqual(databaseList, [{ name: 'app' }, { name: 'logs' }]);

    await Disconnect();
    assert.equal(closeCallCount, 1);
  });
});
