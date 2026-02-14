import assert from 'node:assert/strict';
import { afterEach, describe, it } from 'node:test';
import type { AggregationCursor, Collection, Db } from 'mongodb';
import { Database, ListDatabases } from '../../../interfaces/database/beta';
import {
  internal as databaseInternal,
  processQuery,
  type TranslationContext,
} from '../../../implementations/database/beta';
import * as connectionModule from '../../../connection';

type QueryBuilderContext = Parameters<typeof processQuery>[0];
type GetCollectionType = typeof connectionModule.GetCollection;
type GetDatabaseType = typeof connectionModule.GetDatabase;
type ListDatabasesType = typeof connectionModule.ListDatabases;

type MutableConnectionModule = {
  GetCollection: GetCollectionType;
  GetDatabase: GetDatabaseType;
  ListDatabases: ListDatabasesType;
};

const connectionMutable = connectionModule as unknown as MutableConnectionModule;
const originalGetCollection = connectionModule.GetCollection;
const originalGetDatabase = connectionModule.GetDatabase;
const originalListDatabases = connectionModule.ListDatabases;

interface FakeAggregationCursor extends AsyncIterableIterator<Record<string, unknown>> {
  close: () => Promise<void>;
  on: (event: string, callback: () => void) => void;
  closeCalled: boolean;
}

class CursorStub implements FakeAggregationCursor {
  private readonly closeCallbacks: Array<() => void> = [];
  private index = 0;
  closeCalled = false;

  constructor(private readonly documents: Record<string, unknown>[]) {}

  async next(): Promise<IteratorResult<Record<string, unknown>, void>> {
    if (this.index >= this.documents.length) {
      return { done: true, value: undefined };
    }
    const value = this.documents[this.index];
    this.index += 1;
    return { done: false, value };
  }

  async close(): Promise<void> {
    this.closeCalled = true;
    for (const callback of this.closeCallbacks) {
      callback();
    }
  }

  on(event: string, callback: () => void): void {
    if (event === 'close') {
      this.closeCallbacks.push(callback);
    }
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<Record<string, unknown>> {
    return this;
  }
}

function createTranslationContext(): TranslationContext {
  return {
    vars: [],
    args: {},
  };
}

function extractQueryContext(query: unknown): QueryBuilderContext {
  const runtimeQuery = query as {
    build: (value: unknown) => { type: string; value: QueryBuilderContext };
  };
  const queryArg = runtimeQuery.build(runtimeQuery);
  assert.equal(queryArg.type, 'query');
  return queryArg.value;
}

afterEach(async () => {
  connectionMutable.GetCollection = originalGetCollection;
  connectionMutable.GetDatabase = originalGetDatabase;
  connectionMutable.ListDatabases = originalListDatabases;
  await databaseInternal.closeCursor(42);
});

describe('database interface', () => {
  it('maps tableCreate options and keeps only collection argument for mongodb mode', () => {
    const context = extractQueryContext(Database('app').tableCreate('users', { primary: 'user_id' }));
    const tableCreateTerm = context[context.length - 1];

    assert.ok(tableCreateTerm);
    assert.equal(tableCreateTerm.id, 'tableCreate');
    assert.deepEqual(tableCreateTerm.opts, { primary_key: 'user_id' });

    const translated = processQuery(context, createTranslationContext());
    assert.equal(translated.mode, 'tableCreate');
    assert.deepEqual(translated.args, ['users']);
  });

  it('returns database names through ListDatabases.run()', async () => {
    connectionMutable.ListDatabases = async () => [{ name: 'app' }, { name: 'analytics' }];

    const queryContext = extractQueryContext(ListDatabases());
    const result = await databaseInternal.runQuery(queryContext);

    assert.deepEqual(result, ['app', 'analytics']);
  });

  it('returns table names through Database(...).tableList().run()', async () => {
    let requestedDatabase = '';
    connectionMutable.GetDatabase = async (database) => {
      requestedDatabase = database;
      return {
        collections: async () => [{ collectionName: 'users' }, { collectionName: 'logs' }],
      } as unknown as Db;
    };

    const queryContext = extractQueryContext(Database('app').tableList());
    const result = await databaseInternal.runQuery(queryContext);

    assert.equal(requestedDatabase, 'app');
    assert.deepEqual(result, ['users', 'logs']);
  });

  it('reuses the same opened cursor for repeated readCursor calls', async () => {
    let aggregateCallCount = 0;
    const cursor = new CursorStub([{ id: 1 }, { id: 2 }]);
    connectionMutable.GetCollection = async () =>
      ({
        aggregate: () => {
          aggregateCallCount += 1;
          return cursor as unknown as AggregationCursor;
        },
      }) as unknown as Collection;

    const queryContext = extractQueryContext(Database('app').table('users'));
    const firstResult = await databaseInternal.readCursor(42, queryContext);
    const secondResult = await databaseInternal.readCursor(42, queryContext);
    const thirdResult = await databaseInternal.readCursor(42, queryContext);

    assert.deepEqual(firstResult, { done: false, value: { id: 1 } });
    assert.deepEqual(secondResult, { done: false, value: { id: 2 } });
    assert.deepEqual(thirdResult, { done: true, value: undefined });
    assert.equal(aggregateCallCount, 1);

    await databaseInternal.closeCursor(42);
    assert.equal(cursor.closeCalled, true);
  });
});
