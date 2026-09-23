import sinon from "sinon";
import { expect } from "chai";
import { Db, type MongoClient, MongoServerError } from "mongodb";
import { internal as mongoInternal } from "@antelopejs/interface-mongodb";

import * as connection from "../connection";
import { BOOKKEEPING_COLLECTION } from "../implementations/database/utils";

const CONCURRENT_STARTUPS = 4;
const FRESH_DATABASE = "antelopejs_test_bookkeeping_race";
const NAMESPACE_EXISTS_CODE = 48;
const UNAUTHORIZED_CODE = 13;
const BOOKKEEPING_INDEX = "schemaId_instanceId";

function getConnectionUrl(client: MongoClient): string {
  const hosts = client.options.hosts.join(",");
  const replicaSet = client.options.replicaSet;
  const query = replicaSet
    ? `?replicaSet=${encodeURIComponent(replicaSet)}`
    : "";
  return `mongodb://${hosts}/${query}`;
}

function stubEmptyCollectionListing() {
  const emptyCursor = { toArray: () => Promise.resolve([]) };
  sinon
    .stub(Db.prototype, "listCollections")
    .returns(emptyCursor as unknown as ReturnType<Db["listCollections"]>);
}

async function withFreshDatabase(run: () => Promise<void>) {
  const client = await mongoInternal.client;
  const url = getConnectionUrl(client);
  const database = connection.GetConfiguredDatabaseName();
  await connection.Connect(url, FRESH_DATABASE, { monitorCommands: true });
  try {
    await run();
  } finally {
    const freshClient = await mongoInternal.client;
    await freshClient.db(FRESH_DATABASE).dropDatabase();
    await connection.Connect(url, database, { monitorCommands: true });
  }
}

describe("bookkeeping collection", () => {
  afterEach(() => {
    sinon.restore();
  });

  it("tolerates concurrent startups against a fresh database", async () => {
    await withFreshDatabase(async () => {
      const startups = Array.from({ length: CONCURRENT_STARTUPS }, () =>
        connection.EnsureBookkeepingCollection(),
      );
      await Promise.all(startups);

      const collection = await connection.GetCollection(BOOKKEEPING_COLLECTION);
      const indexes = await collection.indexes();
      expect(indexes.map((index) => index.name)).to.include(BOOKKEEPING_INDEX);
    });
  });

  it("tolerates NamespaceExists when another process created the collection first", async () => {
    const namespaceExists = new MongoServerError({
      code: NAMESPACE_EXISTS_CODE,
      errmsg: "Collection already exists",
    });
    stubEmptyCollectionListing();
    sinon.stub(Db.prototype, "createCollection").rejects(namespaceExists);

    await connection.EnsureBookkeepingCollection();

    const collection = await connection.GetCollection(BOOKKEEPING_COLLECTION);
    const indexes = await collection.indexes();
    expect(indexes.map((index) => index.name)).to.include(BOOKKEEPING_INDEX);
  });

  it("propagates collection creation errors other than NamespaceExists", async () => {
    const failure = new MongoServerError({
      code: UNAUTHORIZED_CODE,
      errmsg: "not authorized",
    });
    stubEmptyCollectionListing();
    sinon.stub(Db.prototype, "createCollection").rejects(failure);

    let caught: unknown;
    try {
      await connection.EnsureBookkeepingCollection();
    } catch (error) {
      caught = error;
    }
    expect(caught).to.equal(failure);
  });
});
