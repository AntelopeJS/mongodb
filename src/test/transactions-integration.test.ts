import { expect } from "chai";
import { Worker } from "node:worker_threads";
import { Schema } from "@antelopejs/interface-database";
import { MongoMemoryServer } from "mongodb-memory-server-core";
import { RunInTransaction } from "@antelopejs/interface-database/transactions";

const SCHEMA_ID = "transaction_public_resolver";
const TENANT_ONE = "tenant-one";
const TENANT_TWO = "tenant-two";
const OBJECT_ID = "shared-object";

interface RegistryRecord {
  _id: string;
  owner: string | null;
}

interface DocumentRecord {
  _id: string;
  objectId: string;
}

interface TransactionSchema {
  documents: DocumentRecord;
  registry: RegistryRecord;
}

const schema = new Schema<TransactionSchema>(SCHEMA_ID, {
  documents: { fields: { objectId: "string" }, indexes: {} },
  registry: { fields: { owner: "string" }, indexes: {} },
});
const firstTenant = schema.instance(TENANT_ONE);
const secondTenant = schema.instance(TENANT_TWO);
const registry = firstTenant.table("registry");
const firstDocuments = firstTenant.table("documents");
const secondDocuments = secondTenant.table("documents");

async function clearTables(): Promise<void> {
  await Promise.all([
    registry.delete().run(),
    firstDocuments.delete().run(),
    secondDocuments.delete().run(),
  ]);
}

async function claim(owner: string): Promise<void> {
  await RunInTransaction(async () => {
    const modified = await registry
      .filter((record) =>
        record.key("_id").eq(OBJECT_ID).and(record.key("owner").eq(null)),
      )
      .update({ owner })
      .run();
    if (modified !== 1) {
      throw new Error("Object claim lost");
    }
    await secondDocuments.insert({ _id: owner, objectId: OBJECT_ID }).run();
  });
}

async function expectRejection(
  promise: Promise<unknown>,
  message: string,
): Promise<void> {
  try {
    await promise;
    expect.fail("Expected transaction rejection");
  } catch (error) {
    expect(error).to.be.instanceOf(Error);
    if (error instanceof Error) {
      expect(error.message).to.include(message);
    }
  }
}

async function runStandaloneProbe(uri: string): Promise<string> {
  const workerSource = `
    const { parentPort, workerData } = require("node:worker_threads");
    const provider = require(workerData.providerPath);
    const { RunInTransaction } = require("@antelopejs/interface-database/transactions");
    (async () => {
      await provider.construct({ url: workerData.uri, database: "standalone_test" });
      try {
        await RunInTransaction(async () => undefined);
      } catch (error) {
        parentPort.postMessage(error.message);
      } finally {
        await provider.destroy();
      }
    })().catch((error) => parentPort.postMessage("probe failure: " + error.message));
  `;
  return new Promise((resolve, reject) => {
    const worker = new Worker(workerSource, {
      eval: true,
      workerData: { providerPath: require.resolve("../index"), uri },
    });
    worker.once("message", resolve);
    worker.once("error", reject);
    worker.once("exit", (code) => {
      if (code !== 0) reject(new Error(`Standalone probe exited with ${code}`));
    });
  });
}

describe("public transaction integration", () => {
  before(async () => {
    await schema.createInstance(TENANT_ONE).run();
    await schema.createInstance(TENANT_TWO).run();
  });

  beforeEach(clearTables);

  it("commits and rolls back public AQL writes and reads across tables and tenants", async () => {
    const count = await RunInTransaction(async () => {
      await registry.insert({ _id: "committed", owner: "owner" }).run();
      await secondDocuments
        .insert({ _id: "document", objectId: "committed" })
        .run();
      return firstTenant.table("registry").count().run();
    });
    expect(count).to.equal(1);

    await expectRejection(
      RunInTransaction(async () => {
        await registry.insert({ _id: "rolled-back", owner: "owner" }).run();
        await firstDocuments
          .insert({ _id: "rolled-back", objectId: "rolled-back" })
          .run();
        throw new Error("rollback requested");
      }),
      "rollback requested",
    );

    expect(await registry.count().run()).to.equal(1);
    expect(await firstDocuments.count().run()).to.equal(0);
    expect(await secondDocuments.count().run()).to.equal(1);
  });

  it("commits one concurrent claim and aborts the losing document", async () => {
    await registry.insert({ _id: OBJECT_ID, owner: null }).run();

    const results = await Promise.allSettled([claim("first"), claim("second")]);
    expect(
      results.filter((result) => result.status === "fulfilled"),
    ).to.have.lengthOf(1);
    expect(
      results.filter((result) => result.status === "rejected"),
    ).to.have.lengthOf(1);
    expect(await secondDocuments.count().run()).to.equal(1);
  });

  it("rejects a real standalone MongoDB server without changing the suite connection", async function () {
    this.timeout(60_000);
    const standalone = await MongoMemoryServer.create({
      binary: { version: "8.0.8" },
    });
    try {
      expect(await runStandaloneProbe(standalone.getUri())).to.include(
        "require a replica set or sharded cluster",
      );
    } finally {
      await standalone.stop();
    }

    await registry.insert({ _id: "still-connected", owner: "owner" }).run();
    expect(await registry.count().run()).to.equal(1);
  });
});
