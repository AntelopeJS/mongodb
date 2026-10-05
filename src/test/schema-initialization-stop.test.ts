import sinon from "sinon";
import { expect } from "chai";
import { Logging } from "@antelopejs/interface-core/logging";
import type { CommandStartedEvent, MongoClient } from "mongodb";
import { internal as mongoInternal } from "@antelopejs/interface-mongodb";
import type { SchemaDefinition } from "@antelopejs/interface-database/schema";
import { internal as coreInternal } from "@antelopejs/interface-core/internal";

import * as connection from "../connection";
import { destroy, start, stop } from "../index";
import { AllowSchemaInitializations } from "../schema-initialization";
import { GetSchema, Schemas } from "../implementations/database/schema";
import {
  collectionName,
  instanceIndexName,
} from "../implementations/database/utils";
import {
  createDeferred,
  getConnectionUrl,
  schema,
  useFakeGlobalTimeouts,
  waitForNextTurn,
} from "./schema-initialization-helpers";

const REAL_SCHEMA_ID = "schema-drain-integration";
const RESUMED_SCHEMA_ID = "schema-resume-integration";
const RESUMED_TABLE_COUNT = 20;
const DRAIN_GRACE_PERIOD_MS = 2_000;
const DRAIN_GRACE_WARNING =
  "Schema initialization still running after 2000ms, abandoning it until the module starts again";

type InitializeSchemaStub = sinon.SinonStub<
  Parameters<typeof connection.InitializeSchema>,
  Promise<void>
>;
function rejectOnAbort(
  _schemaId: string,
  _schema: connection.SchemaDefinition,
  signal?: AbortSignal,
): Promise<void> {
  return new Promise((_resolve, reject) => {
    signal?.addEventListener("abort", () => reject(signal.reason));
  });
}

function attemptSignal(initialize: InitializeSchemaStub, call: number) {
  return initialize.getCall(call).args[2];
}

function createTables(count: number): SchemaDefinition {
  return Object.fromEntries(
    Array.from({ length: count }, (_, index) => [
      `table${index}`,
      schema.records,
    ]),
  );
}

function waitForCommand(client: MongoClient, commandName: string) {
  return new Promise<void>((resolve) => {
    const onCommand = (event: CommandStartedEvent) => {
      if (event.commandName !== commandName) {
        return;
      }
      client.off("commandStarted", onCommand);
      resolve();
    };
    client.on("commandStarted", onCommand);
  });
}

async function listSchemaCollections(schemaId: string): Promise<string[]> {
  const client = await mongoInternal.client;
  const collections = await client
    .db(connection.GetConfiguredDatabaseName())
    .listCollections({ name: { $regex: `^${collectionName(schemaId, "")}` } })
    .toArray();
  return collections.map((collection) => collection.name);
}

async function reconnect(url: string, database: string): Promise<void> {
  await connection.Connect(url, database, { monitorCommands: true });
  AllowSchemaInitializations();
}

describe("schema initialization stop", () => {
  let warn: sinon.SinonStub;
  let info: sinon.SinonStub;

  beforeEach(() => {
    warn = sinon.stub(Logging, "Warn");
    info = sinon.stub(Logging, "Info");
    AllowSchemaInitializations();
  });

  afterEach(() => {
    sinon.restore();
    Schemas.unregister("first");
    Schemas.unregister(REAL_SCHEMA_ID);
    Schemas.unregister(RESUMED_SCHEMA_ID);
  });

  it("disconnects promptly after a normal stop once initialization is done", async () => {
    const clock = useFakeGlobalTimeouts();
    sinon.stub(connection, "InitializeSchema").resolves();
    const disconnect = sinon.stub(connection, "Disconnect").resolves();

    Schemas.register("first", schema);
    await waitForNextTurn();
    stop();
    await destroy();

    expect(disconnect.calledOnce).to.equal(true);
    expect(warn.called).to.equal(false);
    expect(clock.countTimers()).to.equal(0);
  });

  it("abandons a stuck initialization after a bounded grace period on destroy", async () => {
    const clock = useFakeGlobalTimeouts();
    const stuck = createDeferred<void>();
    const initialize = sinon
      .stub(connection, "InitializeSchema")
      .returns(stuck.promise);
    const disconnect = sinon.stub(connection, "Disconnect").resolves();

    Schemas.register("first", schema);
    await waitForNextTurn();
    stop();
    let isDestroyed = false;
    const teardown = destroy().then(() => {
      isDestroyed = true;
    });
    await clock.tickAsync(DRAIN_GRACE_PERIOD_MS - 1);
    expect(isDestroyed).to.equal(false);
    expect(attemptSignal(initialize, 0)?.aborted).to.equal(true);
    await clock.tickAsync(1);
    await teardown;

    expect(disconnect.calledOnce).to.equal(true);
    expect(warn.calledOnceWith(DRAIN_GRACE_WARNING)).to.equal(true);
    expect(clock.countTimers()).to.equal(0);
    stuck.resolve();
    await waitForNextTurn();
  });

  it("interrupts initialization on stop without a failure and resumes it on start", async () => {
    const clock = useFakeGlobalTimeouts();
    const initialize = sinon.stub(connection, "InitializeSchema");
    initialize.onFirstCall().callsFake(rejectOnAbort);
    initialize.onSecondCall().resolves();

    Schemas.register("first", schema);
    await waitForNextTurn();
    stop();
    await waitForNextTurn();
    expect(warn.called).to.equal(false);
    expect(clock.countTimers()).to.equal(0);
    expect(initialize.callCount).to.equal(1);
    start();
    await waitForNextTurn();

    expect(initialize.callCount).to.equal(2);
    expect(attemptSignal(initialize, 1)?.aborted).to.equal(false);
    expect(info.called).to.equal(false);
    expect(GetSchema("first")).to.equal(schema);
  });

  it("resumes an interrupted initialization when started before it settles", async () => {
    const interrupted = createDeferred<void>();
    const initialize = sinon.stub(connection, "InitializeSchema");
    initialize.onFirstCall().returns(interrupted.promise);
    initialize.onSecondCall().resolves();

    Schemas.register("first", schema);
    await waitForNextTurn();
    stop();
    start();
    expect(initialize.callCount).to.equal(1);
    interrupted.reject(attemptSignal(initialize, 0)?.reason);
    await waitForNextTurn();

    expect(initialize.callCount).to.equal(2);
    expect(warn.called).to.equal(false);
  });

  it("lets in-flight index creation finish before disconnecting without errors", async () => {
    const client = await mongoInternal.client;
    const url = getConnectionUrl(client);
    const database = connection.GetConfiguredDatabaseName();
    const runtimeErrors: unknown[] = [];
    const unhandledErrors: unknown[] = [];
    const previousReporter = coreInternal.runtimeErrorReporter;
    const onUnhandled = (error: unknown) => unhandledErrors.push(error);
    coreInternal.runtimeErrorReporter = (error) => runtimeErrors.push(error);
    process.on("unhandledRejection", onUnhandled);

    try {
      const indexCreation = waitForCommand(client, "createIndexes");
      Schemas.register(REAL_SCHEMA_ID, schema);
      await indexCreation;
      stop();
      await destroy();
      await waitForNextTurn();

      expect(mongoInternal.connected).to.equal(false);
      expect(warn.called).to.equal(false);
      expect(runtimeErrors).to.deep.equal([]);
      expect(unhandledErrors).to.deep.equal([]);
    } finally {
      process.off("unhandledRejection", onUnhandled);
      coreInternal.runtimeErrorReporter = previousReporter;
      if (!mongoInternal.connected) {
        await reconnect(url, database);
      }
    }
  });

  it("stops a real initialization early and completes the schema on restart", async () => {
    const client = await mongoInternal.client;
    const url = getConnectionUrl(client);
    const database = connection.GetConfiguredDatabaseName();
    const tables = createTables(RESUMED_TABLE_COUNT);
    const expectedCollections = Object.keys(tables).map((tableId) =>
      collectionName(RESUMED_SCHEMA_ID, tableId),
    );

    try {
      const collectionCreation = waitForCommand(client, "create");
      Schemas.register(RESUMED_SCHEMA_ID, tables);
      await collectionCreation;
      stop();
      await destroy();
      expect(warn.called).to.equal(false);
      await connection.Connect(url, database, { monitorCommands: true });
      const createdBeforeRestart =
        await listSchemaCollections(RESUMED_SCHEMA_ID);
      expect(createdBeforeRestart.length).to.be.lessThan(RESUMED_TABLE_COUNT);

      const initialize = sinon.spy(connection, "InitializeSchema");
      start();
      await waitForNextTurn();
      await initialize.firstCall.returnValue;

      const createdAfterRestart =
        await listSchemaCollections(RESUMED_SCHEMA_ID);
      expect(createdAfterRestart).to.have.members(expectedCollections);
      const lastCollection = (await mongoInternal.client)
        .db(database)
        .collection(expectedCollections[RESUMED_TABLE_COUNT - 1]);
      const indexNames = (await lastCollection.indexes()).map(
        (index) => index.name,
      );
      expect(indexNames).to.include(instanceIndexName("externalId"));
    } finally {
      if (!mongoInternal.connected) {
        await reconnect(url, database);
      }
    }
  });
});
