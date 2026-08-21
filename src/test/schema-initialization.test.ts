import { internal as coreInternal } from "@antelopejs/interface-core/internal";
import type { SchemaDefinition } from "@antelopejs/interface-database/schema";
import { internal as mongoInternal } from "@antelopejs/interface-mongodb";
import { expect } from "chai";
import type { CommandStartedEvent, MongoClient } from "mongodb";
import sinon from "sinon";
import * as connection from "../connection";
import { GetSchema, Schemas } from "../implementations/database/schema";
import { destroy, stop } from "../index";
import { AllowSchemaInitializations } from "../schema-initialization";

interface Deferred<Value> {
  promise: Promise<Value>;
  resolve: (value: Value) => void;
  reject: (error: unknown) => void;
}

const schema: SchemaDefinition = {
  records: {
    fields: { externalId: "string" },
    indexes: { externalId: { fields: ["externalId"] } },
  },
};
const REAL_SCHEMA_ID = "schema-drain-integration";

function createDeferred<Value>(): Deferred<Value> {
  let resolve!: (value: Value) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<Value>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function waitForNextTurn(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

function getConnectionUrl(client: MongoClient): string {
  const hosts = client.options.hosts.join(",");
  const replicaSet = client.options.replicaSet;
  const query = replicaSet
    ? `?replicaSet=${encodeURIComponent(replicaSet)}`
    : "";
  return `mongodb://${hosts}/${query}`;
}

describe("schema initialization lifecycle", () => {
  beforeEach(() => {
    AllowSchemaInitializations();
  });

  afterEach(() => {
    sinon.restore();
    Schemas.unregister("first");
    Schemas.unregister("second");
    Schemas.unregister("late");
    Schemas.unregister(REAL_SCHEMA_ID);
  });

  it("drains every started schema initialization before disconnecting", async () => {
    const first = createDeferred<void>();
    const second = createDeferred<void>();
    const initialize = sinon.stub(connection, "InitializeSchema");
    initialize.onFirstCall().returns(first.promise);
    initialize.onSecondCall().returns(second.promise);
    const disconnect = sinon.stub(connection, "Disconnect").resolves();

    void Schemas.register("first", schema);
    void Schemas.register("second", schema);
    let isDestroyed = false;
    const teardown = destroy().then(() => {
      isDestroyed = true;
    });

    await Promise.resolve();
    expect(initialize.callCount).to.equal(2);
    expect(disconnect.called).to.equal(false);
    first.resolve();
    await Promise.resolve();
    expect(isDestroyed).to.equal(false);
    expect(disconnect.called).to.equal(false);
    second.resolve();
    await teardown;
    expect(disconnect.calledOnce).to.equal(true);
  });

  it("prevents new schema initialization after stop begins", async () => {
    const first = createDeferred<void>();
    const initialize = sinon
      .stub(connection, "InitializeSchema")
      .returns(first.promise);
    const disconnect = sinon.stub(connection, "Disconnect").resolves();

    Schemas.register("first", schema);
    stop();
    Schemas.register("late", schema);
    expect(initialize.calledOnce).to.equal(false);
    first.resolve();
    await destroy();

    expect(initialize.calledOnceWith("first", schema)).to.equal(true);
    expect(() => GetSchema("late")).to.throw();
    expect(disconnect.calledOnce).to.equal(true);
  });

  it("waits for sibling work before propagating an initialization error", async () => {
    const first = createDeferred<void>();
    const second = createDeferred<void>();
    const failure = new Error("schema initialization failed");
    const initialize = sinon.stub(connection, "InitializeSchema");
    initialize.onFirstCall().returns(first.promise);
    initialize.onSecondCall().returns(second.promise);
    const disconnect = sinon.stub(connection, "Disconnect").resolves();
    const runtimeErrors: unknown[] = [];
    const unhandledErrors: unknown[] = [];
    const previousReporter = coreInternal.runtimeErrorReporter;
    const onUnhandled = (error: unknown) => unhandledErrors.push(error);
    coreInternal.runtimeErrorReporter = (error) => runtimeErrors.push(error);
    process.on("unhandledRejection", onUnhandled);

    try {
      Schemas.register("first", schema);
      Schemas.register("second", schema);
      let teardownError: unknown;
      let isSettled = false;
      const teardown = destroy().catch((error) => {
        teardownError = error;
        isSettled = true;
      });
      await Promise.resolve();
      first.reject(failure);
      await waitForNextTurn();
      expect(isSettled).to.equal(false);
      expect(disconnect.called).to.equal(false);
      second.resolve();
      await teardown;
      await waitForNextTurn();
      expect(teardownError).to.equal(failure);
      expect(disconnect.calledOnce).to.equal(true);
      expect(runtimeErrors).to.deep.equal([]);
      expect(unhandledErrors).to.deep.equal([]);
    } finally {
      process.off("unhandledRejection", onUnhandled);
      coreInternal.runtimeErrorReporter = previousReporter;
    }
  });

  it("aggregates initialization and disconnect failures in operation order", async () => {
    const first = createDeferred<void>();
    const second = createDeferred<void>();
    const firstFailure = new Error("first initialization failed");
    const secondFailure = new Error("second initialization failed");
    const disconnectFailure = new Error("disconnect failed");
    const initialize = sinon.stub(connection, "InitializeSchema");
    initialize.onFirstCall().returns(first.promise);
    initialize.onSecondCall().returns(second.promise);
    sinon.stub(connection, "Disconnect").rejects(disconnectFailure);

    Schemas.register("first", schema);
    Schemas.register("second", schema);
    const teardown = destroy();
    await Promise.resolve();
    second.reject(secondFailure);
    first.reject(firstFailure);

    let teardownError: unknown;
    try {
      await teardown;
    } catch (error) {
      teardownError = error;
    }
    expect(teardownError).to.be.instanceOf(AggregateError);
    expect((teardownError as AggregateError).errors).to.deep.equal([
      firstFailure,
      secondFailure,
      disconnectFailure,
    ]);
  });

  it("drains real index creation without runtime or unhandled errors", async () => {
    const client = await mongoInternal.client;
    const url = getConnectionUrl(client);
    const database = connection.GetConfiguredDatabaseName();
    const startedCommands: string[] = [];
    const runtimeErrors: unknown[] = [];
    const unhandledErrors: unknown[] = [];
    const previousReporter = coreInternal.runtimeErrorReporter;
    const onCommand = (event: CommandStartedEvent) => {
      startedCommands.push(event.commandName);
    };
    const onUnhandled = (error: unknown) => unhandledErrors.push(error);
    coreInternal.runtimeErrorReporter = (error) => runtimeErrors.push(error);
    client.on("commandStarted", onCommand);
    process.on("unhandledRejection", onUnhandled);

    try {
      Schemas.register(REAL_SCHEMA_ID, schema);
      stop();
      await destroy();
      await waitForNextTurn();

      expect(startedCommands).to.include("createIndexes");
      expect(mongoInternal.connected).to.equal(false);
      expect(runtimeErrors).to.deep.equal([]);
      expect(unhandledErrors).to.deep.equal([]);
    } finally {
      client.off("commandStarted", onCommand);
      process.off("unhandledRejection", onUnhandled);
      coreInternal.runtimeErrorReporter = previousReporter;
      if (!mongoInternal.connected) {
        await connection.Connect(url, database, { monitorCommands: true });
        AllowSchemaInitializations();
      }
    }
  });
});
