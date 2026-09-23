import sinon from "sinon";
import { expect } from "chai";
import timers from "node:timers";
import { Logging } from "@antelopejs/interface-core/logging";
import type { CommandStartedEvent, MongoClient } from "mongodb";
import { internal as mongoInternal } from "@antelopejs/interface-mongodb";
import type { SchemaDefinition } from "@antelopejs/interface-database/schema";
import { internal as coreInternal } from "@antelopejs/interface-core/internal";

import * as connection from "../connection";
import { destroy, start, stop } from "../index";
import { AllowSchemaInitializations } from "../schema-initialization";
import { GetSchema, Schemas } from "../implementations/database/schema";

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
const ONE_MINUTE_MS = 60_000;
const networkFailure = new Error("network lost");

function createDeferred<Value>(): Deferred<Value> {
  let resolve!: (value: Value) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<Value>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function useFakeGlobalTimeouts(): sinon.SinonFakeTimers {
  const driverTimers = {
    setTimeout: timers.setTimeout,
    clearTimeout: timers.clearTimeout,
  };
  const clock = sinon.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  Object.assign(timers, driverTimers);
  return clock;
}

function retryWarning(attempt: number, delay: number): string {
  return `Schema "first" initialization attempt ${attempt} failed: network lost. Retrying in ${delay}ms`;
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

    Schemas.register("first", schema);
    Schemas.register("second", schema);
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

  it("resumes and drains schema initialization after restart", async () => {
    const first = createDeferred<void>();
    const initialize = sinon
      .stub(connection, "InitializeSchema")
      .returns(first.promise);
    const disconnect = sinon.stub(connection, "Disconnect").resolves();

    stop();
    start();
    Schemas.register("first", schema);
    const teardown = destroy();
    await Promise.resolve();

    expect(initialize.calledOnceWith("first", schema)).to.equal(true);
    expect(disconnect.called).to.equal(false);
    first.resolve();
    await teardown;
    expect(disconnect.calledOnce).to.equal(true);
  });

  it("waits for sibling work without propagating an initialization error", async () => {
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
      let isSettled = false;
      const teardown = destroy().then(() => {
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
      expect(disconnect.calledOnce).to.equal(true);
      expect(GetSchema("first")).to.equal(schema);
      expect(warn.calledOnce).to.equal(true);
      expect(runtimeErrors).to.deep.equal([]);
      expect(unhandledErrors).to.deep.equal([]);
    } finally {
      process.off("unhandledRejection", onUnhandled);
      coreInternal.runtimeErrorReporter = previousReporter;
    }
  });

  it("throws only the disconnect failure from destroy", async () => {
    const first = createDeferred<void>();
    const second = createDeferred<void>();
    const disconnectFailure = new Error("disconnect failed");
    const initialize = sinon.stub(connection, "InitializeSchema");
    initialize.onFirstCall().returns(first.promise);
    initialize.onSecondCall().returns(second.promise);
    sinon.stub(connection, "Disconnect").rejects(disconnectFailure);

    Schemas.register("first", schema);
    Schemas.register("second", schema);
    const teardown = destroy().catch((error: unknown) => error);
    await Promise.resolve();
    second.reject(new Error("second initialization failed"));
    first.reject(new Error("first initialization failed"));

    expect(await teardown).to.equal(disconnectFailure);
    expect(warn.calledTwice).to.equal(true);
  });

  it("ignores the failure of a superseded in-flight attempt", async () => {
    const clock = useFakeGlobalTimeouts();
    const first = createDeferred<void>();
    const second = createDeferred<void>();
    const newerSchema: SchemaDefinition = { ...schema };
    const initialize = sinon.stub(connection, "InitializeSchema");
    initialize.onFirstCall().returns(first.promise);
    initialize.onSecondCall().returns(second.promise);

    Schemas.register("first", schema);
    Schemas.register("first", newerSchema);
    await waitForNextTurn();
    second.resolve();
    first.reject(new Error("older initialization failed"));
    await waitForNextTurn();

    expect(GetSchema("first")).to.equal(newerSchema);
    expect(warn.called).to.equal(false);
    expect(clock.countTimers()).to.equal(0);
    await clock.tickAsync(ONE_MINUTE_MS);
    expect(initialize.callCount).to.equal(2);
  });

  it("keeps the registration and logs a warning when initialization fails", async () => {
    useFakeGlobalTimeouts();
    sinon.stub(connection, "InitializeSchema").rejects(networkFailure);

    Schemas.register("first", schema);
    await waitForNextTurn();

    expect(GetSchema("first")).to.equal(schema);
    expect(warn.calledOnceWith(retryWarning(1, 1_000))).to.equal(true);
  });

  it("retries with an exponential backoff capped at 30 seconds", async () => {
    const clock = useFakeGlobalTimeouts();
    const initialize = sinon
      .stub(connection, "InitializeSchema")
      .rejects(networkFailure);
    const delays = [1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000];

    Schemas.register("first", schema);
    await waitForNextTurn();
    for (const [index, delay] of delays.entries()) {
      expect(warn.lastCall.args[0]).to.equal(retryWarning(index + 1, delay));
      await clock.tickAsync(delay - 1);
      expect(initialize.callCount).to.equal(index + 1);
      await clock.tickAsync(1);
      await waitForNextTurn();
      expect(initialize.callCount).to.equal(index + 2);
    }
    expect(GetSchema("first")).to.equal(schema);
  });

  it("logs an info message once initialization succeeds after a failure", async () => {
    const clock = useFakeGlobalTimeouts();
    const initialize = sinon.stub(connection, "InitializeSchema");
    initialize.onFirstCall().rejects(networkFailure);
    initialize.onSecondCall().resolves();

    Schemas.register("first", schema);
    await waitForNextTurn();
    expect(info.called).to.equal(false);
    await clock.tickAsync(1_000);
    await waitForNextTurn();

    expect(
      info.calledOnceWith(
        'Schema "first" initialized after 1 failed attempt(s)',
      ),
    ).to.equal(true);
    expect(clock.countTimers()).to.equal(0);
    await clock.tickAsync(ONE_MINUTE_MS);
    expect(initialize.callCount).to.equal(2);
  });

  it("does not log an info message when the first attempt succeeds", async () => {
    sinon.stub(connection, "InitializeSchema").resolves();

    Schemas.register("first", schema);
    await waitForNextTurn();

    expect(info.called).to.equal(false);
    expect(warn.called).to.equal(false);
  });

  it("cancels pending retries on destroy without leaving timers", async () => {
    const clock = useFakeGlobalTimeouts();
    const initialize = sinon
      .stub(connection, "InitializeSchema")
      .rejects(networkFailure);
    const disconnect = sinon.stub(connection, "Disconnect").resolves();

    Schemas.register("first", schema);
    await waitForNextTurn();
    expect(clock.countTimers()).to.equal(1);
    await destroy();

    expect(disconnect.calledOnce).to.equal(true);
    expect(clock.countTimers()).to.equal(0);
    await clock.tickAsync(ONE_MINUTE_MS);
    expect(initialize.callCount).to.equal(1);
  });

  it("cancels pending retries on stop and resumes them on start", async () => {
    const clock = useFakeGlobalTimeouts();
    const initialize = sinon.stub(connection, "InitializeSchema");
    initialize.onFirstCall().rejects(networkFailure);
    initialize.onSecondCall().resolves();

    Schemas.register("first", schema);
    await waitForNextTurn();
    stop();
    expect(clock.countTimers()).to.equal(0);
    await clock.tickAsync(ONE_MINUTE_MS);
    expect(initialize.callCount).to.equal(1);
    start();
    await waitForNextTurn();

    expect(initialize.callCount).to.equal(2);
    expect(info.calledOnce).to.equal(true);
    expect(clock.countTimers()).to.equal(0);
  });

  it("resumes on start an attempt that failed while stopped", async () => {
    const clock = useFakeGlobalTimeouts();
    const first = createDeferred<void>();
    const initialize = sinon.stub(connection, "InitializeSchema");
    initialize.onFirstCall().returns(first.promise);
    initialize.onSecondCall().resolves();

    Schemas.register("first", schema);
    await waitForNextTurn();
    stop();
    first.reject(networkFailure);
    await waitForNextTurn();
    expect(
      warn.calledOnceWith(
        'Schema "first" initialization attempt 1 failed: network lost. Retrying when the module starts again',
      ),
    ).to.equal(true);
    expect(clock.countTimers()).to.equal(0);
    start();
    await waitForNextTurn();

    expect(initialize.callCount).to.equal(2);
    expect(GetSchema("first")).to.equal(schema);
  });

  it("supersedes the retry loop of an older registration", async () => {
    const clock = useFakeGlobalTimeouts();
    const newerSchema: SchemaDefinition = { ...schema };
    const initialize = sinon.stub(connection, "InitializeSchema");
    initialize.onFirstCall().rejects(networkFailure);
    initialize.onSecondCall().resolves();

    Schemas.register("first", schema);
    await waitForNextTurn();
    expect(clock.countTimers()).to.equal(1);
    Schemas.register("first", newerSchema);
    await waitForNextTurn();

    expect(clock.countTimers()).to.equal(0);
    expect(initialize.secondCall.args).to.deep.equal(["first", newerSchema]);
    await clock.tickAsync(ONE_MINUTE_MS);
    expect(initialize.callCount).to.equal(2);
    expect(GetSchema("first")).to.equal(newerSchema);
  });

  it("cancels the retry loop when the schema is unregistered", async () => {
    const clock = useFakeGlobalTimeouts();
    const initialize = sinon
      .stub(connection, "InitializeSchema")
      .rejects(networkFailure);

    Schemas.register("first", schema);
    await waitForNextTurn();
    Schemas.unregister("first");

    expect(clock.countTimers()).to.equal(0);
    await clock.tickAsync(ONE_MINUTE_MS);
    expect(initialize.callCount).to.equal(1);
    expect(() => GetSchema("first")).to.throw();
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
