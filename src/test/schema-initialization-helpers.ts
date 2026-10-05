import sinon from "sinon";
import timers from "node:timers";
import type { MongoClient } from "mongodb";
import type { SchemaDefinition } from "@antelopejs/interface-database/schema";

interface Deferred<Value> {
  promise: Promise<Value>;
  resolve: (value: Value) => void;
  reject: (error: unknown) => void;
}

export const schema: SchemaDefinition = {
  records: {
    fields: { externalId: "string" },
    indexes: { externalId: { fields: ["externalId"] } },
  },
};

export function createDeferred<Value>(): Deferred<Value> {
  let resolve!: (value: Value) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<Value>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

export function useFakeGlobalTimeouts(): sinon.SinonFakeTimers {
  const driverTimers = {
    setTimeout: timers.setTimeout,
    clearTimeout: timers.clearTimeout,
  };
  const clock = sinon.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  Object.assign(timers, driverTimers);
  return clock;
}

export function waitForNextTurn(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

export function getConnectionUrl(client: MongoClient): string {
  const hosts = client.options.hosts.join(",");
  const replicaSet = client.options.replicaSet;
  const query = replicaSet
    ? `?replicaSet=${encodeURIComponent(replicaSet)}`
    : "";
  return `mongodb://${hosts}/${query}`;
}
