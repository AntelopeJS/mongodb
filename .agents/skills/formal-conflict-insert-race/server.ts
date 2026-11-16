/**
 * Points the driver at the fake collection: `MongoClient.connect` answers a
 * client whose `db()` handle serves the collection of the run under way, so
 * `Connect` in src/connection.ts runs unchanged.
 */
import { MongoClient } from "mongodb";

import { SetOptions } from "../../../src/config";
import { Connect } from "../../../src/connection";
import { FakeCollection, Store } from "./fake-collection";

const URL = "mongodb://fake";
const DATABASE = "formal";

/** The store each run puts here; the collection the code under test gets reads through to it. */
export const server = { store: new Store() };

/** Connects an owned instance whose every collection is the fake one. */
export async function connectFake(): Promise<void> {
  const current = { collection: () => new FakeCollection(server.store) };
  const client = { db: () => current, close: async () => undefined };
  Reflect.set(MongoClient, "connect", async () => client);
  SetOptions({ url: URL, database: DATABASE });
  await Connect(URL, DATABASE);
}
