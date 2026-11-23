import { MongoClient } from "mongodb";

import { SetOptions } from "../../../src/config";
import { Connect } from "../../../src/connection";
import { FakeCollection, Server } from "./fake-driver";

const URL = "mongodb://fake";
const DATABASE = "formal";

/** The server each run puts here; the collection the code under test gets reads through to it. */
export const world = { server: new Server({ kind: "table", items: 0, batchSize: 1 }) };

/**
 * Points the driver at the fake server: `MongoClient.connect` answers a
 * client whose `db()` handle serves the collection of the run under way, so
 * `Connect` in src/connection.ts runs unchanged.
 *
 * Connects an owned instance whose every collection is the fake one.
 */
export async function connectFake(): Promise<void> {
  const database = { collection: () => new FakeCollection(world.server) };
  const client = { db: () => database, close: async () => undefined };
  Reflect.set(MongoClient, "connect", async () => client);
  SetOptions({ url: URL, database: DATABASE });
  await Connect(URL, DATABASE);
}
