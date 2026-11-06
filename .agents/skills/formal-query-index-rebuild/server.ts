/**
 * Points the driver at the fake database: `MongoClient.connect` answers a
 * client whose `db()` is the database of the scenario under way, so
 * `Connect` in src/connection.ts runs unchanged.
 */
import { MongoClient } from "mongodb";

import { SetOptions } from "../../../src/config";
import { Connect } from "../../../src/connection";
import { AllowSchemaInitializations } from "../../../src/schema-initialization";
import { FakeDatabase } from "./fake-mongo";

const URL = "mongodb://fake";
const DATABASE = "formal";

/** The database the driver answers with; each scenario puts a fresh one here. */
export const server = { database: new FakeDatabase() };

/** Connects an owned instance to the fake database and lets schemas initialize. */
export async function connectFake(): Promise<void> {
  const client = { db: () => server.database, close: async () => undefined };
  Reflect.set(MongoClient, "connect", async () => client);
  SetOptions({ url: URL, database: DATABASE });
  await Connect(URL, DATABASE);
  AllowSchemaInitializations();
}
