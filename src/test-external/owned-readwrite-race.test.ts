import { expect } from "chai";
import { type Db, MongoClient } from "mongodb";
import sinon from "sinon";

import { GetOptions, type ResolvedOptions, SetOptions } from "../config";
import {
  Connect,
  GetDatabase,
  InitializeSchema,
  type SchemaDefinition,
} from "../connection";
import { collectionName } from "../implementations/database/utils";
import { AdminClient } from "./fixture";

const OWNED_DATABASE = "antelopejs_owned_race";
const OWNED_USER = "owned_read_write";
const OWNED_PASSWORD = "owned-password";
const SCHEMA_ID = "replicas";
const TABLE_ID = "orders";
const SCHEMA: SchemaDefinition = { [TABLE_ID]: { indexes: {} } };
const INSTANCE_INDEX = "_instance";

function OwnedUrl(url: string): string {
  const target = new URL(url);
  target.username = OWNED_USER;
  target.password = OWNED_PASSWORD;
  target.searchParams.set("authSource", OWNED_DATABASE);
  return target.toString();
}

describe("an owned instance on a readWrite account", () => {
  let admin: MongoClient;
  let otherReplica: MongoClient;
  let configured: ResolvedOptions;

  before(async () => {
    configured = { ...GetOptions() };
    admin = await AdminClient();
    const db = admin.db(OWNED_DATABASE);
    await db.dropDatabase();
    await db.command({
      createUser: OWNED_USER,
      pwd: OWNED_PASSWORD,
      roles: [{ role: "readWrite", db: OWNED_DATABASE }],
    });
    const url = OwnedUrl(configured.url);
    SetOptions({ mode: "owned", url, database: OWNED_DATABASE });
    await Connect(url, OWNED_DATABASE);
    otherReplica = await MongoClient.connect(url);
  });

  after(async () => {
    sinon.restore();
    await otherReplica.close();
    SetOptions(configured);
    await Connect(configured.url, configured.database, configured.options);
    const db = admin.db(OWNED_DATABASE);
    await db.command({ dropUser: OWNED_USER });
    await db.dropDatabase();
    await admin.close();
  });

  it("initializes a collection another replica creates with other options after its listing", async () => {
    const db: Db = await GetDatabase();
    const createCollection = db.createCollection.bind(db);
    sinon.stub(db, "createCollection").callsFake(async (name, options) => {
      await otherReplica.db(OWNED_DATABASE).createCollection(name);
      return createCollection(name, options);
    });
    await InitializeSchema(SCHEMA_ID, SCHEMA);
    const indexes = await admin
      .db(OWNED_DATABASE)
      .collection(collectionName(SCHEMA_ID, TABLE_ID))
      .indexes();
    expect(indexes.map((index) => index.name)).to.include(INSTANCE_INDEX);
  });
});
