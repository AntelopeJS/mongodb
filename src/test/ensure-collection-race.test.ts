import { expect } from "chai";
import sinon from "sinon";

import {
  GetDatabase,
  InitializeSchema,
  type SchemaDefinition,
} from "../connection";
import { collectionName } from "../implementations/database/utils";

const SCHEMA_ID = "ensure-collection-race";
const TABLE_ID = "rows";
const SCHEMA: SchemaDefinition = { [TABLE_ID]: { indexes: {} } };
const COLLECTION = collectionName(SCHEMA_ID, TABLE_ID);

describe("schema initialization on an account that may run collMod", () => {
  afterEach(async () => {
    sinon.restore();
    await (await GetDatabase()).collection(COLLECTION).drop();
  });

  it("records pre-images on a collection another writer created without them after its listing", async () => {
    const db = await GetDatabase();
    const createCollection = db.createCollection.bind(db);
    sinon.stub(db, "createCollection").callsFake(async (name, options) => {
      await db.collection(name).insertOne({ writer: "other" });
      return createCollection(name, options);
    });
    await InitializeSchema(SCHEMA_ID, SCHEMA);
    const [collection] = await db
      .listCollections({ name: COLLECTION }, { nameOnly: false })
      .toArray();
    expect(collection?.options?.changeStreamPreAndPostImages).to.deep.equal({
      enabled: true,
    });
  });
});
