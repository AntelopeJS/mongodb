import { expect } from "chai";
import {
  Schema,
  type SchemaDefinition,
} from "@antelopejs/interface-database/schema";

import * as connection from "../connection";
import { collectionName } from "../implementations/database/utils";

interface TaggedRecord {
  _id: string;
  tags: string[];
  kept?: string[];
  keptCopy?: string[];
  upper?: string[];
  upperCopy?: string[];
}

interface TaggedTables {
  records: TaggedRecord;
}

const SCHEMA_ID = "shared-lambda-update";
const TABLE = "records";
const TENANT = "tenant-one";
const RECORD_ID = "record-one";

const definition: SchemaDefinition = {
  [TABLE]: { fields: {}, indexes: {} },
};

let schema: Schema<TaggedTables>;

function table() {
  return schema.instance(TENANT).table(TABLE);
}

describe("update reusing one list-function expression in two fields", () => {
  before(async () => {
    const collection = await connection.GetCollection(
      collectionName(SCHEMA_ID, TABLE),
    );
    await collection.drop().catch(() => undefined);
    await connection.InitializeSchema(SCHEMA_ID, definition);
    schema = new Schema<TaggedTables>(SCHEMA_ID, definition);
  });

  beforeEach(async () => {
    await table().delete().run();
    await table()
      .insert({ _id: RECORD_ID, tags: ["a", "b", "c"] })
      .run();
  });

  it("writes a shared filter lambda to both fields", async () => {
    await table()
      .get(RECORD_ID)
      .update((row) => {
        const kept = row.key("tags").filter((tag) => tag.ne("b"));
        return { kept, keptCopy: kept };
      })
      .run();

    const stored = await table().get(RECORD_ID).run();
    expect(stored).to.deep.include({ kept: ["a", "c"], keptCopy: ["a", "c"] });
  });

  it("writes a shared map lambda to both fields", async () => {
    await table()
      .get(RECORD_ID)
      .update((row) => {
        const upper = row.key("tags").map((tag) => tag.upcase());
        return { upper, upperCopy: upper };
      })
      .run();

    const stored = await table().get(RECORD_ID).run();
    expect(stored).to.deep.include({
      upper: ["A", "B", "C"],
      upperCopy: ["A", "B", "C"],
    });
  });
});
