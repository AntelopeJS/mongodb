import { expect } from "chai";
import { ValueProxy } from "@antelopejs/interface-database";
import {
  Schema,
  type SchemaDefinition,
} from "@antelopejs/interface-database/schema";

import * as connection from "../connection";
import { collectionName } from "../implementations/database/utils";

interface LiteralRecord {
  _id: string;
  label: string;
  other: string;
  count: number;
  tags: string[];
  nested: Record<string, string>;
}

type SelectionKey = string | number | boolean;

interface LiteralTables {
  records: LiteralRecord;
}

const SCHEMA_ID = "literal-values";
const TABLE = "records";
const TENANT = "tenant-one";
const RECORD_ID = "record-one";
const DOLLAR_ID = "$dms.id";
const DOLLAR_VALUE = "$dms.x";
const OTHER_DOLLAR_VALUE = "$dms.y";
const DOLLAR_TAG = "$tag";
const PLAIN_VALUE = "plain";
const INITIAL_COUNT = 1;

const definition: SchemaDefinition = {
  [TABLE]: {
    fields: {},
    indexes: {
      label: {},
      pair: { fields: ["label", "other"] },
    },
  },
};

function record(id: string, label: string): LiteralRecord {
  return {
    _id: id,
    label,
    other: OTHER_DOLLAR_VALUE,
    count: INITIAL_COUNT,
    tags: [DOLLAR_TAG, PLAIN_VALUE],
    nested: { path: OTHER_DOLLAR_VALUE, "dotted.key": DOLLAR_VALUE },
  };
}

function expressionKey(value: string | string[]): ValueProxy<SelectionKey> {
  return ValueProxy.constant(value).cast<SelectionKey>();
}

function ids(documents: LiteralRecord[]): string[] {
  return documents.map((document) => document._id).sort();
}

let schema: Schema<LiteralTables>;
let schemaSetup: Promise<Schema<LiteralTables>> | undefined;

async function createSchema(): Promise<Schema<LiteralTables>> {
  const collection = await connection.GetCollection(
    collectionName(SCHEMA_ID, TABLE),
  );
  await collection.drop().catch(() => undefined);
  await connection.InitializeSchema(SCHEMA_ID, definition);
  return new Schema<LiteralTables>(SCHEMA_ID, definition);
}

function table() {
  return schema.instance(TENANT).table(TABLE);
}

function seedRecords() {
  before(async () => {
    schemaSetup ??= createSchema();
    schema = await schemaSetup;
  });

  beforeEach(async () => {
    await table().delete().run();
    await table()
      .insert([record(RECORD_ID, DOLLAR_VALUE), record(DOLLAR_ID, PLAIN_VALUE)])
      .run();
  });
}

describe("$-prefixed string values: writes", () => {
  seedRecords();

  it("stores them verbatim on insert", async () => {
    const stored = await table().get(RECORD_ID).run();

    expect(stored).to.deep.include(record(RECORD_ID, DOLLAR_VALUE));
  });

  it("stores them verbatim on an upserting insert", async () => {
    await table()
      .insert(record(RECORD_ID, OTHER_DOLLAR_VALUE), { conflict: "update" })
      .run();

    const stored = await table().get(RECORD_ID).run();
    expect(stored?.label).to.equal(OTHER_DOLLAR_VALUE);
  });

  it("stores them verbatim on a document update", async () => {
    await table()
      .get(RECORD_ID)
      .update({
        label: OTHER_DOLLAR_VALUE,
        tags: [DOLLAR_VALUE],
        nested: { path: DOLLAR_VALUE },
      })
      .run();

    const stored = await table().get(RECORD_ID).run();
    expect(stored).to.deep.include({
      label: OTHER_DOLLAR_VALUE,
      tags: [DOLLAR_VALUE],
      nested: { path: DOLLAR_VALUE },
    });
  });

  it("stores them verbatim on a function update mixing field references", async () => {
    await table()
      .get(RECORD_ID)
      .update((row) => ({
        label: OTHER_DOLLAR_VALUE,
        other: row.key("label"),
        count: row.key("count").add(1),
        nested: { path: DOLLAR_VALUE, "dotted.key": row.key("label") },
      }))
      .run();

    const stored = await table().get(RECORD_ID).run();
    expect(stored).to.deep.include({
      label: OTHER_DOLLAR_VALUE,
      other: DOLLAR_VALUE,
      count: INITIAL_COUNT + 1,
      nested: { path: DOLLAR_VALUE, "dotted.key": DOLLAR_VALUE },
    });
  });

  it("keeps $-prefixed keys of updated documents", async () => {
    await table()
      .get(RECORD_ID)
      .update({ nested: { $set: DOLLAR_VALUE } })
      .run();

    const stored = await table().get(RECORD_ID).run();
    expect(stored?.nested).to.deep.equal({ $set: DOLLAR_VALUE });
  });
});

describe("$-prefixed string values: selections", () => {
  seedRecords();

  it("gets a document by a $-prefixed primary key", async () => {
    const found = await table().get(DOLLAR_ID).run();

    expect(found?._id).to.equal(DOLLAR_ID);
  });

  it("updates a document selected by a $-prefixed primary key", async () => {
    await table().get(DOLLAR_ID).update({ label: DOLLAR_VALUE }).run();

    const stored = await table().get(DOLLAR_ID).run();
    expect(stored?.label).to.equal(DOLLAR_VALUE);
  });

  it("gets documents by $-prefixed index values", async () => {
    const single = await table().getAll(DOLLAR_VALUE, "label").run();
    const several = await table()
      .getAll([DOLLAR_VALUE, PLAIN_VALUE], "label")
      .run();

    expect(ids(single)).to.deep.equal([RECORD_ID]);
    expect(ids(several)).to.deep.equal([DOLLAR_ID, RECORD_ID]);
  });

  it("gets documents by $-prefixed index values given as expressions", async () => {
    const single = await table()
      .getAll(expressionKey(DOLLAR_VALUE), "label")
      .run();
    const several = await table()
      .getAll([expressionKey(DOLLAR_VALUE), PLAIN_VALUE], "label")
      .run();

    expect(ids(single)).to.deep.equal([RECORD_ID]);
    expect(ids(several)).to.deep.equal([DOLLAR_ID, RECORD_ID]);
  });

  it("gets documents by $-prefixed compound index values", async () => {
    const literal = await table()
      .getAll([DOLLAR_VALUE, OTHER_DOLLAR_VALUE], "pair")
      .run();
    const expression = await table()
      .getAll(expressionKey([DOLLAR_VALUE, OTHER_DOLLAR_VALUE]), "pair")
      .run();

    expect(ids(literal)).to.deep.equal([RECORD_ID]);
    expect(ids(expression)).to.deep.equal([RECORD_ID]);
  });

  it("selects documents between $-prefixed bounds", async () => {
    const found = await table().between("label", "$dms", "$dms.z").run();

    expect(ids(found)).to.deep.equal([RECORD_ID]);
  });
});

describe("$-prefixed string values: expressions", () => {
  seedRecords();

  it("filters on equality with a $-prefixed value", async () => {
    const equal = await table()
      .filter((row) => row.key("label").eq(DOLLAR_VALUE))
      .run();
    const different = await table()
      .filter((row) => row.key("label").ne(DOLLAR_VALUE))
      .run();

    expect(ids(equal)).to.deep.equal([RECORD_ID]);
    expect(ids(different)).to.deep.equal([DOLLAR_ID]);
  });

  it("filters with other operators on $-prefixed values", async () => {
    const lower = await table()
      .filter((row) => row.key("label").le(DOLLAR_VALUE))
      .run();
    const included = await table()
      .filter((row) => row.key("tags").includes(DOLLAR_TAG))
      .run();
    const constant = await table()
      .filter((row) => ValueProxy.constant(DOLLAR_VALUE).eq(row.key("label")))
      .run();

    expect(ids(lower)).to.deep.equal([RECORD_ID]);
    expect(ids(included)).to.deep.equal([DOLLAR_ID, RECORD_ID]);
    expect(ids(constant)).to.deep.equal([RECORD_ID]);
  });

  it("keeps $-prefixed values inside mapped objects and defaults", async () => {
    const mapped = await table()
      .get(RECORD_ID)
      .do((row) => ({
        label: row.key("label"),
        constant: DOLLAR_VALUE,
        missing: row.key("missing" as "label", DOLLAR_VALUE),
      }))
      .run();
    const defaulted = await table()
      .map((row) => row.key("missing" as "label"))
      .default(DOLLAR_VALUE)
      .run();

    expect(mapped).to.deep.equal({
      label: DOLLAR_VALUE,
      constant: DOLLAR_VALUE,
      missing: DOLLAR_VALUE,
    });
    expect(defaulted).to.deep.equal([DOLLAR_VALUE, DOLLAR_VALUE]);
  });

  it("splits on a $-prefixed separator", async () => {
    const parts = await table()
      .get(RECORD_ID)
      .do((row) => row.key("label").split("$"))
      .run();

    expect(parts).to.deep.equal(["", "dms.x"]);
  });
});
