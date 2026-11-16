import assert from "node:assert/strict";
import { ValueProxy } from "@antelopejs/interface-database";
import { Schema } from "@antelopejs/interface-database/schema";

const SCHEMA_ID = "dotted-field-key";
const INSTANCE = "one";
const DOTTED = "a.b";

interface Row {
  _id: string;
  "a.b": string;
  a: { b: string };
  other?: string;
}

const schema = new Schema<{ rows: Row }>(SCHEMA_ID, {
  rows: { fields: {}, indexes: {} },
});

function Rows() {
  return schema.instance(INSTANCE).table("rows");
}

describe("key on a top-level field whose name holds a dot", () => {
  before(async () => {
    await schema.createInstance(INSTANCE).run();
    await Rows()
      .insert([
        { _id: "one", "a.b": "flat-one", a: { b: "nested-one" } },
        { _id: "two", "a.b": "flat-two", a: { b: "nested-two" } },
      ])
      .run();
  });

  after(async () => {
    await schema.destroyInstance(INSTANCE).run();
  });

  it("a key stage reads the flat field, not the nested path", async () => {
    assert.deepEqual(await Rows().orderBy("_id").key(DOTTED).run(), [
      "flat-one",
      "flat-two",
    ]);
  });

  it("a key stage named by an expression reads the flat field", async () => {
    assert.deepEqual(
      await Rows().orderBy("_id").key(ValueProxy.constant<"a.b">(DOTTED)).run(),
      ["flat-one", "flat-two"],
    );
  });

  it("a key in a filter reads the flat field", async () => {
    assert.deepEqual(
      await Rows()
        .filter((row) => row.key(DOTTED).eq("flat-two"))
        .key("_id")
        .run(),
      ["two"],
    );
  });

  it("a key in a map reads the flat field", async () => {
    assert.deepEqual(
      await Rows()
        .orderBy("_id")
        .map((row) => row.key(DOTTED))
        .run(),
      ["flat-one", "flat-two"],
    );
  });

  it("a key with a default falls back only when the flat field is absent", async () => {
    assert.deepEqual(await Rows().orderBy("_id").key("other", "none").run(), [
      "none",
      "none",
    ]);
  });

  it("a key chain still reads a nested path", async () => {
    assert.deepEqual(
      await Rows()
        .orderBy("_id")
        .map((row) => row.key("a").key("b"))
        .run(),
      ["nested-one", "nested-two"],
    );
  });
});
