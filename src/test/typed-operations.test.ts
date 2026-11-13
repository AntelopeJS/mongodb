import assert from "node:assert/strict";
import { ValueProxy } from "@antelopejs/interface-database";
import { Schema } from "@antelopejs/interface-database/schema";

const SCHEMA_ID = "typed-operations";
const INSTANCE = "one";
const BORN_AT = 5000;

interface Row {
  _id: string;
  name: string;
  suffix: string;
  tags: string[];
  more: string[];
  age: number;
  bornAt: Date;
  field: "name" | "suffix";
  low: number;
  high: number;
}

const schema = new Schema<{ rows: Row }>(SCHEMA_ID, {
  rows: {
    fields: {
      name: "string",
      suffix: "string",
      tags: "string[]",
      more: "string[]",
      age: "number",
      bornAt: "date",
      field: "string",
      low: "number",
      high: "number",
    },
    indexes: {},
  },
});

function Rows() {
  return schema.instance(INSTANCE).table("rows");
}

/** Creates the instance and its one row before a describe's tests, and destroys it after. */
function UseRows() {
  before(async () => {
    await schema.createInstance(INSTANCE).run();
    await Rows()
      .insert({
        _id: "ada",
        name: "ada",
        suffix: "!",
        tags: ["a"],
        more: ["b"],
        age: 36,
        bornAt: new Date(BORN_AT),
        field: "name",
        low: 0,
        high: 10_000,
      })
      .run();
  });

  after(async () => {
    await schema.destroyInstance(INSTANCE).run();
  });
}

describe("the add operation interface-database types", () => {
  UseRows();

  it("add concatenates a string with a constant or a field", async () => {
    assert.deepEqual(
      await Rows()
        .map((row) => row.key("name").add("!"))
        .run(),
      ["ada!"],
    );
    assert.deepEqual(
      await Rows()
        .map((row) => row.key("name").add(row.key("suffix")))
        .run(),
      ["ada!"],
    );
  });

  it("add concatenates a constant string receiver with a constant or a field", async () => {
    assert.deepEqual(
      await Rows()
        .map(() => ValueProxy.constant("a").add("b"))
        .run(),
      ["ab"],
    );
    assert.deepEqual(
      await Rows()
        .map((row) => ValueProxy.constant("a").add(row.key("name")))
        .run(),
      ["aada"],
    );
  });

  it("add concatenates a list with a constant or a field", async () => {
    assert.deepEqual(
      await Rows()
        .map((row) => row.key("tags").add(["x"]))
        .run(),
      [["a", "x"]],
    );
    assert.deepEqual(
      await Rows()
        .map((row) => row.key("tags").add(row.key("more")))
        .run(),
      [["a", "b"]],
    );
  });

  it("add still adds two numbers, constant or field", async () => {
    assert.deepEqual(
      await Rows()
        .map((row) => row.key("age").add(1))
        .run(),
      [37],
    );
    assert.deepEqual(
      await Rows()
        .map((row) => row.key("age").add(row.key("age")))
        .run(),
      [72],
    );
  });

  it("add answers null for a null operand on every receiver", async () => {
    assert.deepEqual(
      await Rows()
        .map((row) => ({
          number: row.key("age").add(null),
          date: row.key("bornAt").add(null),
          text: row.key("name").add(null),
          list: row.key("tags").add(null),
        }))
        .run(),
      [{ number: null, date: null, text: null, list: null }],
    );
  });
});

describe("the other operations interface-database types", () => {
  UseRows();

  it("during reads a number as milliseconds since the UNIX epoch", async () => {
    assert.deepEqual(
      await Rows()
        .map((row) => ({
          inside: row.key("bornAt").during(0, 10_000),
          low: row.key("bornAt").during(BORN_AT, 10_000),
          high: row.key("bornAt").during(0, BORN_AT),
          fields: row.key("bornAt").during(row.key("low"), row.key("high")),
          before: row.key("bornAt").during(6000, 10_000),
        }))
        .run(),
      [{ inside: true, low: true, high: false, fields: true, before: false }],
    );
  });

  it("during still compares dates", async () => {
    assert.deepEqual(
      await Rows()
        .map((row) => row.key("bornAt").during(new Date(0), new Date(10_000)))
        .run(),
      [true],
    );
  });

  it("key reads the field a constant expression names", async () => {
    assert.deepEqual(
      await Rows().key(ValueProxy.constant<"name">("name")).run(),
      ["ada"],
    );
  });

  it("min and max compare strings and dates", async () => {
    assert.deepEqual(
      await Rows()
        .map(() => ({
          min: ValueProxy.constant(["b", "a"]).min(),
          max: ValueProxy.constant([new Date(5), new Date(9)]).max(),
        }))
        .run(),
      [{ min: "a", max: new Date(9) }],
    );
  });

  it("filter keeps a number unless it is 0", async () => {
    assert.deepEqual(
      await Rows()
        .map(() => ValueProxy.constant([0, 1, 2]).filter((value) => value))
        .run(),
      [[1, 2]],
    );
  });
});
