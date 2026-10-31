import assert from "node:assert/strict";
import { ValueProxy } from "@antelopejs/interface-database";
import { Schema } from "@antelopejs/interface-database/schema";

const SCHEMA_ID = "untyped-operations";
const INSTANCE = "one";
const EPOCH_TEXT = "1970-01-01T00:00:00.000Z";
const LATER_TEXT = "1970-01-01T00:00:10.000Z";

interface Row {
  _id: string;
  name: string;
  age: number;
  bornAt: Date;
  low: number;
  high: number;
  earliest: string;
  latest: string;
}

const schema = new Schema<{ rows: Row }>(SCHEMA_ID, {
  rows: {
    fields: {
      name: "string",
      age: "number",
      bornAt: "date",
      low: "number",
      high: "number",
      earliest: "string",
      latest: "string",
    },
    indexes: {},
  },
});

function Rows() {
  return schema.instance(INSTANCE).table("rows");
}

function TextBound(text: string) {
  return ValueProxy.constant(text).cast<Date>();
}

/** An `add` stage as a route rebuilds it from a request's JSON: any number of operands, read after `receiver`. */
function JsonAdd(receiver: ValueProxy<number>, args: number[]) {
  const rebuilt: unknown = JSON.parse(JSON.stringify({ stage: "add", args }));
  return Object.assign(receiver.add(0), {
    stages: [...receiver.build(), rebuilt],
  });
}

describe("the operations a caller reaches past the contract, with cast or a JSON stage", () => {
  before(async () => {
    await schema.createInstance(INSTANCE).run();
    await Rows()
      .insert({
        _id: "ada",
        name: "ada",
        age: 36,
        bornAt: new Date(5000),
        low: 0,
        high: 100,
        earliest: EPOCH_TEXT,
        latest: LATER_TEXT,
      })
      .run();
  });

  after(async () => {
    await schema.destroyInstance(INSTANCE).run();
  });

  it("during compares a number field with number bounds as numbers", async () => {
    assert.deepEqual(
      await Rows().map((row) => {
        const age = row.key("age").cast<Date>();
        return {
          inside: age.during(0, 100),
          above: age.during(40, 100),
          fields: age.during(row.key("low"), row.key("high")),
        };
      }),
      [{ inside: true, above: false, fields: true }],
    );
  });

  it("during compares a string field with string bounds as strings", async () => {
    assert.deepEqual(
      await Rows().map((row) => {
        const name = row.key("name").cast<Date>();
        return {
          inside: name.during(TextBound("a"), TextBound("c")),
          above: name.during(TextBound("b"), TextBound("c")),
          fields: name.during(
            row.key("earliest").cast<Date>(),
            row.key("name").cast<Date>(),
          ),
        };
      }),
      [{ inside: true, above: false, fields: false }],
    );
  });

  it("during refuses no text bound on a date, and orders it by type", async () => {
    assert.deepEqual(
      await Rows().map((row) => ({
        constants: row
          .key("bornAt")
          .during(TextBound(EPOCH_TEXT), TextBound(LATER_TEXT)),
        fields: row
          .key("bornAt")
          .during(
            row.key("earliest").cast<Date>(),
            row.key("latest").cast<Date>(),
          ),
      })),
      [{ constants: false, fields: false }],
    );
  });

  it("during refuses no number bound that new Date cannot read", async () => {
    assert.deepEqual(
      await Rows().map((row) => ({
        hugeOnNumber: row.key("age").cast<Date>().during(0, 1e300),
        infiniteOnNumber: row.key("age").cast<Date>().during(0, Infinity),
        hugeOnDate: row.key("bornAt").during(1e300, 1e301),
        hugeFieldOnDate: row
          .key("bornAt")
          .during(row.key("high").mul(1e300), row.key("high").mul(1e301)),
      })),
      [
        {
          hugeOnNumber: true,
          infiniteOnNumber: true,
          hugeOnDate: false,
          hugeFieldOnDate: false,
        },
      ],
    );
  });

  it("add sends every operand of a hand-written JSON stage", async () => {
    assert.deepEqual(
      await Rows().map((row) => ({
        two: JsonAdd(row.key("age"), [1, 2]),
        none: JsonAdd(row.key("age"), []),
      })),
      [{ two: 39, none: 36 }],
    );
  });
});
