import assert from "node:assert/strict";
import { ValueProxy } from "@antelopejs/interface-database";
import { Schema } from "@antelopejs/interface-database/schema";

const SCHEMA_ID = "date-arithmetic";
const INSTANCE = "one";
const BORN_AT = 5000;
const AGE = 36;

interface Row {
  _id: string;
  age: number;
  bornAt: Date;
}

const schema = new Schema<{ rows: Row }>(SCHEMA_ID, {
  rows: {
    fields: { age: "number", bornAt: "date" },
    indexes: {},
  },
});

function Rows() {
  return schema.instance(INSTANCE).table("rows");
}

describe("add and sub on a date", () => {
  before(async () => {
    await schema.createInstance(INSTANCE).run();
    await Rows()
      .insert({ _id: "ada", age: AGE, bornAt: new Date(BORN_AT) })
      .run();
  });

  after(async () => {
    await schema.destroyInstance(INSTANCE).run();
  });

  it("add and sub read a number as seconds on a date", async () => {
    assert.deepEqual(
      await Rows()
        .map((row) => ({
          addConstant: row.key("bornAt").add(1),
          addField: row.key("bornAt").add(row.key("age")),
          addToConstant: ValueProxy.constant(new Date(0)).add(1),
          subConstant: row.key("bornAt").sub(1),
          subField: row.key("bornAt").sub(row.key("age")),
          subFromConstant: ValueProxy.constant(new Date(1000)).sub(1),
        }))
        .run(),
      [
        {
          addConstant: new Date(BORN_AT + 1000),
          addField: new Date(BORN_AT + AGE * 1000),
          addToConstant: new Date(1000),
          subConstant: new Date(BORN_AT - 1000),
          subField: new Date(BORN_AT - AGE * 1000),
          subFromConstant: new Date(0),
        },
      ],
    );
  });

  it("sub answers the seconds between two dates", async () => {
    assert.deepEqual(
      await Rows()
        .map((row) => ({
          constant: row.key("bornAt").sub(new Date(0)),
          field: row.key("bornAt").sub(row.key("bornAt")),
          constants: ValueProxy.constant(new Date(BORN_AT)).sub(new Date(0)),
        }))
        .run(),
      [{ constant: BORN_AT / 1000, field: 0, constants: BORN_AT / 1000 }],
    );
  });

  it("add and sub still read numbers on a number", async () => {
    assert.deepEqual(
      await Rows()
        .map((row) => ({
          add: row.key("age").add(1),
          subConstant: row.key("age").sub(1),
          subField: row.key("age").sub(row.key("age")),
        }))
        .run(),
      [{ add: AGE + 1, subConstant: AGE - 1, subField: 0 }],
    );
  });
});
