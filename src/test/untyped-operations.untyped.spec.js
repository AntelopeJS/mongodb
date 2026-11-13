import assert from "node:assert/strict";
import { ValueProxy } from "@antelopejs/interface-database";
import { Schema } from "@antelopejs/interface-database/schema";

const SCHEMA_ID = "untyped-operations-json";
const INSTANCE = "one";

const schema = new Schema(SCHEMA_ID, {
  rows: {
    fields: { name: "string", age: "number" },
    indexes: {},
  },
});

function Rows() {
  return schema.instance(INSTANCE).table("rows");
}

describe("the operations a caller reaches past the contract with a JSON stage", () => {
  before(async () => {
    await schema.createInstance(INSTANCE).run();
    await Rows().insert({ _id: "ada", name: "ada", age: 36 }).run();
  });

  after(async () => {
    await schema.destroyInstance(INSTANCE).run();
  });

  it("add sends every operand of a hand-written JSON stage", async () => {
    assert.deepEqual(
      await Rows().map((row) => ({
        two: new ValueProxy({ stage: "add", args: [1, 2] }, row.key("age")),
        none: new ValueProxy({ stage: "add", args: [] }, row.key("age")),
        thenNull: new ValueProxy(
          { stage: "add", args: [1, 2, null] },
          row.key("age"),
        ),
        text: new ValueProxy(
          { stage: "add", args: ["x", "y"] },
          row.key("name"),
        ),
      })),
      [{ two: 39, none: 36, thenNull: null, text: "adaxy" }],
    );
  });
});
