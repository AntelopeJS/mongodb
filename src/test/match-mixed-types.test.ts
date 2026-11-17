import assert from "node:assert/strict";
import { Schema } from "@antelopejs/interface-database/schema";

const SCHEMA_ID = "match-mixed-types";
const INSTANCE = "one";

interface Row {
  _id: string;
  label?: unknown;
}

const schema = new Schema<{ rows: Row }>(SCHEMA_ID, {
  rows: { fields: {}, indexes: {} },
});

function Rows() {
  return schema.instance(INSTANCE).table("rows");
}

describe("match on a column holding text and non-text cells", () => {
  before(async () => {
    await schema.createInstance(INSTANCE).run();
    await Rows()
      .insert([
        { _id: "text-hit", label: "alpha" },
        { _id: "text-miss", label: "beta" },
        { _id: "number", label: 42 },
        { _id: "boolean", label: true },
        { _id: "object", label: { name: "alpha" } },
        { _id: "array", label: ["alpha"] },
        { _id: "date", label: new Date(0) },
        { _id: "null", label: null },
        { _id: "missing" },
      ])
      .run();
  });

  after(async () => {
    await schema.destroyInstance(INSTANCE).run();
  });

  it("a filter keeps the matching text rows and skips the non-text cells", async () => {
    assert.deepEqual(
      await Rows()
        .filter((row) => row.key("label").cast<string>().match("^a"))
        .key("_id")
        .run(),
      ["text-hit"],
    );
  });

  it("a map answers false for each non-text cell", async () => {
    assert.deepEqual(
      await Rows()
        .orderBy("_id")
        .map((row) => row.key("label").cast<string>().match("^a"))
        .run(),
      [false, false, false, false, false, false, false, true, false],
    );
  });
});
