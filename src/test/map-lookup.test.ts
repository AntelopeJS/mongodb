import assert from "node:assert/strict";
import { Schema } from "@antelopejs/interface-database/schema";

interface Category {
  _id: string;
  label: string;
}

interface Product {
  _id: string;
  name: string;
}

interface Tables {
  cats: Category;
  prods: Product;
}

const SCHEMA_ID = "map-lookup-keys";
const INSTANCE = "t";

let schema: Schema<Tables>;

describe("a key lookup inside a map", () => {
  before(async () => {
    schema = new Schema<Tables>(SCHEMA_ID, {
      cats: { fields: {}, indexes: {} },
      prods: { fields: {}, indexes: {} },
    });
    await schema.createInstance(INSTANCE).run();
    const instance = schema.instance(INSTANCE);
    await instance
      .table("cats")
      .insert([
        { _id: "constructor", label: "C" },
        { _id: "other", label: "O" },
      ])
      .run();
    await instance.table("prods").insert({ _id: "p", name: "P" }).run();
  });

  it("reads an id named after an Object.prototype member like any other id", async () => {
    const cases: [string, string | undefined][] = [
      ["other", "O"],
      ["constructor", "C"],
      ["toString", undefined],
    ];
    for (const [key, label] of cases) {
      const instance = schema.instance(INSTANCE);
      const rows = await instance
        .table("prods")
        .map((row) => row.merge({ cat: instance.table("cats").get(key) }))
        .run();
      assert.equal(rows.length, 1, `key ${key}`);
      const [row] = rows;
      assert.equal(row.cat?.label, label, `key ${key}`);
    }
  });
});
