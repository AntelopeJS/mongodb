import assert from "node:assert/strict";
import { Schema } from "@antelopejs/interface-database/schema";

import { CloseCursor, ReadCursor } from "../implementations/database/query";

interface Row {
  _id: string;
  name: string;
}

interface NestedRow {
  _id: string;
  n: number;
  nested?: string | null;
}

const SCHEMA_ID = "read-cursor-end";
const INSTANCE = "t";

let schema: Schema<{ rows: Row; nested: NestedRow }>;

describe("reading a cursor past its last row", () => {
  before(async () => {
    schema = new Schema<{ rows: Row; nested: NestedRow }>(SCHEMA_ID, {
      rows: { fields: {}, indexes: {} },
      nested: { fields: {}, indexes: {} },
    });
    await schema.createInstance(INSTANCE).run();
    await schema
      .instance(INSTANCE)
      .table("rows")
      .insert({ _id: "a", name: "A" })
      .run();
    await schema
      .instance(INSTANCE)
      .table("nested")
      .insert([
        { _id: "a", n: 1, nested: "x" },
        { _id: "b", n: 2, nested: null },
        { _id: "c", n: 3 },
        { _id: "d", n: 4, nested: "y" },
        { _id: "e", n: 5, nested: null },
        { _id: "f", n: 6 },
      ])
      .run();
  });

  it("ends a mapped stream like a plain one", async () => {
    const rows = schema.instance(INSTANCE).table("rows");
    const streams = [rows.build(), rows.map((row) => row.key("name")).build()];
    for (const [index, stages] of streams.entries()) {
      const reqId = 7200 + index;
      try {
        const first = await ReadCursor(reqId, stages);
        assert.equal(first.done, false, `stream ${index} yields its row`);
        const last = await ReadCursor(reqId, stages);
        assert.deepEqual(
          last,
          { done: true, value: undefined },
          `stream ${index} ends`,
        );
      } finally {
        await CloseCursor(reqId);
      }
    }
  });

  it("yields every mapped row, null and missing values included, as run() does", async () => {
    const query = schema
      .instance(INSTANCE)
      .table("nested")
      .orderBy("n")
      .map((row) => row.key("nested"));
    const cursorValues: unknown[] = [];
    for await (const value of query.cursor()) {
      cursorValues.push(value);
    }
    const expected = ["x", null, null, "y", null, null];
    assert.deepEqual(cursorValues, expected, "the cursor yields every row");
    assert.deepEqual(await query.run(), expected, "run() returns every row");
  });
});
