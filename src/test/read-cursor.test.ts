import assert from "node:assert/strict";
import { Schema } from "@antelopejs/interface-database/schema";

import { CloseCursor, ReadCursor } from "../implementations/database/query";

interface Row {
  _id: string;
  name: string;
}

const SCHEMA_ID = "read-cursor-end";
const INSTANCE = "t";

let schema: Schema<{ rows: Row }>;

describe("reading a cursor past its last row", () => {
  before(async () => {
    schema = new Schema<{ rows: Row }>(SCHEMA_ID, {
      rows: { fields: {}, indexes: {} },
    });
    await schema.createInstance(INSTANCE).run();
    await schema
      .instance(INSTANCE)
      .table("rows")
      .insert({ _id: "a", name: "A" })
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
          { done: true, value: null },
          `stream ${index} ends`,
        );
      } finally {
        await CloseCursor(reqId);
      }
    }
  });
});
