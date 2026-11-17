import assert from "node:assert/strict";
import { Schema } from "@antelopejs/interface-database/schema";

const SCHEMA_ID = "group-mapper-limits";
const LARGE = "large-group";
const SMALL = "small-group";
const LARGE_ROWS = 150_000;
const INSERT_BATCH = 10_000;
const BEYOND_INT32 = 2 ** 40;

interface Reading {
  _id: string;
  g: string;
  k: number;
}

interface Slot {
  _id: string;
  g: number;
  k: number;
}

const schema = new Schema<{ readings: Reading; slots: Slot }>(SCHEMA_ID, {
  readings: {
    fields: {},
    indexes: { g: {}, k: {} },
  },
  slots: {
    fields: {},
    indexes: { g: {}, k: {} },
  },
});

function Readings(instance: string) {
  return schema.instance(instance).table("readings");
}

function RowId(at: number) {
  return `reading-${String(at).padStart(6, "0")}`;
}

describe("group mapper limits", function () {
  this.timeout(180_000);

  before(async () => {
    await schema.createInstance(LARGE).run();
    await schema.createInstance(SMALL).run();
    for (let start = 0; start < LARGE_ROWS; start += INSERT_BATCH) {
      await Readings(LARGE)
        .insert(
          Array.from({ length: INSERT_BATCH }, (_, offset) => ({
            _id: RowId(start + offset),
            g: "one",
            k: LARGE_ROWS - start - offset,
          })),
        )
        .run();
    }
    await Readings(SMALL)
      .insert([
        { _id: "a", g: "one", k: 3 },
        { _id: "b", g: "one", k: 1 },
        { _id: "c", g: "one", k: 2 },
      ])
      .run();
    await schema
      .instance(SMALL)
      .table("slots")
      .insert([
        { _id: "a", g: 2, k: 3 },
        { _id: "b", g: 2, k: 1 },
        { _id: "c", g: 2, k: 2 },
      ])
      .run();
  });

  after(async () => {
    await schema.destroyInstance(LARGE).run();
    await schema.destroyInstance(SMALL).run();
  });

  it("an ordered map over one group of 150,000 rows answers every row in order", async () => {
    const [result] = await Readings(LARGE)
      .group("g", (rows) => rows.orderBy("k").map((row) => row.key("_id")))
      .run();
    assert.equal(result.length, LARGE_ROWS);
    assert.equal(result[0], RowId(LARGE_ROWS - 1));
    assert.equal(result[LARGE_ROWS - 1], RowId(0));
  });

  it("an ordered nth over one group of 150,000 rows answers the first row", async () => {
    const result = await Readings(LARGE)
      .group("g", (rows) => rows.orderBy("k").nth(0).key("_id"))
      .run();
    assert.deepEqual(result, [RowId(LARGE_ROWS - 1)]);
  });

  it("a slice whose count is past 32 bits answers every row, as a top-level slice does", async () => {
    const top = await Readings(SMALL)
      .orderBy("k")
      .slice(0, BEYOND_INT32)
      .map((row) => row.key("_id"))
      .run();
    const result = await Readings(SMALL)
      .group("g", (rows) =>
        rows
          .orderBy("k")
          .slice(0, BEYOND_INT32)
          .map((row) => row.key("_id")),
      )
      .run();
    assert.deepEqual(top, ["b", "c", "a"]);
    assert.deepEqual(result, [top]);
  });

  it("a slice and an nth whose arguments come from the group key answer the rows they name", async () => {
    const slots = schema.instance(SMALL).table("slots");
    const sliced = await slots
      .group("g", (rows, g) =>
        rows
          .orderBy("k")
          .slice(0, g)
          .map((row) => row.key("_id")),
      )
      .run();
    const picked = await slots
      .group("g", (rows, g) => rows.orderBy("k").nth(g.sub(1)).key("_id"))
      .run();
    assert.deepEqual(sliced, [["b", "c"]]);
    assert.deepEqual(picked, ["c"]);
  });

  it("nth of a negative index is refused, as a top-level nth refuses it", async () => {
    const refusal = { message: /invalid argument to \$skip stage/ };
    await assert.rejects(Readings(SMALL).orderBy("k").nth(-1).run(), refusal);
    await assert.rejects(
      Readings(SMALL)
        .group("g", (rows) => rows.orderBy("k").nth(-1))
        .run(),
      refusal,
    );
  });
});
