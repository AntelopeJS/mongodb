import assert from "node:assert/strict";
import type { Stream } from "@antelopejs/interface-database";
import { CROSS_INSTANCE, Schema } from "@antelopejs/interface-database/schema";

const SCHEMA_ID = "group-mapper-stages";
const FIRST = "first-tenant";
const SECOND = "second-tenant";
const MIXED = "mixed-tenant";

interface Invoice {
  _id: string;
  currency: string;
  total: number;
  updatedAt: number;
  tags?: number[] | number | null;
  note?: string;
}

const schema = new Schema<{ invoices: Invoice }>(SCHEMA_ID, {
  invoices: {
    fields: {},
    indexes: { currency: {}, updatedAt: {}, tags: {} },
  },
});

function Invoices(instance: string) {
  return schema.instance(instance).table("invoices");
}

function ByKey<T extends Record<string, unknown>>(rows: T[], key: keyof T) {
  return [...rows].sort((a, b) => String(a[key]).localeCompare(String(b[key])));
}

describe("group mapper stages before a reduction", () => {
  before(async () => {
    await schema.createInstance(FIRST).run();
    await schema.createInstance(SECOND).run();
    await Invoices(FIRST)
      .insert([
        {
          _id: "eur-500",
          currency: "EUR",
          total: 500,
          updatedAt: 1,
          tags: [1, 9],
          note: "paid",
        },
        { _id: "eur-0-a", currency: "EUR", total: 0, updatedAt: 3, tags: [5] },
        { _id: "eur-0-b", currency: "EUR", total: 0, updatedAt: 2, tags: [4] },
        { _id: "usd-0", currency: "USD", total: 0, updatedAt: 4, tags: [0] },
      ])
      .run();
    await Invoices(SECOND)
      .insert([
        { _id: "gbp-7", currency: "GBP", total: 7, updatedAt: 6, tags: [] },
        { _id: "gbp-9", currency: "GBP", total: 9, updatedAt: 5, tags: [] },
      ])
      .run();
  });

  after(async () => {
    await schema.destroyInstance(FIRST).run();
    await schema.destroyInstance(SECOND).run();
  });

  filterCases();
  orderCases();
  refusalCases();
});

function filterCases() {
  it("a filter of one field leaves its sibling fields and the group keys whole", async () => {
    const result = await Invoices(FIRST)
      .group("currency", (rows, currency) => ({
        currency,
        all: rows.count(),
        nonZero: rows.filter((row) => row.key("total").ne(0)).count(),
      }))
      .run();
    assert.deepEqual(ByKey(result, "currency"), [
      { currency: "EUR", all: 3, nonZero: 1 },
      { currency: "USD", all: 1, nonZero: 0 },
    ]);
  });

  it("each reduction after a filter reads only the rows its own filter keeps", async () => {
    const result = await Invoices(FIRST)
      .group("currency", (rows, currency) => ({
        currency,
        sum: rows.filter((row) => row.key("total").eq(0)).sum("total"),
        max: rows.filter((row) => row.key("updatedAt").lt(3)).max("updatedAt"),
        min: rows.filter((row) => row.key("total").eq(0)).min("updatedAt"),
        ids: rows.filter((row) => row.key("total").ne(0)).distinct("_id"),
        totals: rows
          .filter((row) => row.key("updatedAt").gt(1))
          .map((row) => row.key("total"))
          .count(),
      }))
      .run();
    assert.deepEqual(ByKey(result, "currency"), [
      { currency: "EUR", sum: 0, max: 2, min: 2, ids: ["eur-500"], totals: 2 },
      { currency: "USD", sum: 0, max: null, min: 4, ids: [], totals: 1 },
    ]);
  });

  it("a count of distinct values keeps its sibling fields and the group key", async () => {
    const result = await Invoices(FIRST)
      .group("currency", (rows, currency) => ({
        currency,
        all: rows.count(),
        totals: rows.count("total"),
      }))
      .run();
    assert.deepEqual(ByKey(result, "currency"), [
      { currency: "EUR", all: 3, totals: 2 },
      { currency: "USD", all: 1, totals: 1 },
    ]);
  });

  it("a map after a filter never runs on the rows the filter dropped", async () => {
    const result = await Invoices(FIRST)
      .group("currency", (rows, currency) => ({
        currency,
        shares: rows
          .filter((row) => row.key("total").ne(0))
          .map((row) => row.key("updatedAt").div(row.key("total")))
          .count(),
      }))
      .run();
    assert.deepEqual(ByKey(result, "currency"), [
      { currency: "EUR", shares: 1 },
      { currency: "USD", shares: 0 },
    ]);
  });

  it("chained filters without a reduction keep the rows both filters keep", async () => {
    const result = await Invoices(FIRST)
      .group("currency", (rows, currency) => ({
        currency,
        kept: rows
          .filter((row) => row.key("total").ne(0))
          .filter((row) => row.key("updatedAt").lt(3))
          .map((row) => row.key("_id")),
      }))
      .run();
    assert.deepEqual(ByKey(result, "currency"), [
      { currency: "EUR", kept: ["eur-500"] },
      { currency: "USD", kept: [] },
    ]);
  });
}

function refusalCases() {
  const refusals: [
    string,
    (rows: Stream<Invoice>) => ReturnType<Stream<Invoice>["count"]>,
  ][] = [
    [
      "a filter after a slice",
      (rows) =>
        rows
          .slice(0, 2)
          .filter((row) => row.key("total").ne(0))
          .count(),
    ],
    ["a reduction after a slice", (rows) => rows.slice(0, 2).count()],
    ["a reduction after distinct", (rows) => rows.distinct("total").count()],
    [
      "a join",
      (rows) =>
        rows
          .join(
            Invoices(FIRST),
            (left, right) => left.key("_id").eq(right.key("_id")),
            (_left, right) => right,
          )
          .count(),
    ],
  ];
  for (const [refused, field] of refusals) {
    it(`${refused} in a mapper field is refused instead of answering for the wrong rows`, async () => {
      await assert.rejects(
        Invoices(FIRST)
          .group("currency", (rows) => ({
            all: rows.count(),
            field: field(rows),
          }))
          .run(),
        { message: /^Unsupported group mapper: / },
      );
    });
  }
}

function orderCases() {
  it("an orderBy and nth of a mapper pick one row in every group", async () => {
    const result = await schema
      .instance(CROSS_INSTANCE)
      .table("invoices")
      .group("_instance", (rows) => rows.orderBy("updatedAt", "desc").nth(0))
      .run();
    assert.deepEqual(
      result.map((row) => String(row?._id)).sort((a, b) => a.localeCompare(b)),
      ["gbp-7", "usd-0"],
    );
  });

  it("two fields ordered in opposite directions each keep their own order", async () => {
    const result = await Invoices(FIRST)
      .group("currency", (rows, currency) => ({
        currency,
        latest: rows.orderBy("updatedAt", "desc").nth(0).key("_id"),
        oldestTwo: rows
          .orderBy("updatedAt", "asc")
          .slice(0, 2)
          .map((row) => row.key("_id")),
      }))
      .run();
    assert.deepEqual(ByKey(result, "currency"), [
      { currency: "EUR", latest: "eur-0-a", oldestTwo: ["eur-500", "eur-0-b"] },
      { currency: "USD", latest: "usd-0", oldestTwo: ["usd-0"] },
    ]);
  });

  it("nth past the first row skips the rows before it", async () => {
    const result = await Invoices(FIRST)
      .group("currency", (rows, currency) => ({
        currency,
        second: rows.orderBy("updatedAt", "desc").nth(1).key("_id"),
      }))
      .run();
    assert.deepEqual(ByKey(result, "currency"), [
      { currency: "EUR", second: "eur-0-b" },
      { currency: "USD", second: null },
    ]);
  });

  it("an ordered map leaves out the rows whose mapped value is missing", async () => {
    const result = await Invoices(FIRST)
      .group("currency", (rows, currency) => ({
        currency,
        notes: rows.orderBy("updatedAt", "asc").map((row) => row.key("note")),
      }))
      .run();
    assert.deepEqual(ByKey(result, "currency"), [
      { currency: "EUR", notes: ["paid"] },
      { currency: "USD", notes: [] },
    ]);
  });

  it("an orderBy on an array field sorts as a top-level orderBy does", async () => {
    const top = await Invoices(FIRST)
      .filter((row) => row.key("currency").eq("EUR"))
      .orderBy("tags", "desc")
      .map((row) => row.key("_id"))
      .run();
    const result = await Invoices(FIRST)
      .group("currency", (rows, currency) => ({
        currency,
        ids: rows.orderBy("tags", "desc").map((row) => row.key("_id")),
      }))
      .run();
    assert.deepEqual(top, ["eur-500", "eur-0-a", "eur-0-b"]);
    assert.deepEqual(ByKey(result, "currency")[0], {
      currency: "EUR",
      ids: top,
    });

    try {
      await schema.createInstance(MIXED).run();
      await Invoices(MIXED)
        .insert([
          { _id: "1", currency: "EUR", total: 0, updatedAt: 1, tags: null },
          { _id: "2", currency: "EUR", total: 0, updatedAt: 2 },
          { _id: "3", currency: "EUR", total: 0, updatedAt: 3, tags: [] },
          { _id: "4", currency: "EUR", total: 0, updatedAt: 4, tags: [5, 1] },
          { _id: "5", currency: "EUR", total: 0, updatedAt: 5, tags: 3 },
        ])
        .run();
      const mixedTop = await Invoices(MIXED)
        .orderBy("tags", "asc")
        .map((row) => row.key("_id"))
        .run();
      const mixed = await Invoices(MIXED)
        .group("currency", (rows, currency) => ({
          currency,
          ids: rows.orderBy("tags", "asc").map((row) => row.key("_id")),
        }))
        .run();
      assert.deepEqual(mixedTop, ["3", "1", "2", "4", "5"]);
      assert.deepEqual(mixed[0].ids, mixedTop);
    } finally {
      await schema.destroyInstance(MIXED).run();
    }
  });
}
