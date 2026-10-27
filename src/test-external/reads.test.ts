import { expect } from "chai";
import { Decimal128 } from "mongodb";
import {
  CROSS_INSTANCE,
  type Schema,
} from "@antelopejs/interface-database/schema";

import {
  type ErpRow,
  type ErpTables,
  Native,
  NativeCount,
  RegisterErp,
} from "./erp";
import { CustomerId, OrderId, SupplierId } from "./fixture";

type Row = ErpRow;

const PARIS = "Europe/Paris";
const DAY_MS = 86_400_000;

let erp: Schema<ErpTables>;

function Table(name: string) {
  return erp.instance().table(name);
}

describe("reading an external collection by key", () => {
  before(async () => {
    erp = await RegisterErp();
  });

  const KEYS: [string, (row: Row) => void][] = [
    ["customers", (row) => expect(row._id).to.match(/^[0-9a-f]{24}$/)],
    ["products", (row) => expect(row._id).to.match(/^SKU-/)],
    ["invoices", (row) => expect(row._id).to.match(/^1000\d\d$/)],
    [
      "suppliers",
      (row) => expect(row._id).to.match(/^0b7c6d1e-0000-4000-8000-/),
    ],
    [
      "order_lines",
      (row) => expect(row._id).to.match(/^\{"order":\{"\$oid":"/),
    ],
  ];

  for (const [table, rendered] of KEYS) {
    it(`gets each ${table} row back by the _id a list answered`, async () => {
      const rows = await Table(table).orderBy("_id").slice(0, 3);
      expect(rows).to.have.length(3);
      const ids = rows
        .map((row) => row._id)
        .filter((id): id is string => id?.constructor === String);
      expect(ids).to.have.length(rows.length);
      for (const [position, row] of rows.entries()) {
        rendered(row);
        expect(await Table(table).get(ids[position])).to.deep.equal(row);
      }
    });
  }

  for (const [table, key] of [
    ["customers", "nope"],
    ["customers", "65f0"],
    ["products", "SKU-none"],
    ["invoices", "1e400"],
    ["invoices", "abc"],
    ["suppliers", "not-a-uuid"],
    ["order_lines", "{bad"],
  ]) {
    it(`answers not found for the key ${key} on ${table}, rather than an error`, async () => {
      expect(await Table(table).get(key)).to.equal(undefined);
    });
  }

  it("answers getAll by _id with the rows whose key matches, and nothing for a key that cannot", async () => {
    const known = SupplierId(3).toHexString();
    const rows = await Table("suppliers").getAll([known, "not-a-uuid"], "_id");
    expect(rows.map((row) => row._id)).to.deep.equal([known]);
  });
});

describe("the values an external collection answers", () => {
  before(async () => {
    erp = await RegisterErp();
  });

  it("answers _id and the declared fields only, not the undeclared phone", async () => {
    const row = await Table("customers").get(CustomerId(7).toHexString());
    expect(Object.keys(row)).to.have.members([
      "_id",
      "name",
      "email",
      "segment",
      "createdAt",
      "address",
      "tags",
      "points",
      "ref",
      "score",
    ]);
    expect(row).to.include({ name: "Customer 7", email: "user7@example.com" });
    expect(row.tags).to.deep.equal(["vip", "b2b"]);
  });

  it("reads a Long and a Decimal128 declared number as doubles", async () => {
    const row = await Table("products").get("SKU-00007");
    expect(row).to.include({
      stock: 9007199254740992,
      cost: 7.1,
      price: 70.99,
    });
  });

  it("reads a Long and a Decimal128 declared string as their exact text", async () => {
    const exact = await RegisterErp(
      {
        products: {
          fields: { _id: "string", stock: "string", cost: "string" },
          indexes: {},
        },
      },
      "erp_exact",
    );
    const row = await exact.instance().table("products").get("SKU-00007");
    expect(row).to.deep.equal({
      _id: "SKU-00007",
      stock: "9007199254740993",
      cost: "7.10",
    });
  });

  it("reads a value of a type its declaration does not read as null, without parsing it", async () => {
    const row = await Table("products").get("SKU-legacy");
    expect(row).to.include({ label: "Legacy", price: null, active: null });
  });

  it("reads an undeclared key, such as the _internal data-api plucks, as absent, and keeps _id as pluck does", async () => {
    const row = await Table("customers")
      .get(CustomerId(1).toHexString())
      .pluck("_internal", "name");
    expect(row).to.deep.equal({
      _id: CustomerId(1).toHexString(),
      name: "Customer 1",
    });
  });
});

describe("listing an external collection as a TableView does", () => {
  before(async () => {
    erp = await RegisterErp();
  });

  it("lists with a filter, a (?i) search, a sort and a page", async () => {
    const rows = await Table("orders")
      .orderBy("_id", "desc")
      .filter((row) => row.key("status").eq("paid"))
      .filter((row) => row.key("status").match("(?i)PAI"))
      .slice(0, 5)
      .pluck("_internal", "_id", "status", "total");
    const expected = await Native("orders", [
      { $match: { status: "paid" } },
      { $sort: { _id: -1 } },
      { $limit: 5 },
      {
        $project: {
          _id: { $toString: "$_id" },
          status: 1,
          total: { $toDouble: "$total" },
        },
      },
    ]);
    expect(rows).to.deep.equal(expected);
  });

  it("counts the same filtered rows, as a TableView's total does", async () => {
    const total = await Table("orders")
      .filter((row) => row.key("status").eq("paid"))
      .filter((row) => row.key("status").match("(?i)PAI"))
      .count();
    expect(total).to.equal(await NativeCount("orders", { status: "paid" }));
  });

  it("answers a date filter 'is' on orderedAt as that UTC day", async () => {
    const day = new Date("2025-03-02T00:00:00.000Z");
    const next = new Date(day.getTime() + DAY_MS);
    const total = await Table("orders")
      .filter((row) =>
        row.key("orderedAt").ge(day).and(row.key("orderedAt").lt(next)),
      )
      .count();
    expect(total).to.equal(
      await NativeCount("orders", { orderedAt: { $gte: day, $lt: next } }),
    );
    expect(total).to.be.greaterThan(0);
  });

  it("resolves a relation from orders to customers, the string-stored customerIds included", async () => {
    const rows = await Table("orders")
      .orderBy("_id")
      .slice(0, 3)
      .lookup(Table("customers"), "customerId", "_id")
      .pluck("_id", "customerId");
    expect(rows.map((row) => row._id)).to.deep.equal(
      [0, 1, 2].map((index) => OrderId(index).toHexString()),
    );
    rows.forEach((row, index) =>
      expect(row.customerId).to.include({
        _id: CustomerId(index * 7).toHexString(),
        name: `Customer ${index * 7}`,
      }),
    );
  });

  it("buckets a builder series by day in Europe/Paris", async () => {
    const series = await Table("orders")
      .filter((row) =>
        row.key("orderedAt").ge(new Date("2025-03-01T00:00:00Z")),
      )
      .map((row) => ({
        bucket: row
          .key("orderedAt")
          .year(PARIS)
          .mul(100)
          .add(row.key("orderedAt").month(PARIS))
          .mul(100)
          .add(row.key("orderedAt").day(PARIS)),
      }))
      .group("bucket", (rows, group) => ({ x: group, y: rows.count() }))
      .orderBy("x", "asc")
      .slice(0, 3);
    const expected = await Native("orders", [
      { $match: { orderedAt: { $gte: new Date("2025-03-01T00:00:00Z") } } },
      {
        $group: {
          _id: {
            $dateToString: {
              date: "$orderedAt",
              format: "%Y%m%d",
              timezone: PARIS,
            },
          },
          y: { $count: {} },
        },
      },
      { $sort: { _id: 1 } },
      { $limit: 3 },
    ]);
    expect(series).to.deep.equal(
      expected.map((bucket) => ({ x: Number(bucket._id), y: bucket.y })),
    );
  });
});

describe("comparisons whose operand converts as the stored values do", () => {
  before(async () => {
    erp = await RegisterErp();
  });

  it("finds a stored Decimal128 by the number its read answers", async () => {
    const total = await Table("orders")
      .filter((row) => row.key("total").eq(11.1))
      .count();
    expect(total).to.be.above(0);
    expect(total).to.equal(
      await NativeCount("orders", { total: Decimal128.fromString("11.10") }),
    );
  });

  it("finds a Long beyond 2^53 by the double its read answers", async () => {
    const rows = await Table("customers")
      .filter((row) => row.key("points").eq(9007199254740992))
      .count();
    expect(rows).to.be.above(0);
  });

  it("matches no array element when a number field's array reads as null", async () => {
    const rows = await Table("products")
      .filter((row) => row.key("price").eq(5))
      .count();
    expect(rows).to.equal(0);
  });

  it("answers a range of hex bounds on an ObjectId field as its read text compares", async () => {
    const low = CustomerId(0).toHexString();
    const high = CustomerId(70).toHexString();
    const rows = await Table("orders").between("customerId", low, high).count();
    const read = await Native("orders", [
      { $project: { hex: { $toString: "$customerId" } } },
      { $match: { hex: { $gte: low, $lt: high } } },
    ]);
    expect(rows).to.be.above(0);
    expect(rows).to.equal(read.length);
  });
});

describe("keys, joins and lookups an external read must not widen", () => {
  before(async () => {
    erp = await RegisterErp();
  });

  it("finds nothing for object-key text shaped like an operator", async () => {
    expect(await Table("order_lines").get('{"$ne":null}')).to.equal(undefined);
    expect(
      await Table("order_lines").getAll(['{"$ne":null}', '{"$gt":{}}']).count(),
    ).to.equal(0);
  });

  it("joins on normalized rows when the mapper reads another collection", async () => {
    const rows = await Table("invoices")
      .orderBy("_id")
      .slice(0, 2)
      .join(
        Table("orders"),
        (invoice, order) => invoice.key("orderRef").eq(order.key("_id")),
        (invoice, order) => ({
          invoice: invoice.key("_id"),
          order: order.key("_id"),
          customer: Table("customers").get(
            order.key("customerId").cast<string>(),
          ),
        }),
      );
    expect(rows.map((row) => row.order)).to.deep.equal(
      [0, 1].map((index) => OrderId(index).toHexString()),
    );
    rows.forEach((row) => expect(row.customer).to.have.property("name"));
  });

  it("refuses a get on an external collection inside a map over an array", async () => {
    let refusal: unknown;
    try {
      await Table("customers")
        .orderBy("_id")
        .slice(0, 1)
        .map((customer) => ({
          products: customer
            .key("tags")
            .map((tag) => Table("products").get(tag.cast<string>())),
        }));
    } catch (error) {
      refusal = error;
    }
    expect(String(refusal)).to.contain("inside a map over an array");
  });

  it("looks up nothing for a value that is no ObjectId, rather than the rows missing the field", async () => {
    const [row] = await Table("customers")
      .orderBy("_id")
      .slice(0, 1)
      .lookup(Table("orders"), "name", "customerId")
      .pluck("_id", "name");
    expect(row.name ?? null).to.equal(null);
  });
});

describe("collections whose documents answer no _id", () => {
  before(async () => {
    erp = await RegisterErp();
  });

  for (const collection of ["mixed_ids", "big_customers", "metrics"]) {
    it(`counts ${collection}, and its rows answer no _id`, async () => {
      expect(await Table(collection).count()).to.equal(
        await NativeCount(collection),
      );
      const [row] = await Table(collection).slice(0, 1);
      expect(row).to.not.have.property("_id");
    });
  }
});

describe("instances over an external collection", () => {
  before(async () => {
    erp = await RegisterErp();
  });

  it("reads every document through CROSS_INSTANCE, since an external collection has no instances", async () => {
    expect(await erp.instance(CROSS_INSTANCE).table("orders").count()).to.equal(
      await NativeCount("orders"),
    );
  });

  it("refuses a named instance rather than answer documents nobody scoped to it", async () => {
    let refusal: Error | undefined;
    try {
      await erp.instance("tenant-1").table("orders").count();
    } catch (error) {
      refusal = error instanceof Error ? error : undefined;
    }
    expect(refusal?.message).to.contain("orders").and.to.contain("tenant-1");
  });
});
