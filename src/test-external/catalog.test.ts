import { expect } from "chai";
import {
  CheckReference,
  DescribeRelations,
  Info,
  ListRelations,
  Profile,
  SampleRows,
} from "@antelopejs/interface-database-catalog";

import { GetOptions, type Options, SetOptions } from "../config";
import { SetMeasureTimeoutForTests } from "../implementations/catalog/measures";
import { AdminClient, CLIENT_DATABASE, WRITER_USER } from "./fixture";

const KEYED = [
  "customers",
  "invoices",
  "order_lines",
  "orders",
  "products",
  "suppliers",
];
const KEYLESS = ["big_customers", "legacy_pairs", "metrics", "mixed_ids"];
const STRUCTURE_COMMANDS = [
  "create",
  "collMod",
  "createIndexes",
  "dropIndexes",
  "drop",
];
const WRITE_OPERATIONS = ["insert", "update", "remove"];

function Relation(name: string): string {
  return `${CLIENT_DATABASE}.${name}`;
}

async function Refusal<T>(work: () => PromiseLike<T>): Promise<string> {
  try {
    await work();
  } catch (error) {
    return error instanceof Error ? error.message : String(error);
  }
  throw new Error("the catalog call was expected to be refused");
}

const started = new Date();

describe("the relations an external MongoDB catalog lists and describes", () => {
  it("answers the engine, the database and the connected account", async () => {
    expect(await Info()).to.include({
      engine: "mongodb",
      database: CLIENT_DATABASE,
      role: WRITER_USER,
    });
  });

  it("lists each collection with _id as its key only when its keys share one type class", async () => {
    const relations = await ListRelations();
    const keys = Object.fromEntries(
      relations.map((relation) => [relation.name, relation.key]),
    );
    for (const name of KEYED) {
      expect(keys[name], name).to.deep.equal({
        columns: ["_id"],
        source: "primary",
      });
    }
    for (const name of KEYLESS) {
      expect(keys[name], name).to.equal(null);
    }
    expect(
      relations.some((relation) => relation.name.startsWith("system.")),
    ).to.equal(false);
    expect(
      relations.find((relation) => relation.name === "big_customers")?.kind,
    ).to.equal("view");
  });

  it("answers what the connected account may write, per collection", async () => {
    const relations = await ListRelations([CLIENT_DATABASE]);
    const writes = Object.fromEntries(
      relations.map((relation) => [relation.name, relation.writes]),
    );
    expect(writes.customers).to.deep.equal({
      insert: true,
      update: true,
      delete: true,
    });
    expect(writes.products).to.deep.equal({
      insert: false,
      update: false,
      delete: false,
    });
    expect(await ListRelations(["another"])).to.deep.equal([]);
  });

  it("describes a mixed field by its shares, and proposes the declaration of its dominant type", async () => {
    const [orders] = await DescribeRelations([Relation("orders")]);
    const customerId = orders.columns.find(
      (column) => column.name === "customerId",
    );
    expect(customerId?.native).to.equal("objectId 98% | string 2%");
    expect(customerId?.accepts[0]).to.deep.include({ declared: "string" });
    expect(orders.columns[0].name).to.equal("_id");
    expect(orders.columns[0].hasDefault).to.equal(true);
    const status = orders.columns.find((column) => column.name === "status");
    expect(status).to.include({ comment: "Order state" });
    expect(status?.enumLabels).to.deep.equal([
      "draft",
      "paid",
      "shipped",
      "cancelled",
    ]);
  });

  it("marks a declaration lossy, and arrays and objects read-only", async () => {
    const [products, customers] = await DescribeRelations([
      Relation("products"),
      Relation("customers"),
    ]);
    const price = products.columns.find((column) => column.name === "price");
    expect(price?.accepts[0].declared).to.equal("number");
    expect(price?.accepts[0].lossy).to.contain(
      "1 of 301 values are string and read as null",
    );
    expect(products.columns[0].hasDefault).to.equal(false);
    const tags = customers.columns.find((column) => column.name === "tags");
    expect(tags?.accepts[0].declared).to.equal("string[]");
    expect(tags?.readOnly).to.be.a("string");
  });
});

describe("the profiles and samples of an external MongoDB catalog", () => {
  it("profiles categories of a small set of values, and never names an email", async () => {
    const orders = await Profile({
      relation: Relation("orders"),
      columns: ["status"],
    });
    const [status] = orders.columns;
    expect(status.traits).to.include("enum-like");
    expect(status.categories?.status).to.equal("ok");
    const customers = await Profile({ relation: Relation("customers") });
    expect(JSON.stringify(customers)).to.not.contain("example.com");
    const email = customers.columns.find((column) => column.name === "email");
    expect(email?.categories).to.equal(undefined);
  });

  it("answers timeout for a measure that runs out of time, and answers again once it has time", async () => {
    SetMeasureTimeoutForTests(1);
    try {
      const slow = await Profile({
        relation: Relation("events"),
        columns: ["kind"],
      });
      expect(slow.columns[0].distinct).to.deep.equal({ status: "timeout" });
    } finally {
      SetMeasureTimeoutForTests();
    }
    const again = await Profile({
      relation: Relation("events"),
      columns: ["kind"],
    });
    expect(again.columns[0].distinct).to.deep.equal({ status: "ok", value: 4 });
  });

  it("finds invoices.orderRef in orders._id through the ObjectId its hex addresses", async () => {
    const match = await CheckReference({
      from: { relation: Relation("invoices"), column: "orderRef" },
      to: { relation: Relation("orders"), column: "_id" },
    });
    expect(match).to.deep.equal({
      status: "ok",
      value: {
        containment: 1,
        checkedRows: 300,
        sampled: false,
        targetIsKey: true,
      },
    });
  });

  it("samples at most the rows asked, as text cut at 200 characters, documents as JSON", async () => {
    const sample = await SampleRows({
      relation: Relation("orders"),
      columns: ["note", "lines"],
      limit: 20,
    });
    expect(sample.rows).to.have.length(20);
    const notes = sample.rows.map(([note]) => note);
    expect(notes).to.deep.include({ text: "x".repeat(200), truncated: true });
    expect(sample.rows[0][1]).to.have.property("text").that.contains('"sku"');
    expect(
      await Refusal(() =>
        SampleRows({ relation: Relation("orders"), limit: 21 }),
      ),
    ).to.contain("from 1 to 20");
  });
});

describe("what an external MongoDB catalog refuses", () => {
  let configured: Options;

  beforeEach(() => {
    configured = { ...GetOptions() };
  });

  afterEach(() => {
    SetOptions(configured);
  });

  it("refuses outside development unless enabled", async () => {
    SetOptions({ ...configured, catalog: { enabled: false } });
    expect(await Refusal(Info)).to.contain("development tool");
  });

  it("refuses on an owned instance", async () => {
    SetOptions({ ...configured, mode: "owned" });
    expect(await Refusal(Info)).to.contain("owns its collections");
  });
});

describe("what an external MongoDB catalog writes", () => {
  it("sends the client's database no write and no structure command", async () => {
    const admin = await AdminClient();
    try {
      const entries = await admin
        .db(CLIENT_DATABASE)
        .collection("system.profile")
        .find({
          user: `${WRITER_USER}@${CLIENT_DATABASE}`,
          ts: { $gte: started },
        })
        .toArray();
      const offending = entries.filter(
        (entry) =>
          WRITE_OPERATIONS.some((operation) => operation === entry.op) ||
          STRUCTURE_COMMANDS.some(
            (name) => entry.command?.[name] !== undefined,
          ),
      );
      expect(entries.length).to.be.greaterThan(0);
      expect(offending).to.deep.equal([]);
    } finally {
      await admin.close();
    }
  });
});
