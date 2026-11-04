import { expect } from "chai";
import { BSON, type Document, type MongoClient, ObjectId } from "mongodb";
import { IsDuplicateKeyError } from "@antelopejs/interface-database";
import type { Schema } from "@antelopejs/interface-database/schema";

import { type ErpTables, RegisterErp } from "./erp";
import {
  AdminClient,
  CLIENT_DATABASE,
  CustomerId,
  OrderId,
  WRITER_USER,
} from "./fixture";

const CANONICAL = { relaxed: false };
const BULK_SIZE = 1000;
const BULK_PREFIX = "Bulk customer";

let erp: Schema<ErpTables>;
let admin: MongoClient;

function Table(name: string) {
  return erp.instance().table(name);
}

function Stored(collection: string, id: ObjectId): Promise<Document | null> {
  return admin.db(CLIENT_DATABASE).collection(collection).findOne({ _id: id });
}

function Canonical(document: Document | null): string {
  return BSON.EJSON.stringify(document, CANONICAL);
}

function CanonicalField(
  document: Document | null | undefined,
  field: string,
): string {
  return BSON.EJSON.stringify(document?.[field], CANONICAL);
}

async function Refusal<T>(work: () => PromiseLike<T>): Promise<Error> {
  try {
    await work();
  } catch (error) {
    if (error instanceof Error) {
      return error;
    }
  }
  throw new Error("the write was expected to be refused");
}

async function WritesTo(collection: string): Promise<number> {
  return admin
    .db(CLIENT_DATABASE)
    .collection("system.profile")
    .countDocuments({
      user: `${WRITER_USER}@${CLIENT_DATABASE}`,
      ns: `${CLIENT_DATABASE}.${collection}`,
      op: { $in: ["insert", "update", "remove"] },
    });
}

describe("writing back what an external collection answered", () => {
  const customer = CustomerId(91);
  const order = OrderId(30);

  before(async () => {
    erp = await RegisterErp();
    admin = await AdminClient();
  });

  after(async () => {
    await admin.close();
  });

  it("changes nothing when a whole read row comes back unedited, Long, Decimal128 and ObjectId included", async () => {
    const before = Canonical(await Stored("customers", customer));
    const row = await Table("customers").get(customer.toHexString());
    expect(
      await Table("customers").get(customer.toHexString()).update(row),
    ).to.equal(0);
    expect(Canonical(await Stored("customers", customer))).to.equal(before);
    expect(row.ref).to.equal(null);
    const orderBefore = Canonical(await Stored("orders", order));
    const orderRow = await Table("orders").get(order.toHexString());
    expect(
      await Table("orders").get(order.toHexString()).update(orderRow),
    ).to.equal(0);
    expect(Canonical(await Stored("orders", order))).to.equal(orderBefore);
  });

  it("changes only the field edited, keeping the others' native types and the undeclared phone", async () => {
    const before = await Stored("orders", order);
    const row = await Table("orders").get(order.toHexString());
    const edited = { ...row, status: "cancelled" };
    expect(
      await Table("orders").get(order.toHexString()).update(edited),
    ).to.equal(1);
    const after = await Stored("orders", order);
    const originalStatus: unknown = before?.status;
    expect(Canonical({ ...after, status: originalStatus })).to.equal(
      Canonical(before),
    );
    expect(after?.status).to.equal("cancelled");
    const customerBefore = await Stored("customers", customer);
    await Table("customers")
      .get(customer.toHexString())
      .update({ name: "Renamed 91" });
    const customerAfter = await Stored("customers", customer);
    expect(customerAfter?.phone).to.equal(customerBefore?.phone);
    expect(CanonicalField(customerAfter, "points")).to.equal(
      CanonicalField(customerBefore, "points"),
    );
  });

  it("ignores an object field on update, rather than writing back what a read altered", async () => {
    const before = CanonicalField(
      await Stored("customers", customer),
      "address",
    );
    await Table("customers")
      .get(customer.toHexString())
      .update({ address: { city: "Nowhere" } });
    expect(
      CanonicalField(await Stored("customers", customer), "address"),
    ).to.equal(before);
  });

  it("writes nothing when the payload's _id names another document", async () => {
    const other = CustomerId(92).toHexString();
    expect(
      await Table("customers")
        .get(customer.toHexString())
        .update({ _id: other, name: "Moved" }),
    ).to.equal(0);
    expect((await Stored("customers", customer))?.name).to.not.equal("Moved");
  });
});

describe("inserting into an external collection", () => {
  before(async () => {
    erp = await RegisterErp();
    admin = await AdminClient();
  });

  after(async () => {
    await admin.close();
  });

  it("generates ObjectIds and answers their hex in input order", async () => {
    const values = Array.from({ length: BULK_SIZE }, (_, index) => ({
      name: `${BULK_PREFIX} ${index}`,
      email: `bulk${index}@example.com`,
    }));
    const ids = await Table("customers").insert(values);
    expect(ids).to.have.length(BULK_SIZE);
    const stored = await admin
      .db(CLIENT_DATABASE)
      .collection("customers")
      .find({ _id: { $in: ids.map((id) => new ObjectId(id)) } })
      .toArray();
    const names = new Map(
      stored.map((document): [string, unknown] => [
        document._id.toHexString(),
        document.name,
      ]),
    );
    expect(ids.map((id) => names.get(id))).to.deep.equal(
      values.map((value) => value.name),
    );
  });

  it("deletes what a search on a normalized value selects", async () => {
    const deleted = await Table("customers")
      .filter((row) => row.key("name").match(`^${BULK_PREFIX} `))
      .delete();
    expect(deleted).to.equal(BULK_SIZE);
  });

  it("stores a customerId given as hex as an ObjectId, the field's dominant type", async () => {
    const [id] = await Table("orders").insert({
      customerId: CustomerId(3).toHexString(),
      status: "draft",
      orderedAt: new Date("2026-01-01T00:00:00Z"),
      total: 12.5,
    });
    const stored = await Stored("orders", new ObjectId(id));
    expect(stored?.customerId).to.be.instanceOf(ObjectId);
    expect(stored?.total?._bsontype).to.equal("Decimal128");
    await admin
      .db(CLIENT_DATABASE)
      .collection("orders")
      .deleteOne({ _id: new ObjectId(id) });
  });

  it("upserts on conflict through the write-back guard, keeping undeclared fields", async () => {
    const customer = CustomerId(14);
    const before = await Stored("customers", customer);
    const ids = await Table("customers").insert(
      { _id: customer.toHexString(), name: "Upserted 14" },
      { conflict: "update" },
    );
    expect(ids).to.deep.equal([customer.toHexString()]);
    const after = await Stored("customers", customer);
    expect(after).to.include({ name: "Upserted 14" });
    expect(after?.phone).to.equal(before?.phone);
    expect(CanonicalField(after, "points")).to.equal(
      CanonicalField(before, "points"),
    );
  });

  it("writes a whole number as a double into a field of ints and doubles, which ints would not hold", async () => {
    const [id] = await Table("customers").insert({ name: "Scored", score: 4 });
    const stored = await admin
      .db(CLIENT_DATABASE)
      .collection("customers")
      .countDocuments({ _id: new ObjectId(id), score: { $type: "double" } });
    expect(stored).to.equal(1);
  });

  it("refuses an insert into a string-keyed collection without _id, naming it", async () => {
    const error = await Refusal(() =>
      Table("products").insert({ label: "No key" }),
    );
    expect(error.message).to.contain("'_id'");
  });
});

describe("what an external write refuses", () => {
  const customer = CustomerId(5).toHexString();

  before(async () => {
    erp = await RegisterErp();
    admin = await AdminClient();
  });

  after(async () => {
    await admin.close();
  });

  for (const key of ["phone", "_internal"]) {
    it(`refuses the undeclared key ${key} by name`, async () => {
      const error = await Refusal(() =>
        Table("customers")
          .get(customer)
          .update({ [key]: "x" }),
      );
      expect(error.message).to.contain(`'${key}'`);
    });
  }

  it("refuses an object value on insert", async () => {
    const error = await Refusal(() =>
      Table("customers").insert({
        name: "Addressed",
        address: { city: "Paris" },
      }),
    );
    expect(error.message).to.contain("'address'").and.to.contain("read-only");
  });

  it("refuses a malformed ObjectId and a fraction for a Long before anything is sent", async () => {
    const writesBefore = await WritesTo("orders");
    const error = await Refusal(() =>
      Table("orders")
        .get(OrderId(2).toHexString())
        .update({ customerId: "not-hex" }),
    );
    expect(error.message).to.contain("'customerId'").and.to.contain("ObjectId");
    expect(await WritesTo("orders")).to.equal(writesBefore);
  });

  it("writes a fraction into a Long field as a double rather than refuse it", async () => {
    await Table("customers").get(customer).update({ points: 1.5 });
    const stored = await Stored("customers", new ObjectId(customer));
    expect(CanonicalField(stored, "points")).to.equal(
      '{"$numberDouble":"1.5"}',
    );
  });

  it("names the property a validator refuses", async () => {
    const error = await Refusal(() =>
      Table("orders").get(OrderId(2).toHexString()).update({ status: "lost" }),
    );
    expect(error).to.have.property("kind", "invalid");
    expect(error).to.have.property("fields").that.deep.equals(["status"]);
  });

  it("raises DuplicateKeyError on a duplicate email, quoting no value", async () => {
    const error = await Refusal(() =>
      Table("customers").get(customer).update({ email: "user6@example.com" }),
    );
    expect(IsDuplicateKeyError(error)).to.equal(true);
    expect(error.message).to.not.contain("example.com");
  });

  it("names the index and its fields of a duplicate key an insert hits", async () => {
    const error = await Refusal(() =>
      Table("customers").insert({ name: "Twin", email: "user6@example.com" }),
    );
    expect(error).to.include({ index: "email_1" });
    expect(error).to.have.property("fields").that.deep.equals(["email"]);
  });

  it("names the property a validator refuses on insert", async () => {
    const error = await Refusal(() =>
      Table("orders").insert({ status: "lost", orderedAt: new Date() }),
    );
    expect(error).to.have.property("fields").that.deep.equals(["status"]);
  });

  it("answers forbidden for a collection the account may not write", async () => {
    const error = await Refusal(() =>
      Table("products").get("SKU-00001").update({ label: "Changed" }),
    );
    expect(error).to.have.property("kind", "forbidden");
    expect(error.message).to.not.contain("Changed");
  });
});

describe("replacing a document of an external collection", () => {
  const customer = CustomerId(11);

  before(async () => {
    erp = await RegisterErp();
    admin = await AdminClient();
  });

  after(async () => {
    await admin.close();
  });

  it("removes the declared fields left out and keeps the undeclared and the read-only ones", async () => {
    const before = await Stored("customers", customer);
    const row = await Table("customers").get(customer.toHexString());
    const replaced = await Table("customers")
      .get(customer.toHexString())
      .replace({ name: "Replaced 11", email: row?.email ?? null });
    expect(replaced).to.equal(1);
    const after = await Stored("customers", customer);
    expect(after).to.not.have.property("segment");
    expect(after).to.not.have.property("points");
    expect(after).to.include({ name: "Replaced 11" });
    expect(after?.email).to.equal(before?.email);
    expect(CanonicalField(after, "address")).to.equal(
      CanonicalField(before, "address"),
    );
  });
});
