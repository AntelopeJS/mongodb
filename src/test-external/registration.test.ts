import sinon from "sinon";
import { expect } from "chai";
import { randomUUID } from "node:crypto";
import type { Document, MongoClient } from "mongodb";
import { Logging } from "@antelopejs/interface-core/logging";
import {
  Schema,
  type SchemaDefinition,
} from "@antelopejs/interface-database/schema";

import { Schemas } from "../implementations/database/schema";
import { AdminClient, CLIENT_DATABASE, WRITER_USER } from "./fixture";

const STRUCTURE_COMMANDS = [
  "create",
  "collMod",
  "createIndexes",
  "dropIndexes",
  "drop",
];
const WRITE_OPERATIONS = ["insert", "update", "remove"];

const FIXTURE_COLLECTIONS = [
  "customers",
  "products",
  "orders",
  "invoices",
  "order_lines",
  "suppliers",
  "mixed_ids",
  "legacy_pairs",
  "big_customers",
  "metrics",
  "audit",
];

const registered: string[] = [];

function Register(definition: SchemaDefinition): Schema {
  const schemaId = `erp_${randomUUID().replace(/-/g, "")}`;
  registered.push(schemaId);
  return new Schema(schemaId, definition);
}

function Count(schema: Schema, table: string): Promise<number> {
  return schema.instance().table(table).count().run();
}

async function Rejection(query: Promise<number>): Promise<string> {
  try {
    await query;
  } catch (error) {
    return error instanceof Error ? error.message : String(error);
  }
  throw new Error(
    "the query answered, where its registration should have failed",
  );
}

function Logged(stub: sinon.SinonStub): string {
  return stub.args.map((call) => call.join(" ")).join("\n");
}

async function Structure(admin: MongoClient): Promise<Document[]> {
  const db = admin.db(CLIENT_DATABASE);
  const collections = await db
    .listCollections({}, { nameOnly: false })
    .toArray();
  return Promise.all(
    collections
      .filter((info) => !info.name.startsWith("system."))
      .map(async (info) => ({
        name: info.name,
        options: info.options,
        indexes:
          info.type === "view" ? [] : await db.collection(info.name).indexes(),
      })),
  );
}

describe("registration of an external schema", () => {
  let admin: MongoClient;
  let warn: sinon.SinonStub;
  let info: sinon.SinonStub;

  before(async () => {
    admin = await AdminClient();
  });

  after(async () => {
    registered.forEach((schemaId) => Schemas.unregister(schemaId));
    await admin.close();
  });

  beforeEach(() => {
    warn = sinon.stub(Logging, "Warn");
    info = sinon.stub(Logging, "Info");
  });

  afterEach(() => {
    sinon.restore();
  });

  it("fails on a collection the database does not have, rather than creating it", async () => {
    const schema = Register({ invoices_2019: { fields: {}, indexes: {} } });
    expect(await Rejection(Count(schema, "invoices_2019"))).to.include(
      `reads collection invoices_2019, which database ${CLIENT_DATABASE} does not have`,
    );
    const names = await admin
      .db(CLIENT_DATABASE)
      .listCollections({ name: "invoices_2019" })
      .toArray();
    expect(names).to.deep.equal([]);
  });

  for (const collection of [
    "mixed_ids",
    "legacy_pairs",
    "big_customers",
    "metrics",
  ]) {
    it(`fails when _id is declared on ${collection}, whose _id is unusable`, async () => {
      const schema = Register({
        [collection]: { fields: { _id: "string" }, indexes: {} },
      });
      expect(await Rejection(Count(schema, collection))).to.include(
        `collection ${collection} has no usable _id`,
      );
    });
  }

  it("registers mixed_ids without _id, and says why it answers none", async () => {
    const schema = Register({
      mixed_ids: { fields: { v: "string" }, indexes: {} },
    });
    await Count(schema, "mixed_ids");
    expect(Logged(info)).to.include(
      "its _id values range from int to date, more than one type class",
    );
  });

  it("warns about a declared index the collection does not have, and creates none", async () => {
    const before = await admin
      .db(CLIENT_DATABASE)
      .collection("orders")
      .indexes();
    const schema = Register({
      orders: {
        fields: { status: "string", note: "string" },
        indexes: { note: { fields: ["note"] }, status: { fields: ["status"] } },
      },
    });
    await Count(schema, "orders");
    const after = await admin
      .db(CLIENT_DATABASE)
      .collection("orders")
      .indexes();
    expect(after).to.deep.equal(before);
    expect(Logged(warn)).to.include("Index 'note'");
    expect(Logged(warn)).to.not.include("Index 'status'");
  });

  it("warns with the share of sampled values a declaration reads as null", async () => {
    const schema = Register({
      products: { fields: { price: "number", label: "string" }, indexes: {} },
    });
    await Count(schema, "products");
    expect(Logged(warn))
      .to.include("declared number, but 2 of 301 sampled values (0.7%) are")
      .and.to.include("string");
    expect(Logged(warn)).to.not.include("products.label");
  });

  it("refuses a field type it does not know, naming it", async () => {
    const schema = Register({
      customers: { fields: { name: "text" }, indexes: {} },
    });
    expect(await Rejection(Count(schema, "customers"))).to.include(
      "Unsupported field type 'text'",
    );
  });

  it("registers every fixture collection with reads only, and changes no option or index", async () => {
    const before = await Structure(admin);
    const definition: SchemaDefinition = {};
    for (const collection of FIXTURE_COLLECTIONS) {
      definition[collection] = { fields: {}, indexes: {} };
    }
    const schema = Register(definition);
    await Count(schema, "customers");
    const entries = await admin
      .db(CLIENT_DATABASE)
      .collection("system.profile")
      .find({ user: `${WRITER_USER}@${CLIENT_DATABASE}` })
      .toArray();
    const offending = entries.filter(
      (entry) =>
        WRITE_OPERATIONS.some((operation) => operation === entry.op) ||
        STRUCTURE_COMMANDS.some((name) => entry.command?.[name] !== undefined),
    );
    expect(entries.length).to.be.greaterThan(0);
    expect(offending).to.deep.equal([]);
    expect(await Structure(admin)).to.deep.equal(before);
  });
});
