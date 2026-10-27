import type { Document } from "mongodb";
import {
  Schema,
  type SchemaDefinition,
} from "@antelopejs/interface-database/schema";

import { SchemaInitialized } from "../schema-initialization";
import { AdminClient, CLIENT_DATABASE } from "./fixture";

const ERP = "erp";

/**
 * The fixture declared as a generator writes it from the catalog: native names, `_id` on each
 * collection whose keys share one type class, and none on the others.
 */
const ERP_DEFINITION: SchemaDefinition = {
  customers: {
    fields: {
      _id: "string",
      name: "string",
      email: "string",
      segment: "string",
      createdAt: "date",
      address: "object",
      tags: "string[]",
      points: "number",
      ref: "string",
      score: "number",
    },
    indexes: {
      email: { fields: ["email"], unique: true },
      segment: { fields: ["segment", "createdAt"] },
    },
  },
  products: {
    fields: {
      _id: "string",
      label: "string",
      price: "number",
      cost: "number",
      stock: "number",
      active: "boolean",
    },
    indexes: {},
  },
  orders: {
    fields: {
      _id: "string",
      customerId: "string",
      status: "string",
      orderedAt: "date",
      total: "number",
      lines: "object[]",
      note: "string",
    },
    indexes: {
      customerId: { fields: ["customerId"] },
      status: { fields: ["status", "orderedAt"] },
    },
  },
  invoices: {
    fields: {
      _id: "string",
      orderRef: "string",
      amount: "number",
      issuedOn: "date",
    },
    indexes: {},
  },
  suppliers: {
    fields: {
      _id: "string",
      name: "string",
      country: "string",
      rating: "number",
    },
    indexes: {},
  },
  order_lines: {
    fields: { _id: "string", sku: "string", qty: "number" },
    indexes: {},
  },
  mixed_ids: { fields: { v: "string" }, indexes: {} },
  big_customers: { fields: { name: "string", segment: "string" }, indexes: {} },
  metrics: {
    fields: { at: "date", sensor: "string", value: "number" },
    indexes: {},
  },
};

/** A value a row of the fixture answers. */
type ErpValue =
  | string
  | number
  | boolean
  | Date
  | null
  | string[]
  | ErpObject
  | ErpObject[];

/** An embedded document of the fixture: an address, or an order line. */
interface ErpObject {
  [field: string]: string | number;
}

export interface ErpRow {
  [field: string]: ErpValue;
}

export interface ErpTables {
  [table: string]: ErpRow;
}

/** Registers a declaration and waits for it, as a generated module's startup does. */
export async function RegisterErp(
  definition: SchemaDefinition = ERP_DEFINITION,
  schemaId: string = ERP,
): Promise<Schema<ErpTables>> {
  const schema = new Schema<ErpTables>(schemaId, definition);
  await SchemaInitialized(schemaId);
  return schema;
}

/** The answer a native aggregation over the same documents gives, as the client's owner runs it. */
export async function Native(
  collection: string,
  pipeline: Document[],
): Promise<Document[]> {
  const admin = await AdminClient();
  try {
    return await admin
      .db(CLIENT_DATABASE)
      .collection(collection)
      .aggregate(pipeline)
      .toArray();
  } finally {
    await admin.close();
  }
}

export async function NativeCount(
  collection: string,
  filter: Document = {},
): Promise<number> {
  const [counted] = await Native(collection, [
    { $match: filter },
    { $count: "n" },
  ]);
  return counted?.n ?? 0;
}
