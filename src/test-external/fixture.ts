import {
  Binary,
  Decimal128,
  type Db,
  type Document,
  Long,
  MongoClient,
  ObjectId,
  UUID,
} from "mongodb";

/**
 * The client database the external suite reads: created by its owner before the module constructs,
 * and never by the adapter. Every value is derived from the document number, so a test can state
 * its expected answer as a native query over the same documents.
 */
export const CLIENT_DATABASE = "antelopejs_external";

const READER_USER = "erp_reader";

export const WRITER_USER = "erp_writer";

export const ACCOUNT_PASSWORD = "erp-password";

/** The environment variable the test project hands the suites the client owner's URL through. */
export const ADMIN_URL_VARIABLE = "MONGO_TEST_ADMIN_URL";

const CUSTOMER_COUNT = 2000;

const PRODUCT_COUNT = 300;

const ORDER_COUNT = 3000;

const INVOICE_COUNT = 300;

const ORDER_LINE_COUNT = 200;

/** Enough events that a measure limited to 1 ms cannot finish over them. */
const EVENT_COUNT = 100_000;
const EVENT_BATCH = 10_000;
const EVENT_KINDS = ["view", "click", "buy", "share"];

const WRITER_ROLE = "erp_customers_orders_writer";
const WRITABLE_COLLECTIONS = ["customers", "orders"];
const WRITE_ACTIONS = ["insert", "update", "remove"];
const SEGMENTS = ["retail", "wholesale", "partner", "internal"];
const STATUSES = ["draft", "paid", "shipped", "cancelled"];
const CITIES = ["Paris", "Lyon", "Lille"];
const COUNTRIES = ["FR", "DE", "IT"];
const HOUR_MS = 3_600_000;
const DAY_MS = 86_400_000;
const CUSTOMERS_START = Date.UTC(2024, 0, 1);
const ORDERS_START = Date.UTC(2025, 0, 1);
const STRING_CUSTOMER_EVERY = 47;
const LONG_BEYOND_DOUBLE = "9007199254740993";
const CAPPED_SIZE = 1 << 20;
const ARRAY_PRICED = 11;
const UNASSIGNED_EVERY = 997;
const UNASSIGNED_AT = 500;

function Hex(prefix: number, index: number): ObjectId {
  return new ObjectId(
    `${prefix.toString(16).padStart(8, "0")}${index.toString(16).padStart(16, "0")}`,
  );
}

export function CustomerId(index: number): ObjectId {
  return Hex(0x65f00001, index);
}

export function OrderId(index: number): ObjectId {
  return Hex(0x65f00002, index);
}

function ProductId(index: number): string {
  return `SKU-${String(index).padStart(5, "0")}`;
}

export function SupplierId(index: number): UUID {
  return new UUID(`0b7c6d1e-0000-4000-8000-${String(index).padStart(12, "0")}`);
}

function Customer(index: number): Document {
  const customer: Document = {
    _id: CustomerId(index),
    name: `Customer ${index}`,
    email: `user${index}@example.com`,
    segment: SEGMENTS[index % SEGMENTS.length],
    createdAt: new Date(CUSTOMERS_START + index * 5 * HOUR_MS),
    address: {
      city: CITIES[index % CITIES.length],
      zip: String(75000 + (index % 20)),
    },
    tags: index % 3 ? ["vip", "b2b"].slice(0, 1 + (index % 2)) : [],
    ref:
      index % 13 === 0
        ? new Binary(Buffer.from([index % 256, 1, 2]), Binary.SUBTYPE_DEFAULT)
        : `R${index}`,
    score: index % 5 === 0 ? index + 0.5 : index,
  };
  customer.points =
    index % 13 === 0
      ? Long.fromString(LONG_BEYOND_DOUBLE)
      : Long.fromNumber(index);
  if (index % 10 === 0) delete customer.address;
  if (index % 7 === 0)
    customer.phone = `+33 6 ${String(index).padStart(8, "0")}`;
  return customer;
}

function Product(index: number): Document {
  return {
    _id: ProductId(index),
    label: `Product ${index}`,
    price:
      index === ARRAY_PRICED
        ? [5, 7]
        : index % 5 === 0
          ? index * 10
          : index * 10 + 0.99,
    cost: Decimal128.fromString(`${index}.10`),
    stock:
      index === 7
        ? Long.fromString(LONG_BEYOND_DOUBLE)
        : Long.fromNumber(index * 3),
    active: index % 9 !== 0,
  };
}

function OrderedAt(index: number): Date {
  return new Date(ORDERS_START + (index % 365) * DAY_MS);
}

function OrderTotal(index: number): Decimal128 {
  return Decimal128.fromString((((index * 37) % 100000) / 100).toFixed(2));
}

function Order(index: number): Document {
  const customer = CustomerId((index * 7) % CUSTOMER_COUNT);
  const order: Document = {
    _id: OrderId(index),
    customerId:
      index % STRING_CUSTOMER_EVERY === 0 ? customer.toHexString() : customer,
    status: STATUSES[index % STATUSES.length],
    orderedAt: OrderedAt(index),
    total: OrderTotal(index),
    lines: [{ sku: ProductId(index % PRODUCT_COUNT), qty: 1 + (index % 5) }],
    note: index % 4 === 0 ? "x".repeat(300 + (index % 500)) : null,
  };
  if (index % UNASSIGNED_EVERY === UNASSIGNED_AT) delete order.customerId;
  return order;
}

function Range<T>(count: number, make: (index: number) => T): T[] {
  return Array.from({ length: count }, (_, index) => make(index));
}

const MIXED_IDS: readonly Document[] = [
  { _id: 1, v: "int" },
  { _id: Long.fromNumber(2), v: "long" },
  { _id: 3.5, v: "double" },
  { _id: "abc", v: "string" },
  { _id: new ObjectId("65f000000000000000000001"), v: "oid" },
  { _id: "65f000000000000000000001", v: "string that looks like the oid" },
  { _id: { a: 1, b: 2 }, v: "object" },
  { _id: new UUID("0b7c6d1e-3f4a-4b5c-8d9e-0f1a2b3c4d5e"), v: "uuid" },
  { _id: new Date(Date.UTC(2025, 0, 1)), v: "date" },
];

const ORDER_VALIDATOR = {
  $jsonSchema: {
    properties: { status: { enum: STATUSES, description: "Order state" } },
  },
};

const SUPPLIER_VALIDATOR = {
  $jsonSchema: {
    bsonType: "object",
    required: ["name", "country"],
    properties: {
      name: { bsonType: "string", description: "Trading name" },
      country: { bsonType: "string", enum: COUNTRIES },
      rating: { bsonType: ["int", "double"] },
      uuid: { bsonType: "binData" },
    },
  },
};

async function SeedCollections(db: Db) {
  await db.collection("customers").insertMany(Range(CUSTOMER_COUNT, Customer));
  await db.collection("customers").createIndex({ email: 1 }, { unique: true });
  await db.collection("customers").createIndex({ segment: 1, createdAt: -1 });
  const products = Range(PRODUCT_COUNT, Product);
  products.push({
    _id: "SKU-legacy",
    label: "Legacy",
    price: "12.50",
    active: "yes",
  });
  await db.collection("products").insertMany(products);
  await db.createCollection("orders", { validator: ORDER_VALIDATOR });
  await db.collection("orders").insertMany(Range(ORDER_COUNT, Order));
  await db.collection("orders").createIndex({ customerId: 1 });
  await db.collection("orders").createIndex({ status: 1, orderedAt: -1 });
  await db.collection("invoices").insertMany(
    Range<Document>(INVOICE_COUNT, (index) => ({
      _id: 100000 + index,
      orderRef: OrderId(index).toHexString(),
      amount: OrderTotal(index),
      issuedOn: OrderedAt(index),
    })),
  );
  await db.collection("order_lines").insertMany(
    Range<Document>(ORDER_LINE_COUNT, (index) => ({
      _id: { order: OrderId(index), line: 1 },
      sku: ProductId(index % PRODUCT_COUNT),
      qty: 1 + (index % 5),
    })),
  );
}

async function SeedSpecialCollections(db: Db) {
  await db.createCollection("suppliers", { validator: SUPPLIER_VALIDATOR });
  await db.collection("suppliers").insertMany(
    Range<Document>(40, (index) => ({
      _id: SupplierId(index),
      name: `Supplier ${index}`,
      country: COUNTRIES[index % COUNTRIES.length],
      rating: index % 5,
    })),
  );
  await db.collection("mixed_ids").insertMany([...MIXED_IDS]);
  await db.collection<Document>("legacy_pairs").insertMany(
    Range<Document>(10, (index) => ({
      _id: { "1": index, "0": "a" },
      v: index,
    })),
  );
  await db.createCollection("big_customers", {
    viewOn: "customers",
    pipeline: [{ $match: { segment: "wholesale" } }],
  });
  await db.createCollection("metrics", {
    timeseries: { timeField: "at", metaField: "sensor" },
  });
  await db.collection("metrics").insertMany(
    Range(1000, (index) => ({
      at: new Date(ORDERS_START + index * 60_000),
      sensor: `s${index % 3}`,
      value: index,
    })),
  );
  await db.createCollection("audit", { capped: true, size: CAPPED_SIZE });
  await db.collection("audit").insertMany(
    Range(500, (index) => ({
      at: new Date(ORDERS_START + index * 1000),
      msg: `m${index}`,
    })),
  );
}

async function SeedEvents(db: Db) {
  for (let start = 0; start < EVENT_COUNT; start += EVENT_BATCH) {
    await db.collection("events").insertMany(
      Range(EVENT_BATCH, (offset) => ({
        kind: EVENT_KINDS[(start + offset) % EVENT_KINDS.length],
        at: new Date(ORDERS_START + (start + offset) * 1000),
        n: (start + offset) % 997,
      })),
    );
  }
}

async function CreateAccounts(db: Db) {
  await db.command({
    createRole: WRITER_ROLE,
    privileges: WRITABLE_COLLECTIONS.map((collection) => ({
      resource: { db: CLIENT_DATABASE, collection },
      actions: WRITE_ACTIONS,
    })),
    roles: [{ role: "read", db: CLIENT_DATABASE }],
  });
  await db.command({
    createUser: READER_USER,
    pwd: ACCOUNT_PASSWORD,
    roles: [{ role: "read", db: CLIENT_DATABASE }],
  });
  await db.command({
    createUser: WRITER_USER,
    pwd: ACCOUNT_PASSWORD,
    roles: [{ role: WRITER_ROLE, db: CLIENT_DATABASE }],
  });
}

/**
 * Builds the client database as its owner would, with a reader and a writer account, and turns
 * on the profiler so a suite can prove which commands the module sent.
 */
export async function SeedClientDatabase(adminUrl: string) {
  const client = await MongoClient.connect(adminUrl);
  try {
    const db = client.db(CLIENT_DATABASE);
    await db.dropDatabase();
    await SeedCollections(db);
    await SeedSpecialCollections(db);
    await SeedEvents(db);
    await CreateAccounts(db);
    await db.command({ profile: 2 });
  } finally {
    await client.close();
  }
}

/** A client connected as the client database's owner, for assertions the module cannot make. */
export async function AdminClient(): Promise<MongoClient> {
  const url = process.env[ADMIN_URL_VARIABLE];
  if (!url) {
    throw new Error(
      `${ADMIN_URL_VARIABLE} is not set: run the external test project`,
    );
  }
  return MongoClient.connect(url);
}
