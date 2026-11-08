import type { Journey, JourneyContext } from "@antelopejs/tooling-configs/verify";
import { Schema } from "@antelopejs/interface-database/staged-query";

import {
  OWNED_DATABASE,
  answered,
  mongosh,
  queryEntry,
  register,
  run,
  same,
  stages,
  start,
  type Stack,
} from "./mongo.ts";
import { isNumber, pick, type Json } from "./json.ts";

interface Product {
  _id: string;
  name: string;
  price: number;
  revision: string;
}

interface Order {
  _id: string;
  productId: string;
  quantity: number;
}

const SHOP = new Schema<{ products: Product; orders: Order }>("shop", {
  products: { fields: { name: "string", price: "number", revision: "string" }, indexes: {} },
  orders: { fields: { productId: "string", quantity: "number" }, indexes: {} },
});
const TENANT_A = SHOP.instance("tenant-a");
const TENANT_B = SHOP.instance("tenant-b");

async function seedShop(stack: Stack): Promise<boolean> {
  const { context } = stack;
  const registered = await register(stack, SHOP);
  const created = await Promise.all([run(stack, "createInstance tenant-a", SHOP.createInstance("tenant-a")), run(stack, "createInstance tenant-b", SHOP.createInstance("tenant-b"))]);
  const products = await run(
    stack,
    "insert tenant-a products",
    TENANT_A.table("products").insert([
      { _id: "pen", name: "pen", price: 2, revision: "r1" },
      { _id: "ink", name: "ink", price: 5, revision: "r1" },
      { _id: "pad", name: "pad", price: 3, revision: "r1" },
    ]),
  );
  const other = await Promise.all([
    run(stack, "insert a tenant-b product", TENANT_B.table("products").insert({ _id: "pen-b", name: "pen", price: 2, revision: "r1" })),
    run(stack, "insert a tenant-b order", TENANT_B.table("orders").insert({ _id: "o3", productId: "pen-b", quantity: 1 })),
  ]);
  const orders = await run(
    stack,
    "insert tenant-a orders",
    TENANT_A.table("orders").insert([
      { _id: "o1", productId: "pen", quantity: 3 },
      { _id: "o2", productId: "ink", quantity: 1 },
    ]),
  );
  return context.expect(orders.folder, [registered, ...created, products, ...other, orders].map(answered), { critical: true });
}

async function readsScopedToInstance(stack: Stack): Promise<void> {
  const { context } = stack;
  const products = TENANT_A.table("products");
  const pen = await run(stack, "get pen in tenant-a", products.get("pen"));
  context.expect(pen.folder, [same("get answers the stored row with its instance", pen.value, { _id: "pen", _instance: "tenant-a", name: "pen", price: 2, revision: "r1" })]);
  const dearest = await run(stack, "the two dearest products", products.orderBy("price", "desc").slice(0, 2).key("_id"));
  context.expect(dearest.folder, [same("orderBy, slice and key answer ink then pad", dearest.value, ["ink", "pad"])]);
  const keyed = await run(stack, "getAll pen and ink", products.getAll(["pen", "ink"]).orderBy("_id").pluck("_id", "price"));
  context.expect(keyed.folder, [same("getAll answers the two keys", keyed.value, [{ _id: "ink", price: 5 }, { _id: "pen", price: 2 }])]);
  const totals = await Promise.all([run(stack, "count tenant-a products", products.count()), run(stack, "count tenant-b products", TENANT_B.table("products").count()), run(stack, "sum tenant-a prices", products.sum("price"))]);
  context.expect(totals[2].folder, [same("each instance counts and sums only its own rows", totals.map((total) => total.value ?? null), [3, 1, 10])]);
  const filtered = await run(stack, "products dearer than 2", products.filter((row) => row.key("price").gt(2)).orderBy("_id").key("_id"));
  const joined = await run(
    stack,
    "orders joined to their product's name",
    TENANT_A.table("orders")
      .orderBy("_id")
      .map((order) => order.merge({ product: products.get(order.key("productId")).key("name") }))
      .pluck("_id", "product"),
  );
  context.expect(joined.folder, [
    same("filter answers ink and pad", filtered.value, ["ink", "pad"]),
    same("a get inside map joins each order to its own instance's product", joined.value, [
      { _id: "o1", product: "pen" },
      { _id: "o2", product: "ink" },
    ]),
  ]);
}

async function mutate(stack: Stack): Promise<void> {
  const { context } = stack;
  const products = TENANT_A.table("products");
  const updated = await run(stack, "update pen's price", products.get("pen").update({ price: 4 }));
  const deleted = await run(stack, "delete pad", products.get("pad").delete());
  context.expect(deleted.folder, [same("update and delete each answer one row", [updated.value ?? null, deleted.value ?? null], [1, 1])]);
  const request = { type: "update", revisionField: "revision", expectedRevision: "r1", nextRevision: "r2", patch: { price: 6 } } as const;
  const applied = await run(stack, "atomicMutation ink at r1", products.atomicMutation("ink", request));
  const stale = await run(stack, "atomicMutation ink at r1 again", products.atomicMutation("ink", request));
  context.expect(stale.folder, [same("the first mutation applies and the stale one does not", [applied.value ?? null, stale.value ?? null], ["applied", "not-applied"])]);
  const listed = await run(stack, "listInstances", SHOP.listInstances());
  context.expect(listed.folder, [same("listInstances answers both tenants", listed.value, ["tenant-a", "tenant-b"])]);
  const destroyed = await run(stack, "destroyInstance tenant-b", SHOP.destroyInstance("tenant-b"));
  context.expect(destroyed.folder, [answered(destroyed)]);
}

const SHOP_STATE = `({
  products: db.getCollection("shop__products").find().sort({ _id: 1 }).toArray(),
  orders: db.getCollection("shop__orders").find({}, { _id: 1, _instance: 1 }).sort({ _id: 1 }).toArray(),
  instances: db.getCollection("__antelope_instances").find({}, { _id: 0, schemaId: 1, instanceId: 1 }).sort({ instanceId: 1 }).toArray(),
})`;

const rowsAcrossInstances: Journey = {
  title: "rows written to two instances are read, ordered, counted, updated, deleted and mutated atomically within their instance, and destroying an instance removes its rows from every table and its bookkeeping",
  entryPoint: "`RunQuery` of `@antelopejs/interface-database` through core: insert, get, getAll, orderBy, slice, key, pluck, count, sum, update, delete, atomicMutation and the instance lifecycle",
  async run(context) {
    const stack = await start(context);
    if (!(await seedShop(stack))) return;
    await readsScopedToInstance(stack);
    await mutate(stack);
    const stored = await mongosh(context, "mongosh reads the shop collections and the bookkeeping", OWNED_DATABASE, SHOP_STATE);
    context.expect(stored.folder, [
      same(
        "tenant-a's rows carry their instance and their last write",
        pick(stored.value, "products"),
        [
          { _id: "ink", _instance: "tenant-a", name: "ink", price: 6, revision: "r2" },
          { _id: "pen", _instance: "tenant-a", name: "pen", price: 4, revision: "r1" },
        ],
      ),
      same("tenant-b's order went with its instance", pick(stored.value, "orders"), [
        { _id: "o1", _instance: "tenant-a" },
        { _id: "o2", _instance: "tenant-a" },
      ]),
      same("only tenant-a is left in __antelope_instances", pick(stored.value, "instances"), [{ schemaId: "shop", instanceId: "tenant-a" }]),
    ]);
  },
};

interface Event {
  _id: string;
  title: string;
}

const FEED = new Schema<{ events: Event }>("feed", { events: { fields: { title: "string" }, indexes: {} } });
const FEED_EVENTS = FEED.instance("tenant-a").table("events");
const CURSOR = 1;
const OPEN_POLLS = 40;
const OPEN_STREAMS = `db.getSiblingDB("admin").aggregate([{ $currentOp: { idleCursors: true } }, { $match: { ns: "${OWNED_DATABASE}.feed__events" } }]).toArray().length`;

async function openStreams(context: JourneyContext, name: string): Promise<{ folder: string; value: Json }> {
  return mongosh(context, name, OWNED_DATABASE, OPEN_STREAMS);
}

/** Polls `$currentOp` until the change stream `ReadCursor` opened shows up, so the writes after it reach the stream. */
async function waitForStream(context: JourneyContext): Promise<{ folder: string; opened: boolean }> {
  let folder = "";
  for (let poll = 1; poll <= OPEN_POLLS; poll += 1) {
    const open = await openStreams(context, `mongosh polls for the open change stream (${poll})`);
    folder = open.folder;
    if (isNumber(open.value) && open.value > 0) return { folder, opened: true };
  }
  return { folder, opened: false };
}

function changeOf(value: Json | undefined): Json {
  return [pick(value, "done") ?? null, pick(value, "value", "changeType") ?? null, pick(value, "value", "oldValue") ?? null, pick(value, "value", "newValue") ?? null];
}

const changeFeed: Journey = {
  title: "a change feed on one instance streams that instance's insert, update and delete in order, and closing it closes the server's change stream",
  entryPoint: "`ReadCursor` and `CloseCursor` of `@antelopejs/interface-database` through core on `table.changes()`, as `for await` over a change feed does",
  async run(context) {
    const stack = await start(context);
    const registered = await register(stack, FEED);
    context.expect(registered.folder, [answered(registered)], { critical: true });
    const changes = stages(FEED_EVENTS.changes());
    const first = queryEntry(stack, "ReadCursor waits for the first change", "ReadCursor", [CURSOR, changes]);
    const { folder, opened } = await waitForStream(context);
    context.expect(folder, [{ name: "the change stream opened on feed__events", pass: opened, detail: opened ? "open" : `not open after ${OPEN_POLLS} polls` }], { critical: true });
    await run(stack, "insert into tenant-b", FEED.instance("tenant-b").table("events").insert({ _id: "other", title: "elsewhere" }));
    await run(stack, "insert into tenant-a", FEED_EVENTS.insert({ _id: "e1", title: "first" }));
    const added = await first;
    await run(stack, "update in tenant-a", FEED_EVENTS.get("e1").update({ title: "second" }));
    const modified = await queryEntry(stack, "ReadCursor reads the second change", "ReadCursor", [CURSOR, changes]);
    await run(stack, "delete in tenant-a", FEED_EVENTS.get("e1").delete());
    const removed = await queryEntry(stack, "ReadCursor reads the third change", "ReadCursor", [CURSOR, changes]);
    context.expect(removed.folder, [
      same("the first change is tenant-a's insert, not tenant-b's", changeOf(added.value), [false, "added", null, { _id: "e1", _instance: "tenant-a", title: "first" }]),
      same("the second is the update with the row before and after", changeOf(modified.value), [false, "modified", { _id: "e1", _instance: "tenant-a", title: "first" }, { _id: "e1", _instance: "tenant-a", title: "second" }]),
      same("the third is the delete with the row before it", changeOf(removed.value), [false, "removed", { _id: "e1", _instance: "tenant-a", title: "second" }, null]),
    ]);
    const closed = await queryEntry(stack, "CloseCursor", "CloseCursor", [CURSOR]);
    const left = await openStreams(context, "mongosh looks for the change stream after CloseCursor");
    context.expect(left.folder, [answered(closed), same("no change stream is left open on feed__events", left.value, 0)]);
  },
};

export const queryJourneys = { "rows-across-instances": rowsAcrossInstances, "change-feed": changeFeed };
