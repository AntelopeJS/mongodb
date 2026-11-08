import type { Journey } from "@antelopejs/tooling-configs/verify";

import { pick } from "./json.ts";
import { same } from "./mongo.ts";
import { ran, startScenario } from "./scenario.ts";

const SCENARIO = `const shop = new db.Schema("calc", {
  products: { fields: { name: "string", price: "number", tags: "array" }, indexes: {} },
  orders: { fields: { productId: "string", quantity: "number" }, indexes: {} },
  bundles: { fields: { items: "array" }, indexes: {} },
});
await shop.createInstance("a");
await shop.createInstance("b");
const a = shop.instance("a");
const products = a.table("products");
await products.insert([
  { _id: "pen", name: "Pen", price: 2, tags: ["office", "cheap"] },
  { _id: "ink", name: "Ink", price: 5, tags: ["office"] },
  { _id: "pad", name: "Pad", price: 3, tags: [] },
]);
const other = shop.instance("b").table("products");
const sharedKey = await other.insert({ _id: "pen", name: "Other pen", price: 99, tags: [] }).then(() => "inserted", (error) => error.name);
await other.insert({ _id: "pen-b", name: "Other pen", price: 99, tags: [] });
await other.insert({ _id: "x", name: "b's x", price: 1, tags: [] });
await a.table("orders").insert([{ _id: "o1", productId: "pen", quantity: 3 }, { _id: "o2", productId: "ink", quantity: 1 }]);
await a.table("bundles").insert({ _id: "b1", items: ["pad", "x", "pen"] });
return {
  sharedKey,
  dearer: await products.filter((p) => p.key("price").gt(2)).orderBy("_id").key("_id"),
  computed: await products.orderBy("_id").map((p) => ({ id: p.key("_id"), label: p.key("name").upcase(), doubled: p.key("price").mul(2), office: p.key("tags").includes("office"), tags: p.key("tags").count() })),
  joined: await a.table("orders").orderBy("_id").map((o) => o.merge({ product: products.get(o.key("productId")).key("name") })).pluck("_id", "product"),
  bundled: await a.table("bundles").get("b1").do((bundle) => bundle.key("items").map((id) => products.get(id))),
};`;

const pen = { _id: "pen", _instance: "a", name: "Pen", price: 2, tags: ["office", "cheap"] };
const pad = { _id: "pad", _instance: "a", name: "Pad", price: 3, tags: [] };

const expressionsJoins: Journey = {
  title: "filter, map and do lambdas compute on the server, a `get` inside them joins rows of the same instance, and `_id` stays unique across instances",
  entryPoint: "`table.filter`, `map`, `do` and `get` inside a lambda, run from a consumer module with `await`",
  async run(context) {
    const app = await startScenario(context);
    const answer = await app.run("filter, compute and join in instance a", SCENARIO);
    context.expect(answer.folder, [ran(answer)], { critical: true });
    context.expect(answer.folder, [
      same("README.md: _id is unique across instances, so instance b cannot insert instance a's pen", pick(answer.value, "sharedKey"), "DuplicateKeyError"),
      same("filter keeps the products dearer than 2", pick(answer.value, "dearer"), ["ink", "pad"]),
      same("map computes upcase, mul, includes and count per row", pick(answer.value, "computed"), [
        { id: "ink", label: "INK", doubled: 10, office: true, tags: 1 },
        { id: "pad", label: "PAD", doubled: 6, office: false, tags: 0 },
        { id: "pen", label: "PEN", doubled: 4, office: true, tags: 2 },
      ]),
      same("a get inside map joins each order to its product", pick(answer.value, "joined"), [
        { _id: "o1", product: "Pen" },
        { _id: "o2", product: "Ink" },
      ]),
      same("a get inside a map over an array answers each element's row of instance a in order, and null for instance b's x", pick(answer.value, "bundled"), [pad, null, pen]),
    ]);
  },
};

export const expressionJourneys = { "expressions-joins": expressionsJoins };
