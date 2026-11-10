import { has, lacks, type Journey } from "@antelopejs/tooling-configs/verify";
import { Schema } from "@antelopejs/interface-database/staged-query";

import { OWNED_DATABASE, answered, mongosh, register, run, same, start, thrown, type Stack } from "./mongo.ts";
import { pick, text } from "./json.ts";

interface Item {
  _id: string;
  sku: string;
  name: string;
  price: number;
  email: string;
}

const STORE = new Schema<{ items: Item }>("store", {
  items: {
    fields: { sku: "string", name: "string", price: "number", email: "string" },
    indexes: { name: {}, skuPrice: { fields: ["sku", "price"] }, sku: { crossInstance: true }, email: { unique: true } },
  },
});

function indexesOf(collection: string): string {
  return `db.getCollection("${collection}").getIndexes().map((index) => ({ name: index.name, key: index.key, unique: index.unique === true })).sort((a, b) => a.name.localeCompare(b.name))`;
}

function index(name: string, key: { [field: string]: number }, unique = false) {
  return { name, key, unique };
}

/** The declared index, not its physical `<index>__i`, is what a refused duplicate names. */
function duplicateRefused(result: Awaited<ReturnType<typeof run>>, declared: string) {
  const [name, message] = [pick(thrown(result), 0), text(pick(thrown(result), 1))];
  return [
    same("the write is refused with DuplicateKeyError", name, "DuplicateKeyError"),
    has("the error message", message, new RegExp(`index "${declared}"`)),
    lacks("the error message", message, new RegExp(`${declared}__i`)),
  ];
}

async function seedItems(stack: Stack): Promise<boolean> {
  const tenantA = STORE.instance("tenant-a").table("items");
  const tenantB = STORE.instance("tenant-b").table("items");
  const registered = await register(stack, STORE);
  const inserted = await run(
    stack,
    "insert tenant-a items",
    tenantA.insert([
      { _id: "i1", sku: "A1", name: "anvil", price: 10, email: "a@example.test" },
      { _id: "i2", sku: "B2", name: "bolt", price: 2, email: "b@example.test" },
      { _id: "i3", sku: "C3", name: "cog", price: 5, email: "c@example.test" },
    ]),
  );
  const elsewhere = await run(stack, "insert tenant-b's item with tenant-a's email", tenantB.insert({ _id: "i4", sku: "A1", name: "anvil", price: 11, email: "a@example.test" }));
  return stack.context.expect(elsewhere.folder, [registered, inserted, elsewhere].map(answered), { critical: true });
}

const indexLayout: Journey = {
  title: "declared indexes become `<index>__i` indexes led by `_instance`, a cross-instance index also gets `<index>`, a unique index refuses a duplicate within one instance only, and an index the module does not own stays",
  entryPoint: "`Schemas.register` with secondary indexes, then `getAll`, `between` and `insert` through `RunQuery`",
  async run(context) {
    const own = await mongosh(context, "mongosh creates an index of the operator's own", OWNED_DATABASE, `db.getCollection("store__items").createIndex({ name: 1 }, { name: "name" })`);
    context.expect(own.folder, [same("the operator's index exists before the schema", own.value, "name")], { critical: true });
    const stack = await start(context);
    if (!(await seedItems(stack))) return;
    const items = STORE.instance("tenant-a").table("items");
    const duplicate = await run(stack, "insert a second a@example.test in tenant-a", items.insert({ _id: "i5", sku: "D4", name: "drill", price: 7, email: "a@example.test" }));
    context.expect(duplicate.folder, duplicateRefused(duplicate, "email"));
    const byName = await run(stack, "getAll bolt on the name index", items.getAll(["bolt"], "name").key("_id"));
    const range = await run(stack, "between b and d on the name index", items.between("name", "b", "d").orderBy("name").key("_id"));
    context.expect(range.folder, [same("getAll on an index answers its row", byName.value, ["i2"]), same("between on an index answers bolt then cog", range.value, ["i2", "i3"])]);
    const indexes = await mongosh(context, "mongosh lists the indexes of store__items", OWNED_DATABASE, indexesOf("store__items"));
    context.expect(indexes.folder, [
      same("every declared index is led by _instance, and only email__i is unique", indexes.value, [
        index("_id_", { _id: 1 }),
        index("email__i", { _instance: 1, email: 1 }, true),
        index("name", { name: 1 }),
        index("name__i", { _instance: 1, name: 1 }),
        index("sku", { sku: 1 }),
        index("sku__i", { _instance: 1, sku: 1 }),
        index("skuPrice__i", { _instance: 1, sku: 1, price: 1 }),
      ]),
    ]);
  },
};

interface Account {
  _id: string;
  email: string;
  nickname: string;
}

const ACCOUNT_FIELDS = { email: "string", nickname: "string" };
const ACCOUNTS_V1 = new Schema<{ accounts: Account }>("members", { accounts: { fields: ACCOUNT_FIELDS, indexes: { email: {}, nickname: { crossInstance: true } } } });
const ACCOUNTS_V2 = new Schema<{ accounts: Account }>("members", { accounts: { fields: ACCOUNT_FIELDS, indexes: { email: { unique: true }, nickname: {} } } });

const indexRebuild: Journey = {
  title: "a schema upgraded across a restart rebuilds an index whose `unique` flag changed, and leaves the cross-instance index it no longer declares",
  entryPoint: "`Schemas.register` of a changed definition in a second `ajs project dev`, then `insert` through `RunQuery`",
  async run(context) {
    const first = await start(context);
    const registered = await register(first, ACCOUNTS_V1);
    const seeded = await run(
      first,
      "insert two accounts under the first schema",
      ACCOUNTS_V1.instance("tenant-a").table("accounts").insert([
        { _id: "a1", email: "x@example.test", nickname: "x" },
        { _id: "a2", email: "y@example.test", nickname: "y" },
      ]),
    );
    context.expect(seeded.folder, [answered(registered), answered(seeded)], { critical: true });
    await context.stop("stop the first app", first.app);
    const second = await start(context);
    const upgraded = await register(second, ACCOUNTS_V2);
    const duplicate = await run(second, "insert a second x@example.test in tenant-a", ACCOUNTS_V2.instance("tenant-a").table("accounts").insert({ _id: "a3", email: "x@example.test", nickname: "z" }));
    context.expect(duplicate.folder, [answered(upgraded), ...duplicateRefused(duplicate, "email")]);
    const other = await run(second, "insert x@example.test in tenant-b", ACCOUNTS_V2.instance("tenant-b").table("accounts").insert({ _id: "a4", email: "x@example.test", nickname: "x" }));
    context.expect(other.folder, [answered(other)]);
    const indexes = await mongosh(context, "mongosh lists the indexes of members__accounts", OWNED_DATABASE, indexesOf("members__accounts"));
    context.expect(indexes.folder, [
      same("email__i was rebuilt unique and the undeclared nickname index stays", indexes.value, [
        index("_id_", { _id: 1 }),
        index("email__i", { _instance: 1, email: 1 }, true),
        index("nickname", { nickname: 1 }),
        index("nickname__i", { _instance: 1, nickname: 1 }),
      ]),
    ]);
  },
};

export const indexJourneys = { "index-layout": indexLayout, "index-rebuild": indexRebuild };
