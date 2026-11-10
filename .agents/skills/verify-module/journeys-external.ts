import type { Check, Journey } from "@antelopejs/tooling-configs/verify";

import { ADA, CLIENT_COLLECTIONS, ERP, startOnClientDatabase } from "./client-db.ts";
import { CLIENT_DATABASE, answered, mongosh, register, run, same, thrown, type Stack } from "./mongo.ts";
import { isString, pick, text } from "./json.ts";

const CUSTOMERS = ERP.instance().table("customers");

async function reads(stack: Stack): Promise<void> {
  const { context } = stack;
  const ada = await run(stack, "get Ada by her ObjectId", CUSTOMERS.get(ADA));
  const names = await run(stack, "customers ordered by name", CUSTOMERS.orderBy("name").key("name"));
  const total = await run(stack, "sum of credit", CUSTOMERS.sum("credit"));
  const view = await run(stack, "count of the view", ERP.instance().table("big_customers").count());
  context.expect(view.folder, [
    same("get answers _id as hex and the declared fields only", ada.value, { _id: ADA, name: "Ada", credit: 10, tier: "gold" }),
    same("orderBy answers Ada then Bob", names.value, ["Ada", "Bob"]),
    same("Long credits sum as numbers", total.value, 30),
    same("the view answers its one row", view.value, 1),
  ]);
}

function refused(name: string, result: Awaited<ReturnType<typeof run>>, message: RegExp): Check {
  const error = text(pick(thrown(result), 1));
  return { name, pass: !result.ok && message.test(error), detail: result.ok ? `answered ${JSON.stringify(result.value)}` : error };
}

async function writes(stack: Stack): Promise<void> {
  const { context } = stack;
  const inserted = await run(stack, "insert Cy without an _id", CUSTOMERS.insert({ name: "Cy", credit: 30, tier: "gold" }));
  const insertedId = pick(inserted.value, 0);
  context.expect(inserted.folder, [answered(inserted), { name: "insert answers a new ObjectId as hex", pass: isString(insertedId) && /^[0-9a-f]{24}$/.test(insertedId), detail: JSON.stringify(insertedId ?? null) }]);
  const undeclared = await run(stack, "insert with an undeclared key", CUSTOMERS.insert({ name: "Dee", credit: 1, tier: "gold", city: "Nice" }));
  const updated = await run(stack, "update Ada's credit", CUSTOMERS.get(ADA).update({ credit: 11 }));
  context.expect(updated.folder, [same("the update answers one row", updated.value, 1)]);
  const toView = await run(stack, "insert into the view", ERP.instance().table("big_customers").insert({ name: "Eve", credit: 99 }));
  const instance = await run(stack, "createInstance", ERP.createInstance("tenant-a"));
  context.expect(instance.folder, [
    refused("a key the schema does not declare is refused by name", undeclared, /Field 'city' of customers is not a declared field/),
    refused("a write to a view is refused", toView, /big_customers .* read-only/),
    refused("createInstance is refused", instance, /'createInstance' stage manages instances/),
  ]);
}

const CLIENT_STATE = `({
  customers: db.customers.aggregate([
    { $project: { _id: 0, name: 1, credit: 1, city: 1, tier: 1, id: { $type: "$_id" }, creditType: { $type: "$credit" }, instance: { $type: "$_instance" } } },
    { $sort: { name: 1 } },
  ]).toArray(),
  collections: db.getCollectionNames().sort(),
  indexes: db.customers.getIndexes().map((index) => index.name).sort(),
})`;

const externalReadsWrites: Journey = {
  title: "an external instance reads a client's collection and view by their own names and types, writes new values as the stored native types, refuses what external mode forbids and never changes the database's structure",
  entryPoint: "`Schemas.register` and `RunQuery` of `@antelopejs/interface-database` through core on a module configured with `mode: \"external\"`",
  async run(context) {
    const stack = await startOnClientDatabase(context);
    if (stack === undefined) return;
    const registered = await register(stack, ERP);
    context.expect(registered.folder, [answered(registered)], { critical: true });
    await reads(stack);
    await writes(stack);
    const stored = await mongosh(context, "mongosh reads the client's database", CLIENT_DATABASE, CLIENT_STATE);
    const native = { id: "objectId", creditType: "long", instance: "missing" };
    context.expect(stored.folder, [
      same("writes kept ObjectId keys and Long credits, stamped no instance and left undeclared fields", pick(stored.value, "customers"), [
        { name: "Ada", credit: 11, city: "Paris", tier: "gold", ...native },
        { name: "Bob", credit: 20, city: "Lyon", tier: "silver", ...native },
        { name: "Cy", credit: 30, tier: "gold", ...native },
      ]),
      same("no collection was created", pick(stored.value, "collections"), CLIENT_COLLECTIONS),
      same("no index was created", pick(stored.value, "indexes"), ["_id_", "by_name"]),
    ]);
  },
};

export const externalJourneys = { "external-reads-writes": externalReadsWrites };
