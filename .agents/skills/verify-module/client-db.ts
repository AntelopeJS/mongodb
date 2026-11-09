import type { JourneyContext } from "@antelopejs/tooling-configs/verify";
import { Schema } from "@antelopejs/interface-database/staged-query";

import { CLIENT_DATABASE, externalConfig, mongosh, same, SERVICE, start, type Stack } from "./mongo.ts";

export const ADA = "650000000000000000000001";

/**
 * A client's database the module did not create: `customers` with ObjectId keys, a Long field, a
 * validator with a description and an enum, an index of its own and a view.
 */
const SEED = `(() => {
  db.createCollection("customers", {
    validator: {
      $jsonSchema: {
        bsonType: "object",
        required: ["name"],
        properties: {
          name: { bsonType: "string", description: "Display name" },
          credit: { bsonType: "long" },
          tier: { enum: ["gold", "silver"] },
        },
      },
    },
  });
  db.customers.insertMany([
    { _id: ObjectId("${ADA}"), name: "Ada", credit: Long(10), city: "Paris", tier: "gold" },
    { _id: ObjectId("650000000000000000000002"), name: "Bob", credit: Long(20), city: "Lyon", tier: "silver" },
  ]);
  db.customers.createIndex({ name: 1 }, { name: "by_name" });
  db.createView("big_customers", "customers", [{ $match: { credit: { $gt: 15 } } }]);
  return db.getCollectionNames().sort();
})()`;

export const CLIENT_COLLECTIONS = ["big_customers", "customers", "system.views"];

/** A customer as the application types it; `city` is in the documents but not in the schema. */
export interface Customer {
  _id: string;
  name: string;
  credit: number;
  tier: string;
  city?: string;
}

/** The schema an application declares to mirror the client's database, names verbatim. */
export const ERP = new Schema<{ customers: Customer; big_customers: Customer }>("erp", {
  customers: { fields: { _id: "string", name: "string", credit: "number", tier: "string" }, indexes: {} },
  big_customers: { fields: { name: "string", credit: "number" }, indexes: {} },
});

/** Seeds the client's database with mongosh, then starts an external instance on it. */
export async function startOnClientDatabase(context: JourneyContext): Promise<Stack | undefined> {
  const seeded = await mongosh(context, "mongosh seeds the client's database", CLIENT_DATABASE, SEED);
  if (!context.expect(seeded.folder, [same("the client's collections and view exist", seeded.value, CLIENT_COLLECTIONS)], { critical: true })) return undefined;
  return start(context, externalConfig(context.service(SERVICE)));
}
