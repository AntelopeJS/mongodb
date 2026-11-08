import type { Journey } from "@antelopejs/tooling-configs/verify";
import { Schema } from "@antelopejs/interface-database/staged-query";

import { OWNED_DATABASE, answered, mongosh, register, run, same, start } from "./mongo.ts";

export const NOTES = new Schema("smoke", { notes: { fields: { text: "string" }, indexes: {} } });

const writeThroughMongo: Journey = {
  title: "a consumer registers a schema and inserts a row, and mongosh finds it in `<schema>__<table>` stamped with its instance",
  entryPoint: "`Schemas.register` and `RunQuery` of `@antelopejs/interface-database` through core",
  async run(context) {
    const stack = await start(context);
    const registered = await register(stack, NOTES);
    context.expect(registered.folder, [answered(registered)], { critical: true });
    const inserted = await run(stack, "insert a note", NOTES.instance("tenant-a").table("notes").insert({ _id: "n1", text: "hello" }));
    context.expect(inserted.folder, [answered(inserted)], { critical: true });
    const stored = await mongosh(context, "mongosh reads smoke__notes", OWNED_DATABASE, `db.getCollection("smoke__notes").find().toArray()`);
    context.expect(stored.folder, [same("the row is stored with its instance", stored.value, [{ _id: "n1", _instance: "tenant-a", text: "hello" }])]);
  },
};

export const smokeJourneys = { "write-through-mongo": writeThroughMongo };
