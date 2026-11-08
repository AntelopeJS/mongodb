import { join } from "node:path";

import { has, type Journey } from "@antelopejs/tooling-configs/verify";

import { OWNED_DATABASE, answered, register, run, same, start, type ModuleConfig } from "./mongo.ts";
import { NOTES } from "./journeys-smoke.ts";

const UNUSED_URL = "mongodb://127.0.0.1:1";

const REFUSALS: [string, ModuleConfig, RegExp][] = [
  ["an unknown mode", { mode: "replica", url: UNUSED_URL }, /mode must be one of owned, external, received replica/],
  ["an external instance without a database", { mode: "external", url: UNUSED_URL }, /mode "external" needs a non-empty config\.database/],
  ["an external instance without a url", { mode: "external", database: "erp" }, /mode "external" needs a non-empty config\.url/],
  ["retryWrites on an external instance", { mode: "external", url: UNUSED_URL, database: "erp", options: { retryWrites: true } }, /options\.retryWrites cannot be set with mode "external"/],
  ["a zero statementTimeoutMs", { url: UNUSED_URL, statementTimeoutMs: 0 }, /statementTimeoutMs must be a positive safe integer, received 0/],
  ["a fractional statementTimeoutMs", { url: UNUSED_URL, statementTimeoutMs: 1.5 }, /statementTimeoutMs must be a positive safe integer, received 1\.5/],
];

const configRefused: Journey = {
  title: "the module refuses to start on a config `src/config.ts` rejects, and says why",
  entryPoint: "`ajs project dev` with an unusable `config` in antelope.config.ts",
  async run(context) {
    const cli = join(context.repo, "node_modules/@antelopejs/core/dist/core/cli/index.js");
    for (const [label, config, message] of REFUSALS) {
      const project = context.fixture({ moduleConfig: config });
      const started = await context.command(`ajs project dev with ${label}`, "node", [cli, "project", "dev"], { cwd: project, timeoutMs: 60_000 });
      context.expect(started.folder, [
        { name: `the project did not come up with ${label}`, pass: started.exitCode !== 0, detail: `exit ${String(started.exitCode)}` },
        has(`the refusal says why (${label})`, `${started.stdout}\n${started.stderr}`, message),
      ]);
    }
  },
};

const rawClient: Journey = {
  title: "`GetClient` of `@antelopejs/interface-mongodb` answers the owned instance's client, which reads what the database interface wrote",
  entryPoint: "`GetClient()` of `@antelopejs/interface-mongodb` through core, chained to `db(...).collection(...).countDocuments(...)`",
  async run(context) {
    const stack = await start(context);
    const registered = await register(stack, NOTES);
    const inserted = await run(stack, "insert two notes", NOTES.instance("tenant-a").table("notes").insert([{ _id: "n1", text: "one" }, { _id: "n2", text: "two" }]));
    context.expect(inserted.folder, [answered(registered), answered(inserted)], { critical: true });
    const counted = await context.call("GetClient counts tenant-a's notes", stack.app, {
      interface: "@antelopejs/interface-mongodb",
      path: ["GetClient"],
      chain: [["db", OWNED_DATABASE], ["collection", "smoke__notes"], ["countDocuments", { _instance: "tenant-a" }]],
    });
    context.expect(counted.folder, [answered(counted), same("the raw client sees both notes", counted.value, 2)]);
  },
};

export const moduleJourneys = { "config-refused": configRefused, "raw-client": rawClient };
