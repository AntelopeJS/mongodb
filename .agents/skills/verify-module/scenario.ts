import { readFileSync } from "node:fs";
import { join } from "node:path";

import type { Check, JourneyContext } from "@antelopejs/tooling-configs/verify";

import { parseJson, pick, recordOf, text, type Json } from "./json.ts";
import { ownedConfig, SERVICE } from "./mongo.ts";

/**
 * The consumer module a scenario journey adds beside the driver. It runs a scenario body inside the
 * project with `db` (`@antelopejs/interface-database`) in scope and answers the JSON it returned. A
 * body keeps `filter`, `map` and `do` lambdas and nested queries, which lose their classes on the
 * driver's JSON wire.
 */
const APP_SOURCE = String.raw`"use strict";
const http = require("node:http");
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
let server;

async function answer(request) {
  let body = "";
  for await (const chunk of request) body += chunk;
  try {
    const value = await new AsyncFunction("db", body)(require("@antelopejs/interface-database"));
    return { ok: true, value: JSON.parse(JSON.stringify(value === undefined ? null : value)) };
  } catch (error) {
    return { ok: false, error: { name: (error && error.name) || "Error", message: String((error && error.message) || error) } };
  }
}

exports.construct = function construct() {};
exports.start = async function start() {
  server = http.createServer((request, response) => {
    answer(request).then((result) => {
      response.setHeader("content-type", "application/json");
      response.end(JSON.stringify(result));
    });
  });
  await new Promise((resolve) => server.listen(Number(process.env.VERIFY_APP_PORT), "127.0.0.1", resolve));
};
exports.stop = function stop() {
  if (server) server.close();
};
exports.destroy = function destroy() {};
`;

/** What a scenario body answered: the value it returned, or the name and message of what it threw. */
export interface Answer {
  folder: string;
  ok: boolean;
  value: Json | undefined;
  error: string;
}

/** A project running the module and the scenario consumer. */
export interface ScenarioApp {
  run(name: string, body: string): Promise<Answer>;
}

function withConsumer(context: JourneyContext, project: string): void {
  const config = parseJson(readFileSync(join(project, "antelope.config.ts"), "utf8").replace(/^export default /, "").replace(/;\s*$/, ""));
  const extended = { ...recordOf(config), modules: { ...recordOf(pick(config, "modules")), "verify-app": { source: { type: "local", path: "./app" } } } };
  context.edit("add the scenario consumer module", join(project, "antelope.config.ts"), `export default ${JSON.stringify(extended, null, 2)};\n`);
  context.edit("scenario consumer package.json", join(project, "app", "package.json"), `${JSON.stringify({ name: "verify-app", version: "1.0.0", main: "index.js", antelopeJs: {} }, null, 2)}\n`);
  context.edit("scenario consumer source", join(project, "app", "index.js"), APP_SOURCE);
}

async function freePort(context: JourneyContext): Promise<number> {
  const picked = await context.command("pick a free port", "node", ["-e", "const s = require('net').createServer(); s.listen(0, '127.0.0.1', () => { console.log(s.address().port); s.close(); });"]);
  return Number(picked.stdout.trim());
}

/** Starts `ajs project dev` on a fixture with an owned instance and the scenario consumer, which shares the driver's interface packages. */
export async function startScenario(context: JourneyContext): Promise<ScenarioApp> {
  const project = context.fixture({ moduleConfig: ownedConfig(context.service(SERVICE)) });
  withConsumer(context, project);
  const link = await context.command("link the driver's interface packages", "ln", ["-s", "../driver/node_modules", join(project, "app", "node_modules")]);
  context.expect(link.folder, [{ name: "the consumer resolves the driver's interface packages", pass: link.exitCode === 0, detail: link.stderr }], { critical: true });
  const port = await freePort(context);
  await context.dev("ajs project dev", project, { env: { VERIFY_APP_PORT: String(port) } });
  return {
    async run(name, body) {
      const result = await context.http(name, `http://127.0.0.1:${port}/`, { method: "POST", body });
      const error = pick(result.json, "error");
      return {
        folder: result.folder,
        ok: pick(result.json, "ok") === true,
        value: pick(result.json, "value"),
        error: error === undefined ? "" : `${text(pick(error, "name"))}: ${text(pick(error, "message"))}`,
      };
    },
  };
}

/** The scenario body ran to its end. */
export function ran(answer: Answer): Check {
  return { name: "the scenario ran", pass: answer.ok, detail: answer.ok ? "ok" : answer.error };
}
