import {
  equal,
  type App,
  type CallResult,
  type Check,
  type JourneyContext,
  type ServiceDefinition,
  type ServiceEndpoint,
} from "@antelopejs/tooling-configs/verify";
import type { Schema, StagedObject } from "@antelopejs/interface-database/staged-query";

import { canonical, parseJson, pick, type Json } from "./json.ts";

/** A module configuration, as a fixture project takes it. */
export type ModuleConfig = NonNullable<NonNullable<Parameters<JourneyContext["fixture"]>[0]>["moduleConfig"]>;

export const SERVICE = "mongo";
/** The database an owned instance creates its collections in. */
export const OWNED_DATABASE = "verify_owned";
/** The client's database an external instance serves; journeys seed it with mongosh. */
export const CLIENT_DATABASE = "verify_client";

const DATABASE_INTERFACE = "@antelopejs/interface-database";
const CATALOG_INTERFACE = "@antelopejs/interface-database-catalog";

/** The image `setup` builds from `mongo/Dockerfile`: `mongo:8.0.8` started as the replica set `rs0`. */
export const MONGO_IMAGE = "antelopejs-verify-mongodb/mongo-rs:8.0.8";

const INITIATE = `try { rs.status() } catch (error) { rs.initiate({ _id: "rs0", members: [{ _id: 0, host: "127.0.0.1:27017" }] }) }`;

/** A single-node replica set, which change feeds need; readiness initiates it once and waits for the primary. */
export const MONGO_SERVICE: ServiceDefinition = {
  image: MONGO_IMAGE,
  port: 27017,
  ready: ["mongosh", "--quiet", "--eval", `${INITIATE}; quit(db.hello().isWritablePrimary ? 0 : 1)`],
  reset: [
    "mongosh",
    "--quiet",
    "--eval",
    `for (const name of ["${OWNED_DATABASE}", "${CLIENT_DATABASE}"]) db.getSiblingDB(name).dropDatabase()`,
  ],
};

function mongoUrl(service: ServiceEndpoint): string {
  return `mongodb://${service.host}:${service.port}/?directConnection=true`;
}

export function ownedConfig(service: ServiceEndpoint): ModuleConfig {
  return { url: mongoUrl(service), database: OWNED_DATABASE };
}

export function externalConfig(service: ServiceEndpoint): ModuleConfig {
  return { mode: "external", url: mongoUrl(service), database: CLIENT_DATABASE };
}

/** A running fixture project: the module as `local` and the driver module that calls it through core. */
export interface Stack {
  context: JourneyContext;
  app: App;
}

/** Writes a fixture project with `config` as the module config, the owned instance by default, and starts `ajs project dev`. */
export async function start(context: JourneyContext, config?: ModuleConfig): Promise<Stack> {
  const project = context.fixture({ moduleConfig: config ?? ownedConfig(context.service(SERVICE)) });
  const app = await context.dev("ajs project dev", project);
  return { context, app };
}

/** The JSON form of a built query or a schema definition, as `JSON.stringify` writes it: an `undefined` key is dropped and an `undefined` element is `null`. */
function wire(value: ReturnType<StagedObject["build"]> | Schema["definition"]): Json {
  return parseJson(JSON.stringify(value));
}

/** Registers a schema through core, as `new Schema(id, definition)` does in an application module. */
export function register(stack: Stack, schema: Schema): Promise<CallResult> {
  return stack.context.call(`register schema ${schema.id}`, stack.app, {
    interface: DATABASE_INTERFACE,
    entry: "./schema",
    path: ["Schemas", "register"],
    args: [schema.id, wire(schema.definition)],
  });
}

/** Runs a query through core, as `await query` does in an application module. */
export function run(stack: Stack, name: string, query: StagedObject): Promise<CallResult> {
  return stack.context.call(name, stack.app, {
    interface: DATABASE_INTERFACE,
    entry: "./query",
    path: ["RunQuery"],
    args: [wire(query.build())],
  });
}

/** Calls a member of the database interface's `./query` entry, such as `ReadCursor`. */
export function queryEntry(stack: Stack, name: string, member: string, args: Json[]): Promise<CallResult> {
  return stack.context.call(name, stack.app, { interface: DATABASE_INTERFACE, entry: "./query", path: [member], args });
}

/** Calls a function of the catalog interface through core. */
export function catalog(stack: Stack, name: string, member: string, args: Json[]): Promise<CallResult> {
  return stack.context.call(name, stack.app, { interface: CATALOG_INTERFACE, path: [member], args });
}

function parsed(text: string): Json {
  try {
    return parseJson(text);
  } catch {
    return { unparsed: text };
  }
}

/** Evaluates `expression` with mongosh inside the service container, against `database`, and answers its relaxed Extended JSON. */
export async function mongosh(
  context: JourneyContext,
  name: string,
  database: string,
  expression: string,
): Promise<{ folder: string; value: Json }> {
  const result = await context.native(name, SERVICE, [
    "mongosh",
    "--quiet",
    database,
    "--eval",
    `print(EJSON.stringify(${expression}, { relaxed: true }))`,
  ]);
  return { folder: result.folder, value: parsed(result.stdout.trim()) };
}

/** The name and message of the error a call threw, or `[null, null]` when it answered. */
export function thrown(result: CallResult): Json {
  return [pick(result.json, "error", "name") ?? null, pick(result.json, "error", "message") ?? null];
}

/** The call went through core and the module answered without throwing. */
export function answered(result: CallResult): Check {
  return { name: "the call through core answered", pass: result.ok, detail: result.ok ? "ok" : result.body };
}

/** `equal` that ignores key order; an absent value reads as `null`. */
export function same(name: string, actual: Json | undefined, expected: Json): Check {
  return equal(name, canonical(actual), canonical(expected));
}

/** The JSON form of a staged query's built stages, as `ReadCursor` takes them. */
export function stages(query: StagedObject): Json {
  return wire(query.build());
}
