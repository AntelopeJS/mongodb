import type { MongoClientOptions } from "mongodb";
import { ImplementInterface } from "@antelopejs/interface-core";

import { Connect, Disconnect, EnsureBookkeepingCollection } from "./connection";
import {
  AllowSchemaInitializations,
  DrainSchemaInitializations,
  PreventSchemaInitializations,
} from "./schema-initialization";

export interface Options {
  url: string;
  database: string;
  options?: MongoClientOptions;
}

export async function construct(options: Options) {
  await Connect(options.url, options.database, options.options);
  await EnsureBookkeepingCollection();
  AllowSchemaInitializations();

  ImplementInterface(
    await import("@antelopejs/interface-database/query"),
    await import("./implementations/database/query"),
  );
  ImplementInterface(
    await import("@antelopejs/interface-database/schema"),
    await import("./implementations/database/schema"),
  );
}

export function start(): void {
  AllowSchemaInitializations();
}

export function stop(): void {
  PreventSchemaInitializations();
}

export async function destroy() {
  PreventSchemaInitializations();
  await DrainSchemaInitializations();
  await Disconnect();
}
