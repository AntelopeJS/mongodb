import { ImplementInterface } from "@antelopejs/interface-core";
import type { MongoClientOptions } from "mongodb";
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

  await ImplementInterface(
    await import("@antelopejs/interface-database/query"),
    await import("./implementations/database/query"),
  );
  await ImplementInterface(
    await import("@antelopejs/interface-database/schema"),
    await import("./implementations/database/schema"),
  );
}

export function stop(): void {
  PreventSchemaInitializations();
}

async function collectDisconnectErrors(): Promise<unknown[]> {
  try {
    await Disconnect();
    return [];
  } catch (error) {
    return [error];
  }
}

function throwDestroyErrors(errors: unknown[]): void {
  if (errors.length === 1) {
    throw errors[0];
  }
  if (errors.length > 1) {
    throw new AggregateError(errors, "Failed to destroy MongoDB module");
  }
}

export async function destroy() {
  PreventSchemaInitializations();
  const initializationErrors = await DrainSchemaInitializations();
  const disconnectErrors = await collectDisconnectErrors();
  throwDestroyErrors([...initializationErrors, ...disconnectErrors]);
}
