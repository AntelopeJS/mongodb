import { ImplementInterface } from "@antelopejs/interface-core";

import { IsExternal, type Options, SetOptions } from "./config";
import { Connect, Disconnect, EnsureBookkeepingCollection } from "./connection";
import {
  AllowSchemaInitializations,
  DrainSchemaInitializations,
  PreventSchemaInitializations,
} from "./schema-initialization";

export type { CatalogOptions, Mode, Options } from "./config";

export async function construct(options?: Options) {
  const resolved = SetOptions(options);
  await Connect(resolved.url, resolved.database, resolved.options);
  if (!IsExternal()) {
    await EnsureBookkeepingCollection();
  }
  AllowSchemaInitializations();

  ImplementInterface(
    await import("@antelopejs/interface-database/query"),
    await import("./implementations/database/query"),
  );
  ImplementInterface(
    await import("@antelopejs/interface-database/schema"),
    await import("./implementations/database/schema"),
  );
  ImplementInterface(
    await import("@antelopejs/interface-database-catalog"),
    await import("./implementations/catalog"),
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
