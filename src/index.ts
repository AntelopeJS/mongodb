import { ImplementInterface } from "@antelopejs/interface-core";
import type { MongoClientOptions } from "mongodb";
import { Connect, Disconnect, EnsureBookkeepingCollection } from "./connection";

export interface Options {
  url: string;
  database: string;
  options?: MongoClientOptions;
}

export async function construct(options: Options) {
  await Connect(options.url, options.database, options.options);
  await EnsureBookkeepingCollection();

  void ImplementInterface(
    await import("@antelopejs/interface-database/query"),
    await import("./implementations/database/query"),
  );
  void ImplementInterface(
    await import("@antelopejs/interface-database/schema"),
    await import("./implementations/database/schema"),
  );
}

export async function destroy() {
  await Disconnect();
}
