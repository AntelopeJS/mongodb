import { CROSS_INSTANCE } from "@antelopejs/interface-database/schema";

import type { ExternalCollection } from "./external-descriptor";

const UNSCOPED_INSTANCES: readonly (string | symbol | null | undefined)[] = [
  undefined,
  null,
  "",
  CROSS_INSTANCE,
];

/**
 * Refuses `atomicMutation`, whose contract needs a revision field the client's documents do not
 * have, and writes the adapter does not guard.
 */
export function ExternalAtomicRefusal(table: string | undefined): Error {
  return new Error(
    `The 'atomicMutation' stage is not supported on collection ${table}, an external collection: its contract writes revision fields the client's documents do not have`,
  );
}

/**
 * Refuses a `get` or `getAll` inside a `map` over an array: that path matches the raw documents of
 * the other collection, which an external read never answers.
 */
export function RefuseExternalArrayLookup(stream: { tableName: string }) {
  if ("external" in stream && stream.external !== undefined) {
    throw new Error(
      `A get or getAll on collection ${stream.tableName} inside a map over an array is not supported: it is an external collection, whose rows are read through their declaration. Use a lookup or a join instead`,
    );
  }
}

/** Refuses a feed, which needs change streams with pre-images the adapter does not configure. */
export function ExternalFeedRefusal(table: string): Error {
  return new Error(
    `changes() needs change streams with pre-images, which collection ${table} does not have: it is an external collection, and the adapter alters nothing on it`,
  );
}

/** Refuses an instance lifecycle stage, since an external collection has no instances to manage. */
export function ExternalInstanceRefusal(stage: string): Error {
  return new Error(
    `The '${stage}' stage manages instances, and an external instance's collections have none: read them through the default instance`,
  );
}

/**
 * The default instance and `CROSS_INSTANCE` both read every document of an external collection;
 * a named instance is refused rather than answered with documents nobody scoped to it.
 */
export function RefuseNamedInstance(
  table: string,
  id: string | symbol | null | undefined,
): void {
  if (!UNSCOPED_INSTANCES.includes(id)) {
    throw new Error(
      `Collection ${table} is an external collection and has no instances, so instance '${String(id)}' names nothing: read it through the default instance`,
    );
  }
}

/** Refuses reading by key a collection whose documents answer no `_id`. */
export function KeylessRefusal(collection: ExternalCollection): Error {
  return new Error(
    `Collection ${collection.collection} has no usable _id type class, so its documents have no _id and cannot be read by key: filter on a field, or getAll by a field name`,
  );
}

/** Refuses a write to a collection whose documents answer no `_id`, so cannot be addressed. */
export function KeylessWriteRefusal(collection: ExternalCollection): Error {
  return new Error(
    `Collection ${collection.collection} has no usable _id type class, so its documents cannot be written: it is read-only through this instance`,
  );
}

/** Refuses an update computed from the row, which the write-back guard cannot check. */
export function ExternalMapperRefusal(collection: ExternalCollection): Error {
  return new Error(
    `An update of collection ${collection.collection} computed from the row is not supported on an external collection: pass the new field values`,
  );
}
