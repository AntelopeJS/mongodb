import type { AnyBulkWriteOperation, Collection } from "mongodb";

import { PrimaryKeyDuplicateAt } from "./indexes";

/**
 * Runs ordered upserts to the end. An upsert that loses an `_id` race to a
 * concurrent insert resumes from that operation, which then matches the
 * winner's row. An operation refused twice in a row throws: its `_id` is held
 * by another instance, or was deleted and inserted again between the attempts.
 */
export async function UpsertAll(
  collection: Collection,
  operations: AnyBulkWriteOperation[],
): Promise<void> {
  let start = 0;
  let refused: number | undefined;
  while (start < operations.length) {
    try {
      await collection.bulkWrite(operations.slice(start));
      return;
    } catch (error) {
      const at = PrimaryKeyDuplicateAt(error);
      if (at === undefined || start + at === refused) {
        throw error;
      }
      refused = start + at;
      start = refused;
    }
  }
}
