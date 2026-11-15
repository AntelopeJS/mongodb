import type { Collection, Document } from "mongodb";
import { CROSS_INSTANCE } from "@antelopejs/interface-database/schema";

import { INSTANCE_FIELD, normalizeInstanceId } from "./utils";

export type InstanceContext =
  | { kind: "scoped"; instanceId: string | null }
  | { kind: "cross" };

export function resolveInstanceContext(
  instanceId: string | typeof CROSS_INSTANCE | undefined,
): InstanceContext {
  if (instanceId === CROSS_INSTANCE) {
    return { kind: "cross" };
  }
  return { kind: "scoped", instanceId: normalizeInstanceId(instanceId) };
}

/**
 * Whether a change event belongs to `instanceId`: it carries the document after the change or
 * before it, and the instance field of each one it carries equals `instanceId`. A stored `null`
 * is the default instance. The document after an update is read when the event is, so it can be
 * a row another instance created since under the same `_id`: an update belongs to an instance
 * only with the document before it, which the write recorded. An event that carries no document
 * belongs to no instance.
 */
function ChangeOfInstance(instanceId: string | null): Document {
  const owns = (image: string) => ({
    $eq: [`${image}.${INSTANCE_FIELD}`, { $literal: instanceId }],
  });
  const hasAfter = { $eq: [{ $type: "$fullDocument" }, "object"] };
  const hasBefore = { $ne: ["$fullDocumentBeforeChange", "$documentKey"] };
  return {
    $and: [
      { $or: [hasAfter, hasBefore] },
      { $or: [{ $ne: ["$operationType", "update"] }, hasBefore] },
      { $or: [{ $not: [hasAfter] }, owns("$fullDocument")] },
      { $or: [{ $not: [hasBefore] }, owns("$fullDocumentBeforeChange")] },
    ],
  };
}

export function buildInitialPipeline(
  instance: InstanceContext,
  isChangeStream: boolean,
): Document[] {
  if (instance.kind !== "scoped") {
    return [];
  }
  if (isChangeStream) {
    return [{ $match: { $expr: ChangeOfInstance(instance.instanceId) } }];
  }
  return [{ $match: { [INSTANCE_FIELD]: instance.instanceId } }];
}

/**
 * Refuses a scoped change feed on a collection that records no pre-images. Its filter proves which
 * instance an update or a delete belongs to from the document before it, so without them the feed
 * would silently miss every update and delete. The module creates its collections with pre-images;
 * one created outside it, or with them turned off, needs a `collMod`, which the module does not run
 * because a `readWrite` account may not.
 */
export async function RequirePreImages(collection: Collection): Promise<void> {
  const name = collection.collectionName;
  const recording = await collection.db
    .listCollections(
      { name, "options.changeStreamPreAndPostImages.enabled": true },
      { nameOnly: false },
    )
    .toArray();
  if (recording.length === 0) {
    throw new Error(
      `changes() of one instance needs change stream pre-images, which collection ${name} does not record, so the feed would miss its updates and deletes: enable them with db.runCommand({ collMod: "${name}", changeStreamPreAndPostImages: { enabled: true } })`,
    );
  }
}
