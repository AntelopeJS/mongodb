import type { Document } from "mongodb";
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
