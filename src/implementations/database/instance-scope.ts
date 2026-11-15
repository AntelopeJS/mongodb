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
 * Whether a change event belongs to `instanceId`: the instance field of the document after the
 * change, or before it when the change left none (a delete), equals it. A stored `null` is the
 * default instance; an event whose document is unknown, with no image, belongs to no instance.
 */
function ChangeOfInstance(instanceId: string | null): Document {
  return {
    $eq: [
      {
        $let: {
          vars: {
            image: {
              $cond: [
                { $eq: [{ $type: "$fullDocument" }, "object"] },
                "$fullDocument",
                "$fullDocumentBeforeChange",
              ],
            },
          },
          in: `$$image.${INSTANCE_FIELD}`,
        },
      },
      { $literal: instanceId },
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
