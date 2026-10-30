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

export function buildInitialPipeline(
  instance: InstanceContext,
  isChangeStream: boolean,
): Document[] {
  if (instance.kind !== "scoped") {
    return [];
  }
  if (isChangeStream) {
    return [
      {
        $match: {
          $expr: {
            $eq: [
              {
                $ifNull: [
                  `$fullDocument.${INSTANCE_FIELD}`,
                  `$fullDocumentBeforeChange.${INSTANCE_FIELD}`,
                ],
              },
              { $literal: instance.instanceId },
            ],
          },
        },
      },
    ];
  }
  return [{ $match: { [INSTANCE_FIELD]: instance.instanceId } }];
}
