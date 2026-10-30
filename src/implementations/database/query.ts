import assert from "node:assert";

import { isString, type QueryStage } from "./utils";
import { SelectionQuery } from "./selection";
import { RunAtomicMutation } from "./atomic";
import type { AggregationPipeline } from "./pipeline";
import { CreateInstance, DestroyInstance, ListInstances } from "./instances";
import { SchemaInitialized } from "../../schema-initialization";
import { IsExternal } from "../../config";
import {
  ExternalAtomicRefusal,
  ExternalInstanceRefusal,
} from "./external-refusals";

type LifecycleHandler = (
  schemaId: string,
  stage: QueryStage,
) => Promise<string | string[] | void>;

const LIFECYCLE_HANDLERS = new Map<string, LifecycleHandler>([
  [
    "createInstance",
    (schemaId, stage) => CreateInstance(schemaId, stage.options?.id),
  ],
  [
    "destroyInstance",
    (schemaId, stage) => DestroyInstance(schemaId, stage.options?.id),
  ],
  ["listInstances", (schemaId) => ListInstances(schemaId)],
]);

/**
 * The lifecycle stages on an external instance, whose collections have no instances: creating or
 * destroying one is refused rather than recorded nowhere, and the list of them is empty.
 */
const EXTERNAL_LIFECYCLE_HANDLERS = new Map<string, LifecycleHandler>([
  [
    "createInstance",
    (_schemaId, stage) => Promise.reject(ExternalInstanceRefusal(stage.stage)),
  ],
  [
    "destroyInstance",
    (_schemaId, stage) => Promise.reject(ExternalInstanceRefusal(stage.stage)),
  ],
  ["listInstances", () => Promise.resolve([])],
]);

function tryHandleLifecycle(
  stages: QueryStage[],
): ReturnType<LifecycleHandler> | undefined {
  if (stages.length !== 2 || stages[0]?.stage !== "schema") {
    return undefined;
  }
  const handler = (
    IsExternal() ? EXTERNAL_LIFECYCLE_HANDLERS : LIFECYCLE_HANDLERS
  ).get(stages[1].stage);
  if (!handler) {
    return undefined;
  }
  const schemaId: unknown = stages[0].options?.id;
  assert(isString(schemaId), "Lifecycle query missing schema id");
  return SchemaInitialized(schemaId).then(() => handler(schemaId, stages[1]));
}

async function schemaInitialized(stages: QueryStage[]): Promise<void> {
  if (stages[0]?.stage === "schema") {
    await SchemaInitialized(stages[0].options?.id);
  }
}

export async function RunQuery(stages: QueryStage[]) {
  if (stages.some((stage) => stage.stage === "atomicMutation")) {
    await schemaInitialized(stages);
    if (IsExternal()) {
      throw ExternalAtomicRefusal(stages[2]?.options?.id);
    }
    return RunAtomicMutation(stages);
  }
  const lifecycle = tryHandleLifecycle(stages);
  if (lifecycle) {
    return await lifecycle;
  }
  const query = await SelectionQuery.decode(stages);
  return await query.run();
}

interface OpenQuery {
  opening: Promise<AggregationPipeline>;
  query?: AggregationPipeline;
}

const openQueries: Record<number, OpenQuery> = {};

async function openQuery(
  reqId: number,
  stages: QueryStage[],
): Promise<AggregationPipeline | undefined> {
  const entry = openQueries[reqId] ?? {
    opening: SelectionQuery.decode(stages),
  };
  openQueries[reqId] = entry;
  try {
    const query = await entry.opening;
    if (openQueries[reqId] !== entry) {
      return undefined;
    }
    entry.query = query;
    return query;
  } catch (error) {
    if (openQueries[reqId] === entry) {
      delete openQueries[reqId];
    }
    throw error;
  }
}

export async function ReadCursor(reqId: number, stages: QueryStage[]) {
  assert(
    !stages.some((stage) => stage.stage === "atomicMutation"),
    "atomicMutation cannot be read as a cursor",
  );
  const query = await openQuery(reqId, stages);
  if (!query) {
    return { done: true, value: null };
  }
  try {
    const next = await query.readCursor();
    if (next === null) {
      delete openQueries[reqId];
    }

    return { done: next === null, value: next };
  } catch (error) {
    delete openQueries[reqId];
    try {
      await query.closeCursor();
    } catch {}
    throw error;
  }
}

export async function CloseCursor(reqId: number) {
  const query = openQueries[reqId]?.query;
  delete openQueries[reqId];
  await query?.closeCursor().catch(() => undefined);
}
