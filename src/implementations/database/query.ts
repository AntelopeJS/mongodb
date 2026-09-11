import assert from "node:assert";

import type { QueryStage } from "./utils";
import { SelectionQuery } from "./selection";
import type { AggregationPipeline } from "./pipeline";
import { CreateInstance, DestroyInstance, ListInstances } from "./instances";

const LIFECYCLE_HANDLERS: Record<
  string,
  (schemaId: string, stage: QueryStage) => Promise<unknown>
> = {
  createInstance: (schemaId, stage) =>
    CreateInstance(schemaId, stage.options?.id),
  destroyInstance: (schemaId, stage) =>
    DestroyInstance(schemaId, stage.options?.id),
  listInstances: (schemaId) => ListInstances(schemaId),
};

function tryHandleLifecycle(
  stages: QueryStage[],
): Promise<unknown> | undefined {
  if (stages.length !== 2 || stages[0]?.stage !== "schema") {
    return undefined;
  }
  const handler = LIFECYCLE_HANDLERS[stages[1].stage];
  if (!handler) {
    return undefined;
  }
  const schemaId = stages[0].options?.id;
  assert(typeof schemaId === "string", "Lifecycle query missing schema id");
  return handler(schemaId, stages[1]);
}

export async function RunQuery(stages: QueryStage[]) {
  const lifecycle = tryHandleLifecycle(stages);
  if (lifecycle) {
    return await lifecycle;
  }
  const query = await SelectionQuery.decode(stages);
  return await query.run();
}

const openQueries: Record<number, AggregationPipeline> = {};
export async function ReadCursor(reqId: number, stages: QueryStage[]) {
  if (!(reqId in openQueries)) {
    const query = await SelectionQuery.decode(stages);
    openQueries[reqId] = query;
  }
  try {
    const next = await openQueries[reqId].readCursor();
    if (next === null) {
      delete openQueries[reqId];
    }

    return { done: next === null, value: next };
  } catch (error) {
    const query = openQueries[reqId];
    delete openQueries[reqId];
    try {
      await query?.closeCursor();
    } catch {
      // Ignore close failures; the read error is the one worth surfacing.
    }
    throw error;
  }
}

export async function CloseCursor(reqId: number) {
  if (reqId in openQueries) {
    const query = openQueries[reqId];
    delete openQueries[reqId];
    try {
      await query.closeCursor();
    } catch {
      // Ignore close failures; the entry is already evicted.
    }
  }
}
