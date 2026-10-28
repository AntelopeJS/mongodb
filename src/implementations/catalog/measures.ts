import type {
  Measure,
  SampleInfo,
} from "@antelopejs/interface-database-catalog";
import {
  type Collection,
  type Document,
  MongoServerError,
  ReadPreference,
} from "mongodb";

import { GetDatabase } from "../../connection";

const DEFAULT_MEASURE_TIMEOUT_MS = 5000;
const MAX_TIME_EXCEEDED_CODE = 50;
const PERCENT = 100;

/** Above this estimate a relation is sampled rather than read whole. */
const SAMPLE_THRESHOLD_ROWS = 1_000_000;

/** How many documents a sample of a large collection holds: `$sample` uses its random cursor below 5%. */
const SAMPLE_SIZE = 50_000;

let measureTimeoutMs = DEFAULT_MEASURE_TIMEOUT_MS;

/**
 * Overrides the per-measure time limit, for the suite that proves a slow measure answers
 * `timeout`; no configuration reaches it.
 */
export function SetMeasureTimeoutForTests(ms?: number): void {
  measureTimeoutMs = ms ?? DEFAULT_MEASURE_TIMEOUT_MS;
}

/** The documents a measure reads: the whole collection, or a `$sample` of a large one. */
export interface Source {
  collection: Collection;
  stages: Document[];
  sample: SampleInfo | null;
  estimatedRows: number | null;
}

/** A collection read for measures: from a secondary when the deployment has one. */
export async function MeasuredCollection(name: string): Promise<Collection> {
  return (await GetDatabase()).collection(name, {
    readPreference: ReadPreference.secondaryPreferred,
  });
}

export async function EstimatedRows(
  collection: Collection,
): Promise<number | null> {
  try {
    return await collection.estimatedDocumentCount({
      maxTimeMS: DEFAULT_MEASURE_TIMEOUT_MS,
    });
  } catch {
    return null;
  }
}

/** The documents §3 reads: the whole collection under the threshold, else a `$sample`. */
export async function SourceFor(name: string): Promise<Source> {
  const collection = await MeasuredCollection(name);
  const estimatedRows = await EstimatedRows(collection);
  if (estimatedRows === null || estimatedRows <= SAMPLE_THRESHOLD_ROWS) {
    return { collection, stages: [], sample: null, estimatedRows };
  }
  return {
    collection,
    stages: [{ $sample: { size: SAMPLE_SIZE } }],
    sample: {
      percent: (SAMPLE_SIZE / estimatedRows) * PERCENT,
      rows: SAMPLE_SIZE,
    },
    estimatedRows,
  };
}

function Failed<T>(error: Error): Measure<T> {
  if (error instanceof MongoServerError) {
    return Number(error.code) === MAX_TIME_EXCEEDED_CODE
      ? { status: "timeout" }
      : { status: "error", code: String(error.codeName ?? error.code) };
  }
  throw error;
}

async function Aggregated<T extends Document>(
  source: Source,
  stages: Document[],
  maxTimeMS: number,
): Promise<Measure<T[]>> {
  try {
    const value = await source.collection
      .aggregate<T>([...source.stages, ...stages], {
        maxTimeMS,
        allowDiskUse: false,
      })
      .toArray();
    return { status: "ok", value };
  } catch (error) {
    if (error instanceof Error) {
      return Failed(error);
    }
    throw error;
  }
}

/** One profile measure over a source, bounded in time, never spilling to disk. */
export function Measured<T extends Document>(
  source: Source,
  stages: Document[],
): Promise<Measure<T[]>> {
  return Aggregated<T>(source, stages, measureTimeoutMs);
}

/** One read of a description, bounded by the default time limit whatever a test sets. */
export function Described<T extends Document>(
  source: Source,
  stages: Document[],
): Promise<Measure<T[]>> {
  return Aggregated<T>(source, stages, DEFAULT_MEASURE_TIMEOUT_MS);
}

export function FirstRow<T, R>(
  measured: Measure<T[]>,
  read: (row: T | undefined) => R,
): Measure<R> {
  return measured.status === "ok"
    ? { status: "ok", value: read(measured.value[0]) }
    : measured;
}

export function Mapped<T, R>(
  measured: Measure<T>,
  read: (value: T) => R,
): Measure<R> {
  return measured.status === "ok"
    ? { status: "ok", value: read(measured.value) }
    : measured;
}

export function ValueOf<T>(measured: Measure<T> | undefined): T | undefined {
  return measured?.status === "ok" ? measured.value : undefined;
}

/** Runs `work` over `items` one after another, never two reads at once. */
export async function Sequentially<T, R>(
  items: T[],
  work: (item: T) => Promise<R>,
): Promise<R[]> {
  const results: R[] = [];
  for (const item of items) {
    results.push(await work(item));
  }
  return results;
}

let queue: Promise<void> = Promise.resolve();

/** Runs one catalog call at a time per instance, so profiling never fans out over the client's pool. */
export function Serialized<T>(work: () => Promise<T>): Promise<T> {
  const run = queue.then(work, work);
  queue = run.then(
    () => undefined,
    () => undefined,
  );
  return run;
}
