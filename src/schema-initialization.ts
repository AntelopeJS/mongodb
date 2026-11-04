import { Logging } from "@antelopejs/interface-core/logging";

type SchemaInitialization = (signal: AbortSignal) => Promise<void>;

interface InitializationTask {
  schemaId: string;
  initialize: SchemaInitialization;
  failedAttempts: number;
  runningAttempt?: AbortController;
  retryTimer?: NodeJS.Timeout;
  readiness: Readiness;
}

interface Readiness {
  state: "pending" | "failed" | "halted";
  promise: Promise<void>;
  resolve: () => void;
  reject: (error: Error) => void;
}

const INITIAL_RETRY_DELAY_MS = 1_000;
const MAX_RETRY_DELAY_MS = 30_000;
const RETRY_BACKOFF_FACTOR = 2;
const DRAIN_GRACE_PERIOD_MS = 2_000;

const incompleteTasks = new Map<string, InitializationTask>();
const runningAttempts = new Set<Promise<void>>();
const latestAttempts = new Map<string, Promise<void>>();
let isAcceptingInitializations = false;

export function AllowSchemaInitializations(): void {
  isAcceptingInitializations = true;
  for (const task of incompleteTasks.values()) {
    if (task.readiness.state === "halted") {
      task.readiness = pendingReadiness();
    }
    if (!task.runningAttempt && !task.retryTimer) {
      runAttempt(task);
    }
  }
}

export function PreventSchemaInitializations(): void {
  isAcceptingInitializations = false;
  incompleteTasks.forEach(haltTask);
}

/**
 * Starts the initialization of a schema, superseding the one under way. The
 * new build starts once the superseded attempt has settled, so two builds
 * never run on the same collection at once: a stale snapshot of the indexes
 * would otherwise drop or recreate indexes the new build just made.
 */
export function StartSchemaInitialization(
  schemaId: string,
  initialize: SchemaInitialization,
): boolean {
  if (!isAcceptingInitializations) {
    return false;
  }
  CancelSchemaInitialization(schemaId);
  const task: InitializationTask = {
    schemaId,
    initialize,
    failedAttempts: 0,
    readiness: pendingReadiness(),
  };
  incompleteTasks.set(schemaId, task);
  runAttempt(task);
  return true;
}

export function CancelSchemaInitialization(schemaId: string): void {
  const task = incompleteTasks.get(schemaId);
  if (!task) {
    return;
  }
  haltTask(task);
  incompleteTasks.delete(schemaId);
}

/**
 * Resolves once the schema's initialization has built its indexes, so a query
 * that runs after it sees every unique index of the schema. Rejects, naming
 * the failure, from the first failed attempt until a retry succeeds: a query
 * never runs without the indexes nor waits through the retries. Rejects too
 * when the initialization is halted by an unregister or a stop; a start
 * gates the schema again until the resumed build ends. Resolves at once for
 * a schema with no initialization under way.
 */
export function SchemaInitialized(schemaId: string): Promise<void> {
  return incompleteTasks.get(schemaId)?.readiness.promise ?? Promise.resolve();
}

function pendingReadiness(): Readiness {
  let resolve!: () => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<void>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  promise.catch(() => undefined);
  return { state: "pending", promise, resolve, reject };
}

function refuseQueries(
  task: InitializationTask,
  state: Exclude<Readiness["state"], "pending">,
  reason: string,
): void {
  const isPending = task.readiness.state === "pending";
  if (state === "failed" && !isPending) {
    return;
  }
  task.readiness.state = state;
  if (isPending) {
    task.readiness.reject(
      new Error(`Schema "${task.schemaId}" is not initialized: ${reason}`),
    );
  }
}

export async function DrainSchemaInitializations(): Promise<void> {
  let graceTimer: NodeJS.Timeout | undefined;
  const gracePeriod = new Promise<boolean>((resolve) => {
    graceTimer = setTimeout(() => resolve(false), DRAIN_GRACE_PERIOD_MS);
  });
  const isDrained = await Promise.race([
    settleRunningAttempts().then(() => true),
    gracePeriod,
  ]);
  clearTimeout(graceTimer);
  if (!isDrained) {
    Logging.Warn(
      `Schema initialization still running after ${DRAIN_GRACE_PERIOD_MS}ms, abandoning it until the module starts again`,
    );
  }
}

async function settleRunningAttempts(): Promise<void> {
  while (runningAttempts.size) {
    await Promise.all(runningAttempts);
  }
}

function isCurrentTask(task: InitializationTask): boolean {
  return incompleteTasks.get(task.schemaId) === task;
}

function clearRetry(task: InitializationTask): void {
  clearTimeout(task.retryTimer);
  task.retryTimer = undefined;
}

function haltTask(task: InitializationTask): void {
  clearRetry(task);
  task.runningAttempt?.abort();
  refuseQueries(task, "halted", "its initialization was halted");
}

function runAttempt(task: InitializationTask): void {
  const controller = new AbortController();
  task.runningAttempt = controller;
  const previousAttempt =
    latestAttempts.get(task.schemaId) ?? Promise.resolve();
  let attempt!: Promise<void>;
  attempt = previousAttempt
    .then(() => task.initialize(controller.signal))
    .then(
      () => handleSuccess(task),
      (error: unknown) => {
        task.runningAttempt = undefined;
        if (!isCurrentTask(task)) {
          return;
        }
        if (controller.signal.aborted && error === controller.signal.reason) {
          resumeIfAccepting(task);
          return;
        }
        task.failedAttempts++;
        const message = error instanceof Error ? error.message : String(error);
        refuseQueries(task, "failed", message);
        retryAfterFailure(task, message);
      },
    )
    .finally(() => {
      runningAttempts.delete(attempt);
      if (latestAttempts.get(task.schemaId) === attempt) {
        latestAttempts.delete(task.schemaId);
      }
    });
  runningAttempts.add(attempt);
  latestAttempts.set(task.schemaId, attempt);
}

function handleSuccess(task: InitializationTask): void {
  task.runningAttempt = undefined;
  if (!isCurrentTask(task)) {
    return;
  }
  incompleteTasks.delete(task.schemaId);
  task.readiness.resolve();
  if (task.failedAttempts > 0) {
    Logging.Info(
      `Schema "${task.schemaId}" initialized after ${task.failedAttempts} failed attempt(s)`,
    );
  }
}

function retryAfterFailure(task: InitializationTask, message: string): void {
  const failure = `Schema "${task.schemaId}" initialization attempt ${task.failedAttempts} failed: ${message}`;
  if (!isAcceptingInitializations) {
    Logging.Warn(`${failure}. Retrying when the module starts again`);
    return;
  }
  const delay = getRetryDelay(task.failedAttempts);
  Logging.Warn(`${failure}. Retrying in ${delay}ms`);
  scheduleRetry(task, delay);
}

function resumeIfAccepting(task: InitializationTask): void {
  if (isAcceptingInitializations) {
    runAttempt(task);
  }
}

function getRetryDelay(failedAttempts: number): number {
  const delay =
    INITIAL_RETRY_DELAY_MS * RETRY_BACKOFF_FACTOR ** (failedAttempts - 1);
  return Math.min(delay, MAX_RETRY_DELAY_MS);
}

function scheduleRetry(task: InitializationTask, delay: number): void {
  task.retryTimer = setTimeout(() => {
    task.retryTimer = undefined;
    runAttempt(task);
  }, delay);
  task.retryTimer.unref();
}
