import { Logging } from "@antelopejs/interface-core/logging";

type SchemaInitialization = () => Promise<void>;

interface InitializationTask {
  schemaId: string;
  initialize: SchemaInitialization;
  failedAttempts: number;
  isRunning: boolean;
  retryTimer?: NodeJS.Timeout;
}

const INITIAL_RETRY_DELAY_MS = 1_000;
const MAX_RETRY_DELAY_MS = 30_000;
const RETRY_BACKOFF_FACTOR = 2;

const incompleteTasks = new Map<string, InitializationTask>();
const runningAttempts = new Set<Promise<void>>();
let isAcceptingInitializations = false;

export function AllowSchemaInitializations(): void {
  isAcceptingInitializations = true;
  for (const task of incompleteTasks.values()) {
    if (!task.isRunning && !task.retryTimer) {
      runAttempt(task);
    }
  }
}

export function PreventSchemaInitializations(): void {
  isAcceptingInitializations = false;
  incompleteTasks.forEach(clearRetry);
}

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
    isRunning: false,
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
  clearRetry(task);
  incompleteTasks.delete(schemaId);
}

export async function DrainSchemaInitializations(): Promise<void> {
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

function runAttempt(task: InitializationTask): void {
  task.isRunning = true;
  let attempt!: Promise<void>;
  attempt = Promise.resolve()
    .then(task.initialize)
    .then(
      () => handleSuccess(task),
      (error: unknown) => handleFailure(task, error),
    )
    .finally(() => runningAttempts.delete(attempt));
  runningAttempts.add(attempt);
}

function handleSuccess(task: InitializationTask): void {
  task.isRunning = false;
  if (!isCurrentTask(task)) {
    return;
  }
  incompleteTasks.delete(task.schemaId);
  if (task.failedAttempts > 0) {
    Logging.Info(
      `Schema "${task.schemaId}" initialized after ${task.failedAttempts} failed attempt(s)`,
    );
  }
}

function handleFailure(task: InitializationTask, error: unknown): void {
  task.isRunning = false;
  if (!isCurrentTask(task)) {
    return;
  }
  task.failedAttempts++;
  const message = error instanceof Error ? error.message : String(error);
  const failure = `Schema "${task.schemaId}" initialization attempt ${task.failedAttempts} failed: ${message}`;
  if (!isAcceptingInitializations) {
    Logging.Warn(`${failure}. Retrying when the module starts again`);
    return;
  }
  const delay = getRetryDelay(task.failedAttempts);
  Logging.Warn(`${failure}. Retrying in ${delay}ms`);
  scheduleRetry(task, delay);
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
