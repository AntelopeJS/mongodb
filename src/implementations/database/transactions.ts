import { AsyncLocalStorage } from "node:async_hooks";
import { type ClientSession, MongoServerError } from "mongodb";

import { GetClient } from "../../connection";

const TRANSACTION_TIMEOUT_MS = 30_000;
const MAX_COMMIT_ATTEMPTS = 3;
const UNKNOWN_COMMIT_RESULT = "UnknownTransactionCommitResult";

interface TransactionScope {
  isActive: boolean;
  session: ClientSession;
}

interface TransactionOperationOptions {
  session?: ClientSession;
}

interface MongoHello {
  msg?: string;
  setName?: string;
}

interface TransactionTimeout {
  promise: Promise<never>;
  timer: NodeJS.Timeout;
}

const transactionStorage = new AsyncLocalStorage<TransactionScope>();

function createTimeout(scope: TransactionScope): TransactionTimeout {
  let timer: NodeJS.Timeout;
  const promise = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      scope.isActive = false;
      reject(
        new Error(
          `MongoDB transaction timed out after ${TRANSACTION_TIMEOUT_MS}ms`,
        ),
      );
    }, TRANSACTION_TIMEOUT_MS);
    timer.unref();
  });
  return { promise, timer: timer! };
}

async function assertTransactionSupport(): Promise<void> {
  const client = await GetClient();
  const hello = (await client.db("admin").command({ hello: 1 })) as MongoHello;
  if (!hello.setName && hello.msg !== "isdbgrid") {
    throw new Error(
      "MongoDB transactions require a replica set or sharded cluster",
    );
  }
}

function canRetryCommit(error: unknown, attempt: number): boolean {
  return (
    attempt < MAX_COMMIT_ATTEMPTS &&
    error instanceof MongoServerError &&
    error.hasErrorLabel(UNKNOWN_COMMIT_RESULT)
  );
}

async function commit(scope: TransactionScope): Promise<void> {
  for (let attempt = 1; attempt <= MAX_COMMIT_ATTEMPTS; attempt += 1) {
    try {
      await scope.session.commitTransaction();
      return;
    } catch (error) {
      if (!canRetryCommit(error, attempt)) {
        throw error;
      }
    }
  }
}

async function abort(session: ClientSession): Promise<void> {
  if (session.inTransaction()) {
    await session.abortTransaction();
  }
}

export function GetTransactionOptions(): TransactionOperationOptions {
  const scope = transactionStorage.getStore();
  if (!scope) {
    return {};
  }
  if (!scope.isActive) {
    throw new Error("MongoDB transaction scope has expired");
  }
  return { session: scope.session };
}

export function AssertCursorAllowed(): void {
  if (transactionStorage.getStore()) {
    throw new Error(
      "MongoDB cursors and change streams are not supported in transactions",
    );
  }
}

export async function RunInTransaction<T>(
  callback: () => Promise<T>,
): Promise<T> {
  if (transactionStorage.getStore()) {
    throw new Error("Nested MongoDB transactions are not supported");
  }
  await assertTransactionSupport();
  const session = (await GetClient()).startSession();
  const scope: TransactionScope = { isActive: true, session };
  const timeout = createTimeout(scope);
  session.startTransaction({ maxCommitTimeMS: TRANSACTION_TIMEOUT_MS });
  try {
    const result = await Promise.race([
      transactionStorage.run(scope, callback),
      timeout.promise,
    ]);
    scope.isActive = false;
    await commit(scope);
    return result;
  } catch (error) {
    scope.isActive = false;
    await abort(session).catch(() => undefined);
    throw error;
  } finally {
    clearTimeout(timeout.timer);
    scope.isActive = false;
    await session.endSession();
  }
}
