import { AsyncLocalStorage } from "node:async_hooks";
import {
  type AnyBulkWriteOperation,
  type Document,
  MongoBulkWriteError,
  type WriteError,
} from "mongodb";

/**
 * An in-memory collection that answers the one driver call a conflict insert
 * makes, `bulkWrite` of ordered upserts, as mongod does when the filter is
 * `{ _id, _instance }`: the server never retries a duplicate key there, since
 * the filter does not cover the key of the unique `_id_` index that refused
 * it. An upsert is two steps: it looks for a row that matches, then writes.
 * The write of an upsert that found none inserts and meets E11000 when any
 * row holds the `_id` by then. Each step takes effect in one piece when the
 * explorer releases it.
 */
const DUPLICATE_KEY_CODE = 11000;

/** The instance the writers of a scenario insert into. */
export const OWN = "tenant";
/** The instance that holds rows the writers must never touch. */
export const FOREIGN = "other";

/** A stored document: the writers set one boolean field each. */
export type Row = Record<string, string | boolean>;

/** What happened to a row, in the order the steps ran. */
export type Event =
  | { kind: "apply"; writer: string; id: string }
  | { kind: "remove"; id: string }
  | { kind: "foreign"; id: string };

/** Releases a step when the explorer picks it; `apply` runs it. */
export type Step = <Result>(apply: () => Result) => Promise<Result>;

/** The actor whose code makes a driver call: a writer, by name. */
export const actor = new AsyncLocalStorage<string>();

/** What a plain insert throws when a row already holds the `_id`. */
export class HeldId extends Error {}

/** An upsert `UpsertAll` sent: the filter that picks its row and the row it writes over a base. */
interface Upsert {
  filter: Row;
  make: (base: Row) => Row;
}

/** The rows, the events and the trace of one run. */
export class Store {
  rows: Row[] = [];
  readonly events: Event[] = [];
  readonly trace: string[] = [];
  /** The `_id` each writer's last refused operation held, by writer. */
  readonly refused = new Map<string, string>();
  step: Step = (apply) => Promise.resolve(apply());

  find(id: string, instance?: string): Row | undefined {
    return instance === undefined
      ? this.match({ _id: id })
      : this.match({ _id: id, _instance: instance });
  }

  /** The first row whose fields equal every field of `filter`. */
  match(filter: Row): Row | undefined {
    return this.rows.find((row) => Object.entries(filter).every(([name, value]) => row[name] === value));
  }

  /** A plain insert: throws `HeldId` when any row holds the `_id`. */
  insert(row: Row): void {
    if (this.rows.some((held) => held._id === row._id)) {
      throw new HeldId();
    }
    this.rows.push(row);
  }

  remove(id: string, instance: string): void {
    this.rows = this.rows.filter((row) => !(row._id === id && row._instance === instance));
    this.events.push({ kind: "remove", id });
  }

  /** The write step of an upsert that found its row: updates it, or reports that a delete took it. */
  update(writer: string, upsert: Upsert): boolean {
    const row = this.match(upsert.filter);
    if (!row) {
      return false;
    }
    this.rows = this.rows.map((each) => (each === row ? upsert.make(row) : each));
    this.events.push({ kind: "apply", writer, id: String(upsert.filter._id) });
    return true;
  }

  /** The write step of an upsert that found no row to update: inserts, or is refused when a row holds the `_id`. */
  upsertInsert(writer: string, upsert: Upsert): boolean {
    try {
      this.insert(upsert.make({ ...upsert.filter }));
    } catch (error) {
      if (!(error instanceof HeldId)) {
        throw error;
      }
      this.refused.set(writer, String(upsert.filter._id));
      return false;
    }
    this.events.push({ kind: "apply", writer, id: String(upsert.filter._id) });
    return true;
  }
}

interface ReplaceStage {
  $replaceWith: { $mergeObjects: unknown };
}

interface LiteralOf {
  $literal: Row;
}

function isRow<Value>(value: Value): value is Value & Row {
  return typeof value === "object" && value !== null;
}

function isReplaceStage<Value>(value: Value): value is Value & ReplaceStage {
  return (
    isRow(value) &&
    "$replaceWith" in value &&
    isRow(value.$replaceWith) &&
    "$mergeObjects" in value.$replaceWith
  );
}

function isLiteralOf<Value>(value: Value): value is Value & LiteralOf {
  return isRow(value) && "$literal" in value && isRow(value.$literal);
}

/** The document an update pipeline `$replaceWith`s over the row: `[{ $replaceWith: { $mergeObjects: ["$$ROOT", { $literal: doc }] } }]`. */
function mergedLiteral(update: Document | Document[]): Row {
  const stage: unknown = Array.isArray(update) ? update[0] : undefined;
  const merged: unknown = isReplaceStage(stage) ? stage.$replaceWith.$mergeObjects : undefined;
  const literal: unknown = Array.isArray(merged) ? merged[1] : undefined;
  if (!isLiteralOf(literal)) {
    throw new Error(`unsupported update pipeline: ${JSON.stringify(update)}`);
  }
  return literal.$literal;
}

/** The filter of an upsert: equality on `_id` and on the other fields it names. */
function equalities(filter: Document): Row {
  const row: unknown = filter;
  if (!isRow(row) || !("_id" in row)) {
    throw new Error(`unsupported upsert filter: ${JSON.stringify(filter)}`);
  }
  return row;
}

/** Reads the `updateOne` and `replaceOne` upserts `UpsertAll` receives; any other operation stops the check. */
function parse(operation: AnyBulkWriteOperation): Upsert {
  if ("updateOne" in operation && operation.updateOne.upsert) {
    const literal = mergedLiteral(operation.updateOne.update);
    return { filter: equalities(operation.updateOne.filter), make: (base) => ({ ...base, ...literal }) };
  }
  if ("replaceOne" in operation && operation.replaceOne.upsert) {
    const { filter, replacement } = operation.replaceOne;
    const row = { ...replacement };
    return { filter: equalities(filter), make: () => (isRow(row) ? row : {}) };
  }
  throw new Error(`unsupported operation: ${JSON.stringify(operation)}`);
}

function isBulkWriteError(value: unknown): value is MongoBulkWriteError {
  return value instanceof MongoBulkWriteError;
}

/** The error the driver throws for a bulk write refused at operation `index` of the batch sent. */
function duplicateKeyError(index: number, id: string): MongoBulkWriteError {
  const message = `E11000 duplicate key error collection: formal.rows index: _id_ dup key: { _id: "${id}" }`;
  const err = { code: DUPLICATE_KEY_CODE, index, errmsg: message, errInfo: {}, op: {} };
  const writeError: WriteError = {
    err,
    ...err,
    getOperation: () => err.op,
    toJSON: () => err,
    toString: () => `WriteError(${message})`,
  };
  const error: unknown = Reflect.construct(MongoBulkWriteError, [
    { message, code: DUPLICATE_KEY_CODE, writeErrors: [writeError] },
    {},
  ]);
  if (!isBulkWriteError(error)) {
    throw new Error("the driver built no MongoBulkWriteError");
  }
  return error;
}

/** The collection a writer's `bulkWrite` reaches. */
export class FakeCollection {
  private readonly store: Store;

  constructor(store: Store) {
    this.store = store;
  }

  /** Runs the upserts in order and stops at the first refused one, as an ordered bulk write does. */
  async bulkWrite(operations: AnyBulkWriteOperation[]): Promise<void> {
    const writer = actor.getStore() ?? "unknown writer";
    const { store } = this;
    for (const [index, operation] of operations.entries()) {
      const upsert = parse(operation);
      const id = String(upsert.filter._id);
      const matched = await store.step(() => {
        const found = store.match(upsert.filter) !== undefined;
        store.trace.push(`${writer}: looks for ${id}, ${found ? "finds its row" : "finds none"}`);
        return found;
      });
      const written = await store.step(() => {
        const done = (matched && store.update(writer, upsert)) || store.upsertInsert(writer, upsert);
        store.trace.push(`${writer}: writes ${id}${done ? "" : ", E11000 on _id_"}`);
        return done;
      });
      if (!written) {
        throw duplicateKeyError(index, id);
      }
    }
  }
}
