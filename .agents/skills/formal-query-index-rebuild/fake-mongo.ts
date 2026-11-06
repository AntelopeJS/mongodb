/**
 * An in-memory MongoDB database that answers the driver calls
 * `InitializeSchema` (src/connection.ts) makes, with the error codes mongod
 * 8.0.8 returns. Each call is one step: it takes effect in one piece when
 * `step` releases it, so the check can put the steps of concurrent builds
 * and queries in any order.
 */
import { AsyncLocalStorage } from "node:async_hooks";
import { MongoServerError } from "mongodb";

const NAMESPACE_NOT_FOUND = 26;
const INDEX_NOT_FOUND = 27;
const NAMESPACE_EXISTS = 48;
const INDEX_OPTIONS_CONFLICT = 85;
const INDEX_KEY_SPECS_CONFLICT = 86;

/** One index of a collection: its name, its key fields in order, and whether it is unique. */
export interface PhysicalIndex {
  name: string;
  key: string[];
  unique: boolean;
}

/** The indexes of every collection, by collection name. */
export type Snapshot = Map<string, PhysicalIndex[]>;

/** Releases a step when the explorer picks it; `apply` runs it. */
export type Step = <Result>(label: string, apply: () => Result) => Promise<Result>;

/** The actor whose code makes a driver call: a registration's build or a query. */
export const actor = new AsyncLocalStorage<string>();

/** Key patterns compare as their field lists. */
export function keyOf(fields: string[]): string {
  return fields.join(",");
}

function serverError(code: number, message: string): MongoServerError {
  return new MongoServerError({ code, message, errmsg: message });
}

function describeIndex(index: PhysicalIndex): string {
  return `${index.name} {${keyOf(index.key)}}${index.unique ? " unique" : ""}`;
}

function primaryKeyIndex(): PhysicalIndex {
  return { name: "_id_", key: ["_id"], unique: false };
}

/**
 * Creates an index as mongod 8.0.8 does: a same-name index with another
 * spec is error 86, a same-key index with the same options under another
 * name is error 85, and a same-key index that differs in uniqueness is kept
 * beside the new one.
 */
function createIndex(indexes: PhysicalIndex[], wanted: PhysicalIndex): void {
  const sameName = indexes.find((index) => index.name === wanted.name);
  if (sameName) {
    if (
      keyOf(sameName.key) === keyOf(wanted.key) &&
      sameName.unique === wanted.unique
    ) {
      return;
    }
    throw serverError(
      INDEX_KEY_SPECS_CONFLICT,
      `An existing index has the same name as the requested index: ${describeIndex(sameName)}`,
    );
  }
  const sameOptions = indexes.find(
    (index) =>
      keyOf(index.key) === keyOf(wanted.key) && index.unique === wanted.unique,
  );
  if (sameOptions) {
    throw serverError(
      INDEX_OPTIONS_CONFLICT,
      `Index already exists with a different name: ${sameOptions.name}`,
    );
  }
  indexes.push(wanted);
}

interface IndexDocument {
  v: number;
  key: Record<string, number>;
  name: string;
  unique?: boolean;
}

function indexDocument(index: PhysicalIndex): IndexDocument {
  const document: IndexDocument = {
    v: 2,
    key: Object.fromEntries(index.key.map((field) => [field, 1])),
    name: index.name,
  };
  if (index.unique) {
    document.unique = true;
  }
  return document;
}

/**
 * One database of the fake server. `step` decides when a call takes
 * effect: at once while the starting state is built, and when the explorer
 * releases it while a scenario runs.
 */
export class FakeDatabase {
  readonly collections: Snapshot = new Map();
  readonly trace: string[] = [];
  step: Step = (_label, apply) => Promise.resolve().then(apply);

  /** Runs a step labelled with its actor and records it in the trace. */
  call<Result>(label: string, apply: () => Result): Promise<Result> {
    const line = `${actor.getStore() ?? "setup"}: ${label}`;
    return this.step(line, () => {
      try {
        const result = apply();
        this.trace.push(line);
        return result;
      } catch (error) {
        const code =
          error instanceof MongoServerError ? String(error.code) : "thrown";
        this.trace.push(`${line} -> error ${code}`);
        throw error;
      }
    });
  }

  /** A copy of the indexes of every collection. */
  snapshot(): Snapshot {
    return new Map(
      [...this.collections].map(([name, indexes]) => [
        name,
        indexes.map((index) => ({ ...index, key: [...index.key] })),
      ]),
    );
  }

  listCollections() {
    return {
      toArray: () =>
        this.call("listCollections", () =>
          [...this.collections.keys()].map((name) => ({ name })),
        ),
    };
  }

  createCollection(name: string): Promise<void> {
    return this.call(`createCollection ${name}`, () => {
      if (this.collections.has(name)) {
        throw serverError(NAMESPACE_EXISTS, `Collection ${name} already exists`);
      }
      this.collections.set(name, [primaryKeyIndex()]);
    });
  }

  command(command: { collMod: string }): Promise<{ ok: number }> {
    return this.call(`collMod ${command.collMod}`, () => ({ ok: 1 }));
  }

  collection(name: string) {
    return {
      indexes: () =>
        this.call(`${name} indexes`, () => {
          const indexes = this.collections.get(name);
          if (!indexes) {
            throw serverError(NAMESPACE_NOT_FOUND, `ns ${name} does not exist`);
          }
          return indexes.map(indexDocument);
        }),
      createIndex: (fields: string[], options: { name: string; unique?: boolean }) => {
        const wanted = {
          name: options.name,
          key: [...fields],
          unique: options.unique === true,
        };
        return this.call(`${name} createIndex ${describeIndex(wanted)}`, () => {
          const indexes = this.collections.get(name) ?? [primaryKeyIndex()];
          this.collections.set(name, indexes);
          createIndex(indexes, wanted);
          return wanted.name;
        });
      },
      dropIndex: (indexName: string) =>
        this.call(`${name} dropIndex ${indexName}`, () => {
          const indexes = this.collections.get(name) ?? [];
          const at = indexes.findIndex((index) => index.name === indexName);
          if (at < 0) {
            throw serverError(
              INDEX_NOT_FOUND,
              `index not found with name [${indexName}]`,
            );
          }
          indexes.splice(at, 1);
        }),
    };
  }
}
