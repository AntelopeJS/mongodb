import type { Document } from "mongodb";

/** What a scenario draws: the cursor's kind and what the server holds for it. */
export interface Source {
  kind: "table" | "changes";
  /** Rows of the table, or change events that arrive during the run. */
  items: number;
  batchSize: number;
}

/** Releases a step when the explorer picks it; `apply` runs it. */
export type Step = <Result>(apply: () => Result) => Promise<Result>;

/** A cursor id as the driver names it: `isZero` tells a cursor the server has finished. */
export class CursorId {
  readonly value: number;

  constructor(value: number) {
    this.value = value;
  }

  isZero(): boolean {
    return this.value === 0;
  }
}

interface ServerCursor {
  left: number;
  /** A getMore that waits for a change event, woken by an event or a kill. */
  waiting?: () => void;
}

interface Batch {
  docs: Document[];
  id: number;
}

/**
 * What the server holds and what the run asked of it.
 *
 * An in-memory MongoDB server and the driver cursor in front of it, as the
 * driver (6.21) and mongod 8.0.8 behave for the calls a query cursor makes
 * (see SKILL.md). Each request is two steps, which the explorer releases in
 * any order against the others: the server runs it, then its answer reaches
 * the client. A `close()` that lands between the two finds the cursor id
 * still unknown to the client, so it ends the session and kills nothing, and
 * the answer then names a live server cursor.
 */
export class Server {
  readonly cursors = new Map<number, ServerCursor>();
  readonly trace: string[] = [];
  /** The requests a query sent after its close was called. */
  readonly afterClose: string[] = [];
  closeCalled = false;
  private nextId = 1;
  private events = 0;
  readonly source: Source;
  step: Step = (apply) => Promise.resolve(apply());

  constructor(source: Source) {
    this.source = source;
  }

  /** The aggregate: opens a cursor, which holds the rest of the rows or waits for events. */
  aggregate(): Batch {
    const { kind, items, batchSize } = this.source;
    const first = kind === "table" ? Math.min(items, batchSize) : 0;
    const left = kind === "table" ? items - first : 0;
    const id = kind === "changes" || left > 0 ? this.nextId++ : 0;
    if (id !== 0) {
      this.cursors.set(id, { left });
    }
    this.trace.push(`server: aggregate opens cursor ${id}, ${first} rows`);
    return { docs: this.rows(first), id };
  }

  /** The getMore of a table cursor, or of a change stream that has an event; a stream with none waits. */
  getMore(id: number): Batch | "waits" {
    const cursor = this.cursors.get(id);
    if (!cursor) {
      throw new Error(`CursorNotFound: cursor id ${id} not found`);
    }
    if (this.source.kind === "changes") {
      if (this.events === 0) {
        return "waits";
      }
      this.events -= 1;
      this.trace.push(`server: getMore ${id} returns an event`);
      return { docs: this.rows(1), id };
    }
    const taken = Math.min(cursor.left, this.source.batchSize);
    cursor.left -= taken;
    const done = cursor.left === 0;
    if (done) {
      this.cursors.delete(id);
    }
    this.trace.push(`server: getMore ${id} returns ${taken} rows${done ? ", cursor done" : ""}`);
    return { docs: this.rows(taken), id: done ? 0 : id };
  }

  /** Parks a getMore of a change stream until an event arrives or the cursor is killed. */
  park(id: number, wake: () => void): void {
    const cursor = this.cursors.get(id);
    if (cursor) {
      cursor.waiting = wake;
    }
  }

  /** A change event arrives: it wakes the getMore that waits, if any. */
  event(): void {
    this.events += 1;
    this.trace.push("server: a change event arrives");
    for (const cursor of this.cursors.values()) {
      cursor.waiting?.();
      cursor.waiting = undefined;
    }
  }

  kill(ids: number[]): void {
    for (const id of ids) {
      const cursor = this.cursors.get(id);
      this.cursors.delete(id);
      cursor?.waiting?.();
      this.trace.push(`server: killCursors ${id}${cursor ? "" : ", already gone"}`);
    }
  }

  private rows(count: number): Document[] {
    return Array.from({ length: count }, (_, at) => ({ _id: `row-${at}` }));
  }
}

/** The driver cursor `collection.aggregate` returns. */
export class FakeCursor {
  private readonly server: Server;
  private known: CursorId | undefined;
  private started = false;
  private closed = false;
  private buffer: Document[] = [];

  constructor(server: Server) {
    this.server = server;
  }

  /** The id the driver learned from the server's answer; unknown while the first aggregate is in flight. */
  get id(): CursorId | undefined {
    return this.known;
  }

  get readPreference(): undefined {
    return undefined;
  }

  readBufferedDocuments(): Document[] {
    return this.buffer.splice(0);
  }

  /**
   * The next row; waits for one while the server holds the cursor open. A cursor closed before or
   * while its answer arrives ends at once, keeping the row that answer carried, if any.
   */
  async next(): Promise<Document | null> {
    while (this.buffer.length === 0 && !(this.closed && this.started)) {
      if (this.known?.isZero()) {
        return null;
      }
      await this.fetch();
      if (this.closed) {
        break;
      }
    }
    return this.buffer.shift() ?? null;
  }

  /** Ends the cursor: a known live id is killed, an unknown one cannot be, and the session ends either way. */
  async close(): Promise<void> {
    this.closed = true;
    const known = this.known;
    this.server.trace.push(`driver: close() with ${known ? `cursor ${known.value} known` : "no cursor id known"}`);
    if (known && !known.isZero()) {
      await this.server.step(() => this.server.kill([known.value]));
    }
  }

  private async fetch(): Promise<void> {
    const { server } = this;
    const first = !this.started;
    this.started = true;
    if (server.closeCalled) {
      server.afterClose.push(first ? "aggregate" : "getMore");
    }
    const known = this.known;
    server.trace.push(`driver: sends ${first ? "the aggregate" : "a getMore"}`);
    const batch = await server.step(() => (first ? server.aggregate() : this.runGetMore(known)));
    const answer = batch === "waits" ? await this.waitForEvent(known) : batch;
    await server.step(() => {
      this.known = new CursorId(answer.id);
      this.buffer = answer.docs;
      server.trace.push(
        `driver: the answer reaches the client, cursor ${answer.id}, ${answer.docs.length} rows${this.closed ? ", after its close" : ""}`,
      );
    });
  }

  private runGetMore(known: CursorId | undefined): Batch | "waits" {
    if (!known || known.isZero()) {
      return { docs: [], id: 0 };
    }
    return this.server.getMore(known.value);
  }

  private async waitForEvent(known: CursorId | undefined): Promise<Batch> {
    const id = known?.value ?? 0;
    await new Promise<void>((wake) => this.server.park(id, wake));
    return this.server.step(() => {
      if (!this.server.cursors.has(id)) {
        throw new Error("MongoServerError: Executor error during getMore :: caused by :: operation was interrupted");
      }
      const again = this.server.getMore(id);
      return again === "waits" ? { docs: [], id } : again;
    });
  }
}

/** The collection a query's aggregate and the `killCursors` command reach. */
export class FakeCollection {
  readonly collectionName = "formal__rows";
  readonly db: FakeDatabase;
  private readonly server: Server;

  constructor(server: Server) {
    this.server = server;
    this.db = new FakeDatabase(server);
  }

  aggregate(): FakeCursor {
    return new FakeCursor(this.server);
  }
}

/** The database the pre-image check and the `killCursors` command reach. */
export class FakeDatabase {
  private readonly server: Server;

  constructor(server: Server) {
    this.server = server;
  }

  listCollections() {
    return {
      toArray: async () =>
        this.server.step(() => {
          this.server.trace.push("server: listCollections answers the pre-image check");
          return [{ name: "formal__rows" }];
        }),
    };
  }

  async command(request: { killCursors: string; cursors: CursorId[] }): Promise<void> {
    this.server.trace.push("code: sends killCursors");
    await this.server.step(() => this.server.kill(request.cursors.map((id) => id.value)));
  }
}
