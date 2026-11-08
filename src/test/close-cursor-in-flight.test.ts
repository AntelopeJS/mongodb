import assert from "node:assert/strict";
import type { QueryStage } from "@antelopejs/interface-database/common";
import { Schema } from "@antelopejs/interface-database/schema";
import type { Long } from "mongodb";
import { internal as mongoInternal } from "@antelopejs/interface-mongodb";

import * as connection from "../connection";
import { CloseCursor, ReadCursor } from "../implementations/database/query";
import { collectionName } from "../implementations/database/utils";

interface Row {
  _id: string;
}

const SCHEMA_ID = "close-cursor-in-flight";
const TABLE = "rows";
const INSTANCE = "t";
const ROW_COUNT = 200;
const AGGREGATE_DELAY_MS = 500;
const CLOSE_AFTER_MS = 100;
const SETTLE_TIMEOUT_MS = 3000;
const REQ_ID = 7300;

let schema: Schema<{ rows: Row }>;

async function setAggregateDelay(enabled: boolean): Promise<void> {
  const client = await mongoInternal.client;
  await client.db("admin").command({
    configureFailPoint: "failCommand",
    mode: enabled ? { times: 1 } : "off",
    data: {
      failCommands: ["aggregate"],
      blockConnection: true,
      blockTimeMS: AGGREGATE_DELAY_MS,
    },
  });
}

async function serverCursors(): Promise<Long[]> {
  const database = await connection.GetDatabase();
  const namespace = `${database.databaseName}.${collectionName(SCHEMA_ID, TABLE)}`;
  const client = await mongoInternal.client;
  const operations = await client
    .db("admin")
    .aggregate<{ cursor: { cursorId: Long } }>([
      { $currentOp: { idleCursors: true, allUsers: true } },
      { $match: { ns: namespace, cursor: { $exists: true } } },
    ])
    .toArray();
  return operations.map((operation) => operation.cursor.cursorId);
}

async function openServerCursors(): Promise<number> {
  return (await serverCursors()).length;
}

async function killLeftoverCursors(): Promise<void> {
  const cursors = await serverCursors();
  if (cursors.length > 0) {
    const collection = await connection.GetCollection(
      collectionName(SCHEMA_ID, TABLE),
    );
    await collection.db.command({
      killCursors: collection.collectionName,
      cursors,
    });
  }
}

async function waitForNoServerCursor(): Promise<number> {
  const deadline = Date.now() + SETTLE_TIMEOUT_MS;
  let open = await openServerCursors();
  while (open > 0 && Date.now() < deadline) {
    await delay(50);
    open = await openServerCursors();
  }
  return open;
}

function delay(ms: number): Promise<"timeout"> {
  return new Promise((resolve) => setTimeout(() => resolve("timeout"), ms));
}

/** Reads the first row of `stages`, closes the cursor `CLOSE_AFTER_MS` later, and reports how the read settled. */
async function closeDuringFirstRead(stages: QueryStage[]) {
  const started = Date.now();
  const read = ReadCursor(REQ_ID, stages).then(
    (value) => ({ value, elapsed: Date.now() - started }),
    () => ({ value: "rejected" as const, elapsed: Date.now() - started }),
  );
  await delay(CLOSE_AFTER_MS);
  await CloseCursor(REQ_ID);
  const settled = await Promise.race([read, delay(SETTLE_TIMEOUT_MS)]);
  return { settled, open: await waitForNoServerCursor() };
}

function rows() {
  return schema.instance(INSTANCE).table(TABLE);
}

describe("closing a cursor during its first read", () => {
  before(async () => {
    schema = new Schema<{ rows: Row }>(SCHEMA_ID, {
      [TABLE]: { fields: {}, indexes: {} },
    });
    await schema.createInstance(INSTANCE).run();
    const inserted = Array.from({ length: ROW_COUNT }, (_, index) => ({
      _id: `row-${index}`,
    }));
    await rows().insert(inserted).run();
  });

  afterEach(async () => {
    await setAggregateDelay(false);
    await CloseCursor(REQ_ID);
    await killLeftoverCursors();
  });

  it("closes a table's server cursor once its delayed aggregate returns", async function () {
    this.timeout(10_000);
    await setAggregateDelay(true);
    const { settled, open } = await closeDuringFirstRead(rows().build());

    assert.notEqual(settled, "timeout", "the read settles");
    assert.ok(
      settled !== "timeout" && settled.elapsed >= AGGREGATE_DELAY_MS - 50,
      "the close landed while the delayed aggregate was in flight",
    );
    assert.equal(open, 0, "no server cursor stays open after the close");
    assert.deepEqual(
      settled.value,
      { done: true, value: undefined },
      "the read yields no row",
    );
  });

  it("closes a change stream's server cursor once its delayed aggregate returns", async function () {
    this.timeout(10_000);
    await setAggregateDelay(true);
    const { settled, open } = await closeDuringFirstRead(
      rows().changes().build(),
    );

    assert.notEqual(settled, "timeout", "the read settles");
    assert.equal(open, 0, "no server cursor stays open after the close");
  });

  it("closes a change stream waiting for its first event", async function () {
    this.timeout(10_000);
    const { settled, open } = await closeDuringFirstRead(
      rows().changes().build(),
    );

    assert.notEqual(settled, "timeout", "the read settles");
    assert.equal(open, 0, "no server cursor stays open after the close");
  });
});
