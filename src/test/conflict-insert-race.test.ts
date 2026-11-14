import { expect } from "chai";
import { randomUUID } from "node:crypto";
import { IsDuplicateKeyError } from "@antelopejs/interface-database";
import { Schema } from "@antelopejs/interface-database/schema";

import * as connection from "../connection";
import { SchemaInitialized } from "../schema-initialization";
import { collectionName } from "../implementations/database/utils";

type Row = { _id: string } & Partial<Record<`writer${number}`, boolean>>;

interface RowTables {
  rows: Row;
}

const SCHEMA_ID = "conflict-insert-race";
const TABLE = "rows";
const TENANT = "tenant-one";
const OTHER_TENANT = "tenant-two";
const WRITERS = 8;
const SINGLE_ROW_ROUNDS = 10;
const BATCH_ROUNDS = 50;
const BATCH_SIZE = 200;
const RACE_TIMEOUT_MS = 30_000;

const writerField = (writer: number) => `writer${writer}` as const;

function writerRow(_id: string, writer: number): Row {
  const row: Row = { _id };
  row[writerField(writer)] = true;
  return row;
}

describe("insert with conflict: concurrent writers", () => {
  let schema: Schema<RowTables>;

  before(async () => {
    const collection = await connection.GetCollection(
      collectionName(SCHEMA_ID, TABLE),
    );
    await collection.drop().catch(() => undefined);
    schema = new Schema<RowTables>(SCHEMA_ID, {
      [TABLE]: { fields: {}, indexes: {} },
    });
    await SchemaInitialized(SCHEMA_ID);
  });

  const rows = (tenant = TENANT) => schema.instance(tenant).table(TABLE);

  async function race(
    ids: string[],
    conflict: "update" | "replace",
  ): Promise<void> {
    const writes = Array.from({ length: WRITERS }, (_, writer) =>
      rows()
        .insert(
          ids.map((_id) => writerRow(_id, writer)),
          { conflict },
        )
        .run(),
    );
    const results = await Promise.allSettled(writes);
    expect(
      results.filter((result) => result.status === "rejected"),
    ).to.deep.equal([]);
  }

  async function storedRows(ids: string[]): Promise<Row[]> {
    return rows().getAll(ids).run();
  }

  for (const [label, rounds, size] of [
    ["one new _id", SINGLE_ROW_ROUNDS, 1],
    ["a batch of new _ids", BATCH_ROUNDS, BATCH_SIZE],
  ] as const) {
    it(`applies every concurrent update of ${label}`, async () => {
      for (let round = 0; round < rounds; round++) {
        const ids = Array.from({ length: size }, () => randomUUID());

        await race(ids, "update");

        const stored = await storedRows(ids);
        expect(stored).to.have.length(size);
        for (const row of stored) {
          for (let writer = 0; writer < WRITERS; writer++) {
            expect(row).to.have.property(writerField(writer), true);
          }
        }
      }
    }).timeout(RACE_TIMEOUT_MS);

    it(`resolves every concurrent replace of ${label} and keeps one row each`, async () => {
      for (let round = 0; round < rounds; round++) {
        const ids = Array.from({ length: size }, () => randomUUID());

        await race(ids, "replace");

        expect(await storedRows(ids)).to.have.length(size);
      }
    }).timeout(RACE_TIMEOUT_MS);
  }

  it("still refuses a conflict insert of an _id another instance holds", async () => {
    const id = randomUUID();
    await rows(OTHER_TENANT).insert({ _id: id, writer0: true }).run();

    const error = await rows()
      .insert({ _id: id, writer1: true }, { conflict: "update" })
      .run()
      .then(
        () => undefined,
        (reason: unknown) => reason,
      );

    expect(IsDuplicateKeyError(error)).to.equal(true);
    const held = await rows(OTHER_TENANT).get(id).run();
    expect(held).to.include({ writer0: true });
    expect(held).not.to.have.property("writer1");
  });
});
