import { expect } from "chai";
import { randomUUID } from "node:crypto";
import { IsDuplicateKeyError } from "@antelopejs/interface-database";
import { Schema } from "@antelopejs/interface-database/schema";

import * as connection from "../connection";
import { SchemaInitialized } from "../schema-initialization";
import { collectionName } from "../implementations/database/utils";

interface Counter {
  _id: string;
  writer: number;
}

interface CounterTables {
  counters: Counter;
}

const SCHEMA_ID = "conflict-insert-race";
const TABLE = "counters";
const TENANT = "tenant-one";
const OTHER_TENANT = "tenant-two";
const CONCURRENT_WRITERS = 8;
const ROUNDS = 10;

describe("insert with conflict: concurrent writers", () => {
  let schema: Schema<CounterTables>;

  before(async () => {
    const collection = await connection.GetCollection(
      collectionName(SCHEMA_ID, TABLE),
    );
    await collection.drop().catch(() => undefined);
    schema = new Schema<CounterTables>(SCHEMA_ID, {
      [TABLE]: { fields: {}, indexes: {} },
    });
    await SchemaInitialized(SCHEMA_ID);
  });

  const counters = (tenant = TENANT) => schema.instance(tenant).table(TABLE);

  for (const conflict of ["update", "replace"] as const) {
    it(`resolves every concurrent ${conflict} of one new _id and keeps one row`, async () => {
      for (let round = 0; round < ROUNDS; round++) {
        const id = randomUUID();
        const writes = Array.from({ length: CONCURRENT_WRITERS }, (_, writer) =>
          counters().insert({ _id: id, writer }, { conflict }).run(),
        );

        const results = await Promise.allSettled(writes);

        expect(
          results.filter((result) => result.status === "rejected"),
        ).to.deep.equal([]);
        expect(await counters().getAll(id).count().run()).to.equal(1);
      }
    });
  }

  it("still refuses a conflict insert of an _id another instance holds", async () => {
    const id = randomUUID();
    await counters(OTHER_TENANT).insert({ _id: id, writer: 0 }).run();

    const error = await counters()
      .insert({ _id: id, writer: 1 }, { conflict: "update" })
      .run()
      .then(
        () => undefined,
        (reason: unknown) => reason,
      );

    expect(IsDuplicateKeyError(error)).to.equal(true);
    expect((await counters(OTHER_TENANT).get(id).run())?.writer).to.equal(0);
  });
});
