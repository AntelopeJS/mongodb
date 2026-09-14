import assert from "node:assert/strict";
import sinon from "sinon";
import { expect } from "chai";
import type { Collection, CommandStartedEvent, MongoClient } from "mongodb";
import { MongoNetworkError } from "mongodb";
import { Schema, CROSS_INSTANCE } from "@antelopejs/interface-database/schema";
import type {
  AtomicMutation,
  AtomicUpdate,
} from "@antelopejs/interface-database/atomic";

import * as connection from "../connection";
import { RunQuery, ReadCursor } from "../implementations/database/query";
import { collectionName } from "../implementations/database/utils";

interface AtomicRecord {
  _id: string;
  _instance?: string | null;
  revision?: string | null | string[];
  title?: string;
  nested?: Record<string, unknown>;
  expires?: Date | Date[] | null;
}

interface AtomicTables {
  records: AtomicRecord;
}

interface InstrumentedCollection extends Collection<AtomicRecord> {
  client: MongoClient;
}

const SCHEMA_ID = "atomic-mutation-integration";
const RECORD_ID = "record-one";
const FIRST_REVISION = "revision-first";
const SECOND_REVISION = "revision-second";
const TENANT = "tenant-one";
const DUPLICATE_KEY_CODE = 11000;

function update(nextRevision = SECOND_REVISION): AtomicUpdate<AtomicRecord> {
  return {
    type: "update",
    revisionField: "revision",
    expectedRevision: FIRST_REVISION,
    nextRevision,
    patch: { title: "updated" },
  };
}

describe("atomic single-record mutations", () => {
  let schema: Schema<AtomicTables>;
  let collection: InstrumentedCollection;

  before(() => {
    schema = new Schema<AtomicTables>(SCHEMA_ID, {
      records: { fields: {}, indexes: {} },
    });
    collection = connection.GetAtomicCollection(
      collectionName(SCHEMA_ID, "records"),
    ) as unknown as InstrumentedCollection;
  });

  beforeEach(async () => {
    await collection.deleteMany({});
    await schema
      .instance(TENANT)
      .table("records")
      .insert({
        _id: RECORD_ID,
        revision: FIRST_REVISION,
        title: "original",
        nested: { old: true },
      })
      .run();
  });

  afterEach(() => sinon.restore());

  function mutate(
    request: AtomicMutation<AtomicRecord>,
    key = RECORD_ID,
    tenant: string | undefined = TENANT,
  ) {
    return schema
      .instance(tenant)
      .table("records")
      .atomicMutation(key, request)
      .run();
  }

  it("applies exactly one of two competing distinct patches and revisions", async () => {
    const first = update("winner-a");
    first.patch = {
      title: "$literal-title",
      nested: { value: "$literal-value" },
    };
    const second = update("winner-b");
    second.patch = { title: "other-title", nested: { different: true } };
    const outcomes = await Promise.all([mutate(first), mutate(second)]);
    expect(outcomes.filter((outcome) => outcome === "applied")).to.have.length(
      1,
    );
    expect(
      outcomes.filter((outcome) => outcome === "not-applied"),
    ).to.have.length(1);
    const winner = outcomes[0] === "applied" ? first : second;
    expect(await collection.findOne({ _id: RECORD_ID })).to.deep.equal({
      _id: RECORD_ID,
      _instance: TENANT,
      revision: winner.nextRevision,
      ...winner.patch,
    });
  });

  it("rejects a stale delete after an update, then deletes with the current revision", async () => {
    expect(await mutate(update())).to.equal("applied");
    const deletion: AtomicMutation<AtomicRecord> = {
      type: "delete",
      revisionField: "revision",
      expectedRevision: FIRST_REVISION,
    };
    expect(await mutate(deletion)).to.equal("not-applied");
    expect(await collection.countDocuments()).to.equal(1);
    expect(
      await mutate({ ...deletion, expectedRevision: SECOND_REVISION }),
    ).to.equal("applied");
    expect(await collection.countDocuments()).to.equal(0);
  });

  it("does not upsert or mutate the wrong tenant or default instance", async () => {
    expect(await mutate(update(), "missing")).to.equal("not-applied");
    expect(await mutate(update(), RECORD_ID, "other-tenant")).to.equal(
      "not-applied",
    );
    expect(
      await schema
        .instance()
        .table("records")
        .atomicMutation(RECORD_ID, update())
        .run(),
    ).to.equal("not-applied");
    expect(await collection.findOne({ _id: RECORD_ID })).to.include({
      revision: FIRST_REVISION,
      title: "original",
    });
    expect(await collection.countDocuments()).to.equal(1);
  });

  it("matches missing revision only, never null, arrays, or a missing row", async () => {
    const request = {
      ...update(),
      expectedRevision: { kind: "missing" as const },
    };
    await collection.updateOne(
      { _id: RECORD_ID },
      { $set: { revision: null } },
    );
    expect(await mutate(request)).to.equal("not-applied");
    await collection.updateOne(
      { _id: RECORD_ID },
      { $set: { revision: [FIRST_REVISION] } },
    );
    expect(await mutate(update())).to.equal("not-applied");
    await collection.updateOne(
      { _id: RECORD_ID },
      { $unset: { revision: "" } },
    );
    expect(await mutate(request, "missing")).to.equal("not-applied");
    expect(await mutate(request)).to.equal("applied");
    expect(await mutate(request)).to.equal("not-applied");
  });

  it("supports the default instance and advances revision even for an unchanged patch", async () => {
    await collection.updateOne(
      { _id: RECORD_ID },
      { $set: { _instance: null } },
    );
    const request = { ...update(), patch: { title: "original" } };
    expect(
      await schema
        .instance()
        .table("records")
        .atomicMutation(RECORD_ID, request)
        .run(),
    ).to.equal("applied");
    expect(await collection.findOne({ _id: RECORD_ID })).to.include({
      revision: SECOND_REVISION,
      title: "original",
    });
  });

  it("throws validation failures before sending any write", async () => {
    sinon
      .stub(connection, "GetAtomicCollection")
      .returns(collection as unknown as Collection);
    const write = sinon.spy(collection, "updateOne");
    const table = schema.instance(TENANT).table("records");
    const invalid = [
      { ...update(), nextRevision: FIRST_REVISION },
      { ...update(), patch: { _instance: "other-tenant" } },
      { ...update(), patch: { _id: "different" } },
      { ...update(), patch: { revision: "override" } },
    ];
    for (const request of invalid) {
      await assert.rejects(async () =>
        table.atomicMutation(RECORD_ID, request).run(),
      );
    }
    await assert.rejects(async () =>
      schema
        .instance(CROSS_INSTANCE)
        .table("records")
        .atomicMutation(RECORD_ID, update())
        .run(),
    );
    const stages = table.atomicMutation(RECORD_ID, update()).build();
    await assert.rejects(
      RunQuery([...stages, { stage: "get", args: [RECORD_ID] }]),
    );
    await assert.rejects(ReadCursor(1, stages));
    expect(write.called).to.equal(false);
  });

  it("returns unknown after an applied write loses acknowledgement without retrying", async () => {
    const nativeUpdate = collection.updateOne.bind(collection);
    const write = sinon
      .stub(collection, "updateOne")
      .callsFake(async (...args) => {
        await nativeUpdate(...args);
        throw new MongoNetworkError("lost acknowledgement");
      });
    sinon
      .stub(connection, "GetAtomicCollection")
      .returns(collection as unknown as Collection);
    expect(await mutate(update())).to.equal("unknown");
    expect(write.calledOnce).to.equal(true);
    expect(await collection.findOne({ _id: RECORD_ID })).to.include({
      revision: SECOND_REVISION,
    });
  });

  it("keeps duplicate insert-only ids collection-global without overwriting", async () => {
    for (const tenant of [TENANT, "other-tenant"]) {
      await assert.rejects(
        schema
          .instance(tenant)
          .table("records")
          .insert({
            _id: RECORD_ID,
            revision: "duplicate",
            title: "overwrite",
          })
          .run(),
        { code: DUPLICATE_KEY_CODE },
      );
    }
    expect(await collection.findOne({ _id: RECORD_ID })).to.include({
      revision: FIRST_REVISION,
      title: "original",
      _instance: TENANT,
    });
  });

  it("deletes only an exact scalar expiry, never missing, null, arrays or renewed dates", async () => {
    const expiry = new Date("2026-09-01T00:00:00Z");
    const request: AtomicMutation<AtomicRecord> = {
      type: "deleteIfEqual",
      field: "expires",
      expectedValue: expiry,
    };
    expect(await mutate(request)).to.equal("not-applied");
    for (const expires of [null, [expiry], new Date("2026-10-01T00:00:00Z")]) {
      await collection.updateOne({ _id: RECORD_ID }, { $set: { expires } });
      expect(await mutate(request)).to.equal("not-applied");
    }
    await collection.updateOne(
      { _id: RECORD_ID },
      { $set: { expires: expiry } },
    );
    expect(await mutate(request, RECORD_ID, "other-tenant")).to.equal(
      "not-applied",
    );
    expect(await mutate(request)).to.equal("applied");
    expect(await collection.countDocuments()).to.equal(0);
  });

  it("sends single-record native commands without driver retryable-write tokens", async () => {
    const commands: CommandStartedEvent[] = [];
    const client = collection.client;
    const onCommand = (event: CommandStartedEvent) => commands.push(event);
    client.on("commandStarted", onCommand);
    try {
      expect(client.options.retryWrites).to.equal(false);
      expect(client.options.retryReads).to.equal(false);
      expect(await mutate(update())).to.equal("applied");
      expect(
        await mutate({
          type: "delete",
          revisionField: "revision",
          expectedRevision: SECOND_REVISION,
        }),
      ).to.equal("applied");
      const writes = commands.filter((event) =>
        ["update", "delete"].includes(event.commandName),
      );
      expect(writes).to.have.length(2);
      expect(writes[0].command.updates[0].multi ?? false).to.equal(false);
      expect(writes[0].command.updates[0].upsert).to.equal(false);
      expect(writes[1].command.deletes[0].limit).to.equal(1);
      expect(
        writes.every((event) => event.command.txnNumber === undefined),
      ).to.equal(true);
    } finally {
      client.off("commandStarted", onCommand);
    }
  });

  it("returns unknown for unacknowledged writes and a lost delete acknowledgement", async () => {
    sinon
      .stub(connection, "GetAtomicCollection")
      .returns(collection as unknown as Collection);
    sinon.stub(collection, "updateOne").resolves({
      acknowledged: false,
      matchedCount: 0,
      modifiedCount: 0,
      upsertedCount: 0,
      upsertedId: null,
    });
    expect(await mutate(update())).to.equal("unknown");
    const nativeDelete = collection.deleteOne.bind(collection);
    const deletion = sinon
      .stub(collection, "deleteOne")
      .callsFake(async (...args) => {
        await nativeDelete(...args);
        throw new MongoNetworkError("lost delete acknowledgement");
      });
    expect(
      await mutate({
        type: "delete",
        revisionField: "revision",
        expectedRevision: FIRST_REVISION,
      }),
    ).to.equal("unknown");
    expect(deletion.calledOnce).to.equal(true);
    expect(await collection.countDocuments()).to.equal(0);
  });

  it("throws definitive MongoDB document validation errors", async () => {
    const database = collection.client.db(
      connection.GetConfiguredDatabaseName(),
    );
    await database.command({
      collMod: collection.collectionName,
      validator: { title: "original" },
    });
    try {
      await assert.rejects(mutate(update()), { code: 121 });
      expect(await collection.findOne({ _id: RECORD_ID })).to.include({
        revision: FIRST_REVISION,
        title: "original",
      });
    } finally {
      await database.command({
        collMod: collection.collectionName,
        validator: {},
      });
    }
  });
});
