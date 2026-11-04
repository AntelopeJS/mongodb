import sinon from "sinon";
import { expect } from "chai";
import { randomUUID } from "node:crypto";
import type { CommandStartedEvent } from "mongodb";
import { Logging } from "@antelopejs/interface-core/logging";
import { IsDuplicateKeyError } from "@antelopejs/interface-database";
import { GetClient } from "@antelopejs/interface-mongodb";
import {
  Schema,
  type SchemaDefinition,
} from "@antelopejs/interface-database/schema";

import * as connection from "../connection";
import { start, stop } from "../index";
import { Schemas } from "../implementations/database/schema";
import { collectionName } from "../implementations/database/utils";

interface Account {
  _id: string;
  email: string;
}

interface AccountTables {
  accounts: Account;
}

type IndexBuild = typeof connection.InitializeSchema;

const TABLE = "accounts";
const RACING_INSERTS = 4;
const INDEX_BUILD_DELAY_MS = 500;
const RETRY_DEADLINE_MS = 3_000;
const POLL_INTERVAL_MS = 50;
const SLOW_TEST_TIMEOUT_MS = 6_000;
const QUEUED_MS = 20;

const definition: SchemaDefinition = {
  [TABLE]: {
    fields: {},
    indexes: { email: { fields: ["email"], unique: true } },
  },
};

const realBuild = connection.InitializeSchema;

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const delayedBuild: IndexBuild = async (schemaId, schema, signal) => {
  await delay(INDEX_BUILD_DELAY_MS);
  return realBuild(schemaId, schema, signal);
};

function stubIndexBuilds(build: IndexBuild): sinon.SinonStub {
  return sinon.stub(connection, "InitializeSchema").callsFake(build);
}

async function accountsWith(
  schema: Schema<AccountTables>,
  email: string,
): Promise<number> {
  const collection = await connection.GetCollection(
    collectionName(schema.id, TABLE),
  );
  return collection.countDocuments({ email });
}

function insert(schema: Schema<AccountTables>, email: string) {
  return schema.instance().table(TABLE).insert({ email }).run();
}

function race(schema: Schema<AccountTables>, email: string) {
  return Promise.allSettled(
    Array.from({ length: RACING_INSERTS }, () => insert(schema, email)),
  );
}

function refusals(outcomes: PromiseSettledResult<unknown>[]): unknown[] {
  const reasons: unknown[] = [];
  for (const outcome of outcomes) {
    if (outcome.status === "rejected") {
      reasons.push(outcome.reason);
    }
  }
  return reasons;
}

async function expectOneRow(schema: Schema<AccountTables>, email: string) {
  const refused = refusals(await race(schema, email));
  expect(await accountsWith(schema, email)).to.equal(1);
  expect(refused).to.have.length(RACING_INSERTS - 1);
  expect(refused.every(IsDuplicateKeyError)).to.equal(true);
}

async function untilServed(schema: Schema<AccountTables>): Promise<void> {
  const deadline = Date.now() + RETRY_DEADLINE_MS;
  for (;;) {
    try {
      await schema.instance().table(TABLE).count().run();
      return;
    } catch (error) {
      if (Date.now() > deadline) throw error;
      await delay(POLL_INTERVAL_MS);
    }
  }
}

type Accounts = Schema<AccountTables>;

const crossSchemaReads = {
  union: (parent: Accounts, other: Accounts) =>
    parent.instance().table(TABLE).union(other.instance().table(TABLE)),
  lookup: (parent: Accounts, other: Accounts) =>
    parent
      .instance()
      .table(TABLE)
      .lookup(other.instance().table(TABLE), "email", "email"),
  subquery: (parent: Accounts, other: Accounts) =>
    parent
      .instance()
      .table(TABLE)
      .map(() => other.instance().table(TABLE).get("missing")),
};

describe("schema readiness", () => {
  let schemaIds: string[] = [];

  function freshSchema(): Schema<AccountTables> {
    const schemaId = `schema-readiness-${randomUUID()}`;
    schemaIds.push(schemaId);
    return new Schema<AccountTables>(schemaId, definition);
  }

  beforeEach(() => {
    sinon.stub(Logging, "Warn");
    sinon.stub(Logging, "Info");
  });

  afterEach(async () => {
    sinon.restore();
    for (const schemaId of schemaIds) {
      Schemas.unregister(schemaId);
      const collection = await connection.GetCollection(
        collectionName(schemaId, TABLE),
      );
      await collection.drop().catch(() => undefined);
    }
    schemaIds = [];
  });

  it("creates one row when inserts on a fresh unique key race a slow index build", async () => {
    stubIndexBuilds(delayedBuild);
    await expectOneRow(freshSchema(), "racer@example.test");
  });

  it("refuses queries while the index build has failed, then serves them once a retry builds the indexes", async function () {
    this.timeout(SLOW_TEST_TIMEOUT_MS);
    let attempts = 0;
    stubIndexBuilds(async (schemaId, schema, signal) => {
      attempts++;
      await delay(INDEX_BUILD_DELAY_MS);
      if (attempts === 1) throw new Error("index build lost the connection");
      return realBuild(schemaId, schema, signal);
    });
    const schema = freshSchema();
    const email = "failed@example.test";

    const refused = refusals(await race(schema, email));

    expect(await accountsWith(schema, email)).to.equal(0);
    expect(refused).to.have.length(RACING_INSERTS);
    for (const error of refused.map(String)) {
      expect(error).to.include(`Schema "${schema.id}"`);
      expect(error).to.include("index build lost the connection");
    }
    await untilServed(schema);
    expect(attempts).to.equal(2);
    await expectOneRow(schema, email);
  });

  it("refuses the queries waiting on a schema unregistered during its index build, and gates it registered again", async () => {
    stubIndexBuilds(delayedBuild);
    const schema = freshSchema();
    const waiting = race(schema, "first@example.test");
    await delay(QUEUED_MS);

    Schemas.unregister(schema.id);
    Schemas.register(schema.id, definition);

    expect(refusals(await waiting)).to.have.length(RACING_INSERTS);
    expect(await accountsWith(schema, "first@example.test")).to.equal(0);
    await expectOneRow(schema, "second@example.test");
  });

  it("gates queries again when the module stops and starts during the index build", async () => {
    stubIndexBuilds(delayedBuild);
    const schema = freshSchema();
    await delay(QUEUED_MS);

    stop();
    start();

    await expectOneRow(schema, "restarted@example.test");
  });

  it("opens no cursor for a read closed while its index build runs", async () => {
    stubIndexBuilds(delayedBuild);
    const schema = freshSchema();
    const collection = collectionName(schema.id, TABLE);
    const reads: string[] = [];
    const onCommand = (event: CommandStartedEvent) => {
      if (event.command[event.commandName] === collection)
        reads.push(event.commandName);
    };
    const client = await GetClient();
    client.on("commandStarted", onCommand);
    try {
      const cursor = schema.instance().table(TABLE).cursor();
      const next = cursor.next();
      await delay(QUEUED_MS);
      await cursor.return();
      await next;
      await delay(INDEX_BUILD_DELAY_MS * 2);
    } finally {
      client.off("commandStarted", onCommand);
    }
    expect(reads).to.not.include("aggregate");
  });

  it("waits for the index build of every schema a query reads", async function () {
    this.timeout(SLOW_TEST_TIMEOUT_MS);
    const parent = freshSchema();
    await insert(parent, "parent@example.test");
    for (const [form, read] of Object.entries(crossSchemaReads)) {
      let isBuilt = false;
      const build = stubIndexBuilds(async (schemaId, schema, signal) => {
        await delayedBuild(schemaId, schema, signal);
        isBuilt = true;
      });
      const other = freshSchema();
      await read(parent, other).run();
      expect(isBuilt, form).to.equal(true);
      build.restore();
    }
  });
});

function stateAfter(promise: Promise<unknown>, ms: number) {
  return Promise.race([
    promise.then(
      () => "fulfilled",
      () => "rejected",
    ),
    delay(ms).then(() => "pending"),
  ]);
}

describe("schema readiness across a restart or a close", () => {
  const SETTLE_MS = 150;
  let schemaIds: string[] = [];
  let releaseBuild = () => {};

  function freshSchema(): Schema<AccountTables> {
    const schemaId = `schema-readiness-${randomUUID()}`;
    schemaIds.push(schemaId);
    return new Schema<AccountTables>(schemaId, definition);
  }

  function heldBuild(): Promise<void> {
    return new Promise((resolve) => {
      releaseBuild = resolve;
    });
  }

  beforeEach(() => {
    sinon.stub(Logging, "Warn");
    sinon.stub(Logging, "Info");
  });

  afterEach(async () => {
    releaseBuild();
    sinon.restore();
    for (const schemaId of schemaIds) {
      Schemas.unregister(schemaId);
      const collection = await connection.GetCollection(
        collectionName(schemaId, TABLE),
      );
      await collection.drop().catch(() => undefined);
    }
    schemaIds = [];
  });

  it("gates queries on the resumed build after a failed build and a restart", async () => {
    let attempts = 0;
    stubIndexBuilds(async (schemaId, schema, signal) => {
      attempts++;
      if (attempts === 1) throw new Error("index build lost the connection");
      await heldBuild();
      return realBuild(schemaId, schema, signal);
    });
    const schema = freshSchema();
    const count = () => schema.instance().table(TABLE).count().run();
    expect(await stateAfter(count(), SETTLE_MS)).to.equal("rejected");

    stop();
    start();
    const afterRestart = count();

    expect(await stateAfter(afterRestart, SETTLE_MS)).to.equal("pending");
    releaseBuild();
    expect(await stateAfter(afterRestart, SETTLE_MS * 4)).to.equal("fulfilled");
  });

  it("closes a cursor that has not opened without waiting for the build", async () => {
    stubIndexBuilds(() => heldBuild());
    const cursor = freshSchema().instance().table(TABLE).cursor();
    const next = cursor.next();
    next.catch(() => undefined);
    await delay(QUEUED_MS);

    expect(await stateAfter(cursor.return(), SETTLE_MS)).to.equal("fulfilled");
  });

  it("never opens a cursor closed while it fetches its collection", async () => {
    const schema = freshSchema();
    await insert(schema, "open@example.test");
    const collection = collectionName(schema.id, TABLE);
    const realGetCollection = connection.GetCollection;
    let fetching = () => {};
    const fetched = new Promise<void>((resolve) => {
      fetching = resolve;
    });
    sinon.stub(connection, "GetCollection").callsFake(async (name) => {
      if (name === collection) {
        fetching();
        await heldBuild();
      }
      return realGetCollection(name);
    });
    const reads: string[] = [];
    const onCommand = (event: CommandStartedEvent) => {
      if (event.command[event.commandName] === collection)
        reads.push(event.commandName);
    };
    const client = await GetClient();
    client.on("commandStarted", onCommand);
    try {
      const cursor = schema.instance().table(TABLE).cursor();
      const next = cursor.next();
      await fetched;
      await cursor.return();
      releaseBuild();
      await next;
      await delay(SETTLE_MS);
    } finally {
      client.off("commandStarted", onCommand);
    }
    expect(reads).to.not.include("aggregate");
  });
});
