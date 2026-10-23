import { expect } from "chai";
import { randomUUID } from "node:crypto";
import {
  DuplicateKeyError,
  IsDuplicateKeyError,
} from "@antelopejs/interface-database";
import {
  Schema,
  type SchemaDefinition,
} from "@antelopejs/interface-database/schema";

import * as connection from "../connection";
import { collectionName } from "../implementations/database/utils";

interface Member {
  _id: string;
  email: string;
  firstName: string;
  lastName: string;
  team: string;
  revision?: string;
}

interface MemberTables {
  members: Member;
}

const SCHEMA_ID = "duplicate-keys";
const SYNC_SCHEMA_ID = "duplicate-keys-sync";
const TABLE = "members";
const TENANT = "tenant-one";
const OTHER_TENANT = "tenant-two";

function membersDefinition(isEmailUnique: boolean): SchemaDefinition {
  return {
    [TABLE]: {
      fields: {},
      indexes: {
        byEmail: { fields: ["email"], unique: isEmailUnique },
        fullName: { fields: ["lastName", "firstName"], unique: true },
        team: { crossInstance: true, unique: true },
      },
    },
  };
}

function member(id: string, overrides: Partial<Member> = {}): Member {
  return {
    _id: id,
    email: `${id}@example.test`,
    firstName: `first-${id}`,
    lastName: `last-${id}`,
    team: `team-${id}`,
    ...overrides,
  };
}

async function dropMembers(schemaId: string) {
  const collection = await connection.GetCollection(
    collectionName(schemaId, TABLE),
  );
  await collection.drop().catch(() => undefined);
}

async function membersSchema(
  schemaId: string,
  definition: SchemaDefinition,
): Promise<Schema<MemberTables>> {
  const schema = new Schema<MemberTables>(schemaId, definition);
  await connection.InitializeSchema(schemaId, definition);
  return schema;
}

async function refusal(write: PromiseLike<unknown>): Promise<unknown> {
  try {
    await write;
  } catch (error) {
    return error;
  }
  return expect.fail("the write should have been refused");
}

function expectDuplicate(error: unknown, index: string, fields: string[]) {
  expect(IsDuplicateKeyError(error)).to.equal(true);
  expect(error).to.be.instanceOf(DuplicateKeyError);
  expect(error).to.include({ index });
  expect((error as DuplicateKeyError).fields).to.deep.equal(fields);
}

describe("duplicate keys", () => {
  let schema: Schema<MemberTables>;

  before(async () => {
    await dropMembers(SCHEMA_ID);
    schema = await membersSchema(SCHEMA_ID, membersDefinition(true));
  });

  const members = (tenant = TENANT) => schema.instance(tenant).table(TABLE);

  it("refuses a second row with the same _id", async () => {
    await members().insert(member("a")).run();

    const error = await refusal(
      members()
        .insert(member("a", { email: "other@example.test" }))
        .run(),
    );

    expectDuplicate(error, "_id", ["_id"]);
  });

  it("refuses a duplicate of a renamed unique index on insert", async () => {
    await members().insert(member("b")).run();

    const error = await refusal(
      members()
        .insert(member("b2", { email: "b@example.test" }))
        .run(),
    );

    expectDuplicate(error, "byEmail", ["email"]);
    expect(await members().get("b2").run()).to.equal(undefined);
  });

  it("refuses a duplicate of a compound unique index", async () => {
    await members().insert(member("c")).run();

    const error = await refusal(
      members()
        .insert(member("c2", { lastName: "last-c", firstName: "first-c" }))
        .run(),
    );

    expectDuplicate(error, "fullName", ["lastName", "firstName"]);
  });

  it("refuses a duplicate inside a batch insert", async () => {
    const error = await refusal(
      members()
        .insert([member("d"), member("d2", { email: "d@example.test" })])
        .run(),
    );

    expectDuplicate(error, "byEmail", ["email"]);
  });

  it("refuses an update that would duplicate a unique key", async () => {
    await members()
      .insert([member("e"), member("f")])
      .run();

    const error = await refusal(
      members().get("f").update({ email: "e@example.test" }).run(),
    );

    expectDuplicate(error, "byEmail", ["email"]);
    expect((await members().get("f").run())?.email).to.equal("f@example.test");
  });

  it("refuses an atomic mutation that would duplicate a unique key", async () => {
    await members()
      .insert([
        member("g", { revision: randomUUID() }),
        member("h", { revision: "h-1" }),
      ])
      .run();

    const error = await refusal(
      members()
        .atomicMutation("h", {
          type: "update",
          revisionField: "revision",
          expectedRevision: "h-1",
          nextRevision: randomUUID(),
          patch: { email: "g@example.test" },
        })
        .run(),
    );

    expectDuplicate(error, "byEmail", ["email"]);
  });

  it("accepts the key of another instance", async () => {
    await members().insert(member("i")).run();

    await members(OTHER_TENANT)
      .insert(member("i-other", { email: "i@example.test", team: "team-i" }))
      .run();

    expect((await members(OTHER_TENANT).get("i-other").run())?.email).to.equal(
      "i@example.test",
    );
  });
});

describe("duplicate keys: index synchronization", () => {
  before(() => dropMembers(SYNC_SCHEMA_ID));

  async function indexUniqueness(): Promise<Record<string, boolean>> {
    const collection = await connection.GetCollection(
      collectionName(SYNC_SCHEMA_ID, TABLE),
    );
    const indexes = await collection.indexes();
    return Object.fromEntries(
      indexes.map((index) => [index.name, index.unique === true]),
    );
  }

  it("makes an existing index unique once declared so, and back", async () => {
    await membersSchema(SYNC_SCHEMA_ID, membersDefinition(false));
    expect((await indexUniqueness()).byEmail__i).to.equal(false);

    const schema = await membersSchema(SYNC_SCHEMA_ID, membersDefinition(true));
    expect((await indexUniqueness()).byEmail__i).to.equal(true);
    await schema.instance(TENANT).table(TABLE).insert(member("j")).run();
    const error = await refusal(
      schema
        .instance(TENANT)
        .table(TABLE)
        .insert(member("j2", { email: "j@example.test" }))
        .run(),
    );
    expectDuplicate(error, "byEmail", ["email"]);

    await membersSchema(SYNC_SCHEMA_ID, membersDefinition(false));
    expect((await indexUniqueness()).byEmail__i).to.equal(false);
  });

  it("keeps the cross-instance companion of a unique index non-unique", async () => {
    const uniqueness = await indexUniqueness();

    expect(uniqueness.team__i).to.equal(true);
    expect(uniqueness.team).to.equal(false);
  });
});
