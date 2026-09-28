import sinon from "sinon";
import { expect } from "chai";
import assert from "node:assert/strict";
import type { Collection, Document } from "mongodb";
import { Logging } from "@antelopejs/interface-core/logging";
import { type Query, ValueProxy } from "@antelopejs/interface-database";
import {
  CROSS_INSTANCE,
  Schema,
  type SchemaDefinition,
} from "@antelopejs/interface-database/schema";

import * as connection from "../connection";
import { Schemas } from "../implementations/database/schema";
import { SelectionQuery } from "../implementations/database/selection";
import {
  collectionName,
  instanceIndexName,
} from "../implementations/database/utils";

interface Person {
  _id: string;
  email: string;
  firstName: string;
  lastName: string;
  age: number;
  category: string;
  nickname: string;
}

type SelectionKey = string | number | boolean;

interface PeopleTables {
  people: Person;
}

const SCHEMA_ID = "prefixed-indexes";
const CONCURRENT_SCHEMA_ID = "prefixed-indexes-concurrent";
const LEGACY_SCHEMA_ID = "prefixed-indexes-legacy";
const SHARED_FIELDS_SCHEMA_ID = "prefixed-indexes-shared-fields";
const UNINDEXED_SCHEMA_ID = "prefixed-indexes-unindexed";
const WARNING_SCHEMA_ID = "prefixed-indexes-warning";
const UNKNOWN_SCHEMA_ID = "prefixed-indexes-unknown";
const TABLE = "people";
const TENANT = "tenant-one";
const OTHER_TENANT = "tenant-two";
const DEFAULT_TENANT = "default";
const PEOPLE_PER_TENANT = 40;
const CONCURRENT_INITIALIZATIONS = 4;
const INSTANCE_INDEX = "_instance";
const PRIMARY_INDEX = "_id_";

function peopleDefinition(): SchemaDefinition {
  return {
    [TABLE]: {
      fields: {},
      indexes: {
        email: {},
        age: {},
        byNickname: { fields: ["nickname"] },
        fullName: { fields: ["lastName", "firstName"] },
        category: { crossInstance: true },
      },
    },
  };
}

const definition = peopleDefinition();

function person(tenant: string, position: number): Person {
  return {
    _id: `${tenant}-${position}`,
    email: `person-${position}@${tenant}.test`,
    firstName: `first-${position}`,
    lastName: `last-${position % 4}`,
    age: position,
    category: `category-${position}`,
    nickname: `nickname-${position}`,
  };
}

function people(tenant: string): Person[] {
  return Array.from({ length: PEOPLE_PER_TENANT }, (_, position) =>
    person(tenant, position),
  );
}

async function peopleCollection(schemaId: string): Promise<Collection> {
  return connection.GetCollection(collectionName(schemaId, TABLE));
}

async function dropPeopleCollection(schemaId: string) {
  const collection = await peopleCollection(schemaId);
  await collection.drop().catch(() => undefined);
}

async function indexKeysByName(
  schemaId: string,
): Promise<Record<string, Document>> {
  const collection = await peopleCollection(schemaId);
  const indexes = await collection.indexes();
  return Object.fromEntries(indexes.map((index) => [index.name, index.key]));
}

function collectIndexNames(plan: unknown, found: Set<string>) {
  if (plan === null || typeof plan !== "object") {
    return;
  }
  for (const [key, value] of Object.entries(plan)) {
    if (key === "indexName" && typeof value === "string") {
      found.add(value);
    } else {
      collectIndexNames(value, found);
    }
  }
}

function collectWinningIndexes(explain: unknown, found: Set<string>) {
  if (explain === null || typeof explain !== "object") {
    return;
  }
  for (const [key, value] of Object.entries(explain)) {
    if (key === "winningPlan") {
      collectIndexNames(value, found);
    } else {
      collectWinningIndexes(value, found);
    }
  }
}

async function usedIndexes(query: Query<unknown>): Promise<string[]> {
  const selection = await SelectionQuery.decode(query.build());
  const collection = await connection.GetCollection(selection.collection);
  const explain = await collection.aggregate(selection.pipeline).explain();
  const found = new Set<string>();
  collectWinningIndexes(explain, found);
  return [...found];
}

function tupleKey(...values: string[]): ValueProxy<SelectionKey> {
  return ValueProxy.constant(values).cast<SelectionKey>();
}

function sortedIds(documents: Person[]): string[] {
  return documents.map((document) => document._id).sort();
}

let schema: Schema<PeopleTables>;
let indexedSchemaSetup: Promise<Schema<PeopleTables>> | undefined;

async function seedPeople(
  schemaId: string,
  schemaDefinition: SchemaDefinition,
): Promise<Schema<PeopleTables>> {
  await dropPeopleCollection(schemaId);
  const seeded = new Schema<PeopleTables>(schemaId, schemaDefinition);
  await connection.InitializeSchema(schemaId, schemaDefinition);
  await seeded.instance(TENANT).table(TABLE).insert(people(TENANT)).run();
  await seeded
    .instance(OTHER_TENANT)
    .table(TABLE)
    .insert(people(OTHER_TENANT))
    .run();
  await seeded.instance().table(TABLE).insert(people(DEFAULT_TENANT)).run();
  return seeded;
}

function seedIndexedSchema(): Promise<Schema<PeopleTables>> {
  return seedPeople(SCHEMA_ID, definition);
}

function indexedSchema(): Promise<Schema<PeopleTables>> {
  indexedSchemaSetup ??= seedIndexedSchema();
  return indexedSchemaSetup;
}

describe("instance-prefixed indexes: index synchronization", () => {
  before(async () => {
    schema = await indexedSchema();
  });

  afterEach(() => sinon.restore());

  it("leads every declared index with the instance field", async () => {
    const keys = await indexKeysByName(SCHEMA_ID);

    expect(keys[instanceIndexName("email")]).to.deep.equal({
      _instance: 1,
      email: 1,
    });
    expect(keys[instanceIndexName("fullName")]).to.deep.equal({
      _instance: 1,
      lastName: 1,
      firstName: 1,
    });
  });

  it("adds the single-field instance index only to tables without declared indexes", async () => {
    await dropPeopleCollection(UNINDEXED_SCHEMA_ID);
    const unindexed: SchemaDefinition = {
      [TABLE]: { fields: {}, indexes: {} },
    };

    await connection.InitializeSchema(UNINDEXED_SCHEMA_ID, unindexed);

    expect(await indexKeysByName(UNINDEXED_SCHEMA_ID)).to.deep.equal({
      [PRIMARY_INDEX]: { _id: 1 },
      [INSTANCE_INDEX]: { _instance: 1 },
    });
    expect(await indexKeysByName(SCHEMA_ID)).not.to.have.any.keys(
      INSTANCE_INDEX,
    );
  });

  it("adds the unprefixed index only for crossInstance indexes", async () => {
    const keys = await indexKeysByName(SCHEMA_ID);

    expect(keys.category).to.deep.equal({ category: 1 });
    expect(keys[instanceIndexName("category")]).to.deep.equal({
      _instance: 1,
      category: 1,
    });
    expect(keys).not.to.have.any.keys("email", "age", "fullName");
  });

  it("tolerates concurrent initializations of the same collection", async () => {
    await dropPeopleCollection(CONCURRENT_SCHEMA_ID);

    await Promise.all(
      Array.from({ length: CONCURRENT_INITIALIZATIONS }, () =>
        connection.InitializeSchema(CONCURRENT_SCHEMA_ID, definition),
      ),
    );

    const keys = await indexKeysByName(CONCURRENT_SCHEMA_ID);
    expect(keys).to.have.all.keys(
      PRIMARY_INDEX,
      "category",
      ...Object.keys(definition[TABLE].indexes).map(instanceIndexName),
    );
  });

  it("leaves legacy unprefixed and instance indexes in place", async () => {
    await dropPeopleCollection(LEGACY_SCHEMA_ID);
    const collection = await peopleCollection(LEGACY_SCHEMA_ID);
    await collection.createIndex(["email"], { name: "email" });
    await collection.createIndex(["_instance"], { name: INSTANCE_INDEX });

    await connection.InitializeSchema(LEGACY_SCHEMA_ID, definition);

    const keys = await indexKeysByName(LEGACY_SCHEMA_ID);
    expect(keys.email).to.deep.equal({ email: 1 });
    expect(keys[INSTANCE_INDEX]).to.deep.equal({ _instance: 1 });
    expect(keys[instanceIndexName("email")]).to.deep.equal({
      _instance: 1,
      email: 1,
    });
  });

  it("keeps one physical index when two declared indexes share their fields", async () => {
    await dropPeopleCollection(SHARED_FIELDS_SCHEMA_ID);
    const sharedFields: SchemaDefinition = {
      [TABLE]: {
        fields: {},
        indexes: { email: {}, byMail: { fields: ["email"] } },
      },
    };

    await Promise.all(
      Array.from({ length: CONCURRENT_INITIALIZATIONS }, () =>
        connection.InitializeSchema(SHARED_FIELDS_SCHEMA_ID, sharedFields),
      ),
    );

    const keys = await indexKeysByName(SHARED_FIELDS_SCHEMA_ID);
    expect(keys).to.have.all.keys(PRIMARY_INDEX, instanceIndexName("email"));
  });
});

describe("instance-prefixed indexes: query plans", () => {
  before(async () => {
    schema = await indexedSchema();
  });

  afterEach(() => sinon.restore());

  it("serves a scoped getAll with the prefixed index", async () => {
    const query = schema
      .instance(TENANT)
      .table(TABLE)
      .getAll(`person-3@${TENANT}.test`, "email");

    expect(await usedIndexes(query)).to.deep.equal([
      instanceIndexName("email"),
    ]);
  });

  it("serves a default-instance getAll with the prefixed index", async () => {
    const query = schema
      .instance()
      .table(TABLE)
      .getAll(`person-3@${DEFAULT_TENANT}.test`, "email");

    expect(await usedIndexes(query)).to.deep.equal([
      instanceIndexName("email"),
    ]);
  });

  it("serves a scoped between with the prefixed index", async () => {
    const query = schema.instance(TENANT).table(TABLE).between("age", 5, 8);

    expect(await usedIndexes(query)).to.deep.equal([instanceIndexName("age")]);
  });

  it("serves a scoped orderBy with the prefixed index", async () => {
    const query = schema.instance(TENANT).table(TABLE).orderBy("age");

    expect(await usedIndexes(query)).to.deep.equal([instanceIndexName("age")]);
  });

  it("serves a default-instance orderBy with the prefixed index", async () => {
    const query = schema.instance().table(TABLE).orderBy("age");

    expect(await usedIndexes(query)).to.deep.equal([instanceIndexName("age")]);
  });

  it("serves a scoped compound orderBy with the prefixed index", async () => {
    const query = schema.instance(TENANT).table(TABLE).orderBy("fullName");

    expect(await usedIndexes(query)).to.deep.equal([
      instanceIndexName("fullName"),
    ]);
  });

  it("serves a scoped compound getAll with the prefixed index", async () => {
    const query = schema
      .instance(TENANT)
      .table(TABLE)
      .getAll(["last-1", "first-5"], "fullName");

    expect(await usedIndexes(query)).to.deep.equal([
      instanceIndexName("fullName"),
    ]);
  });

  it("serves a cross-instance getAll with the unprefixed index", async () => {
    const query = schema
      .instance(CROSS_INSTANCE)
      .table(TABLE)
      .getAll("category-3", "category");

    expect(await usedIndexes(query)).to.deep.equal(["category"]);
  });

  it("serves a cross-instance orderBy with the unprefixed index", async () => {
    const query = schema
      .instance(CROSS_INSTANCE)
      .table(TABLE)
      .orderBy("category");

    expect(await usedIndexes(query)).to.deep.equal(["category"]);
  });
});

describe("instance-prefixed indexes: getAll on declared indexes", () => {
  before(async () => {
    schema = await indexedSchema();
  });

  afterEach(() => sinon.restore());

  it("matches on the fields of a renamed single-field index", async () => {
    const result = await schema
      .instance(TENANT)
      .table(TABLE)
      .getAll("nickname-7", "byNickname")
      .run();

    expect(sortedIds(result)).to.deep.equal([`${TENANT}-7`]);
  });

  it("reads a flat compound key as a single tuple", async () => {
    const result = await schema
      .instance(TENANT)
      .table(TABLE)
      .getAll(["last-1", "first-5"], "fullName")
      .run();

    expect(sortedIds(result)).to.deep.equal([`${TENANT}-5`]);
  });

  it("matches several tuples of a compound index", async () => {
    sinon.stub(Logging, "Warn");
    const result = await schema
      .instance(CROSS_INSTANCE)
      .table(TABLE)
      .getAll(
        [tupleKey("last-1", "first-5"), tupleKey("last-2", "first-6")],
        "fullName",
      )
      .run();

    expect(sortedIds(result)).to.deep.equal(
      [DEFAULT_TENANT, OTHER_TENANT, TENANT]
        .flatMap((tenant) => [`${tenant}-5`, `${tenant}-6`])
        .sort(),
    );
  });

  it("scopes a default-instance getAll to the default instance", async () => {
    const result = await schema
      .instance()
      .table(TABLE)
      .getAll("nickname-7", "byNickname")
      .run();

    expect(sortedIds(result)).to.deep.equal([`${DEFAULT_TENANT}-7`]);
  });

  it("rejects a compound key of the wrong arity", async () => {
    const query = schema
      .instance(TENANT)
      .table(TABLE)
      .getAll("last-1", "fullName");

    await assert.rejects(() => query.run(), /takes keys of 2 values/);
  });
});

describe("instance-prefixed indexes: schemas without a stored definition", () => {
  let unknown: Schema<PeopleTables>;

  before(async () => {
    unknown = await seedPeople(UNKNOWN_SCHEMA_ID, peopleDefinition());
    Schemas.unregister(UNKNOWN_SCHEMA_ID);
  });

  afterEach(() => sinon.restore());

  it("uses the index name as the field of getAll and between", async () => {
    const warn = sinon.stub(Logging, "Warn");
    const table = unknown.instance(CROSS_INSTANCE).table(TABLE);

    const byEmail = await table
      .getAll(`person-3@${TENANT}.test`, "email")
      .run();
    const byAge = await table.between("age", 5, 7).run();

    expect(sortedIds(byEmail)).to.deep.equal([`${TENANT}-3`]);
    expect(byAge).to.have.length(6);
    expect(warn.called).to.equal(false);
  });

  it("uses the index name as the field of a getAll mapped over an array", async () => {
    const people = unknown.instance(TENANT).table(TABLE);
    const nicknames = ValueProxy.constant<SelectionKey[]>([
      "nickname-2",
      "nickname-3",
    ]);
    const result = await people
      .get(`${TENANT}-1`)
      .do(() =>
        nicknames.map((nickname) => people.getAll(nickname, "nickname")),
      )
      .run();

    expect(result.map(sortedIds)).to.deep.equal([
      [`${TENANT}-2`],
      [`${TENANT}-3`],
    ]);
  });
});

describe("instance-prefixed indexes: cross-instance warnings", () => {
  let warningSchema: Schema<PeopleTables>;

  before(async () => {
    schema = await indexedSchema();
    warningSchema = await seedPeople(WARNING_SCHEMA_ID, peopleDefinition());
  });

  afterEach(() => sinon.restore());

  it("warns once per index named by a cross-instance query", async () => {
    const warn = sinon.stub(Logging, "Warn");
    const table = warningSchema.instance(CROSS_INSTANCE).table(TABLE);

    await table.getAll(`person-1@${TENANT}.test`, "email").run();
    await table.getAll(`person-2@${TENANT}.test`, "email").run();
    await table.between("age", 1, 3).run();
    await table.orderBy("age").run();

    const messages = warn.getCalls().map((call) => String(call.args[0]));
    expect(messages).to.have.length(2);
    expect(messages[0]).to.include('"email"');
    expect(messages[1]).to.include('"age"');
  });

  it("does not warn for crossInstance indexes, scoped queries or filters", async () => {
    const warn = sinon.stub(Logging, "Warn");

    await schema
      .instance(CROSS_INSTANCE)
      .table(TABLE)
      .getAll("category-1", "category")
      .run();
    await schema
      .instance(TENANT)
      .table(TABLE)
      .getAll("nickname-1", "byNickname")
      .run();
    await schema
      .instance(CROSS_INSTANCE)
      .table(TABLE)
      .filter((row) => row.key("firstName").eq("first-1"))
      .run();

    expect(warn.called).to.equal(false);
  });
});
