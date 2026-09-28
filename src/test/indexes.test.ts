import sinon from "sinon";
import { expect } from "chai";
import assert from "node:assert/strict";
import type { Collection, Document } from "mongodb";
import { Logging } from "@antelopejs/interface-core/logging";
import type { Query } from "@antelopejs/interface-database";
import {
  CROSS_INSTANCE,
  Schema,
  type SchemaDefinition,
} from "@antelopejs/interface-database/schema";

import * as connection from "../connection";
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

interface PeopleTables {
  people: Person;
}

const SCHEMA_ID = "prefixed-indexes";
const CONCURRENT_SCHEMA_ID = "prefixed-indexes-concurrent";
const LEGACY_SCHEMA_ID = "prefixed-indexes-legacy";
const SHARED_FIELDS_SCHEMA_ID = "prefixed-indexes-shared-fields";
const TABLE = "people";
const TENANT = "tenant-one";
const OTHER_TENANT = "tenant-two";
const PEOPLE_PER_TENANT = 40;
const CONCURRENT_INITIALIZATIONS = 4;
const INSTANCE_INDEX = "_instance";
const PRIMARY_INDEX = "_id_";

const definition: SchemaDefinition = {
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

function compoundKeys(keys: string[][]): string[] {
  return keys as unknown as string[];
}

function sortedIds(documents: Person[]): string[] {
  return documents.map((document) => document._id).sort();
}

let schema: Schema<PeopleTables>;
let indexedSchemaSetup: Promise<Schema<PeopleTables>> | undefined;

async function seedIndexedSchema(): Promise<Schema<PeopleTables>> {
  await dropPeopleCollection(SCHEMA_ID);
  const seeded = new Schema<PeopleTables>(SCHEMA_ID, definition);
  await connection.InitializeSchema(SCHEMA_ID, definition);
  await seeded.instance(TENANT).table(TABLE).insert(people(TENANT)).run();
  await seeded
    .instance(OTHER_TENANT)
    .table(TABLE)
    .insert(people(OTHER_TENANT))
    .run();
  return seeded;
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
    expect(keys[INSTANCE_INDEX]).to.deep.equal({ _instance: 1 });
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
      INSTANCE_INDEX,
      "category",
      ...Object.keys(definition[TABLE].indexes).map(instanceIndexName),
    );
  });

  it("leaves a legacy unprefixed index in place", async () => {
    await dropPeopleCollection(LEGACY_SCHEMA_ID);
    const collection = await peopleCollection(LEGACY_SCHEMA_ID);
    await collection.createIndex(["email"], { name: "email" });

    await connection.InitializeSchema(LEGACY_SCHEMA_ID, definition);

    const keys = await indexKeysByName(LEGACY_SCHEMA_ID);
    expect(keys.email).to.deep.equal({ email: 1 });
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
    expect(keys).to.have.all.keys(
      PRIMARY_INDEX,
      INSTANCE_INDEX,
      instanceIndexName("email"),
    );
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

  it("serves a scoped between with the prefixed index", async () => {
    const query = schema.instance(TENANT).table(TABLE).between("age", 5, 8);

    expect(await usedIndexes(query)).to.deep.equal([instanceIndexName("age")]);
  });

  it("serves a scoped orderBy with the prefixed index", async () => {
    const query = schema.instance(TENANT).table(TABLE).orderBy("age");

    expect(await usedIndexes(query)).to.deep.equal([instanceIndexName("age")]);
  });

  it("serves a scoped compound getAll with the prefixed index", async () => {
    const keys = [
      ["last-1", "first-5"],
      ["last-2", "first-6"],
    ];
    const query = schema
      .instance(TENANT)
      .table(TABLE)
      .getAll(compoundKeys(keys), "fullName");

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

  it("matches every field of a compound index", async () => {
    sinon.stub(Logging, "Warn");
    const keys = [
      ["last-1", "first-5"],
      ["last-2", "first-6"],
    ];
    const result = await schema
      .instance(CROSS_INSTANCE)
      .table(TABLE)
      .getAll(compoundKeys(keys), "fullName")
      .run();

    expect(sortedIds(result)).to.deep.equal(
      [
        `${OTHER_TENANT}-5`,
        `${OTHER_TENANT}-6`,
        `${TENANT}-5`,
        `${TENANT}-6`,
      ].sort(),
    );
  });

  it("rejects a compound key of the wrong arity", async () => {
    const query = schema
      .instance(TENANT)
      .table(TABLE)
      .getAll("last-1", "fullName");

    await assert.rejects(() => query.run(), /takes keys of 2 values/);
  });
});

describe("instance-prefixed indexes: cross-instance warnings", () => {
  before(async () => {
    schema = await indexedSchema();
  });

  afterEach(() => sinon.restore());

  it("warns once per index named by a cross-instance query", async () => {
    const warn = sinon.stub(Logging, "Warn");
    const table = schema.instance(CROSS_INSTANCE).table(TABLE);

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
