import sinon from "sinon";
import { expect } from "chai";
import { Collection } from "mongodb";

import * as connection from "../connection";
import {
  collectionName,
  INSTANCE_FIELD,
  instanceIndexName,
} from "../implementations/database/utils";

const SCHEMA_ID = "initialize-settle";
const FAILING_TABLE = "failing";
const SLOW_TABLE = "slow";
const SLOW_LISTING_MS = 150;
const INSTANCE_INDEX = "_instance";
const DUPLICATE_SCHEMA_ID = "initialize-duplicates";
const DUPLICATE_TABLE = "accounts";
const DUPLICATE_KEY_CODE = 11000;
const MANY_SCHEMA_ID = "initialize-many";
const MANY_TABLES = 20;
const failure = new Error("listing failed");
const ORDER_SCHEMA_ID = "initialize-order";
const lateFailure = new Error("late listing failure");
const earlyFailure = new Error("early listing failure");
const tables: connection.SchemaDefinition = {
  [FAILING_TABLE]: { indexes: {} },
  [SLOW_TABLE]: { indexes: {} },
};

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

describe("InitializeSchema failure", () => {
  afterEach(async () => {
    sinon.restore();
    const db = await connection.GetDatabase();
    await Promise.all(
      Object.keys(tables).map((tableId) =>
        db
          .dropCollection(collectionName(SCHEMA_ID, tableId))
          .catch(() => false),
      ),
    );
  });

  it("finishes the other tables before it rejects with the first failure", async () => {
    const listIndexes = sinon.stub(Collection.prototype, "indexes");
    listIndexes.callsFake(async function (this: Collection) {
      if (this.collectionName === collectionName(SCHEMA_ID, FAILING_TABLE)) {
        throw failure;
      }
      await delay(SLOW_LISTING_MS);
      return listIndexes.wrappedMethod.call(this);
    });

    const error = await connection.InitializeSchema(SCHEMA_ID, tables).then(
      () => undefined,
      (rejection: unknown) => rejection,
    );

    sinon.restore();
    const db = await connection.GetDatabase();
    const slowIndexes = await db
      .collection(collectionName(SCHEMA_ID, SLOW_TABLE))
      .indexes();
    expect(error).to.equal(failure);
    expect(slowIndexes.map((index) => index.name)).to.include(INSTANCE_INDEX);
  });

  it("keeps the indexes it built before a unique index that the data refuses", async () => {
    const db = await connection.GetDatabase();
    const name = collectionName(DUPLICATE_SCHEMA_ID, DUPLICATE_TABLE);
    await db.collection(name).insertMany([
      { [INSTANCE_FIELD]: "tenant", login: "same", team: "a" },
      { [INSTANCE_FIELD]: "tenant", login: "same", team: "a" },
    ]);

    const error = await connection
      .InitializeSchema(DUPLICATE_SCHEMA_ID, {
        [DUPLICATE_TABLE]: {
          indexes: {
            team: { fields: ["team"] },
            login: { fields: ["login"], unique: true },
          },
        },
      })
      .then(
        () => undefined,
        (rejection: unknown) => rejection,
      );

    const indexes = await db.collection(name).indexes();
    await db.dropCollection(name);
    expect(error).to.have.property("code", DUPLICATE_KEY_CODE);
    expect(indexes.map((index) => index.name)).to.include(
      instanceIndexName("team"),
    );
  });

  it("starts no further table once one has failed", async () => {
    const first = collectionName(MANY_SCHEMA_ID, "t0");
    const listIndexes = sinon.stub(Collection.prototype, "indexes");
    listIndexes.callsFake(async function (this: Collection) {
      if (this.collectionName === first) {
        throw failure;
      }
      return listIndexes.wrappedMethod.call(this);
    });
    const many: connection.SchemaDefinition = Object.fromEntries(
      Array.from({ length: MANY_TABLES }, (_, index) => [
        `t${index}`,
        { indexes: {} },
      ]),
    );

    const error = await connection.InitializeSchema(MANY_SCHEMA_ID, many).then(
      () => undefined,
      (rejection: unknown) => rejection,
    );

    sinon.restore();
    const db = await connection.GetDatabase();
    const created = (await db.listCollections().toArray())
      .map((collection) => collection.name)
      .filter((collection) => collection.startsWith(`${MANY_SCHEMA_ID}__`));
    await Promise.all(
      created.map((collection) => db.dropCollection(collection)),
    );
    expect(error).to.equal(failure);
    expect(created.length).to.be.lessThan(MANY_TABLES);
  });
});

describe("InitializeSchema failure order", () => {
  afterEach(() => {
    sinon.restore();
  });

  it("rejects with the failure that happened first", async () => {
    const late = collectionName(ORDER_SCHEMA_ID, "late");
    const listIndexes = sinon.stub(Collection.prototype, "indexes");
    listIndexes.callsFake(async function (this: Collection) {
      if (this.collectionName === late) {
        await delay(SLOW_LISTING_MS);
        throw lateFailure;
      }
      throw earlyFailure;
    });

    const error = await connection
      .InitializeSchema(ORDER_SCHEMA_ID, {
        late: { indexes: {} },
        early: { indexes: {} },
      })
      .then(
        () => undefined,
        (rejection: unknown) => rejection,
      );

    sinon.restore();
    const db = await connection.GetDatabase();
    await Promise.all(
      ["late", "early"].map((tableId) =>
        db
          .dropCollection(collectionName(ORDER_SCHEMA_ID, tableId))
          .catch(() => false),
      ),
    );
    expect(error).to.equal(earlyFailure);
  });
});
