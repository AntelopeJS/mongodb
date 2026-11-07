import sinon from "sinon";
import { expect } from "chai";
import { Collection } from "mongodb";

import * as connection from "../connection";
import { collectionName } from "../implementations/database/utils";

const SCHEMA_ID = "initialize-settle";
const FAILING_TABLE = "failing";
const SLOW_TABLE = "slow";
const SLOW_LISTING_MS = 150;
const INSTANCE_INDEX = "_instance";
const failure = new Error("listing failed");
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
});
