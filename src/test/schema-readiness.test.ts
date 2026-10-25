import sinon from "sinon";
import { expect } from "chai";
import { randomUUID } from "node:crypto";
import { IsDuplicateKeyError } from "@antelopejs/interface-database";
import {
  Schema,
  type SchemaDefinition,
} from "@antelopejs/interface-database/schema";

import * as connection from "../connection";
import { Schemas } from "../implementations/database/schema";
import { collectionName } from "../implementations/database/utils";

interface Account {
  _id: string;
  email: string;
}

interface AccountTables {
  accounts: Account;
}

const TABLE = "accounts";
const RACING_INSERTS = 4;
const INDEX_BUILD_DELAY_MS = 500;

const definition: SchemaDefinition = {
  [TABLE]: {
    fields: {},
    indexes: { email: { fields: ["email"], unique: true } },
  },
};

function delayIndexBuilds(): void {
  const build = connection.InitializeSchema;
  sinon
    .stub(connection, "InitializeSchema")
    .callsFake(async (schemaId, schema, signal) => {
      await new Promise((resolve) => setTimeout(resolve, INDEX_BUILD_DELAY_MS));
      return build(schemaId, schema, signal);
    });
}

describe("schema readiness", () => {
  const schemaId = `schema-readiness-${randomUUID()}`;

  afterEach(async () => {
    sinon.restore();
    Schemas.unregister(schemaId);
    const collection = await connection.GetCollection(
      collectionName(schemaId, TABLE),
    );
    await collection.drop().catch(() => undefined);
  });

  it("creates one row when inserts on a fresh unique key race a slow index build", async () => {
    delayIndexBuilds();
    const schema = new Schema<AccountTables>(schemaId, definition);
    const email = "racer@example.test";

    const outcomes = await Promise.allSettled(
      Array.from({ length: RACING_INSERTS }, () =>
        schema.instance().table(TABLE).insert({ email }).run(),
      ),
    );

    const collection = await connection.GetCollection(
      collectionName(schemaId, TABLE),
    );
    expect(await collection.countDocuments({ email })).to.equal(1);
    const refusals = outcomes.flatMap((outcome) =>
      outcome.status === "rejected" ? [outcome.reason] : [],
    );
    expect(refusals).to.have.length(RACING_INSERTS - 1);
    expect(refusals.every(IsDuplicateKeyError)).to.equal(true);
  });
});
