import { expect } from "chai";

import { GetCollection } from "../connection";
import {
  GetTransactionOptions,
  RunInTransaction,
} from "../implementations/database/transactions";

const FIRST_COLLECTION = "transaction_first";
const SECOND_COLLECTION = "transaction_second";
const TRANSACTION_TIMEOUT_MS = 30_000;

async function insert(collectionName: string, id: string): Promise<void> {
  const collection = await GetCollection(collectionName);
  await collection.insertOne({ id }, GetTransactionOptions());
}

async function count(collectionName: string): Promise<number> {
  const collection = await GetCollection(collectionName);
  return collection.countDocuments({}, GetTransactionOptions());
}

async function expectRejection(
  promise: Promise<unknown>,
  message: string,
): Promise<void> {
  let rejection: unknown;
  try {
    await promise;
  } catch (error) {
    rejection = error;
  }
  expect(rejection).to.be.instanceOf(Error);
  if (!(rejection instanceof Error)) {
    return;
  }
  expect(rejection.message).to.include(message);
}

describe("transactions", () => {
  beforeEach(async () => {
    await Promise.all(
      [FIRST_COLLECTION, SECOND_COLLECTION].map(async (name) => {
        const collection = await GetCollection(name);
        await collection.deleteMany({});
      }),
    );
  });

  it("commits writes across collections", async () => {
    await RunInTransaction(async () => {
      await insert(FIRST_COLLECTION, "first");
      await insert(SECOND_COLLECTION, "second");
    });

    expect(await count(FIRST_COLLECTION)).to.equal(1);
    expect(await count(SECOND_COLLECTION)).to.equal(1);
  });

  it("rolls back every write and invokes the callback once", async () => {
    let calls = 0;
    await expectRejection(
      RunInTransaction(async () => {
        calls += 1;
        await insert(FIRST_COLLECTION, "rolled-back");
        throw new Error("rollback");
      }),
      "rollback",
    );

    expect(calls).to.equal(1);
    expect(await count(FIRST_COLLECTION)).to.equal(0);
  });

  it("allows only one concurrent conditional owner claim", async () => {
    const registry = await GetCollection(FIRST_COLLECTION);
    await registry.insertOne({ objectId: "object", owner: null });
    const claim = (owner: string) =>
      RunInTransaction(async () => {
        const result = await registry.updateOne(
          { objectId: "object", owner: null },
          { $set: { owner } },
          GetTransactionOptions(),
        );
        return result.modifiedCount;
      });

    const results = await Promise.allSettled([claim("first"), claim("second")]);
    const winners = results.filter(
      (result) => result.status === "fulfilled" && result.value === 1,
    );
    expect(winners).to.have.lengthOf(1);
    expect(await registry.countDocuments({ owner: { $ne: null } })).to.equal(1);
  });

  it("isolates concurrent scopes and rejects nested transactions", async () => {
    const results = await Promise.all([
      RunInTransaction(async () => {
        await insert(FIRST_COLLECTION, "one");
        await expectRejection(
          RunInTransaction(async () => undefined),
          "Nested MongoDB transactions",
        );
        return count(FIRST_COLLECTION);
      }),
      RunInTransaction(async () => {
        await insert(SECOND_COLLECTION, "two");
        return count(SECOND_COLLECTION);
      }),
    ]);
    expect(results).to.deep.equal([1, 1]);
  });

  it("rejects work escaping a completed scope", async () => {
    let release!: () => void;
    let escaped!: Promise<void>;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    await RunInTransaction(async () => {
      escaped = gate.then(() => insert(FIRST_COLLECTION, "late"));
    });

    release();
    await expectRejection(escaped, "transaction scope has expired");
    expect(await count(FIRST_COLLECTION)).to.equal(0);
  });

  it("aborts callbacks exceeding the transaction timeout", async function () {
    this.timeout(TRANSACTION_TIMEOUT_MS + 5_000);
    const never = new Promise<void>(() => undefined);
    const startedAt = Date.now();

    await expectRejection(
      RunInTransaction(async () => never),
      `timed out after ${TRANSACTION_TIMEOUT_MS}ms`,
    );
    expect(Date.now() - startedAt).to.be.at.least(TRANSACTION_TIMEOUT_MS);
  });
});
