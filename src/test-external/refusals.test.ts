import { expect } from "chai";
import type { Schema } from "@antelopejs/interface-database/schema";

import { type ErpTables, NativeCount, RegisterErp } from "./erp";
import { CustomerId } from "./fixture";

let erp: Schema<ErpTables>;

async function Refusal<T>(work: () => PromiseLike<T>): Promise<string> {
  try {
    await work();
  } catch (error) {
    return error instanceof Error ? error.message : String(error);
  }
  throw new Error("the request was expected to be refused");
}

async function FirstEvent<T>(
  feed: AsyncIterable<T>,
): Promise<IteratorResult<T>> {
  const iterator = feed[Symbol.asyncIterator]();
  try {
    return await iterator.next();
  } finally {
    await iterator.return?.();
  }
}

describe("what an external collection refuses", () => {
  before(async () => {
    erp = await RegisterErp();
  });

  it("refuses changes(), since the adapter configures no change stream on the client's collections", async () => {
    const message = await Refusal(() =>
      FirstEvent(erp.instance().table("orders").changes()),
    );
    expect(message).to.contain("changes()").and.to.contain("orders");
  });

  it("refuses atomicMutation, naming the collection, and writes nothing", async () => {
    const before = await NativeCount("customers", { name: "Customer 1" });
    const message = await Refusal(() =>
      erp
        .instance()
        .table("customers")
        .atomicMutation(CustomerId(1).toHexString(), {
          type: "update",
          revisionField: "name",
          expectedRevision: "Customer 1",
          nextRevision: "Customer one",
          patch: {},
        }),
    );
    expect(message).to.contain("atomicMutation").and.to.contain("customers");
    expect(await NativeCount("customers", { name: "Customer 1" })).to.equal(
      before,
    );
  });

  it("refuses creating and destroying an instance", async () => {
    expect(await Refusal(() => erp.createInstance("t1"))).to.contain(
      "createInstance",
    );
    expect(await Refusal(() => erp.destroyInstance("t1"))).to.contain(
      "destroyInstance",
    );
  });

  it("lists no instances", async () => {
    expect(await erp.listInstances()).to.deep.equal([]);
  });

  it("refuses get on a collection with no usable _id, naming it", async () => {
    const message = await Refusal(() =>
      erp.instance().table("mixed_ids").get("1"),
    );
    expect(message).to.contain("mixed_ids").and.to.contain("no _id");
  });

  it("refuses getAll by _id on a collection with no usable _id, naming it", async () => {
    const message = await Refusal(() =>
      erp.instance().table("mixed_ids").getAll(["1"], "_id"),
    );
    expect(message).to.contain("mixed_ids").and.to.contain("no _id");
  });

  it("still reads a keyless collection by a field", async () => {
    const rows = await erp.instance().table("mixed_ids").getAll(["int"], "v");
    expect(rows).to.have.length(1);
  });
});
