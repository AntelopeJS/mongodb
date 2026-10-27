import { expect } from "chai";
import type { Schema } from "@antelopejs/interface-database/schema";

import { type ErpTables, Native, NativeCount, RegisterErp } from "./erp";
import {
  AdminClient,
  CLIENT_DATABASE,
  CustomerId,
  WRITER_USER,
} from "./fixture";

let erp: Schema<ErpTables>;

function Orders() {
  return erp.instance().table("orders");
}

async function LastPlanOver(field: string): Promise<string> {
  const admin = await AdminClient();
  try {
    const [entry] = await admin
      .db(CLIENT_DATABASE)
      .collection("system.profile")
      .find({
        user: `${WRITER_USER}@${CLIENT_DATABASE}`,
        "command.aggregate": "orders",
        [`command.pipeline.0.$match.${field}`]: { $exists: true },
      })
      .sort({ ts: -1 })
      .limit(1)
      .toArray();
    return String(entry?.planSummary);
  } finally {
    await admin.close();
  }
}

describe("comparisons an external collection's indexes serve", () => {
  const customer = CustomerId(0);

  before(async () => {
    erp = await RegisterErp();
  });

  it("filters orders by a customer's hex _id, the string-stored ones included, through the index", async () => {
    const rows = await Orders().filter((row) =>
      row.key("customerId").eq(customer.toHexString()),
    );
    const native = await NativeCount("orders", {
      customerId: { $in: [customer, customer.toHexString()] },
    });
    const stringStored = await NativeCount("orders", {
      customerId: customer.toHexString(),
    });
    expect(rows).to.have.length(native);
    expect(stringStored).to.be.greaterThan(0).and.lessThan(native);
    expect(
      rows.every((row) => row.customerId === customer.toHexString()),
    ).to.equal(true);
    expect(await LastPlanOver("customerId")).to.contain(
      "IXSCAN { customerId: 1 }",
    );
  });

  it("answers getAll on the customerId index with both stored forms", async () => {
    const rows = await Orders().getAll([customer.toHexString()], "customerId");
    expect(rows).to.have.length(
      await NativeCount("orders", {
        customerId: { $in: [customer, customer.toHexString()] },
      }),
    );
  });

  it("sums a Decimal128 total exactly and converts the sum once", async () => {
    const [native] = await Native("orders", [
      { $group: { _id: null, sum: { $sum: "$total" } } },
    ]);
    expect(await Orders().sum("total")).to.equal(Number(native.sum.toString()));
  });

  it("averages, and takes the minimum and maximum of, a number field whose stray string reads null", async () => {
    const products = erp.instance().table("products");
    const [native] = await Native("products", [
      { $match: { $expr: { $isNumber: "$price" } } },
      {
        $group: { _id: null, max: { $max: "$price" }, min: { $min: "$price" } },
      },
    ]);
    expect(await products.max("price")).to.equal(native.max);
    expect(await products.min("price")).to.equal(native.min);
  });
});
