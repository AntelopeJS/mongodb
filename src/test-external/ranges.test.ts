import { expect } from "chai";
import {
  Profile,
  type ValueRange,
  type Measure,
} from "@antelopejs/interface-database-catalog";

import { CLIENT_DATABASE } from "./fixture";

async function RangeOf(
  collection: string,
  column: string,
): Promise<Measure<ValueRange> | undefined> {
  const profile = await Profile({
    relation: `${CLIENT_DATABASE}.${collection}`,
    columns: [column],
  });
  return profile.columns[0]?.range;
}

describe("the range of an external MongoDB number field", () => {
  it("ends at values counted 5 times, past values counted 4 times", async () => {
    expect(await RangeOf("levels", "level")).to.deep.equal({
      status: "ok",
      value: { min: 7002, max: 7008 },
    });
  });

  it("answers empty when every value is seen once", async () => {
    expect(await RangeOf("products", "stock")).to.deep.equal({
      status: "error",
      code: "empty",
    });
  });

  it("counts Long values one double cannot tell apart as distinct values", async () => {
    expect(await RangeOf("snowflakes", "ref")).to.deep.equal({
      status: "error",
      code: "empty",
    });
  });

  it("keeps the true ends of a date, which the time-axis trait reads", async () => {
    const range = await RangeOf("orders", "orderedAt");
    expect(range?.status).to.equal("ok");
  });
});
