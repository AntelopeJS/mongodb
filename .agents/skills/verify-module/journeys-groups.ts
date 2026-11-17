import type { Journey } from "@antelopejs/tooling-configs/verify";

import { pick } from "./json.ts";
import { same } from "./mongo.ts";
import { ran, startScenario } from "./scenario.ts";

const SCENARIO = `const ledger = new db.Schema("ledger", {
  invoices: { fields: { currency: "string", total: "number", at: "number" }, indexes: { currency: {}, at: {} } },
});
await ledger.createInstance("a").run();
const invoices = ledger.instance("a").table("invoices");
await invoices.insert([
  { _id: "e1", currency: "EUR", total: 5, at: 1 },
  { _id: "e2", currency: "EUR", total: 0, at: 3 },
  { _id: "e3", currency: "EUR", total: 7, at: 2 },
  { _id: "u1", currency: "USD", total: 0, at: 4 },
]).run();
const byCurrency = (rows) => rows.sort((x, y) => x.currency.localeCompare(y.currency));
return {
  summary: byCurrency(await invoices.group("currency", (rows, currency) => ({
    currency,
    latest: rows.orderBy("at", "desc").nth(0).key("_id"),
    paid: rows.filter((row) => row.key("total").gt(0)).count(),
    all: rows.count(),
  })).run()),
  ordered: byCurrency(await invoices.group("currency", (rows, currency) => ({
    currency,
    paidOldest: rows.filter((row) => row.key("total").gt(0)).orderBy("at").map((row) => row.key("_id")),
    newestTwo: rows.orderBy("at", "desc").slice(0, 2).map((row) => row.key("_id")),
  })).run()),
  wide: byCurrency(await invoices.group("currency", (rows, currency) => ({
    currency,
    ids: rows.orderBy("at").slice(0, 2 ** 40).map((row) => row.key("_id")),
  })).run()),
  negativeNth: await invoices.group("currency", (rows) => rows.orderBy("at").nth(-1)).run().then(() => "answered", (error) => error.message.includes("invalid argument to $skip stage")),
};`;

const groupMapper: Journey = {
  title: "a group mapper orders, filters and slices each field over its own rows of the group, and reads slice and nth arguments as a top-level query does",
  entryPoint: "`table.group(index, mapper)` with `orderBy`, `filter`, `slice` and `nth` inside the mapper, run from a consumer module with `await`",
  async run(context) {
    const app = await startScenario(context);
    const answer = await app.run("group invoices of instance a by currency", SCENARIO);
    context.expect(answer.folder, [ran(answer)], { critical: true });
    context.expect(answer.folder, [
      same("an ordered nth picks the latest invoice while a filtered count and a count of the same group read every row", pick(answer.value, "summary"), [
        { currency: "EUR", latest: "e2", paid: 2, all: 3 },
        { currency: "USD", latest: "u1", paid: 0, all: 1 },
      ]),
      same("a filtered field and an ordered sibling field each keep their own rows and order", pick(answer.value, "ordered"), [
        { currency: "EUR", paidOldest: ["e1", "e3"], newestTwo: ["e2", "e3"] },
        { currency: "USD", paidOldest: [], newestTwo: ["u1"] },
      ]),
      same("a slice whose count is past 32 bits answers every row of the group", pick(answer.value, "wide"), [
        { currency: "EUR", ids: ["e1", "e3", "e2"] },
        { currency: "USD", ids: ["u1"] },
      ]),
      same("nth of a negative index is refused with the error of a top-level $skip", pick(answer.value, "negativeNth"), true),
    ]);
  },
};

export const groupJourneys = { "group-mapper": groupMapper };
