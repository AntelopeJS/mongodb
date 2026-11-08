import type { Journey } from "@antelopejs/tooling-configs/verify";

import { startOnClientDatabase } from "./client-db.ts";
import { CLIENT_DATABASE, catalog, same, start, thrown, type Stack } from "./mongo.ts";
import { pick, type Json } from "./json.ts";

const RELATION = `${CLIENT_DATABASE}.customers`;

function each(value: Json | undefined, fields: string[]): Json {
  if (!Array.isArray(value)) return null;
  return value.map((entry) => fields.map((field) => pick(entry, field) ?? null));
}

async function lists(stack: Stack): Promise<void> {
  const { context } = stack;
  const info = await catalog(stack, "Info", "Info", []);
  const relations = await catalog(stack, "ListRelations", "ListRelations", []);
  context.expect(relations.folder, [
    same("Info names the engine, server and database", each([info.value ?? null], ["engine", "version", "database"]), [["mongodb", "8.0.8", CLIENT_DATABASE]]),
    same("the collection is a table keyed by _id and the view has no key", each(relations.value, ["name", "kind", "key", "estimatedRows"]), [
      ["big_customers", "view", null, null],
      ["customers", "table", { columns: ["_id"], source: "primary" }, 2],
    ]),
  ]);
}

async function describes(stack: Stack): Promise<void> {
  const { context } = stack;
  const described = await catalog(stack, "DescribeRelations", "DescribeRelations", [[RELATION]]);
  const relation = pick(described.value, 0);
  const columns = pick(relation, "columns");
  context.expect(described.folder, [
    same("each sampled field lists its native types and the declarations that read it", each(columns, ["name", "native", "accepts"]), [
      ["_id", "objectId 100%", [{ declared: "string" }]],
      ["name", "string 100% (validator: string)", [{ declared: "string" }]],
      ["credit", "long 100% (validator: long)", [{ declared: "number", lossy: "long and decimal values read as doubles, exact only within 2^53 and 15 digits" }, { declared: "string" }]],
      ["city", "string 100%", [{ declared: "string" }]],
      ["tier", "string 100%", [{ declared: "string" }]],
    ]),
    same("the validator's description and enum become the comment and the labels", each(columns, ["comment", "enumLabels"]), [
      [null, null],
      ["Display name", null],
      [null, null],
      [null, null],
      [null, ["gold", "silver"]],
    ]),
    same("the collection's own indexes are listed", each(pick(relation, "indexes"), ["name", "columns", "unique"]), [
      ["_id_", ["_id"], false],
      ["by_name", ["name"], false],
    ]),
  ]);
}

function column(name: string): Json {
  return { relation: RELATION, column: name };
}

async function references(stack: Stack): Promise<void> {
  const { context } = stack;
  const key = await catalog(stack, "CheckReference _id to _id", "CheckReference", [{ from: column("_id"), to: column("_id") }]);
  const none = await catalog(stack, "CheckReference name to city", "CheckReference", [{ from: column("name"), to: column("city") }]);
  context.expect(none.folder, [
    same("every _id is held by the key _id", key.value, { status: "ok", value: { containment: 1, checkedRows: 2, sampled: false, targetIsKey: true } }),
    same("no name is a city", none.value, { status: "ok", value: { containment: 0, checkedRows: 2, sampled: false, targetIsKey: false } }),
  ]);
}

async function profiles(stack: Stack): Promise<void> {
  const { context } = stack;
  const profile = await catalog(stack, "Profile", "Profile", [{ relation: RELATION, columns: ["credit", "tier"] }]);
  const sample = await catalog(stack, "SampleRows", "SampleRows", [{ relation: RELATION, columns: ["name", "credit"], limit: 5 }]);
  context.expect(sample.folder, [
    same("the profile counts every row exactly, with no sampling", [pick(profile.value, "rows") ?? null, pick(profile.value, "sample") ?? null], [{ status: "ok", value: { rows: 2, exact: true } }, null]),
    same("credit has a range and tier names no value counted fewer than 5 times", each(pick(profile.value, "columns"), ["name", "range", "categories"]), [
      ["credit", { status: "ok", value: { min: 10, max: 20 } }, { status: "ok", value: { values: [], other: 2 } }],
      ["tier", null, { status: "ok", value: { values: [], other: 2 } }],
    ]),
    same("SampleRows answers each value as text", [pick(sample.value, "columns") ?? null, sortedRows(pick(sample.value, "rows"))], [
      ["name", "credit"],
      [
        [{ text: "Ada", truncated: false }, { text: "10", truncated: false }],
        [{ text: "Bob", truncated: false }, { text: "20", truncated: false }],
      ],
    ]),
  ]);
}

/** The sampled rows in a fixed order, since a sample promises none. */
function sortedRows(rows: Json | undefined): Json {
  if (!Array.isArray(rows)) return null;
  return [...rows].sort((left, right) => JSON.stringify(left).localeCompare(JSON.stringify(right)));
}

const catalogDescribes: Journey = {
  title: "the catalog of an external instance lists, describes, checks references between, profiles and samples the client's collections with the registration rules of the module",
  entryPoint: "`Info`, `ListRelations`, `DescribeRelations`, `CheckReference`, `Profile` and `SampleRows` of `@antelopejs/interface-database-catalog` through core, under `ajs project dev`",
  async run(context) {
    const stack = await startOnClientDatabase(context);
    if (stack === undefined) return;
    await lists(stack);
    await describes(stack);
    await references(stack);
    await profiles(stack);
  },
};

const catalogOwnedRefusal: Journey = {
  title: "an owned instance refuses every catalog call",
  entryPoint: "`Info` of `@antelopejs/interface-database-catalog` through core on an owned instance",
  async run(context) {
    const stack = await start(context);
    const info = await catalog(stack, "Info on an owned instance", "Info", []);
    context.expect(info.folder, [
      { name: "the call is refused", pass: !info.ok, detail: info.body },
      same("the refusal says the instance owns its collections", pick(thrown(info), 1), "the catalog describes a database an external instance was pointed at; this instance owns its collections"),
    ]);
  },
};

export const catalogJourneys = { "catalog-describes": catalogDescribes, "catalog-owned-refusal": catalogOwnedRefusal };
