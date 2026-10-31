import assert from "node:assert/strict";
import { Schema } from "@antelopejs/interface-database/schema";

import { RunQuery } from "../implementations/database/query";

const SCHEMA_ID = "lifecycle-stage-names";
const INHERITED_NAMES = [
  "constructor",
  "toString",
  "hasOwnProperty",
  "valueOf",
];

describe("lifecycle stage dispatch", () => {
  before(() => {
    new Schema(SCHEMA_ID, { rows: { fields: {}, indexes: {} } });
  });

  it("refuses a second stage named after an Object.prototype member like an unknown stage", async () => {
    for (const name of ["nope", ...INHERITED_NAMES]) {
      await assert.rejects(
        RunQuery([
          { stage: "schema", options: { id: SCHEMA_ID }, args: [] },
          { stage: name, args: [] },
        ]),
        /Invalid request/,
        `stage ${name}`,
      );
    }
  });

  it("still lists the instances of a schema", async () => {
    const instances: unknown = await RunQuery([
      { stage: "schema", options: { id: SCHEMA_ID }, args: [] },
      { stage: "listInstances", args: [] },
    ]);
    assert.ok(Array.isArray(instances));
  });
});
