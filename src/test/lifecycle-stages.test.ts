import { expect } from "chai";
import type { QueryStage } from "@antelopejs/interface-database/common";

import { RunQuery } from "../implementations/database/query";

const SCHEMA_ID = "lifecycle-stages";

function schemaThen(stage: string): QueryStage[] {
  return [
    { stage: "schema", options: { id: SCHEMA_ID }, args: [] },
    { stage, args: [] },
  ];
}

describe("lifecycle stages", () => {
  for (const stage of [
    "constructor",
    "toString",
    "hasOwnProperty",
    "valueOf",
    "__proto__",
  ]) {
    it(`reads a second stage named ${stage} as an unknown stage`, async () => {
      let refusal: unknown;
      try {
        await RunQuery(schemaThen(stage));
      } catch (error) {
        refusal = error;
      }
      expect(refusal).to.be.instanceOf(Error);
      expect(refusal)
        .to.have.property("message")
        .that.contains("Invalid request");
    });
  }
});
