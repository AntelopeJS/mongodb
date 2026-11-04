import assert from "node:assert/strict";
import { ValueProxy } from "@antelopejs/interface-database";
import type { QueryStage } from "@antelopejs/interface-database/common";
import { Schema } from "@antelopejs/interface-database/schema";

import { DecodeFunction } from "../implementations/database/expression";
import { DecodingContext } from "../implementations/database/utils";

const SCHEMA_ID = "arg-stage";
const INSTANCE = "one";
const BOUND = "$$bound";
const ITEMS = ["a", "b"];

/** An id as a stage list rebuilt from client JSON can carry it. */
type JsonId =
  | string
  | number
  | boolean
  | null
  | JsonId[]
  | { [key: string]: JsonId };

type IdPair = [funcId: JsonId, argId: JsonId];

const SAME_ID_AS_RECORDED: IdPair[] = [
  ["$x", "$x"],
  ["$x", ["$x"]],
  [["$x"], "$x"],
  [["$x"], ["$x"]],
  [null, null],
  ["null", null],
  [[null], [null]],
];

const OBJECT_FUNC_IDS: JsonId[] = [{}, { a: 1 }, "[object Object]"];
const REWRITTEN_ARG_IDS: JsonId[] = ["$x", null, ["$x"], [null]];

const OTHER_ID_THAN_RECORDED: IdPair[] = OBJECT_FUNC_IDS.flatMap((funcId) =>
  REWRITTEN_ARG_IDS.map((argId): IdPair => [funcId, argId]),
);

const NUMBER_IDS: IdPair[] = [
  [1, 1],
  ["1", 1],
];

const OTHER_NUMBER_IDS: IdPair[] = [
  [1, 2],
  [1, 0],
];

function Func(funcId: number, argId: number): QueryStage<"func"> {
  return {
    stage: "func",
    args: [[funcId], new ValueProxy<string>({ stage: "arg", args: [argId] })],
  };
}

function ForgedFunc([funcId, argId]: IdPair) {
  // @ts-expect-error ids that are not numbers, which a stage list rebuilt from client JSON can hold and the driver reads as recorded
  return Func(funcId, argId);
}

function Bound(pair: IdPair) {
  return DecodeFunction(ForgedFunc(pair), new DecodingContext(), [BOUND]);
}

function Label([funcId, argId]: IdPair) {
  return `func id ${JSON.stringify(funcId)} with arg id ${JSON.stringify(argId)}`;
}

describe("an arg stage reads the argument its func bound under the id both recorded", () => {
  it("binds the argument when the ids are the same, though DecodeValue rewrites them", async () => {
    for (const pair of SAME_ID_AS_RECORDED) {
      assert.equal(await Bound(pair), BOUND, Label(pair));
    }
  });

  it("answers undefined when the func recorded another id, though DecodeValue rewrites the arg id", async () => {
    for (const pair of OTHER_ID_THAN_RECORDED) {
      assert.equal(await Bound(pair), undefined, Label(pair));
    }
  });

  it("binds a number id as before, and no other number", async () => {
    for (const pair of NUMBER_IDS) {
      assert.equal(await Bound(pair), BOUND, Label(pair));
    }
    for (const pair of OTHER_NUMBER_IDS) {
      assert.equal(await Bound(pair), undefined, Label(pair));
    }
  });
});

const schema = new Schema<{ rows: { _id: string } }>(SCHEMA_ID, {
  rows: { fields: {}, indexes: {} },
});

function Rows() {
  return schema.instance(INSTANCE).table("rows");
}

describe("a query whose arg ids DecodeValue rewrites", () => {
  before(async () => {
    await schema.createInstance(INSTANCE).run();
    await Rows().insert({ _id: "one" }).run();
  });

  after(async () => {
    await schema.destroyInstance(INSTANCE).run();
  });

  it("maps over the list with the argument its func bound", async () => {
    for (const pair of SAME_ID_AS_RECORDED) {
      const mapped = new ValueProxy<string[]>(
        { stage: "arr_map", args: [ForgedFunc(pair)] },
        ValueProxy.constant(ITEMS),
      );
      assert.deepEqual(await Rows().map(() => mapped), [ITEMS], Label(pair));
    }
  });
});
