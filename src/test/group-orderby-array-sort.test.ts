import assert from "node:assert/strict";
import { Schema } from "@antelopejs/interface-database/schema";

const SCHEMA_ID = "group-orderby-array-sort";
const INSTANCE = "sort-tenant";
const GROUPS = ["a", "b"] as const;
const DIRECTIONS = ["asc", "desc"] as const;
const SEED = 1705;
const DRAWN_CASES = 200;
const MAX_ROWS = 8;
const MAX_ARRAY = 3;

type Group = (typeof GROUPS)[number];
type Direction = (typeof DIRECTIONS)[number];
type Scalar = number | string | boolean | Date | { x: number } | null;
type Key = Scalar | Scalar[];

interface Drawn {
  g: Group;
  k?: Key;
}

interface Row extends Drawn {
  _id: string;
  run: number;
  s: number;
  r: number;
}

const schema = new Schema<{ rows: Row }>(SCHEMA_ID, {
  rows: {
    fields: {},
    indexes: {
      run: {},
      k: {},
      ks: { fields: ["k", "s"] },
      kr: { fields: ["k", "r"] },
    },
  },
});

const table = () => schema.instance(INSTANCE).table("rows");

const SCALARS: (() => Scalar)[] = [
  () => 0,
  () => 2,
  () => -1,
  () => -1.5,
  () => 0.5,
  () => "",
  () => "a",
  () => "B",
  () => true,
  () => false,
  () => new Date(0),
  () => new Date(86_400_000),
  () => ({ x: 0 }),
  () => ({ x: 1 }),
  () => null,
];

/** A seeded mulberry32 generator, so every run draws the same cases. */
function Random(seed: number) {
  let state = seed;
  return (bound: number) => {
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) % bound;
  };
}

function DrawCases(): Drawn[][] {
  const below = Random(SEED);
  const pick = <T>(items: readonly T[]): T => items[below(items.length)];
  const scalar = () => pick(SCALARS)();
  const keys: (() => Key | undefined)[] = [
    () => undefined,
    scalar,
    () => [],
    () => [null],
    () => Array.from({ length: 1 + below(MAX_ARRAY) }, scalar),
  ];
  return Array.from({ length: DRAWN_CASES }, () =>
    Array.from({ length: 1 + below(MAX_ROWS) }, () => {
      const k = pick(keys)();
      const g = pick(GROUPS);
      return k === undefined ? { g } : { g, k };
    }),
  );
}

let nextRun = 0;

async function Store(drawn: Drawn[]): Promise<number> {
  const run = nextRun++;
  await table()
    .insert(
      drawn.map((row, at) => ({
        ...row,
        _id: `${run}-${at}`,
        run,
        s: at,
        r: -at,
      })),
    )
    .run();
  return run;
}

async function TopLevel(run: number, index: string, direction: Direction) {
  const rows = await table()
    .filter((row) => row.key("run").eq(run))
    .orderBy(index, direction)
    .map((row) => ({ g: row.key("g"), id: row.key("_id") }))
    .run();
  return new Map(
    GROUPS.map((g) => [
      g,
      rows.filter((row) => row.g === g).map((row) => row.id),
    ]),
  );
}

async function Grouped(run: number, direction: Direction) {
  const groups = await table()
    .filter((row) => row.key("run").eq(run))
    .group("g", (rows, g) => ({
      g,
      ids: rows.orderBy("k", direction).map((row) => row.key("_id")),
    }))
    .run();
  return new Map(groups.map((group) => [group.g, group]));
}

/**
 * The rows of `bySequence` that top-level `$sort` ranks equal on `k`: `ks` breaks a tie on `k` by `s`
 * and `kr` by `r`, which is `-s`, so two rows tie exactly when `ks` and `kr` order them apart.
 */
function TieClasses(bySequence: string[], byReverse: string[]): string[][] {
  const reversePosition = new Map(byReverse.map((id, at) => [id, at]));
  const classes: string[][] = [];
  let previous: string | undefined;
  for (const id of bySequence) {
    const tied =
      previous !== undefined &&
      (reversePosition.get(id) ?? 0) < (reversePosition.get(previous) ?? 0);
    const last = classes[classes.length - 1];
    if (tied && last) {
      last.push(id);
    } else {
      classes.push([id]);
    }
    previous = id;
  }
  return classes;
}

function Chunks(order: string[], classes: string[][]): string[][] {
  const chunks: string[][] = [];
  let at = 0;
  for (const tie of classes) {
    chunks.push(order.slice(at, at + tie.length).sort());
    at += tie.length;
  }
  return chunks;
}

async function AnswersTopLevelOrder(drawn: Drawn[]) {
  const run = await Store(drawn);
  for (const direction of DIRECTIONS) {
    const [single, sequence, reverse, grouped] = await Promise.all([
      TopLevel(run, "k", direction),
      TopLevel(run, "ks", direction),
      TopLevel(run, "kr", direction),
      Grouped(run, direction),
    ]);
    for (const g of GROUPS) {
      const bySequence = sequence.get(g) ?? [];
      const byReverse = reverse.get(g) ?? [];
      const group = grouped.get(g);
      if (bySequence.length === 0) {
        assert.equal(group, undefined);
        continue;
      }
      assert.ok(group, `group ${g} answers`);
      const context = `group ${g} ${direction}`;
      const classes = TieClasses(bySequence, byReverse);
      const expected = classes.map((tie) => [...tie].sort());
      assert.deepEqual(
        Chunks(single.get(g) ?? [], classes),
        expected,
        `${context} top-level by k`,
      );
      assert.deepEqual(Chunks(group.ids, classes), expected, context);
    }
  }
}

const FIXED_CASES: Drawn[][] = [
  [
    { g: "a", k: [null, 5] },
    { g: "a", k: 3 },
    { g: "a", k: [] },
    { g: "a", k: null },
    { g: "a" },
    { g: "a", k: [null] },
  ],
  [
    { g: "a", k: [] },
    { g: "a", k: 1 },
    { g: "a", k: null },
  ],
  [{ g: "a", k: [] }, { g: "a", k: [2, 0] }, { g: "a" }, { g: "a", k: -1 }],
];

describe("group mapper orderBy on an array key", () => {
  before(async () => {
    await schema.createInstance(INSTANCE).run();
  });

  after(async () => {
    await schema.destroyInstance(INSTANCE).run();
  });

  it("ranks an array key by its null element and an empty array as a top-level orderBy does, not by $min or $max", async () => {
    for (const drawn of [...FIXED_CASES, ...DrawCases()]) {
      await assert.doesNotReject(
        AnswersTopLevelOrder(drawn),
        `rows ${JSON.stringify(drawn)}`,
      );
    }
  });
});
