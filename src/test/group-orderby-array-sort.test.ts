import assert from "node:assert/strict";
import { Schema } from "@antelopejs/interface-database/schema";

import * as connection from "../connection";
import { collectionName } from "../implementations/database/utils";

const SCHEMA_ID = "group-orderby-array-sort";
const INSTANCE = "sort-tenant";
const GROUPS = ["a", "b"] as const;
const DIRECTIONS = ["asc", "desc"] as const;
const SEED = 1705;
const DRAWN_CASES = 200;
const MAX_ROWS = 8;
const MAX_ARRAY = 3;
const LARGE_ROWS = 110;
const LARGE_PAD_BYTES = 1_000_000;

type Group = (typeof GROUPS)[number];
type Direction = (typeof DIRECTIONS)[number];
type Scalar = number | string | boolean | Date | { x: number } | null;
type Key = Scalar | Scalar[];
type Leaf = Key | (Scalar | Scalar[])[];
type Item = { n?: Leaf; pad?: string } | Scalar;

interface Drawn {
  g: Group;
  k?: Key;
  items?: Item | Item[];
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
      "items.n": {},
      ns: { fields: ["items.n", "s"] },
      nr: { fields: ["items.n", "r"] },
      "items.0": {},
      i0s: { fields: ["items.0", "s"] },
      i0r: { fields: ["items.0", "r"] },
    },
  },
});

const table = () => schema.instance(INSTANCE).table("rows");

/**
 * A key the rows are ordered by: the index named `path` orders on `path` alone, and the indexes
 * `sequence` and `reverse` break a tie on `path` by `s` and by `r`.
 */
interface Order {
  path: string;
  sequence: string;
  reverse: string;
}

const ORDERS: Order[] = [
  { path: "k", sequence: "ks", reverse: "kr" },
  { path: "items.n", sequence: "ns", reverse: "nr" },
];

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
  const leaves: (() => Leaf | undefined)[] = [
    ...keys,
    () => [scalar(), [scalar()]],
  ];
  const item = (): Item => {
    const n = pick(leaves)();
    return below(4) === 0 ? scalar() : n === undefined ? {} : { n };
  };
  const itemsDraws: (() => Item | Item[] | undefined)[] = [
    () => undefined,
    item,
    () => [],
    () => Array.from({ length: 1 + below(MAX_ARRAY) }, item),
    () => Array.from({ length: 1 + below(MAX_ARRAY) }, item),
  ];
  return Array.from({ length: DRAWN_CASES }, () =>
    Array.from({ length: 1 + below(MAX_ROWS) }, () => {
      const k = pick(keys)();
      const items = pick(itemsDraws)();
      const row: Drawn = { g: pick(GROUPS) };
      if (k !== undefined) {
        row.k = k;
      }
      if (items !== undefined) {
        row.items = items;
      }
      return row;
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

function ByGroup(rows: { g: Group; id: string }[]) {
  return new Map(
    GROUPS.map((g) => [
      g,
      rows.filter((row) => row.g === g).map((row) => row.id),
    ]),
  );
}

async function TopLevel(run: number, index: string, direction: Direction) {
  return ByGroup(
    await table()
      .filter((row) => row.key("run").eq(run))
      .orderBy(index, direction)
      .map((row) => ({ g: row.key("g"), id: row.key("_id") }))
      .run(),
  );
}

async function Native(run: number, path: string, direction: Direction) {
  const collection = await connection.GetCollection(
    collectionName(SCHEMA_ID, "rows"),
  );
  const rows = await collection
    .find<Pick<Row, "_id" | "g">>({ run })
    .sort({ [path]: direction === "asc" ? 1 : -1 })
    .toArray();
  return ByGroup(rows.map((row) => ({ g: row.g, id: row._id })));
}

async function Grouped(run: number, index: string, direction: Direction) {
  const groups = await table()
    .filter((row) => row.key("run").eq(run))
    .group("g", (rows, g) => ({
      g,
      ids: rows.orderBy(index, direction).map((row) => row.key("_id")),
      first: rows.orderBy(index, direction).nth(0).key("_id"),
    }))
    .run();
  return new Map(groups.map((group) => [group.g, group]));
}

/**
 * The rows of `bySequence` that top-level `$sort` ranks equal on a key: `bySequence` breaks a tie on the
 * key by `s` and `byReverse` by `r`, which is `-s`, so two rows tie exactly when they order them apart.
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

async function AnswersTopLevelOrder(drawn: Drawn[], orders = ORDERS) {
  const run = await Store(drawn);
  for (const order of orders) {
    for (const direction of DIRECTIONS) {
      const [single, sequence, reverse, native, grouped] = await Promise.all([
        TopLevel(run, order.path, direction),
        TopLevel(run, order.sequence, direction),
        TopLevel(run, order.reverse, direction),
        Native(run, order.path, direction),
        Grouped(run, order.path, direction),
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
        const context = `group ${g} ${order.path} ${direction}`;
        const classes = TieClasses(bySequence, byReverse);
        const expected = classes.map((tie) => [...tie].sort());
        assert.deepEqual(
          Chunks(single.get(g) ?? [], classes),
          expected,
          `${context} top-level`,
        );
        assert.deepEqual(
          Chunks(native.get(g) ?? [], classes),
          expected,
          `${context} find().sort()`,
        );
        assert.deepEqual(Chunks(group.ids, classes), expected, context);
        assert.ok(
          expected[0].includes(group.first),
          `${context} nth(0) is ${group.first}, not one of ${expected[0].join(", ")}`,
        );
      }
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
  [
    { g: "a", items: [{ n: 2 }, {}] },
    { g: "a", items: [{ n: 1 }] },
  ],
  [
    { g: "a", items: [{ n: [5, -1] }] },
    { g: "a", items: [{ n: 0 }] },
  ],
];

describe("group mapper orderBy on an array key", () => {
  before(async () => {
    await schema.createInstance(INSTANCE).run();
  });

  after(async () => {
    await schema.destroyInstance(INSTANCE).run();
  });

  it("ranks an array key, and a dotted key through an array of subdocuments, as a top-level orderBy and find().sort() do", async () => {
    for (const drawn of [...FIXED_CASES, ...DrawCases()]) {
      await assert.doesNotReject(
        AnswersTopLevelOrder(drawn),
        `rows ${JSON.stringify(drawn)}`,
      );
    }
  });

  it("ranks a key through an array index, such as items.0, by that element as a top-level orderBy and find().sort() do", async () => {
    await assert.doesNotReject(
      AnswersTopLevelOrder(
        [
          { g: "a", items: [{ n: 3 }, { n: 0 }] },
          { g: "a", items: [{ n: 2 }] },
          { g: "a", items: [] },
          { g: "a", items: [5, { n: 1 }] },
        ],
        [{ path: "items.0", sequence: "i0s", reverse: "i0r" }],
      ),
    );
  });

  it("two ordered fields collect only the dotted key's leaves, not the whole subdocuments, so a group past the 100 MB $push limit still answers", async () => {
    const pad = "x".repeat(LARGE_PAD_BYTES);
    const run = await Store(
      Array.from({ length: LARGE_ROWS }, (_, n) => ({
        g: "a",
        items: [{ n, pad }],
      })),
    );
    const groups = await table()
      .filter((row) => row.key("run").eq(run))
      .group("g", (rows, g) => ({
        g,
        last: rows.orderBy("items.n", "desc").nth(0).key("_id"),
        first: rows.orderBy("items.n", "asc").nth(0).key("_id"),
      }))
      .run();
    assert.deepEqual(groups, [
      { g: "a", last: `${run}-${LARGE_ROWS - 1}`, first: `${run}-0` },
    ]);
  });
});
