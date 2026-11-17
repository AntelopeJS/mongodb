/** The tables and mappers the check draws, with a fixed seed. */
import fc from "fast-check";

import { COMPARISONS, type Field, type Filter, REDUCTIONS, type Row } from "./reference";

const BUCKETS = ["a", "b", "c"] as const;
const MAX_ROWS = 8;
const MAX_FIELDS = 4;
const MIN_N = -2;
const MAX_N = 2;

/** A case: the rows of the table and the fields of the mapper, in the order the mapper returns them. */
export interface Drawn {
  rows: Row[];
  fields: Field[];
}

const POSITIONS = Array.from({ length: MAX_ROWS }, (_, at) => at);
const SLOTS = Array.from({ length: MAX_FIELDS }, (_, at) => at);

const filter: fc.Arbitrary<Filter> = fc.record({
  comparison: fc.constantFrom(...COMPARISONS),
  threshold: fc.integer({ min: MIN_N, max: MAX_N }),
});

const reduction = fc.constantFrom(...REDUCTIONS);

const unfiltered: fc.Arbitrary<Field> = reduction.map((each) => ({ kind: "reduce", reduction: each }));

const filtered: fc.Arbitrary<Field> = fc.oneof(
  fc.record({ kind: fc.constant("filterReduce" as const), reduction, filter }),
  fc.record({ kind: fc.constant("filterMapCount" as const), filter }),
);

const ordered: fc.Arbitrary<Field> = fc
  .constantFrom("asc" as const, "desc" as const)
  .map((direction) => ({ kind: "orderFirst", direction }));

/**
 * A mapper of 2 to 4 fields. It always holds one unfiltered and one filtered
 * reduction, and up to two more fields of any kind, in any order.
 */
const fields: fc.Arbitrary<Field[]> = fc
  .tuple(
    unfiltered,
    filtered,
    fc.array(fc.oneof(unfiltered, filtered, ordered), { maxLength: MAX_FIELDS - 2 }),
    fc.shuffledSubarray(SLOTS, { minLength: MAX_FIELDS, maxLength: MAX_FIELDS }),
  )
  .map(([first, second, more, order]) => {
    const chosen = [first, second, ...more];
    return order.filter((slot) => slot < chosen.length).map((slot) => chosen[slot]);
  });

/** One to 8 rows in 1 to 3 groups, with a `t` distinct per row. */
const rows: fc.Arbitrary<Row[]> = fc
  .tuple(
    fc.array(
      fc.record({
        bucket: fc.constantFrom(...BUCKETS),
        n: fc.integer({ min: MIN_N, max: MAX_N }),
      }),
      { minLength: 1, maxLength: MAX_ROWS },
    ),
    fc.shuffledSubarray(POSITIONS, { minLength: MAX_ROWS, maxLength: MAX_ROWS }),
  )
  .map(([stored, order]) => stored.map((row, at) => ({ _id: `row-${at}`, ...row, t: order[at] })));

/** `count` cases, the same ones on every run of one seed. */
export function draw(count: number, seed: number): Drawn[] {
  return fc.sample(fc.record({ rows, fields }), { numRuns: count, seed });
}
