import fc from "fast-check";

import type { Type, Value } from "./reference";

const MAX_DATE_MILLISECONDS = 4_000_000_000_000;
const MAX_NUMBER = 1_000_000;

/** The values the check draws for each type. */
const ARBITRARIES: Record<Type, fc.Arbitrary<Value>> = {
  date: fc.integer({ min: 0, max: MAX_DATE_MILLISECONDS }).map((milliseconds) => new Date(milliseconds)),
  number: fc.integer({ min: -MAX_NUMBER, max: MAX_NUMBER }),
  string: fc.string({ maxLength: 6 }),
  list: fc.array(fc.integer({ min: -9, max: 9 }), { maxLength: 3 }),
  null: fc.constant(null),
};

/** `count` values of `type`, the same ones on every run of one seed. */
const ARBITRARY_BY_TYPE = new Map(Object.entries(ARBITRARIES));

function arbitraryOf(type: Type): fc.Arbitrary<Value> {
  const arbitrary = ARBITRARY_BY_TYPE.get(type);
  if (!arbitrary) throw new Error(`no arbitrary for ${type}`);
  return arbitrary;
}

export function draw(type: Type, count: number, seed: number): Value[] {
  return fc.sample(arbitraryOf(type), { numRuns: count, seed });
}
