export const COMPARISONS = ["gt", "lt", "eq", "ne"] as const;

export type Comparison = (typeof COMPARISONS)[number];

export const REDUCTIONS = ["count", "sum", "avg", "min", "max", "distinct"] as const;

export type Reduction = (typeof REDUCTIONS)[number];

export type Direction = "asc" | "desc";

/** A `filter` on `n`: keep the rows whose `n` compares to `threshold`. */
export interface Filter {
  comparison: Comparison;
  threshold: number;
}

/** A stored row: `t` is distinct per row and orders the rows. */
export interface Row {
  _id: string;
  bucket: string;
  n: number;
  t: number;
}

/**
 * One field of the mapper:
 * - `reduce`: a reduction of `n` over the group.
 * - `filterReduce`: the same reduction behind a `filter`.
 * - `filterMapCount`: `filter`, then `map` to the `n` of each row, then `count()`.
 * - `orderFirst`: `orderBy("t", direction).nth(0).key("t")`.
 */
export type Field =
  | { kind: "reduce"; reduction: Reduction }
  | { kind: "filterReduce"; reduction: Reduction; filter: Filter }
  | { kind: "filterMapCount"; filter: Filter }
  | { kind: "orderFirst"; direction: Direction };

/** What a field answers: a number, null for a reduction of no rows, or the distinct values. */
export type Answer = number | null | number[];

const COMPARE: Record<Comparison, (n: number, threshold: number) => boolean> = {
  gt: (n, threshold) => n > threshold,
  lt: (n, threshold) => n < threshold,
  eq: (n, threshold) => n === threshold,
  ne: (n, threshold) => n !== threshold,
};

function passes(row: Row, filter: Filter): boolean {
  return COMPARE[filter.comparison](row.n, filter.threshold);
}

function reduce(reduction: Reduction, rows: Row[]): Answer {
  const values = rows.map((row) => row.n);
  switch (reduction) {
    case "count":
      return values.length;
    case "sum":
      return values.reduce((total, value) => total + value, 0);
    case "avg":
      return values.length === 0 ? null : values.reduce((total, value) => total + value, 0) / values.length;
    case "min":
      return values.length === 0 ? null : Math.min(...values);
    case "max":
      return values.length === 0 ? null : Math.max(...values);
    case "distinct":
      return [...new Set(values)].sort((left, right) => left - right);
  }
}

/** The answer of one field over the rows of one group. */
export function answer(field: Field, group: Row[]): Answer {
  switch (field.kind) {
    case "reduce":
      return reduce(field.reduction, group);
    case "filterReduce":
      return reduce(field.reduction, group.filter((row) => passes(row, field.filter)));
    case "filterMapCount":
      return group.filter((row) => passes(row, field.filter)).length;
    case "orderFirst": {
      const order = group.map((row) => row.t);
      return field.direction === "asc" ? Math.min(...order) : Math.max(...order);
    }
  }
}

/** The rows of each group, by group key. */
export function groupRows(rows: Row[]): Map<string, Row[]> {
  const groups = new Map<string, Row[]>();
  for (const row of rows) {
    groups.set(row.bucket, [...(groups.get(row.bucket) ?? []), row]);
  }
  return groups;
}

/**
 * What `group(key, mapper)` answers by the interface-database contract,
 * computed in plain JavaScript: the mapper sees the stream of the rows in
 * one group, and every field of its result reads only the rows its own
 * `filter`, `orderBy` and `nth` leave. One field's stages never change a
 * sibling field or the group key. A group appears once, whatever its
 * fields filter out. A reduction of no rows answers 0 for `count` and
 * `sum`, null for `avg`, `min` and `max`, and an empty list for `distinct`.
 *
 * Answers the fields by group key, for the groups that have a row.
 */
export function reference(rows: Row[], fields: Field[]): Map<string, Answer[]> {
  return new Map([...groupRows(rows)].map(([bucket, group]) => [bucket, fields.map((field) => answer(field, group))]));
}

/** The `filter` a field reduces behind, if it has one. */
export function filterOf(field: Field): Filter | undefined {
  return field.kind === "filterReduce" || field.kind === "filterMapCount" ? field.filter : undefined;
}

/** Whether a field is a reduction with no `filter` before it. */
export function isUnfiltered(field: Field): boolean {
  return field.kind === "reduce";
}

/** Whether the filter of a field keeps no row of the group, while the group has rows. */
export function emptiesGroup(field: Field, group: Row[]): boolean {
  const filter = filterOf(field);
  return filter !== undefined && group.length > 0 && !group.some((row) => passes(row, filter));
}
