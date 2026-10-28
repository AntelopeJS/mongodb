import type {
  Categories,
  ColumnDescription,
  ColumnProfile,
  ColumnTrait,
  LengthStats,
  Measure,
  MonthCount,
  ProfileRequest,
  RelationProfile,
  RowCount,
  ValueRange,
} from "@antelopejs/interface-database-catalog";
import type { Document } from "mongodb";

import type { DeclaredKind } from "../database/external-descriptor";
import { ReadExpression } from "../database/external-read";
import { type Located, Locate, SelectColumns } from "./locate";
import {
  FirstRow,
  Mapped,
  Measured,
  Sequentially,
  type Source,
  SourceFor,
  ValueOf,
} from "./measures";

/** A column with more distinct values than this has no categories. */
const CATEGORY_LIMIT = 25;
/** A value counted fewer times than this is a record, not a category, and is never named. */
const CATEGORY_MIN_ROWS = 5;
const ENUM_LIKE_MIN_ROWS = 100;
const MONTH_LIMIT = 36;
const LABEL_DISTINCT_SHARE = 0.9;
const LABEL_MAX_LENGTH = 80;
const TIME_AXIS_FILLED_SHARE = 0.9;
const TIME_AXIS_MIN_MONTHS = 3;
const MONTHS_PER_YEAR = 12;
const IDENTIFIER_TYPES = /^(objectId|uuid) /;

const PROFILED_KINDS = new Map<string, DeclaredKind>([
  ["string", "string"],
  ["number", "number"],
  ["boolean", "boolean"],
  ["date", "date"],
]);
const TRAITS: readonly ColumnTrait[] = [
  "enum-like",
  "identifier-like",
  "label-candidate",
  "time-axis",
  "measure",
];
const CATEGORY_KINDS = new Set<DeclaredKind>(["string", "number", "boolean"]);

/** A column on its way to a profile: its name, the kind it is proposed as, how a read sees it. */
interface ProfiledColumn {
  description: ColumnDescription;
  kind: DeclaredKind;
  value: Document;
  isIdentifier: boolean;
}

interface CountRow {
  n: number;
}

interface RangeRow {
  min: number | Date | null;
  max: number | Date | null;
}

interface LengthRow {
  min: number;
  avg: number;
  max: number;
}

interface GroupRow {
  _id: string | number | boolean;
  rows: number;
}

function Profiled(description: ColumnDescription): ProfiledColumn | undefined {
  const kind = PROFILED_KINDS.get(description.accepts[0]?.declared ?? "");
  if (!kind) {
    return undefined;
  }
  return {
    description,
    kind,
    value: {
      $project: {
        v: ReadExpression({ field: description.name, kind, nativeTypes: [] }),
      },
    },
    isIdentifier: IDENTIFIER_TYPES.test(description.native),
  };
}

function Count(measured: Measure<CountRow[]>): Measure<number> {
  return FirstRow(measured, (row) => row?.n ?? 0);
}

async function Nulls(
  source: Source,
  column: ProfiledColumn,
): Promise<Measure<number>> {
  return Count(
    await Measured<CountRow>(source, [
      { $match: { [column.description.name]: null } },
      { $count: "n" },
    ]),
  );
}

async function Distinct(
  source: Source,
  column: ProfiledColumn,
): Promise<Measure<number>> {
  return Count(
    await Measured<CountRow>(source, [
      column.value,
      { $match: { v: { $ne: null } } },
      { $group: { _id: "$v" } },
      { $count: "n" },
    ]),
  );
}

function RangeEnd(value: number | Date): number | string {
  return value instanceof Date ? value.toISOString() : value;
}

async function Range(
  source: Source,
  column: ProfiledColumn,
): Promise<Measure<ValueRange>> {
  const measured = await Measured<RangeRow>(source, [
    column.value,
    { $group: { _id: null, min: { $min: "$v" }, max: { $max: "$v" } } },
  ]);
  const row = ValueOf(measured)?.[0];
  if (measured.status !== "ok") {
    return measured;
  }
  return row && row.min !== null && row.max !== null
    ? {
        status: "ok",
        value: { min: RangeEnd(row.min), max: RangeEnd(row.max) },
      }
    : { status: "error", code: "empty" };
}

async function Length(
  source: Source,
  column: ProfiledColumn,
): Promise<Measure<LengthStats>> {
  const measured = await Measured<LengthRow>(source, [
    column.value,
    { $match: { v: { $type: "string" } } },
    { $project: { length: { $strLenCP: "$v" } } },
    {
      $group: {
        _id: null,
        min: { $min: "$length" },
        avg: { $avg: "$length" },
        max: { $max: "$length" },
      },
    },
  ]);
  const row = ValueOf(measured)?.[0];
  if (measured.status !== "ok") {
    return measured;
  }
  return row
    ? { status: "ok", value: { min: row.min, avg: row.avg, max: row.max } }
    : { status: "error", code: "empty" };
}

/** Values counted often enough to be categories; every rarer value only adds to `other`. */
function AsCategories(rows: GroupRow[]): Categories {
  return {
    values: rows
      .filter((row) => row.rows >= CATEGORY_MIN_ROWS)
      .slice(0, CATEGORY_LIMIT)
      .map((row) => ({ value: row._id, rows: row.rows })),
    other: rows
      .filter(
        (row, index) => row.rows < CATEGORY_MIN_ROWS || index >= CATEGORY_LIMIT,
      )
      .reduce((total, row) => total + row.rows, 0),
  };
}

async function CategoriesOf(
  source: Source,
  column: ProfiledColumn,
): Promise<Measure<Categories>> {
  return Mapped(
    await Measured<GroupRow>(source, [
      column.value,
      { $match: { v: { $ne: null } } },
      { $group: { _id: "$v", rows: { $sum: 1 } } },
      { $sort: { rows: -1, _id: 1 } },
      { $limit: CATEGORY_LIMIT + 1 },
    ]),
    AsCategories,
  );
}

async function Months(
  source: Source,
  column: ProfiledColumn,
): Promise<Measure<MonthCount[]>> {
  return Mapped(
    await Measured<{ _id: string; rows: number }>(source, [
      column.value,
      { $match: { v: { $type: "date" } } },
      {
        $group: {
          _id: {
            $dateToString: { date: "$v", format: "%Y-%m", timezone: "UTC" },
          },
          rows: { $sum: 1 },
        },
      },
      { $sort: { _id: -1 } },
      { $limit: MONTH_LIMIT },
    ]),
    (rows) => rows.map((row) => ({ month: row._id, rows: row.rows })).reverse(),
  );
}

function MonthsSpanned(range: ValueRange): number {
  const start = new Date(String(range.min));
  const end = new Date(String(range.max));
  return (
    (end.getUTCFullYear() - start.getUTCFullYear()) * MONTHS_PER_YEAR +
    end.getUTCMonth() -
    start.getUTCMonth() +
    1
  );
}

interface TraitFacts {
  column: ProfiledColumn;
  profile: ColumnProfile;
  rows: number | undefined;
}

function Filled(facts: TraitFacts): number {
  const nulls = ValueOf(facts.profile.nulls);
  return facts.rows === undefined || nulls === undefined
    ? 0
    : facts.rows - nulls;
}

const TRAIT_RULES: Record<ColumnTrait, (facts: TraitFacts) => boolean> = {
  "enum-like": (facts) =>
    facts.column.description.enumLabels !== undefined ||
    (facts.profile.categories !== undefined &&
      (ValueOf(facts.profile.distinct) ?? Infinity) <= CATEGORY_LIMIT &&
      (facts.rows ?? 0) >= ENUM_LIKE_MIN_ROWS),
  "identifier-like": (facts) =>
    (facts.rows ?? 0) > 0 &&
    ValueOf(facts.profile.nulls) === 0 &&
    ValueOf(facts.profile.distinct) === facts.rows,
  "label-candidate": (facts) => {
    const filled = Filled(facts);
    const length = ValueOf(facts.profile.length);
    return (
      facts.column.kind === "string" &&
      !facts.column.isIdentifier &&
      filled > 0 &&
      (ValueOf(facts.profile.distinct) ?? 0) >= LABEL_DISTINCT_SHARE * filled &&
      length !== undefined &&
      length.avg <= LABEL_MAX_LENGTH
    );
  },
  "time-axis": (facts) => {
    const range = ValueOf(facts.profile.range);
    return (
      facts.column.kind === "date" &&
      range !== undefined &&
      Filled(facts) >= TIME_AXIS_FILLED_SHARE * (facts.rows ?? Infinity) &&
      MonthsSpanned(range) >= TIME_AXIS_MIN_MONTHS
    );
  },
  measure: (facts) =>
    facts.column.kind === "number" && facts.column.description.name !== "_id",
};

function TraitsOf(facts: TraitFacts): ColumnTrait[] {
  return TRAITS.filter((trait) => TRAIT_RULES[trait](facts));
}

async function ProfileColumn(
  source: Source,
  column: ProfiledColumn,
  rows: number | undefined,
): Promise<ColumnProfile> {
  const profile: ColumnProfile = {
    name: column.description.name,
    nulls: await Nulls(source, column),
    distinct: await Distinct(source, column),
    traits: [],
  };
  if (column.kind === "number" || column.kind === "date") {
    profile.range = await Range(source, column);
  }
  if (column.kind === "string") {
    profile.length = await Length(source, column);
  }
  const isCategorical =
    (ValueOf(profile.distinct) ?? Infinity) <= CATEGORY_LIMIT;
  if (
    CATEGORY_KINDS.has(column.kind) &&
    !column.isIdentifier &&
    isCategorical
  ) {
    profile.categories = await CategoriesOf(source, column);
  }
  if (column.kind === "date") {
    profile.months = await Months(source, column);
  }
  profile.traits = TraitsOf({ column, profile, rows });
  return profile;
}

async function RowsOf(
  located: Located,
  source: Source,
): Promise<Measure<RowCount>> {
  if (source.sample) {
    return {
      status: "ok",
      value: { rows: source.estimatedRows ?? 0, exact: false },
    };
  }
  return Mapped(
    Count(await Measured<CountRow>(source, [{ $count: "n" }])),
    (rows) => ({
      rows,
      exact: located.kind !== "view",
    }),
  );
}

/**
 * Aggregates over one collection, each measure one aggregation bounded in time, over the whole
 * collection or its `$sample` above the threshold. A value is named only as a category.
 */
export async function ProfileRelation(
  request: ProfileRequest,
): Promise<RelationProfile> {
  const located = await Locate(request.relation);
  const descriptions = SelectColumns(located, request.columns);
  const source = await SourceFor(located.name);
  const rows = await RowsOf(located, source);
  const measuredRows = source.sample?.rows ?? ValueOf(rows)?.rows;
  const columns = descriptions
    .map(Profiled)
    .filter((column): column is ProfiledColumn => column !== undefined);
  return {
    relation: request.relation,
    rows,
    sample: source.sample,
    columns: await Sequentially(columns, (column) =>
      ProfileColumn(source, column, measuredRows),
    ),
    references: [],
    inbound: [],
  };
}
