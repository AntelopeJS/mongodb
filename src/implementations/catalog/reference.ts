import type {
  Measure,
  ReferenceCheck,
  ReferenceMatch,
} from "@antelopejs/interface-database-catalog";
import type { Document } from "mongodb";

import type { KeyClass } from "../database/external-descriptor";
import { type BsonValue, KeyValues } from "../database/external-read";
import { type Located, Locate, SelectColumns } from "./locate";
import { Measured, SourceFor, ValueOf } from "./measures";
import { TextOf } from "./text";

/** The most distinct child values a check probes the target with. */
const CHECKED_VALUES = 1000;
const ID_FIELD = "_id";

interface ValueRow {
  _id: BsonValue;
  rows: number;
}

/** The values a child value may be stored as on the target side, the way a key is addressed. */
function Candidates(
  text: string,
  value: BsonValue,
  key: KeyClass | undefined,
): BsonValue[] {
  if (key) {
    return KeyValues(key, [text]);
  }
  return [
    value,
    text,
    ...KeyValues("objectId", [text]),
    ...KeyValues("uuid", [text]),
  ];
}

function TargetKey(to: Located, column: string): KeyClass | undefined {
  return column === ID_FIELD ? to.key : undefined;
}

async function FoundTexts(
  to: Located,
  column: string,
  candidates: BsonValue[],
): Promise<Measure<Set<string>>> {
  const source = await SourceFor(to.name);
  const measured = await Measured<{ _id: Document[keyof Document] }>(
    { ...source, stages: [] },
    [
      { $match: { [column]: { $in: candidates } } },
      { $group: { _id: `$${column}` } },
    ],
  );
  return measured.status === "ok"
    ? {
        status: "ok",
        value: new Set(measured.value.flatMap((row) => TextOf(row._id) ?? [])),
      }
    : measured;
}

/**
 * How many of a field's non-null values another field holds, for a reference neither database
 * declares: up to 1000 distinct child values from the sample, each converted the way the target
 * stores it, probed in one indexed `$in`.
 */
export async function CheckColumnReference(
  check: ReferenceCheck,
): Promise<Measure<ReferenceMatch>> {
  const from = await Locate(check.from.relation);
  SelectColumns(from, [check.from.column]);
  const to = await Locate(check.to.relation);
  SelectColumns(to, [check.to.column]);
  const source = await SourceFor(from.name);
  const field = check.from.column;
  const grouped = await Measured<ValueRow>(source, [
    { $match: { [field]: { $ne: null } } },
    { $group: { _id: `$${field}`, rows: { $sum: 1 } } },
    { $sort: { rows: -1 } },
    { $limit: CHECKED_VALUES },
  ]);
  const values = ValueOf(grouped);
  if (grouped.status !== "ok" || !values) {
    return grouped.status === "ok"
      ? { status: "error", code: "empty" }
      : grouped;
  }
  const key = TargetKey(to, check.to.column);
  const texts = values.map((row) => TextOf(row._id) ?? "");
  const found = await FoundTexts(
    to,
    check.to.column,
    values.flatMap((row, index) => Candidates(texts[index], row._id, key)),
  );
  const foundTexts = ValueOf(found);
  if (found.status !== "ok" || !foundTexts) {
    return found.status === "ok" ? { status: "error", code: "empty" } : found;
  }
  const checkedRows = values.reduce((total, row) => total + row.rows, 0);
  const foundRows = values
    .filter((_, index) => foundTexts.has(texts[index]))
    .reduce((total, row) => total + row.rows, 0);
  return {
    status: "ok",
    value: {
      containment: checkedRows > 0 ? foundRows / checkedRows : 0,
      checkedRows,
      sampled: source.sample !== null || values.length === CHECKED_VALUES,
      targetIsKey: check.to.column === ID_FIELD && to.key !== undefined,
    },
  };
}
