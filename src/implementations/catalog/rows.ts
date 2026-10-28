import {
  type RowSample,
  SAMPLE_ROWS_LIMIT,
  SAMPLE_TEXT_LENGTH,
  type SampledValue,
  type SampleRequest,
} from "@antelopejs/interface-database-catalog";
import { Binary, type Document } from "mongodb";

import { Locate, SelectColumns } from "./locate";
import { Measured, SourceFor } from "./measures";
import { TextOf } from "./text";

const UUID_SUBTYPE = 4;

function CheckedLimit(limit: number): number {
  if (!Number.isInteger(limit) || limit < 1 || limit > SAMPLE_ROWS_LIMIT) {
    throw new Error(
      `A sample answers from 1 to ${SAMPLE_ROWS_LIMIT} rows, not ${limit}`,
    );
  }
  return limit;
}

function Sampled(value: Document[keyof Document]): SampledValue {
  if (value instanceof Binary && value.sub_type !== UUID_SUBTYPE) {
    return { bytes: value.length() };
  }
  const text = TextOf(value);
  if (text === undefined) {
    return null;
  }
  return {
    text: text.slice(0, SAMPLE_TEXT_LENGTH),
    truncated: text.length > SAMPLE_TEXT_LENGTH,
  };
}

/**
 * The first documents of a collection, or of its sample above the threshold, as text cut at
 * {@link SAMPLE_TEXT_LENGTH}: a binary value by its length, an embedded one as JSON text.
 */
export async function SampleRelationRows(
  request: SampleRequest,
): Promise<RowSample> {
  const limit = CheckedLimit(request.limit);
  const located = await Locate(request.relation);
  const columns = SelectColumns(located, request.columns).map(
    (column) => column.name,
  );
  const source = await SourceFor(located.name);
  const measured = await Measured(source, [{ $limit: limit }]);
  if (measured.status !== "ok") {
    throw new Error(
      `The sample of '${request.relation}' did not finish: ${measured.status === "timeout" ? "timeout" : measured.code}`,
    );
  }
  return {
    relation: request.relation,
    columns,
    rows: measured.value.map((document) =>
      columns.map((column) => Sampled(document[column])),
    ),
    sample: source.sample && { ...source.sample, rows: measured.value.length },
  };
}
