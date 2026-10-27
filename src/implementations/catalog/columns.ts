import type {
  ColumnDescription,
  DeclaredReading,
} from "@antelopejs/interface-database-catalog";
import { Binary, type Document } from "mongodb";

import {
  type DeclaredKind,
  type KeyFinding,
  READS,
} from "../database/external-descriptor";
import {
  UUID_SUBTYPE,
  type ValidatorProperty,
  type ValidatorSchema,
} from "../database/external-probe";
import { Described, type Source, SourceFor, ValueOf } from "./measures";

/** How many sampled values of one native type one field holds. */
interface FieldType {
  type: string;
  count: number;
}

/** What the sample says about one top-level field. */
interface FieldStats {
  name: string;
  position: number;
  types: FieldType[];
}

interface FieldGroup {
  _id: { field: string; type: string };
  count: number;
  position: number;
  example: Binary | Document | string | number | null;
}

interface CountRow {
  rows: number;
}

const ID_FIELD = "_id";
const DOMINANT_SHARE = 0.8;
const PERCENT = 100;
const NULL_TYPES = new Set(["null", "undefined"]);
const GENERATED_KEYS = new Set(["objectId", "uuid"]);
const READ_ONLY =
  "an embedded value comes back altered by a read, so it is never written";

/** The declaration each native type reads as, and the exact text a number may also be read as. */
const READINGS = new Map<string, DeclaredKind>([
  ["objectId", "string"],
  ["uuid", "string"],
  ["string", "string"],
  ["int", "number"],
  ["long", "number"],
  ["double", "number"],
  ["decimal", "number"],
  ["date", "date"],
  ["bool", "boolean"],
  ["object", "object"],
  ["array", "array"],
]);

const VALIDATOR_TYPES = new Map<string, string>([
  ["number", "double"],
  ["binData", "uuid"],
]);

const FIELD_TYPES: Document[] = [
  { $project: { pairs: { $objectToArray: "$$ROOT" } } },
  { $unwind: { path: "$pairs", includeArrayIndex: "position" } },
  {
    $group: {
      _id: { field: "$pairs.k", type: { $type: "$pairs.v" } },
      count: { $sum: 1 },
      position: { $min: "$position" },
      example: { $first: "$pairs.v" },
    },
  },
];

function NativeType(group: FieldGroup): string {
  const isUuid =
    group._id.type === "binData" &&
    group.example instanceof Binary &&
    group.example.sub_type === UUID_SUBTYPE;
  return isUuid ? "uuid" : group._id.type;
}

interface SampledStats {
  rows: number;
  fields: FieldStats[];
}

const FALLBACK_ROWS = 1000;

/**
 * The sampled native types of every top-level field, with the number of documents sampled. A
 * sample whose read runs out of time is read again from its first 1,000 documents.
 */
async function SampledFields(source: Source): Promise<SampledStats> {
  const smaller = {
    ...source,
    stages: [...source.stages, { $limit: FALLBACK_ROWS }],
  };
  return (
    (await FieldsOf(source)) ??
    (await FieldsOf(smaller)) ?? { rows: 0, fields: [] }
  );
}

async function FieldsOf(source: Source): Promise<SampledStats | undefined> {
  const counted = ValueOf(
    await Described<CountRow>(source, [{ $count: "rows" }]),
  );
  const measured = await Described<FieldGroup>(source, FIELD_TYPES);
  if (measured.status !== "ok") {
    return undefined;
  }
  const groups = measured.value;
  const fields = new Map<string, FieldStats>();
  for (const group of groups) {
    const stats = fields.get(group._id.field) ?? {
      name: group._id.field,
      position: group.position,
      types: [],
    };
    stats.position = Math.min(stats.position, group.position);
    stats.types.push({ type: NativeType(group), count: group.count });
    fields.set(group._id.field, stats);
  }
  return {
    rows: counted?.[0]?.rows ?? 0,
    fields: [...fields.values()].sort((left, right) =>
      left.name === ID_FIELD
        ? -1
        : right.name === ID_FIELD
          ? 1
          : left.position - right.position,
    ),
  };
}

async function ElementTypes(
  source: Source,
  field: string,
): Promise<FieldType[]> {
  const groups =
    ValueOf(
      await Described<{ _id: string; count: number }>(source, [
        { $project: { element: `$${field}` } },
        { $unwind: "$element" },
        { $group: { _id: { $type: "$element" }, count: { $sum: 1 } } },
      ]),
    ) ?? [];
  return groups.map((group) => ({ type: group._id, count: group.count }));
}

function Values(types: FieldType[]): FieldType[] {
  return types
    .filter((entry) => !NULL_TYPES.has(entry.type))
    .sort((left, right) => right.count - left.count);
}

function Total(types: FieldType[]): number {
  return types.reduce((sum, entry) => sum + entry.count, 0);
}

function Dominant(types: FieldType[]): string | undefined {
  const values = Values(types);
  const [first] = values;
  return first && first.count >= DOMINANT_SHARE * Total(values)
    ? first.type
    : undefined;
}

/**
 * The declaration that reads most of a field's values, when it reads 80% of them: int, long,
 * double and decimal all read as `number`, ObjectId, UUID and string as `string`.
 */
function DominantKind(types: FieldType[]): DeclaredKind | undefined {
  const values = Values(types);
  const counts = new Map<DeclaredKind, number>();
  for (const entry of values) {
    const kind = READINGS.get(entry.type);
    if (kind) {
      counts.set(kind, (counts.get(kind) ?? 0) + entry.count);
    }
  }
  const total = Total(values);
  return [...counts.entries()].find(
    ([, count]) => count >= DOMINANT_SHARE * total,
  )?.[0];
}

function Shares(types: FieldType[]): string {
  const values = Values(types);
  const total = Total(values);
  return values
    .map(
      (entry) =>
        `${entry.type} ${Math.round((entry.count / total) * PERCENT)}%`,
    )
    .join(" | ");
}

function Lossy(kind: DeclaredKind, types: FieldType[]): string | undefined {
  const values = Values(types);
  const unread = values.filter((entry) => !READS[kind].includes(entry.type));
  const notes = unread.map(
    (entry) =>
      `${entry.count} of ${Total(values)} values are ${entry.type} and read as null`,
  );
  if (
    kind === "number" &&
    values.some((entry) => entry.type === "long" || entry.type === "decimal")
  ) {
    notes.push(
      "long and decimal values read as doubles, exact only within 2^53 and 15 digits",
    );
  }
  return notes.length > 0 ? notes.join("; ") : undefined;
}

function Reading(
  declared: string,
  kind: DeclaredKind,
  types: FieldType[],
): DeclaredReading {
  const lossy = Lossy(kind, types);
  return lossy ? { declared, lossy } : { declared };
}

function ScalarAccepts(
  kind: DeclaredKind,
  types: FieldType[],
): DeclaredReading[] {
  const accepts = [Reading(kind, kind, types)];
  const hasExactText = Values(types).some(
    (entry) => entry.type === "long" || entry.type === "decimal",
  );
  if (kind === "number" && hasExactText) {
    accepts.push(Reading("string", "string", types));
  }
  return accepts;
}

/** An array whose elements share one scalar or document type reads as `T[]`; a mixed one, as nothing. */
function ArrayAccepts(
  elements: FieldType[],
  types: FieldType[],
): DeclaredReading[] {
  const values = Values(elements);
  const kind = values.length === 1 ? READINGS.get(values[0].type) : undefined;
  return kind && kind !== "array" ? [Reading(`${kind}[]`, "array", types)] : [];
}

function Unsupported(
  types: FieldType[],
  kind: DeclaredKind | undefined,
): string | undefined {
  const [first] = Values(types);
  if (!first) {
    return "the sample holds only null for it";
  }
  if (kind) {
    return undefined;
  }
  return READINGS.get(first.type) || Dominant(types) !== first.type
    ? `no native type covers 80% of the sampled values: ${Shares(types)}`
    : `values of type ${first.type} are not supported`;
}

interface DescribedField {
  stats: FieldStats;
  rows: number;
  property?: ValidatorProperty;
  required: boolean;
  elements: FieldType[];
}

function ValidatorTypes(property: ValidatorProperty | undefined): FieldType[] {
  const bsonType = property?.bsonType;
  const declared = bsonType === undefined ? [] : [bsonType].flat();
  return declared.map((type) => ({
    type: VALIDATOR_TYPES.get(type) ?? type,
    count: 1,
  }));
}

function Accepts(
  field: DescribedField,
  types: FieldType[],
  key: KeyFinding,
): DeclaredReading[] {
  const kind = DominantKind(types);
  if (field.stats.name === ID_FIELD) {
    return key.key ? [{ declared: "string" }] : [];
  }
  if (kind === "array") {
    return ArrayAccepts(field.elements, types);
  }
  return kind ? ScalarAccepts(kind, types) : [];
}

function Native(
  types: FieldType[],
  property: ValidatorProperty | undefined,
): string {
  const validatorType = property?.bsonType
    ? ` (validator: ${[property.bsonType].flat().join(" | ")})`
    : "";
  return `${Shares(types) || "null"}${validatorType}`;
}

/** What a column says beside its declarations: why it has none, why it is read-only, its enum. */
function Remarks(
  field: DescribedField,
  types: FieldType[],
  key: KeyFinding,
): Partial<ColumnDescription> {
  const kind = DominantKind(types);
  const isKeyless = field.stats.name === ID_FIELD && !key.key;
  const remarks: Partial<ColumnDescription> = {};
  const unsupported = isKeyless ? key.reason : Unsupported(types, kind);
  if (unsupported) {
    remarks.unsupported = unsupported;
  }
  if (kind === "object" || kind === "array") {
    remarks.readOnly = READ_ONLY;
  }
  if (field.property?.enum) {
    remarks.enumLabels = field.property.enum.map(String);
  }
  return remarks;
}

function Column(field: DescribedField, key: KeyFinding): ColumnDescription {
  const { stats, property } = field;
  const types = stats.types.length > 0 ? stats.types : ValidatorTypes(property);
  const accepts = Accepts(field, types, key);
  const { unsupported, ...remarks } = Remarks(field, types, key);
  const column: ColumnDescription = {
    name: stats.name,
    native: Native(types, property),
    nullable: Total(Values(stats.types)) < field.rows && !field.required,
    hasDefault: stats.name === ID_FIELD && GENERATED_KEYS.has(key.key ?? ""),
    generated: false,
    identity: null,
    accepts,
    comment: property?.description ?? null,
    ...remarks,
  };
  if (accepts.length === 0 && unsupported) {
    column.unsupported = unsupported;
  }
  return column;
}

function WithValidatorFields(
  fields: FieldStats[],
  validator: ValidatorSchema | undefined,
): FieldStats[] {
  const seen = new Set(fields.map((field) => field.name));
  const named = Object.keys(validator?.properties ?? {}).filter(
    (name) => !seen.has(name),
  );
  return [
    ...fields,
    ...named.map((name, index) => ({
      name,
      position: fields.length + index,
      types: [],
    })),
  ];
}

/**
 * The fields of a collection, one per top-level key the sample holds or the validator names, with
 * `_id` first: their native types and shares, the declarations that read them, and what is lost.
 */
export async function DescribeColumns(
  collection: string,
  validator: ValidatorSchema | undefined,
  key: KeyFinding,
): Promise<ColumnDescription[]> {
  const source = await SourceFor(collection);
  const sampled = await SampledFields(source);
  const columns: ColumnDescription[] = [];
  for (const stats of WithValidatorFields(sampled.fields, validator)) {
    const elements =
      DominantKind(stats.types) === "array"
        ? await ElementTypes(source, stats.name)
        : [];
    columns.push(
      Column(
        {
          stats,
          rows: sampled.rows,
          property: validator?.properties?.[stats.name],
          required: validator?.required?.includes(stats.name) ?? false,
          elements,
        },
        key,
      ),
    );
  }
  return columns;
}
