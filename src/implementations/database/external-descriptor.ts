import type {
  FieldType,
  TableDefinition,
} from "@antelopejs/interface-database/schema";

import {
  type CollectionFacts,
  type IdEnd,
  type SampledType,
  UUID_SUBTYPE,
} from "./external-probe";

/** A declaration's kind, as `TableDefinition.fields` spells it. */
export type DeclaredKind =
  | "string"
  | "number"
  | "boolean"
  | "date"
  | "object"
  | "array"
  | "any";

/** The type class every `_id` of a keyed collection belongs to. */
export type KeyClass =
  | "objectId"
  | "string"
  | "number"
  | "uuid"
  | "object"
  | "date";

/** The BSON type a new value of a scalar field is written as. */
export type WriteType =
  | "objectId"
  | "uuid"
  | "string"
  | "int"
  | "long"
  | "double"
  | "decimal"
  | "date"
  | "bool";

export interface FieldDescriptor {
  field: string;
  kind: DeclaredKind;
  /** The kind of each element of an array declaration. */
  element?: DeclaredKind;
  /** The non-null native types the registration sample saw, most frequent first. */
  nativeTypes: string[];
  /** Present on a scalar declaration: what an insert writes. Objects and arrays are read-only. */
  writeType?: WriteType;
}

export type CollectionKind = "collection" | "view" | "timeseries";

/**
 * What a declared table of an external instance reads: the collection of the configured database
 * under the table's own name, verbatim. No `key` means the collection has no usable `_id` type
 * class, and its documents answer no `_id`.
 */
export interface ExternalCollection {
  collection: string;
  kind: CollectionKind;
  key?: KeyClass;
  fields: Record<string, FieldDescriptor>;
}

export interface DescribedCollection {
  collection: ExternalCollection;
  warnings: string[];
  notices: string[];
}

export interface DeclaredTable {
  schemaId: string;
  tableId: string;
  table: TableDefinition;
}

const ID_FIELD = "_id";
const ARRAY_SUFFIX = "[]";
const DOMINANT_SHARE = 0.8;
const PERCENT = 100;
const NULL_TYPES = new Set(["null", "undefined", "missing"]);
const UNADDRESSABLE_NAME = /^\$|\./;
const INTEGER_LIKE = /^(0|[1-9]\d*)$/;

const SCALAR_KINDS = new Map<string, DeclaredKind>([
  ["string", "string"],
  ["number", "number"],
  ["boolean", "boolean"],
  ["date", "date"],
  ["object", "object"],
  ["any", "any"],
]);

const NUMERIC_TYPES = ["int", "long", "double", "decimal"];

/** The native types each declared kind reads, as §2's matrix gives them. */
export const READS: Record<DeclaredKind, readonly string[]> = {
  string: ["string", "objectId", "uuid", "decimal", "long"],
  number: NUMERIC_TYPES,
  boolean: ["bool"],
  date: ["date"],
  object: ["object"],
  array: ["array"],
  any: [],
};

const WRITES: Partial<Record<DeclaredKind, readonly WriteType[]>> = {
  string: ["string", "objectId", "uuid", "decimal", "long"],
  number: ["int", "long", "double", "decimal"],
  boolean: ["bool"],
  date: ["date"],
};

const DEFAULT_WRITES: Partial<Record<DeclaredKind, WriteType>> = {
  string: "string",
  number: "double",
  boolean: "bool",
  date: "date",
};

const VALIDATOR_ALIASES = new Map<string, readonly string[]>([
  ["number", NUMERIC_TYPES],
  ["binData", ["uuid"]],
]);

const KEY_CLASSES = new Map<string, KeyClass>([
  ["objectId", "objectId"],
  ["string", "string"],
  ["int", "number"],
  ["long", "number"],
  ["double", "number"],
  ["decimal", "number"],
  ["uuid", "uuid"],
  ["object", "object"],
  ["date", "date"],
]);

const SUPPORTED_TOKENS = [...SCALAR_KINDS.keys()].join(", ");

function PathOf(declared: DeclaredTable, field?: string): string {
  const table = `${declared.schemaId}.${declared.tableId}`;
  return field === undefined ? table : `${table}.${field}`;
}

function ParseToken(token: string, path: string): DeclaredKind {
  const normalized = token.trim().toLowerCase();
  if (normalized.endsWith(ARRAY_SUFFIX)) {
    ParseToken(normalized.slice(0, -ARRAY_SUFFIX.length), path);
    return "array";
  }
  const kind = SCALAR_KINDS.get(normalized);
  if (!kind) {
    throw new Error(
      `Unsupported field type '${token}' for '${path}'. Supported: ${SUPPORTED_TOKENS}, optionally suffixed with [], a nested record, or an io-ts codec`,
    );
  }
  return kind;
}

function IsCodec(type: FieldType): boolean {
  return (
    type instanceof Object &&
    "decode" in type &&
    "encode" in type &&
    "is" in type
  );
}

/** The kind a field declaration reads as: a token, an array of one, a nested record, or a codec. */
function ElementKindOf(
  type: FieldType,
  path: string,
): DeclaredKind | undefined {
  if (Array.isArray(type)) {
    return type.length > 0 ? DeclaredKindOf(type[0], path) : undefined;
  }
  if (type instanceof Object) {
    return undefined;
  }
  const token = String(type).trim().toLowerCase();
  return token.endsWith(ARRAY_SUFFIX)
    ? ParseToken(token.slice(0, -ARRAY_SUFFIX.length), path)
    : undefined;
}

function DeclaredKindOf(type: FieldType, path: string): DeclaredKind {
  if (Array.isArray(type)) {
    return "array";
  }
  if (IsCodec(type)) {
    return "any";
  }
  if (type instanceof Object) {
    return "object";
  }
  return ParseToken(String(type), path);
}

function ValueTypes(sample: SampledType[], field: string): SampledType[] {
  return sample.filter(
    (entry) => entry.field === field && !NULL_TYPES.has(entry.type),
  );
}

function Total(types: SampledType[]): number {
  return types.reduce((sum, entry) => sum + entry.count, 0);
}

function Dominant(types: SampledType[]): string | undefined {
  const total = Total(types);
  return types.find((entry) => entry.count >= total * DOMINANT_SHARE)?.type;
}

function ValidatorTypes(facts: CollectionFacts, field: string): string[] {
  const property = facts.validator?.properties?.[field];
  const bsonType: string | string[] | undefined = property?.bsonType;
  if (bsonType === undefined) {
    return [];
  }
  const declared = Array.isArray(bsonType) ? bsonType : [bsonType];
  return declared.flatMap((type) => VALIDATOR_ALIASES.get(type) ?? [type]);
}

interface WriteChoice {
  writeType?: WriteType;
  warning?: string;
}

/** Numeric types from narrowest to broadest: each holds every value of those before it. */
const NUMBER_BREADTH: readonly WriteType[] = [
  "int",
  "long",
  "double",
  "decimal",
];

/**
 * The narrowest numeric type that holds every sampled numeric value, among the allowed ones: a
 * field of ints and a few doubles writes doubles, never ints a fraction would not fit.
 */
function CoveringNumber(
  allowed: readonly WriteType[],
  values: SampledType[],
): WriteType | undefined {
  const sampled = values
    .map((entry) => NUMBER_BREADTH.findIndex((type) => type === entry.type))
    .filter((index) => index >= 0);
  if (sampled.length === 0) {
    return undefined;
  }
  const widest = Math.max(...sampled);
  return NUMBER_BREADTH.slice(widest).find((type) => allowed.includes(type));
}

function ChooseWriteType(
  kind: DeclaredKind,
  validatorTypes: string[],
  values: SampledType[],
): WriteChoice {
  const writable = WRITES[kind];
  if (!writable) {
    return {};
  }
  const fromValidator = validatorTypes.filter((type): type is WriteType =>
    writable.some((candidate) => candidate === type),
  );
  const allowed = fromValidator.length > 0 ? fromValidator : writable;
  if (kind === "number") {
    const covering = CoveringNumber(allowed, values);
    if (covering) {
      return { writeType: covering };
    }
  }
  const dominant = allowed.find((type) => type === Dominant(values));
  if (dominant) {
    return { writeType: dominant };
  }
  if (fromValidator.length > 0) {
    return {
      writeType: fromValidator.includes("double") ? "double" : fromValidator[0],
    };
  }
  const fallback = DEFAULT_WRITES[kind];
  return values.length > 1
    ? {
        writeType: fallback,
        warning: `no native type covers 80% of the sampled values, so new values are written as ${fallback}`,
      }
    : { writeType: fallback };
}

function LossyWarning(
  kind: DeclaredKind,
  values: SampledType[],
): string | undefined {
  const reads = READS[kind];
  if (reads.length === 0) {
    return undefined;
  }
  const unread = values.filter((entry) => !reads.includes(entry.type));
  const count = Total(unread);
  if (count === 0) {
    return undefined;
  }
  const total = Total(values);
  const share = ((count / total) * PERCENT).toFixed(1);
  const types = unread.map((entry) => entry.type).join(", ");
  return `declared ${kind}, but ${count} of ${total} sampled values (${share}%) are ${types}, which read as null`;
}

function DescribeField(
  declared: DeclaredTable,
  facts: CollectionFacts,
  field: string,
  warnings: string[],
): FieldDescriptor {
  const path = PathOf(declared, field);
  if (UNADDRESSABLE_NAME.test(field)) {
    throw new Error(
      `Field '${path}' cannot be declared: MongoDB reads a name with a dot or a leading $ as a path or an operator`,
    );
  }
  const declaredType = declared.table.fields[field];
  const kind = DeclaredKindOf(declaredType, path);
  const values = ValueTypes(facts.sample, field);
  const lossy = LossyWarning(kind, values);
  if (lossy) {
    warnings.push(`Field '${path}' is ${lossy}`);
  }
  const choice = ChooseWriteType(kind, ValidatorTypes(facts, field), values);
  if (choice.warning) {
    warnings.push(`Field '${path}': ${choice.warning}`);
  }
  const nativeTypes = [...values]
    .sort((left, right) => right.count - left.count)
    .map((entry) => entry.type);
  return {
    field,
    kind,
    element: ElementKindOf(declaredType, path),
    nativeTypes,
    writeType: choice.writeType,
  };
}

function EndClass(end: IdEnd): KeyClass | undefined {
  if (end.type === "binData") {
    return end.subtype === UUID_SUBTYPE ? "uuid" : undefined;
  }
  return KEY_CLASSES.get(end.type);
}

function HasIntegerLikeKeys(end: IdEnd): boolean {
  return (end.keys ?? []).some((key) => INTEGER_LIKE.test(key));
}

/** What deciding a collection's key needs of the probe. */
export type KeyFacts = Pick<CollectionFacts, "kind" | "ends">;

export interface KeyFinding {
  key?: KeyClass;
  reason?: string;
}

/** The key type class of a collection from its `_id` index ends, or why it has none. */
export function KeyOf(facts: KeyFacts): KeyFinding {
  if (facts.kind !== "collection") {
    return { reason: `it is a ${facts.kind}, which has no _id index` };
  }
  if (!facts.ends) {
    return { key: "objectId" };
  }
  const [first, last] = facts.ends;
  const firstClass = EndClass(first);
  const lastClass = EndClass(last);
  if (!firstClass || firstClass !== lastClass) {
    return {
      reason: `its _id values range from ${first.type} to ${last.type}, more than one type class`,
    };
  }
  if (firstClass === "object" && [first, last].some(HasIntegerLikeKeys)) {
    return {
      reason:
        "its object _id values have integer-like subfield names, which JavaScript reorders",
    };
  }
  return { key: firstClass };
}

function DeclaresId(table: TableDefinition): boolean {
  return ID_FIELD in (table.fields ?? {});
}

/**
 * Builds a declared table's descriptor from what the probe read, with nothing created: the key
 * type class from the `_id` index ends, and per declared field its kind and its write type. A
 * declaration the collection cannot honour throws, naming it; anything that stays correct but
 * reads lossily, or may be slow, is a warning.
 */
export function DescribeCollection(
  declared: DeclaredTable,
  facts: CollectionFacts,
): DescribedCollection {
  const warnings: string[] = [];
  const notices: string[] = [];
  const keyFinding = KeyOf(facts);
  if (!keyFinding.key) {
    if (DeclaresId(declared.table)) {
      throw new Error(
        `Field '${PathOf(declared, ID_FIELD)}' is declared, but collection ${facts.name} has no usable _id: ${keyFinding.reason}; remove it`,
      );
    }
    notices.push(
      `Collection ${facts.name} answers no _id for '${PathOf(declared)}', since ${keyFinding.reason}: it can be queried, but not read by key`,
    );
  }
  const fields: Record<string, FieldDescriptor> = {};
  for (const field of Object.keys(declared.table.fields ?? {})) {
    if (field !== ID_FIELD) {
      fields[field] = DescribeField(declared, facts, field, warnings);
    }
  }
  warnings.push(...UnservedIndexWarnings(declared, facts));
  return {
    collection: {
      collection: facts.name,
      kind: facts.kind,
      key: keyFinding.key,
      fields,
    },
    warnings,
    notices,
  };
}

function Leads(served: string[], wanted: string[]): boolean {
  return wanted.every((field, position) => served[position] === field);
}

function UnservedIndexWarnings(
  declared: DeclaredTable,
  facts: CollectionFacts,
): string[] {
  return Object.entries(declared.table.indexes ?? {})
    .map(([indexId, index]) => ({ indexId, wanted: index.fields ?? [indexId] }))
    .filter(
      ({ wanted }) => !facts.indexes.some((served) => Leads(served, wanted)),
    )
    .map(
      ({ indexId, wanted }) =>
        `Index '${indexId}' on '${PathOf(declared)}' is declared, but no index of collection ${facts.name} leads with (${wanted.join(", ")}): queries by it stay correct, and may be slow`,
    );
}
