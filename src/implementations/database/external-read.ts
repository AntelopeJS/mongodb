import {
  Binary,
  BSON,
  Decimal128,
  type Document,
  Double,
  Int32,
  Long,
  ObjectId,
  UUID,
} from "mongodb";

import {
  type DeclaredKind,
  type ExternalCollection,
  type FieldDescriptor,
  type KeyClass,
  READS,
} from "./external-descriptor";

/** A document of BSON values, as a compiled query expression or a document the driver answered. */
export interface BsonDocument {
  [field: string]: BsonValue;
}

/** A value a compiled query expression, or a document the driver answered, can hold. */
export type BsonValue =
  | BsonDocument
  | BsonValue[]
  | Binary
  | Double
  | Int32
  | ObjectId
  | Long
  | Decimal128
  | Date
  | string
  | number
  | boolean
  | null
  | undefined;

type KeyValue = ObjectId | UUID | Long | Date | Document | string | number;

const ID_FIELD = "_id";
const OBJECT_ID_TEXT = /^[0-9a-f]{24}$/i;
const UUID_TEXT =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const NUMBER_TEXT = /^-?(0|[1-9]\d*)(\.\d+)?$/;
const INTEGER_TEXT = /^-?(0|[1-9]\d*)$/;
const ROOT_FIELD = /^\$\$ROOT\.([^.$][^.]*)$/;
const UUID_SUBTYPE = 4;
const ELEMENT = "element";
const CANONICAL = { relaxed: false };
const STRING_NATIVE = new Set(["string", "objectId", "uuid"]);

function ParseNumberKey(text: string): KeyValue | undefined {
  if (!NUMBER_TEXT.test(text)) {
    return undefined;
  }
  const number = Number(text);
  const isExactInteger =
    INTEGER_TEXT.test(text) && !Number.isSafeInteger(number);
  return isExactInteger ? Long.fromString(text) : number;
}

function ParseDateKey(text: string): KeyValue | undefined {
  const date = new Date(text);
  return !Number.isNaN(date.getTime()) && date.toISOString() === text
    ? date
    : undefined;
}

/** Whether a parsed key holds a `$`-prefixed field at any depth, which MongoDB reads as an operator. */
function HoldsOperator(value: BsonValue): boolean {
  return (
    IsPlainDocument(value) &&
    Object.entries(value).some(
      ([field, inner]) => field.startsWith("$") || HoldsOperator(inner),
    )
  );
}

function ParseObjectKey(text: string): KeyValue | undefined {
  try {
    const parsed: unknown = BSON.EJSON.parse(text, CANONICAL);
    return IsPlainDocument(parsed) && !HoldsOperator(parsed)
      ? parsed
      : undefined;
  } catch {
    return undefined;
  }
}

const KEY_PARSERS: Record<KeyClass, (text: string) => KeyValue | undefined> = {
  objectId: (text) =>
    OBJECT_ID_TEXT.test(text) ? new ObjectId(text) : undefined,
  string: (text) => text,
  number: ParseNumberKey,
  uuid: (text) => (UUID_TEXT.test(text) ? new UUID(text) : undefined),
  object: ParseObjectKey,
  date: ParseDateKey,
};

export function KeyText(value: BsonValue): string | undefined {
  if (value instanceof Object || value === null || value === undefined) {
    return undefined;
  }
  return String(value);
}

/**
 * The native values a rendered key addresses: at most one, of the collection's key type class.
 * Text that does not parse as that class addresses nothing, rather than raising.
 */
export function KeyValues(key: KeyClass, keys: BsonValue[]): KeyValue[] {
  return keys
    .map((value) => KeyText(value))
    .filter((text): text is string => text !== undefined)
    .map((text) => KEY_PARSERS[key](text))
    .filter((value): value is KeyValue => value !== undefined);
}

/** A `$match` on `_id` for constant rendered keys. */
export function KeyMatch(key: KeyClass, keys: BsonValue[]): Document {
  const values = KeyValues(key, keys);
  return values.length === 1
    ? { _id: { $eq: values[0] } }
    : { _id: { $in: values } };
}

const KEY_CONVERSIONS: Partial<Record<KeyClass, string>> = {
  objectId: "objectId",
  string: "string",
  number: "decimal",
  date: "date",
};

/** An expression converting a runtime value, rendered or native, to the key type class. */
export function KeyExpression(key: KeyClass, value: BsonValue): BsonValue {
  const to = KEY_CONVERSIONS[key];
  return to
    ? { $convert: { input: value, to, onError: null, onNull: null } }
    : value;
}

function Typed(path: string, type: string): Document {
  return { $cond: [{ $eq: [{ $type: path }, type] }, path, null] };
}

const READ_EXPRESSIONS: Record<DeclaredKind, (path: string) => BsonValue> = {
  string: (path) => ({
    $cond: [
      { $in: [{ $type: path }, ["string", "binData"]] },
      path,
      {
        $cond: [
          { $in: [{ $type: path }, ["objectId", "decimal", "long"]] },
          { $toString: path },
          null,
        ],
      },
    ],
  }),
  number: (path) => ({
    $cond: [{ $isNumber: path }, { $toDouble: path }, null],
  }),
  boolean: (path) => Typed(path, "bool"),
  date: (path) => Typed(path, "date"),
  object: (path) => Typed(path, "object"),
  array: (path) => Typed(path, "array"),
  any: (path) => path,
};

/** The expression a declared field reads as, from the stored value at `$<field>`. */
export function ReadExpression(descriptor: FieldDescriptor): BsonValue {
  const path = `$${descriptor.field}`;
  if (descriptor.kind !== "array" || !descriptor.element) {
    return READ_EXPRESSIONS[descriptor.kind](path);
  }
  return {
    $cond: [
      { $isArray: path },
      {
        $map: {
          input: path,
          as: ELEMENT,
          in: READ_EXPRESSIONS[descriptor.element](`$$${ELEMENT}`),
        },
      },
      null,
    ],
  };
}

const KEY_READS: Record<KeyClass, BsonValue> = {
  objectId: { $toString: "$_id" },
  string: "$_id",
  number: { $toString: "$_id" },
  uuid: "$_id",
  object: "$_id",
  date: { $toString: "$_id" },
};

/**
 * The stages that turn a client document into the row a declaration reads: `_id` rendered from
 * the key type class, each declared field converted to its declared kind or null, and every
 * undeclared field dropped.
 */
export function NormalizingStages(collection: ExternalCollection): Document[] {
  const fields = Object.values(collection.fields);
  if (fields.length === 0 && !collection.key) {
    return [{ $replaceWith: { $literal: {} } }];
  }
  const projection: Document = {
    _id: collection.key ? KEY_READS[collection.key] : 0,
  };
  for (const descriptor of fields) {
    projection[descriptor.field] = ReadExpression(descriptor);
  }
  return [{ $project: projection }];
}

function Constant(value: BsonValue): BsonValue {
  return value instanceof Object && "$literal" in value
    ? value.$literal
    : value;
}

const CONSTANT_KINDS = new Map<unknown, DeclaredKind>([
  [String, "string"],
  [Number, "number"],
  [Boolean, "boolean"],
  [Date, "date"],
]);

export function ConstantKind(value: BsonValue): DeclaredKind | undefined {
  return value === null || value === undefined
    ? undefined
    : CONSTANT_KINDS.get(value.constructor);
}

function FieldOf(value: BsonValue): string | undefined {
  const text = ConstantKind(value) === "string" ? KeyText(value) : undefined;
  return text === undefined ? undefined : ROOT_FIELD.exec(text)?.[1];
}

function IsCompiledExpression(value: BsonValue): boolean {
  const text = ConstantKind(value) === "string" ? KeyText(value) : undefined;
  if (text !== undefined) {
    return text.startsWith("$");
  }
  return (
    IsPlainDocument(value) &&
    Object.keys(value).some((key) => key.startsWith("$") && key !== "$literal")
  );
}

function FieldCandidates(
  descriptor: FieldDescriptor,
  constant: BsonValue,
): BsonValue[] | undefined {
  const kind = ConstantKind(constant);
  if (kind !== descriptor.kind) {
    return undefined;
  }
  if (kind === "string") {
    const readsUnparsed = descriptor.nativeTypes.every((type) =>
      STRING_NATIVE.has(type),
    );
    return readsUnparsed
      ? [
          constant,
          ...KeyValues("objectId", [constant]),
          ...KeyValues("uuid", [constant]),
        ]
      : undefined;
  }
  if (
    descriptor.nativeTypes.includes("array") ||
    (kind === "number" && descriptor.nativeTypes.includes("long"))
  ) {
    return undefined;
  }
  if (kind !== "number") {
    return [constant];
  }
  return Number.isFinite(constant)
    ? [constant, ...DecimalCandidates(descriptor, Number(constant))]
    : undefined;
}

/**
 * The Decimal128 a number equals when the field holds decimals: MongoDB compares a double with a
 * decimal by their exact values, so `11.1` alone misses a stored `11.10`.
 */
function DecimalCandidates(
  descriptor: FieldDescriptor,
  constant: number,
): BsonValue[] {
  return descriptor.nativeTypes.includes("decimal")
    ? [Decimal128.fromString(String(constant))]
    : [];
}

function Candidates(
  collection: ExternalCollection,
  field: string,
  constant: BsonValue,
): BsonValue[] | undefined {
  if (field === ID_FIELD) {
    return collection.key && ConstantKind(constant) === "string"
      ? KeyValues(collection.key, [constant])
      : undefined;
  }
  const descriptor = collection.fields[field];
  return descriptor ? FieldCandidates(descriptor, constant) : undefined;
}

const COMPARISONS = new Map<string, (candidates: BsonValue[]) => Document>([
  [
    "$eq",
    (candidates) =>
      candidates.length === 1 ? { $eq: candidates[0] } : { $in: candidates },
  ],
  ["$ne", (candidates) => ({ $nin: candidates })],
]);

function NativeComparison(
  collection: ExternalCollection,
  expression: BsonDocument,
): Document | undefined {
  const entries = Object.entries(expression);
  const [first] = entries;
  const compare = first && COMPARISONS.get(first[0]);
  const operands = first?.[1];
  if (
    !compare ||
    entries.length > 1 ||
    !Array.isArray(operands) ||
    operands.length !== 2
  ) {
    return undefined;
  }
  const [left, right] = operands;
  const field = FieldOf(left) ?? FieldOf(right);
  const constant = FieldOf(left) ? right : left;
  if (!field || IsCompiledExpression(constant)) {
    return undefined;
  }
  const candidates = Candidates(collection, field, Constant(constant));
  return candidates ? { [field]: compare(candidates) } : undefined;
}

/**
 * The native `$match` a compiled filter amounts to, when it is `eq` or `ne` between a declared
 * field or `_id` and a constant, or an `and` of those: the constant converted the way the field
 * reads it, so the index serves the comparison. Anything else answers undefined, and is compared
 * on the normalized row.
 */
export function NativeFilter(
  collection: ExternalCollection,
  expression: BsonValue,
): Document | undefined {
  if (!IsPlainDocument(expression)) {
    return undefined;
  }
  const parts = expression.$and;
  if (!Array.isArray(parts)) {
    return NativeComparison(collection, expression);
  }
  const native = parts.map((part) => NativeFilter(collection, part));
  return native.every((part) => part !== undefined)
    ? { $and: native }
    : undefined;
}

const UNCHANGED_READS: Partial<
  Record<DeclaredKind, readonly (readonly string[])[]>
> = {
  string: [["string"], ["objectId"]],
  number: [READS.number],
  boolean: [READS.boolean],
  date: [READS.date],
};

function SortsAsRead(collection: ExternalCollection, field: string): boolean {
  if (field === ID_FIELD) {
    return collection.key !== undefined;
  }
  const descriptor = collection.fields[field];
  const unchanged = descriptor ? (UNCHANGED_READS[descriptor.kind] ?? []) : [];
  return unchanged.some((types) =>
    descriptor.nativeTypes.every((type) => types.includes(type)),
  );
}

/**
 * Whether sorting on these fields natively orders rows as their normalized values would: each is
 * `_id`, or a field whose sampled values all read unchanged, or are ObjectIds read as their hex.
 */
export function SortsNatively(
  collection: ExternalCollection,
  fields: string[],
): boolean {
  return fields.every((field) => SortsAsRead(collection, field));
}

const COMPARED_READS: Partial<Record<DeclaredKind, readonly string[]>> = {
  string: ["string"],
  number: READS.number,
  boolean: READS.boolean,
  date: READS.date,
};

/**
 * Whether a range on this field compares the stored values as their reads would: an `_id` of
 * string keys, or a field whose sampled values are all of the declared kind's own types. An
 * ObjectId sorts as its hex does, but never falls between two strings.
 */
export function ComparesNatively(
  collection: ExternalCollection,
  field: string,
): boolean {
  if (field === ID_FIELD) {
    return collection.key === "string";
  }
  const descriptor = collection.fields[field];
  const compared = descriptor ? (COMPARED_READS[descriptor.kind] ?? []) : [];
  return (
    descriptor !== undefined &&
    descriptor.nativeTypes.every((type) => compared.includes(type))
  );
}

function IsPlainDocument<Value>(value: Value): value is Value & BsonDocument {
  return (
    value instanceof Object && Object.getPrototypeOf(value) === Object.prototype
  );
}

function RenderBinary(value: Binary): string | null {
  return value.sub_type === UUID_SUBTYPE ? value.toUUID().toHexString() : null;
}

/**
 * Renders what the pipeline could not: UUIDs as canonical text, other binary data as null, and an
 * object `_id` as canonical Extended JSON.
 */
export function RenderOutput(value: BsonValue): BsonValue {
  if (value instanceof Binary) {
    return RenderBinary(value);
  }
  if (Array.isArray(value)) {
    return value.map(RenderOutput);
  }
  if (!IsPlainDocument(value)) {
    return value;
  }
  const rendered: BsonDocument = {};
  for (const [key, entry] of Object.entries(value)) {
    rendered[key] =
      key === ID_FIELD && IsPlainDocument(entry)
        ? BSON.EJSON.stringify(entry, CANONICAL)
        : RenderOutput(entry);
  }
  return rendered;
}
