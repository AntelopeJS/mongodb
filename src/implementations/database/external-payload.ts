import {
  Decimal128,
  type Document,
  Double,
  Int32,
  Long,
  ObjectId,
  UUID,
} from "mongodb";

import type {
  DeclaredKind,
  ExternalCollection,
  FieldDescriptor,
  WriteType,
} from "./external-descriptor";
import { InvalidValues, type InvalidValue } from "./external-errors";
import {
  type BsonDocument,
  type BsonValue,
  ConstantKind,
  KeyText,
  KeyValues,
  ReadExpression,
} from "./external-read";

/** Which statement a payload is split for: the three treat keys and absences differently. */
export type WriteMode = "insert" | "update" | "replace";

/** A payload split for an external collection. */
export interface ExternalPayload {
  /** The key the payload names: written on insert, required of the document otherwise. */
  key?: BsonValue;
  /** Insert: each declared field's native value. Update and replace: its guarded expression. */
  fields: Document;
  /** Replace: the declared scalar fields the payload left out, which are removed. */
  removed: string[];
  /**
   * Update and replace: values that do not convert to their field's write type. Each is refused
   * unless a read of every targeted document answers it already, which the guard then keeps: a
   * written-back Long beyond 2^53 reads as a double no Long converts from.
   */
  unconverted: UnconvertedValue[];
}

/** A value an update carries that does not convert, with the test of whether a document differs. */
export interface UnconvertedValue {
  failure: InvalidValue;
  differs: Document;
}

const ID_FIELD = "_id";
const DROPPED_KEYS = new Set(["_instance"]);
const INT32_MAX = 2 ** 31 - 1;
const INT32_MIN = -(2 ** 31);
const DECIMAL_TEXT = /^-?\d+(\.\d+)?$/;
const GENERATED_KEYS = new Map<string, () => BsonValue>([
  ["objectId", () => new ObjectId()],
  ["uuid", () => new UUID()],
]);
const SCALAR_KINDS = new Set<DeclaredKind>([
  "string",
  "number",
  "boolean",
  "date",
]);

const BSON_TYPE_NAMES: Record<WriteType, string> = {
  objectId: "objectId",
  uuid: "binData",
  string: "string",
  int: "int",
  long: "long",
  double: "double",
  decimal: "decimal",
  date: "date",
  bool: "bool",
};

const WRITE_TYPE_NAMES = new Set<string>(Object.keys(BSON_TYPE_NAMES));

const EXPECTED: Record<WriteType, string> = {
  objectId: "expects an ObjectId: 24 hexadecimal characters",
  uuid: "expects a UUID",
  string: "expects a string",
  int: "expects an integer between -2^31 and 2^31-1",
  long: "expects an integer within the safe range, or its text",
  double: "expects a finite number",
  decimal: "expects a finite number, or its decimal text",
  date: "expects a date",
  bool: "expects a boolean",
};

function NumberOf(value: BsonValue): number | undefined {
  return ConstantKind(value) === "number" ? Number(value) : undefined;
}

function TextOf(value: BsonValue): string | undefined {
  return ConstantKind(value) === "string" ? KeyText(value) : undefined;
}

function DateOf(value: BsonValue): Date | undefined {
  const date =
    value instanceof Date ? value : new Date(TextOf(value) ?? Number.NaN);
  return Number.isNaN(date.getTime()) ? undefined : date;
}

function LongOf(value: BsonValue): Long | undefined {
  const number = NumberOf(value);
  if (number !== undefined) {
    return Number.isSafeInteger(number) ? Long.fromNumber(number) : undefined;
  }
  const text = TextOf(value);
  return text !== undefined && /^-?\d+$/.test(text)
    ? Long.fromString(text)
    : undefined;
}

function DecimalOf(value: BsonValue): Decimal128 | undefined {
  const number = NumberOf(value);
  const text =
    number !== undefined && Number.isFinite(number)
      ? String(number)
      : TextOf(value);
  return text !== undefined && DECIMAL_TEXT.test(text)
    ? Decimal128.fromString(text)
    : undefined;
}

const WRITERS: Record<WriteType, (value: BsonValue) => BsonValue | undefined> =
  {
    objectId: (value) => KeyValues("objectId", [value])[0],
    uuid: (value) => {
      const text = TextOf(value);
      return text === undefined ? undefined : KeyValues("uuid", [text])[0];
    },
    string: (value) => TextOf(value),
    int: (value) => {
      const number = NumberOf(value);
      return number !== undefined &&
        Number.isInteger(number) &&
        number >= INT32_MIN &&
        number <= INT32_MAX
        ? new Int32(number)
        : undefined;
    },
    long: LongOf,
    double: (value) => {
      const number = NumberOf(value);
      return number !== undefined && Number.isFinite(number)
        ? new Double(number)
        : undefined;
    },
    decimal: DecimalOf,
    date: DateOf,
    bool: (value) => (ConstantKind(value) === "boolean" ? value : undefined),
  };

const WIDENED = new Set<WriteType>(["int", "long"]);

/** The value stored as the write type, or a number the integer types cannot hold as a double. */
function Written(
  writeType: WriteType,
  value: BsonValue,
): BsonValue | undefined {
  const converted = WRITERS[writeType](value);
  return converted === undefined && WIDENED.has(writeType)
    ? WRITERS.double(value)
    : converted;
}

/** The value a read of the field would answer for the incoming value, for the write-back guard. */
function ReadForm(descriptor: FieldDescriptor, value: BsonValue): BsonValue {
  return descriptor.kind === "date" ? (DateOf(value) ?? value) : value;
}

interface Walk {
  collection: ExternalCollection;
  mode: WriteMode;
  failures: InvalidValue[];
  unconverted: UnconvertedValue[];
}

function Converted(
  walk: Walk,
  descriptor: FieldDescriptor,
  writeType: WriteType,
  value: BsonValue,
): BsonValue {
  if (value === null || value === undefined) {
    return null;
  }
  const converted = Written(writeType, value);
  if (converted === undefined) {
    walk.failures.push({
      field: descriptor.field,
      reason: EXPECTED[writeType],
    });
  }
  return converted ?? null;
}

/**
 * The value an update stores: converted to the stored value's own type where the field's sampled
 * types allow it, so a mixed field stays as each document had it, else to the field's write type.
 */
function StoredTypeExpression(
  walk: Walk,
  descriptor: FieldDescriptor,
  writeType: WriteType,
  value: BsonValue,
): BsonValue {
  if (value === null || value === undefined) {
    return null;
  }
  const converted = Written(writeType, value);
  if (converted === undefined) {
    walk.unconverted.push({
      failure: { field: descriptor.field, reason: EXPECTED[writeType] },
      differs: {
        $ne: [
          ReadExpression(descriptor),
          { $literal: ReadForm(descriptor, value) },
        ],
      },
    });
  }
  const fallback =
    converted === undefined ? `$${descriptor.field}` : { $literal: converted };
  const others = descriptor.nativeTypes.filter(
    (type): type is WriteType =>
      type !== writeType && WRITE_TYPE_NAMES.has(type),
  );
  return others.reduce<BsonValue>((otherwise, type) => {
    const converted = WRITERS[type](value);
    return converted === undefined
      ? otherwise
      : {
          $cond: [
            { $eq: [{ $type: `$${descriptor.field}` }, BSON_TYPE_NAMES[type]] },
            { $literal: converted },
            otherwise,
          ],
        };
  }, fallback);
}

/**
 * The write-back guard: the stored value stays when a read of it equals the incoming value, so an
 * unchanged field keeps its native type, precision and absence; otherwise the incoming value.
 */
function Guarded(
  walk: Walk,
  descriptor: FieldDescriptor,
  value: BsonValue,
): BsonValue {
  const writeType = descriptor.writeType;
  if (!writeType) {
    return `$${descriptor.field}`;
  }
  const unchanged = {
    $eq: [
      ReadExpression(descriptor),
      { $literal: ReadForm(descriptor, value) },
    ],
  };
  return {
    $cond: [
      IsAbsent(value) && writeType !== "uuid"
        ? { $or: [unchanged, UnreadBinary(descriptor)] }
        : unchanged,
      `$${descriptor.field}`,
      StoredTypeExpression(walk, descriptor, writeType, value),
    ],
  };
}

function IsAbsent(value: BsonValue): boolean {
  return value === null || value === undefined;
}

/**
 * Binary data the read answered as null: a null written back over it is the read's own answer,
 * not a change, so the stored bytes stay.
 */
function UnreadBinary(descriptor: FieldDescriptor): Document {
  return { $eq: [{ $type: `$${descriptor.field}` }, "binData"] };
}

function RefuseKey(walk: Walk, name: string) {
  walk.failures.push({
    field: name,
    reason: `is not a declared field: ${walk.collection.collection} is an external collection, and only its declared fields can be written`,
  });
}

function WriteField(
  walk: Walk,
  payload: ExternalPayload,
  name: string,
  value: BsonValue,
) {
  const descriptor = walk.collection.fields[name];
  if (!descriptor) {
    RefuseKey(walk, name);
    return;
  }
  if (walk.mode === "insert" && value === undefined) {
    return;
  }
  if (!SCALAR_KINDS.has(descriptor.kind) || !descriptor.writeType) {
    if (walk.mode === "insert") {
      walk.failures.push({
        field: name,
        reason: `is read-only: a ${descriptor.kind} value comes back altered by a read, so it is never written`,
      });
    }
    return;
  }
  payload.fields[name] =
    walk.mode === "insert"
      ? Converted(walk, descriptor, descriptor.writeType, value)
      : Guarded(walk, descriptor, value);
}

/**
 * Splits a payload for an external collection, per §5 of the design: declared scalar fields are
 * written, converted to their native write type or guarded; objects and arrays are read-only;
 * `_instance` is dropped; any other key is refused by name. Every failure is collected, and the
 * write is refused before anything is sent.
 */
export function SplitPayload(
  collection: ExternalCollection,
  mode: WriteMode,
  document: BsonDocument,
): ExternalPayload {
  const walk: Walk = { collection, mode, failures: [], unconverted: [] };
  const payload: ExternalPayload = {
    fields: {},
    removed: [],
    unconverted: walk.unconverted,
  };
  for (const [name, value] of Object.entries(document)) {
    if (name === ID_FIELD) {
      payload.key = value;
    } else if (!DROPPED_KEYS.has(name)) {
      WriteField(walk, payload, name, value);
    }
  }
  if (mode === "replace") {
    payload.removed = Object.values(collection.fields)
      .filter((descriptor) => SCALAR_KINDS.has(descriptor.kind))
      .map((descriptor) => descriptor.field)
      .filter((field) => !(field in document));
  }
  if (walk.failures.length > 0) {
    throw InvalidValues(collection.collection, walk.failures);
  }
  return payload;
}

/** A new document's `_id`: the one given, parsed as the key type class, or a generated one. */
export function NewKey(
  collection: ExternalCollection,
  given: BsonValue,
): BsonValue {
  const key = collection.key;
  if (given !== undefined && given !== null && key) {
    const [parsed] = KeyValues(key, [given]);
    if (parsed === undefined) {
      throw InvalidValues(collection.collection, [
        { field: ID_FIELD, reason: `does not parse as a key of type ${key}` },
      ]);
    }
    return parsed;
  }
  const generate = key ? GENERATED_KEYS.get(key) : undefined;
  if (!generate) {
    throw InvalidValues(collection.collection, [
      {
        field: ID_FIELD,
        reason: `is required: keys of type ${key} are not generated`,
      },
    ]);
  }
  return generate();
}
