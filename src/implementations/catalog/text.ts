import { Binary, BSON, type Document } from "mongodb";

const UUID_SUBTYPE = 4;
const RELAXED = { relaxed: true };

/** A stored value as text: ids by their rendering, numbers exactly, documents as Extended JSON. */
export function TextOf(value: Document[keyof Document]): string | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }
  if (value instanceof Binary) {
    return value.sub_type === UUID_SUBTYPE
      ? value.toUUID().toHexString()
      : undefined;
  }
  if (value instanceof BSON.ObjectId) {
    return value.toHexString();
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (value instanceof BSON.Long || value instanceof BSON.Decimal128) {
    return value.toString();
  }
  return value instanceof Object
    ? BSON.EJSON.stringify(value, RELAXED)
    : String(value);
}
