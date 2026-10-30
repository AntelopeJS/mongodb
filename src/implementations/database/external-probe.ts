import {
  Binary,
  type Collection,
  type CollectionInfo,
  type Db,
  type Document,
  MongoServerError,
} from "mongodb";

import { GetOptions } from "../../config";
import type { CollectionKind } from "./external-descriptor";
import { isDocument, isString } from "./utils";

export const UUID_SUBTYPE = 4;

/** One property of a `$jsonSchema` validator, as far as registration and the catalog read it. */
export interface ValidatorProperty {
  bsonType?: string | string[];
  enum?: unknown[];
  description?: string;
}

export interface ValidatorSchema {
  description?: string;
  required?: string[];
  properties?: Record<string, ValidatorProperty>;
}

/** One end of the `_id` index: the `$type` of the value, and what `$type` cannot tell apart. */
export interface IdEnd {
  type: string;
  subtype?: number;
  keys?: string[];
}

/** How many sampled documents hold a top-level field with a value of one native type. */
export interface SampledType {
  field: string;
  type: string;
  count: number;
}

export interface CollectionFacts {
  name: string;
  kind: CollectionKind;
  validator?: ValidatorSchema;
  /** The key patterns of the collection's indexes, as field lists. */
  indexes: string[][];
  /** The `_id` index's first and last entries; absent for an empty collection or no index. */
  ends?: [IdEnd, IdEnd];
  sample: SampledType[];
}

type IdValue = Binary | Document | string | number | Date;

interface EndDocument {
  type: string;
  value: IdValue;
}

interface SampleGroup {
  _id: { field: string; type: string };
  count: number;
  example: IdValue;
}

const SAMPLE_SIZE = 1000;
const UNAUTHORIZED_CODE = 13;
const ASCENDING = 1;
const DESCENDING = -1;
const ID_INDEX = "_id_";

const SAMPLE_TYPES: Document[] = [
  { $project: { pairs: { $objectToArray: "$$ROOT" } } },
  { $unwind: "$pairs" },
  {
    $group: {
      _id: { field: "$pairs.k", type: { $type: "$pairs.v" } },
      count: { $sum: 1 },
      example: { $first: "$pairs.v" },
    },
  },
];

/** The server-side time limit of a read, from `statementTimeoutMs`. */
export function TimeLimit(): Document {
  const maxTimeMS = GetOptions().statementTimeoutMs;
  return maxTimeMS === undefined ? {} : { maxTimeMS };
}

function MissingCollection(db: Db, path: string, name: string): Error {
  return new Error(
    `Table '${path}' reads collection ${name}, which database ${db.databaseName} does not have`,
  );
}

function UnreadableCollection(db: Db, path: string, name: string): Error {
  return new Error(
    `Table '${path}' reads collection ${name}, which the connected account cannot read in database ${db.databaseName}`,
  );
}

function SubtypeOf(value: IdValue): number | undefined {
  return value instanceof Binary ? value.sub_type : undefined;
}

function KeysOf(end: EndDocument): string[] | undefined {
  return end.type === "object" && end.value instanceof Object
    ? Object.keys(end.value)
    : undefined;
}

async function ReadEnd(
  collection: Collection,
  direction: number,
): Promise<IdEnd | undefined> {
  const [end] = await collection
    .aggregate<EndDocument>(
      [
        { $sort: { _id: direction } },
        { $limit: 1 },
        { $project: { _id: 0, type: { $type: "$_id" }, value: "$_id" } },
      ],
      { hint: ID_INDEX, ...TimeLimit() },
    )
    .toArray();
  if (!end) {
    return undefined;
  }
  return { type: end.type, subtype: SubtypeOf(end.value), keys: KeysOf(end) };
}

export async function ReadEnds(
  collection: Collection,
): Promise<[IdEnd, IdEnd] | undefined> {
  const first = await ReadEnd(collection, ASCENDING);
  const last = await ReadEnd(collection, DESCENDING);
  return first && last ? [first, last] : undefined;
}

function NativeType(group: SampleGroup): string {
  const isUuid =
    group._id.type === "binData" && SubtypeOf(group.example) === UUID_SUBTYPE;
  return isUuid ? "uuid" : group._id.type;
}

async function ReadSample(collection: Collection): Promise<SampledType[]> {
  const groups = await collection
    .aggregate<SampleGroup>(
      [{ $sample: { size: SAMPLE_SIZE } }, ...SAMPLE_TYPES],
      { allowDiskUse: false, ...TimeLimit() },
    )
    .toArray();
  return groups.map((group) => ({
    field: group._id.field,
    type: NativeType(group),
    count: group.count,
  }));
}

async function ReadIndexes(collection: Collection): Promise<string[][]> {
  const indexes = await collection.indexes();
  return indexes.map((index) => Object.keys(index.key));
}

/** A listing entry's kind: a time-series collection and a view have no `_id` index. */
export function KindOf(info: Pick<CollectionInfo, "type">): CollectionKind {
  return info.type === "view" || info.type === "timeseries"
    ? info.type
    : "collection";
}

function isStringList(value: unknown): value is string[] {
  return Array.isArray(value) && value.every(isString);
}

function isValidatorProperty(value: unknown): value is ValidatorProperty {
  if (!isDocument(value)) {
    return false;
  }
  const bsonType: unknown = value.bsonType;
  const values: unknown = value.enum;
  const description: unknown = value.description;
  return (
    (bsonType === undefined || isString(bsonType) || isStringList(bsonType)) &&
    (values === undefined || Array.isArray(values)) &&
    (description === undefined || isString(description))
  );
}

function isValidatorSchema(value: unknown): value is ValidatorSchema {
  if (!isDocument(value)) {
    return false;
  }
  const description: unknown = value.description;
  const required: unknown = value.required;
  const properties: unknown = value.properties;
  return (
    (description === undefined || isString(description)) &&
    (required === undefined || isStringList(required)) &&
    (properties === undefined ||
      (isDocument(properties) &&
        Object.values(properties).every(isValidatorProperty)))
  );
}

/** The `$jsonSchema` validator of a listing entry, when the entry has one. */
export function ValidatorOf(
  info: Pick<CollectionInfo, "options">,
): ValidatorSchema | undefined {
  const validator: unknown = info.options?.validator;
  const schema: unknown = isDocument(validator)
    ? validator.$jsonSchema
    : undefined;
  return isValidatorSchema(schema) ? schema : undefined;
}

async function ReadFacts(
  db: Db,
  info: CollectionInfo,
): Promise<CollectionFacts> {
  const kind = KindOf(info);
  const collection = db.collection(info.name);
  const keyed = kind === "collection";
  return {
    name: info.name,
    kind,
    validator: ValidatorOf(info),
    indexes: kind === "view" ? [] : await ReadIndexes(collection),
    ends: keyed ? await ReadEnds(collection) : undefined,
    sample: await ReadSample(collection),
  };
}

function IsUnauthorized(error: Error): boolean {
  return error instanceof MongoServerError && error.code === UNAUTHORIZED_CODE;
}

/**
 * Reads what registration needs of one collection, and writes nothing: its listing entry, its
 * index key patterns, the two ends of its `_id` index, and the native types of one `$sample` of
 * 1000 documents grouped by field.
 */
export async function ProbeCollection(
  db: Db,
  path: string,
  name: string,
): Promise<CollectionFacts> {
  const [info] = await db.listCollections<CollectionInfo>({ name }).toArray();
  if (!info) {
    throw MissingCollection(db, path, name);
  }
  try {
    return await ReadFacts(db, info);
  } catch (error) {
    if (error instanceof Error && IsUnauthorized(error)) {
      throw UnreadableCollection(db, path, name);
    }
    throw error;
  }
}
