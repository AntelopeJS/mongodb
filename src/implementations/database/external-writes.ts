import { DuplicateKeyError } from "@antelopejs/interface-database";
import { BSON, type Collection, type Document } from "mongodb";

import { GetDatabase } from "../../connection";
import type { ExternalCollection, KeyClass } from "./external-descriptor";
import { ExternalWriteFailure, InvalidValues } from "./external-errors";
import {
  NewKey,
  SplitPayload,
  type UnconvertedValue,
  type WriteMode,
} from "./external-payload";
import { TimeLimit } from "./external-probe";
import {
  type BsonValue,
  ConstantKind,
  KeyText,
  KeyValues,
} from "./external-read";
import { KeylessWriteRefusal } from "./external-refusals";

/** How an insert treats a document whose `_id` already exists. */
type ConflictMode = "update" | "replace";

type NativeCollection = Collection<Document>;

const NOTHING: Document = { _id: { $in: [] } };
const CANONICAL = { relaxed: false };

const KEY_RENDERINGS: Record<KeyClass, (key: BsonValue) => string> = {
  objectId: (key) => (key instanceof BSON.ObjectId ? key.toHexString() : ""),
  string: (key) => KeyTextOf(key),
  number: (key) => KeyTextOf(key),
  uuid: (key) => (key instanceof BSON.Binary ? key.toUUID().toHexString() : ""),
  object: (key) => BSON.EJSON.stringify(key, CANONICAL),
  date: (key) => (key instanceof Date ? key.toISOString() : ""),
};

function KeyTextOf(key: BsonValue): string {
  return key instanceof BSON.Long ? key.toString() : (KeyText(key) ?? "");
}

/** A key as reads render it. */
function RenderKey(key: KeyClass, value: BsonValue): string {
  return KEY_RENDERINGS[key](value);
}

/** An `_id` an aggregation answered, rendered or native, as the native key. */
function NativeKey(key: KeyClass, id: BsonValue): BsonValue {
  return ConstantKind(id) === "string" && key !== "string"
    ? KeyValues(key, [id])[0]
    : id;
}

function KeyClassOf(collection: ExternalCollection): KeyClass {
  if (!collection.key) {
    throw KeylessWriteRefusal(collection);
  }
  return collection.key;
}

/**
 * The filter a write applies: the selection's own matches when it is nothing else, otherwise the
 * `_id`s the selection answers, so a sort, a page or a comparison on a normalized value selects
 * the documents written exactly as it selects those read.
 */
async function WriteTarget(
  collection: ExternalCollection,
  pipeline: Document[],
): Promise<Document> {
  const key = KeyClassOf(collection);
  const matches = pipeline.filter((stage) => "$match" in stage);
  if (matches.length === pipeline.length) {
    if (matches.length === 0) {
      return {};
    }
    const conditions: unknown[] = [];
    for (const stage of matches) {
      conditions.push(stage.$match);
    }
    return { $and: conditions };
  }
  const native = (await GetDatabase()).collection(collection.collection);
  const rows = await native
    .aggregate([...pipeline, { $project: { _id: 1 } }], TimeLimit())
    .toArray();
  return {
    _id: {
      $in: rows.map((row) =>
        // oxlint-disable-next-line typescript/no-unsafe-argument -- row is a driver Document, so row._id is the any of mongodb's Document type; the pipeline ends in { $project: { _id: 1 } }, so it holds the stored _id, and naming that type through aggregate<T> or a typed local would only assert it
        NativeKey(key, row._id),
      ),
    },
  };
}

function KeyFilter(key: BsonValue): Document {
  return { _id: { $eq: key } };
}

function KeyAssertion(
  collection: ExternalCollection,
  key: BsonValue,
): Document {
  if (key === undefined) {
    return {};
  }
  const [parsed] = KeyValues(KeyClassOf(collection), [key]);
  return parsed === undefined ? NOTHING : { _id: { $eq: parsed } };
}

function UpdatePipeline(fields: Document, removed: string[]): Document[] {
  const stages: Document[] = [];
  if (Object.keys(fields).length > 0) {
    stages.push({ $set: fields });
  }
  if (removed.length > 0) {
    stages.push({ $unset: removed });
  }
  return stages;
}

async function Insert(
  collection: ExternalCollection,
  native: NativeCollection,
  values: Document[],
  conflict?: ConflictMode,
): Promise<string[]> {
  const key = KeyClassOf(collection);
  const mode: WriteMode = conflict ?? "insert";
  const prepared = values.map((value) => {
    const payload = SplitPayload(collection, mode, value);
    if (payload.unconverted.length > 0) {
      throw InvalidValues(
        collection.collection,
        payload.unconverted.map((unconverted) => unconverted.failure),
      );
    }
    return { ...payload, key: NewKey(collection, payload.key) };
  });
  if (!conflict) {
    const documents = prepared.map((payload): Document => ({
      _id: payload.key,
      ...payload.fields,
    }));
    await native.insertMany(documents, { ordered: true, ...TimeLimit() });
  } else if (prepared.length > 0) {
    await native.bulkWrite(
      prepared.map((payload) => ({
        updateOne: {
          filter: KeyFilter(payload.key),
          update: UpdatePipeline(payload.fields, payload.removed),
          upsert: true,
        },
      })),
      { ordered: true, ...TimeLimit() },
    );
  }
  return prepared.map((payload) => RenderKey(key, payload.key));
}

/** Refuses the values that do not convert unless no targeted document reads differently. */
async function RefuseUnconverted(
  collection: ExternalCollection,
  native: NativeCollection,
  filter: Document,
  unconverted: UnconvertedValue[],
) {
  if (unconverted.length === 0) {
    return;
  }
  const differing = await native.countDocuments(
    {
      $and: [
        filter,
        { $expr: { $or: unconverted.map((value) => value.differs) } },
      ],
    },
    TimeLimit(),
  );
  if (differing > 0) {
    throw InvalidValues(
      collection.collection,
      unconverted.map((value) => value.failure),
    );
  }
}

async function Modify(
  collection: ExternalCollection,
  native: NativeCollection,
  target: Document,
  write: { mode: WriteMode; value: Document },
): Promise<number> {
  const payload = SplitPayload(collection, write.mode, write.value);
  const stages = UpdatePipeline(payload.fields, payload.removed);
  if (stages.length === 0) {
    return 0;
  }
  const filter = { $and: [target, KeyAssertion(collection, payload.key)] };
  await RefuseUnconverted(collection, native, filter, payload.unconverted);
  const result = await native.updateMany(filter, stages, TimeLimit());
  return result.modifiedCount;
}

/** One write on an external collection, as `SelectionQuery` hands it over. */
export interface ExternalWrite {
  kind: string;
  value: Document | Document[];
  pipeline: Document[];
  conflict?: ConflictMode;
}

async function Run(
  collection: ExternalCollection,
  native: NativeCollection,
  write: ExternalWrite,
): Promise<number | string[]> {
  if (write.kind === "insert") {
    const values = [write.value].flat();
    return Insert(collection, native, values, write.conflict);
  }
  const target = await WriteTarget(collection, write.pipeline);
  if (write.kind === "delete") {
    return (await native.deleteMany(target, TimeLimit())).deletedCount;
  }
  const mode: WriteMode = write.kind === "replace" ? "replace" : "update";
  return Modify(collection, native, target, {
    mode,
    value: [write.value].flat()[0],
  });
}

/**
 * Runs an insert, update, replace or delete on an external collection: declared scalar fields
 * only, through the write-back guard, never a whole-document replace, and with the server's
 * refusals mapped to readable errors.
 */
export async function RunExternalWrite(
  collection: ExternalCollection,
  write: ExternalWrite,
): Promise<number | string[]> {
  KeyClassOf(collection);
  const native = (await GetDatabase()).collection(collection.collection);
  try {
    return await Run(collection, native, write);
  } catch (error) {
    throw error instanceof Error
      ? await WithIndexFields(
          native,
          ExternalWriteFailure(error, collection.collection),
        )
      : error;
  }
}

/**
 * A duplicate key whose fields the server did not report, as an insert's write error does not,
 * completed from the collection's own index.
 */
async function WithIndexFields(
  native: NativeCollection,
  error: Error,
): Promise<Error> {
  if (!(error instanceof DuplicateKeyError) || error.fields.length > 0) {
    return error;
  }
  const indexes = await native.indexes().catch(() => []);
  const index = indexes.find((entry) => entry.name === error.index);
  return index
    ? new DuplicateKeyError(error.index, Object.keys(index.key))
    : error;
}
