import assert from "node:assert";
import type { Collection, Document } from "mongodb";
import { MongoInvalidArgumentError, MongoServerError } from "mongodb";
import {
  ValidateAtomicMutation,
  ValidateAtomicMutationTable,
} from "@antelopejs/interface-database/atomic";
import type {
  AtomicMutation,
  AtomicMutationOutcome,
  AtomicUpdate,
} from "@antelopejs/interface-database/atomic";
import { IsQueryStage } from "@antelopejs/interface-database/common";

import { GetAtomicCollection } from "../../connection";
import { TypedWriteError } from "./indexes";
import {
  collectionName,
  INSTANCE_FIELD,
  isString,
  normalizeInstanceId,
  type QueryStage,
} from "./utils";

const ATOMIC_STAGE_PATH = ["schema", "instance", "table", "atomicMutation"];
const ATOMIC_OPTIONS = { collation: { locale: "simple" } };
const VALIDATION_ERROR_CODES = new Set(
  Object.values({
    badValue: 2,
    failedToParse: 9,
    typeMismatch: 14,
    immutableField: 66,
    invalidOptions: 72,
    documentValidationFailure: 121,
    bsonObjectTooLarge: 10334,
    duplicateKey: 11000,
  }),
);

interface AtomicTarget {
  schemaId: string;
  tableName: string;
  instance: QueryStage<"instance">;
  mutation: QueryStage<"atomicMutation">;
}

function validateStages(stages: QueryStage[]): AtomicTarget {
  const [schema, instance, table, mutation] = stages;
  assert(
    stages.length === ATOMIC_STAGE_PATH.length &&
      IsQueryStage(schema, "schema") &&
      IsQueryStage(instance, "instance") &&
      IsQueryStage(table, "table") &&
      IsQueryStage(mutation, "atomicMutation"),
    "atomicMutation requires a direct schema/instance/table target",
  );
  const schemaId: unknown = schema.options?.id;
  assert(isString(schemaId), "Missing schema id");
  const tableName: unknown = table.options?.id;
  assert(isString(tableName), "Missing table id");
  ValidateAtomicMutationTable(stages.slice(0, -1));
  assert(
    mutation.args.length === 2 && mutation.options === undefined,
    "Invalid atomicMutation arguments",
  );
  return { schemaId, tableName, instance, mutation };
}

function conditionFilter(mutation: AtomicMutation<Document>): Document {
  if (mutation.type === "deleteIfEqual") {
    return {
      $expr: {
        $eq: [`$${mutation.field}`, { $literal: mutation.expectedValue }],
      },
    };
  }
  if (!isString(mutation.expectedRevision)) {
    return { [mutation.revisionField]: { $exists: false } };
  }
  return {
    $expr: {
      $eq: [
        `$${mutation.revisionField}`,
        { $literal: mutation.expectedRevision },
      ],
    },
  };
}

export async function RunAtomicMutation(
  stages: QueryStage[],
): Promise<AtomicMutationOutcome> {
  const {
    schemaId,
    tableName,
    instance,
    mutation: stage,
  } = validateStages(stages);
  const [key, mutation] = stage.args;
  ValidateAtomicMutation(key, mutation, [INSTANCE_FIELD]);
  const collection = GetAtomicCollection(collectionName(schemaId, tableName));
  const filter: Document = {
    _id: key,
    [INSTANCE_FIELD]: normalizeInstanceId(instance.options?.id),
    ...conditionFilter(mutation),
  };
  return executeMutation(collection, filter, mutation).catch(
    (error: unknown) => {
      throw TypedWriteError(schemaId, tableName, error);
    },
  );
}

function updatePipeline(mutation: AtomicUpdate<Document>): Document[] {
  return [
    {
      $replaceWith: {
        $mergeObjects: [
          "$$ROOT",
          {
            $literal: {
              ...mutation.patch,
              [mutation.revisionField]: mutation.nextRevision,
            },
          },
        ],
      },
    },
  ];
}

function acknowledgedOutcome(
  acknowledged: boolean,
  count: number,
): AtomicMutationOutcome {
  if (!acknowledged) return "unknown";
  return count === 1 ? "applied" : "not-applied";
}

async function executeMutation(
  collection: Collection,
  filter: Document,
  mutation: AtomicMutation<Document>,
): Promise<AtomicMutationOutcome> {
  try {
    if (mutation.type !== "update") {
      const result = await collection.deleteOne(filter, ATOMIC_OPTIONS);
      return acknowledgedOutcome(result.acknowledged, result.deletedCount);
    }
    const result = await collection.updateOne(
      filter,
      updatePipeline(mutation),
      { ...ATOMIC_OPTIONS, upsert: false },
    );
    return acknowledgedOutcome(result.acknowledged, result.matchedCount);
  } catch (error) {
    if (
      error instanceof MongoInvalidArgumentError ||
      (error instanceof MongoServerError &&
        VALIDATION_ERROR_CODES.has(Number(error.code)))
    ) {
      throw error;
    }
    return "unknown";
  }
}
