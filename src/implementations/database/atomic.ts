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

import { GetAtomicCollection } from "../../connection";
import {
  collectionName,
  INSTANCE_FIELD,
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

function validateStages(stages: QueryStage[]): void {
  assert(
    stages.length === ATOMIC_STAGE_PATH.length &&
      stages.every((stage, index) => stage.stage === ATOMIC_STAGE_PATH[index]),
    "atomicMutation requires a direct schema/instance/table target",
  );
  assert(typeof stages[0].options?.id === "string", "Missing schema id");
  assert(typeof stages[2].options?.id === "string", "Missing table id");
  ValidateAtomicMutationTable(stages.slice(0, -1));
  assert(
    stages[3].args.length === 2 && stages[3].options === undefined,
    "Invalid atomicMutation arguments",
  );
}

function conditionFilter(mutation: AtomicMutation<Document>): Document {
  if (mutation.type === "deleteIfEqual") {
    return {
      $expr: {
        $eq: [`$${mutation.field}`, { $literal: mutation.expectedValue }],
      },
    };
  }
  if (typeof mutation.expectedRevision !== "string") {
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
  validateStages(stages);
  const [key, mutation] = stages[3].args;
  ValidateAtomicMutation(key, mutation, [INSTANCE_FIELD]);
  const collection = GetAtomicCollection(
    collectionName(stages[0].options.id, stages[2].options.id),
  );
  const filter: Document = {
    _id: key,
    [INSTANCE_FIELD]: normalizeInstanceId(stages[1].options?.id),
    ...conditionFilter(mutation),
  };
  return executeMutation(collection, filter, mutation);
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
