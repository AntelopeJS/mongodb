import assert from "node:assert";
import { v4 as uuidv4 } from "uuid";
import { CROSS_INSTANCE } from "@antelopejs/interface-database/schema";
import type { QueryStage } from "@antelopejs/interface-database/common";

import { GetCollection } from "../../connection";
// oxlint-disable-next-line import/no-cycle -- inheritance edge of the AggregationPipeline / SelectionQuery pair; see pipeline.ts
import { AggregationPipeline } from "./pipeline";
import {
  DecodeFunction,
  DecodeLiteralValue,
  DecodeValue,
  IsConstantValue,
} from "./expression";
import { type ResolvedIndex, SingleIndexField } from "./indexes";
import {
  collectionName,
  DecodingContext,
  INSTANCE_FIELD,
  normalizeInstanceId,
} from "./utils";

type InstanceContext =
  | { kind: "scoped"; instanceId: string | null }
  | { kind: "cross" };

const SINGLE_FIELD = 1;
const SINGLE_KEY = 1;
const NO_KEYS_FILTER = { _id: { $in: [] } };

type StageArgument = QueryStage["args"][number];

function toKeyTuple(index: ResolvedIndex, key: unknown): unknown[] {
  const { fields } = index;
  assert(
    Array.isArray(key) && key.length === fields.length,
    `Index "${index.indexId}" takes keys of ${fields.length} values ([${fields.join(", ")}])`,
  );
  return key;
}

function toKeyTuples(index: ResolvedIndex, decodedKeys: unknown): unknown[][] {
  const keys = Array.isArray(decodedKeys) ? decodedKeys : [decodedKeys];
  const isSingleTuple =
    keys.length > 0 && keys.every((key) => !Array.isArray(key));
  const tuples = isSingleTuple ? [keys] : keys;
  return tuples.map((key) => toKeyTuple(index, key));
}

function resolveInstanceContext(instanceId: unknown): InstanceContext {
  if (instanceId === CROSS_INSTANCE) {
    return { kind: "cross" };
  }
  return { kind: "scoped", instanceId: normalizeInstanceId(instanceId) };
}

function buildInitialPipeline(
  instance: InstanceContext,
  isChangeStream: boolean,
): any[] {
  if (instance.kind !== "scoped") {
    return [];
  }
  if (isChangeStream) {
    return [
      {
        $match: {
          $expr: {
            $eq: [
              {
                $ifNull: [
                  `$fullDocument.${INSTANCE_FIELD}`,
                  `$fullDocumentBeforeChange.${INSTANCE_FIELD}`,
                ],
              },
              { $literal: instance.instanceId },
            ],
          },
        },
      },
    ];
  }
  return [{ $match: { [INSTANCE_FIELD]: instance.instanceId } }];
}

export class SelectionQuery extends AggregationPipeline {
  private _newValue: any;
  private _updateValue: unknown;
  private _conflictMode?: "update" | "replace";
  private readonly instance: InstanceContext;
  public readonly instanceId: string | typeof CROSS_INSTANCE | undefined;

  public constructor(
    schemaId: string,
    instanceId: string | typeof CROSS_INSTANCE | undefined,
    tableName: string,
    isChangeStream: boolean,
    context: DecodingContext,
  ) {
    const instance = resolveInstanceContext(instanceId);
    super(
      schemaId,
      tableName,
      collectionName(schemaId, tableName),
      buildInitialPipeline(instance, isChangeStream),
      isChangeStream,
      context,
    );
    this.instanceId = instanceId;
    this.instance = instance;
    this.resultType = "table";
  }

  public static async decode(
    stages: QueryStage[],
    context?: DecodingContext,
  ): Promise<AggregationPipeline> {
    if (stages[0]?.stage === "schema") {
      const schemaId = stages[0]?.options?.id;
      const instanceId = stages[1]?.options?.id;
      assert(stages[0]?.stage === "schema" && schemaId, "Unknown schema");
      assert(
        stages[1].stage === "instance" && stages[2]?.stage === "table",
        "Invalid request",
      );
      const tableName = stages[2].options.id;
      const selection = new SelectionQuery(
        schemaId,
        instanceId,
        tableName,
        stages[stages.length - 1]?.stage === "changes",
        context ?? new DecodingContext(),
      );
      await selection.addStages(stages.slice(3));
      return selection;
    }
    return AggregationPipeline.decode(stages, context);
  }

  private getFilter() {
    const filters = [];
    const filterDoc = {};
    for (let i = 0; i < this.pipeline.length; ++i) {
      if ("$match" in this.pipeline[i]) {
        if ("$expr" in this.pipeline[i].$match) {
          filters.push(this.pipeline[i].$match.$expr);
        } else {
          Object.assign(filterDoc, this.pipeline[i].$match);
        }
      }
    }
    if (filters.length === 0) {
      return filterDoc;
    } else {
      return {
        ...filterDoc,
        $expr: filters.length === 1 ? filters[0] : { $and: filters },
      };
    }
  }

  private async insert() {
    if (this.instance.kind === "cross") {
      throw new Error(
        `Insert into '${this.tableName}' requires a specific instance id (CROSS_INSTANCE is read-only)`,
      );
    }
    const collection = await GetCollection(this.collection);
    const documents = this.prepareInsertDocuments();
    if (!this._conflictMode) {
      const res = await collection.insertMany(documents);
      return Object.values(res.insertedIds);
    }
    return this.insertWithConflict(collection, documents);
  }

  private prepareInsertDocuments() {
    const documents = Array.isArray(this._newValue)
      ? this._newValue
      : [this._newValue];
    assert(this.instance.kind === "scoped");
    const instanceId = this.instance.instanceId;
    for (const document of documents) {
      document._id = document._id ?? uuidv4();
      document[INSTANCE_FIELD] = instanceId;
    }
    return documents;
  }

  private async insertWithConflict(collection: any, documents: any[]) {
    const CONFLICT_OPERATIONS: Record<string, (doc: any) => any> = {
      update: (doc) => ({
        updateOne: {
          filter: { _id: doc._id, [INSTANCE_FIELD]: doc[INSTANCE_FIELD] },
          update: [
            {
              $replaceWith: {
                $mergeObjects: ["$$ROOT", { $literal: doc }],
              },
            },
          ],
          upsert: true,
        },
      }),
      replace: (doc) => ({
        replaceOne: {
          filter: { _id: doc._id, [INSTANCE_FIELD]: doc[INSTANCE_FIELD] },
          replacement: doc,
          upsert: true,
        },
      }),
    };
    const buildOp = CONFLICT_OPERATIONS[this._conflictMode!];
    await collection.bulkWrite(documents.map(buildOp));
    return documents.map((doc) => doc._id);
  }

  private async update() {
    const collection = await GetCollection(this.collection);
    const res = await collection.updateMany(this.getFilter(), [
      {
        $replaceWith: {
          $mergeObjects: [
            "$$ROOT",
            this._updateValue,
            { [INSTANCE_FIELD]: `$${INSTANCE_FIELD}` },
          ],
        },
      },
    ]);
    return res.modifiedCount;
  }

  private async replace() {
    if (this.instance.kind === "cross") {
      throw new Error(
        `Replace on '${this.tableName}' requires a specific instance id (CROSS_INSTANCE would strip the _instance field; use update for cross-instance mutations)`,
      );
    }
    const collection = await GetCollection(this.collection);
    this._newValue[INSTANCE_FIELD] = this.instance.instanceId;
    const res = await collection.findOneAndReplace(
      this.getFilter(),
      this._newValue,
    );
    return res ? 1 : 0;
  }

  private async delete() {
    const collection = await GetCollection(this.collection);
    const res = await collection.deleteMany(this.getFilter());
    return res.deletedCount;
  }

  public async run(): Promise<any> {
    const RUNNERS: Record<string, () => Promise<any>> = {
      insert: () => this.insert(),
      update: () => this.update(),
      replace: () => this.replace(),
      delete: () => this.delete(),
    };
    const runner = RUNNERS[this.resultType];
    if (runner) {
      return runner();
    }
    return super.run();
  }

  protected async stage_get(stage: QueryStage) {
    assert(this.resultType === "table");
    this.resultType = "selection";
    this.singleElement = true;
    const key = stage.args[0];
    if (IsConstantValue(key)) {
      this.pipeline.push({ $match: { _id: key } });
      return;
    }
    const value = await DecodeValue(key, this.context);
    this.pipeline.push({
      $match: { $expr: { $eq: ["$_id", value] } },
    });
  }

  protected async stage_getAll(stage: QueryStage) {
    assert(this.resultType === "table");
    this.resultType = "selection";
    const index = this.resolveQueriedIndex(stage.options?.index);
    const rawValue = stage.args[0];
    if (index.fields.length > SINGLE_FIELD) {
      const isConstant = IsConstantValue(rawValue);
      const keys = isConstant
        ? rawValue
        : await DecodeValue(rawValue, this.context);
      this.pipeline.push({
        $match: this.compoundKeysFilter(
          index.fields,
          toKeyTuples(index, keys),
          isConstant,
        ),
      });
      return;
    }
    this.pipeline.push({
      $match: await this.singleFieldKeysFilter(index.fields[0], rawValue),
    });
  }

  private async singleFieldKeysFilter(field: string, rawValue: StageArgument) {
    const isList = Array.isArray(rawValue);
    if (IsConstantValue(rawValue)) {
      return { [field]: isList ? { $in: rawValue } : rawValue };
    }
    const value = await DecodeValue(rawValue, this.context);
    const operator = isList ? "$in" : "$eq";
    return { $expr: { [operator]: [`$${field}`, value] } };
  }

  private compoundKeysFilter(
    fields: string[],
    tuples: unknown[][],
    isConstant: boolean,
  ) {
    if (tuples.length === 0) {
      return NO_KEYS_FILTER;
    }
    if (!isConstant) {
      const conditions = tuples.map((tuple) => ({
        $and: fields.map((field, i) => ({ $eq: [`$${field}`, tuple[i]] })),
      }));
      return { $expr: { $or: conditions } };
    }
    const documents = tuples.map((tuple) =>
      Object.fromEntries(fields.map((field, i) => [field, tuple[i]])),
    );
    return documents.length === SINGLE_KEY ? documents[0] : { $or: documents };
  }

  protected async stage_between(stage: QueryStage) {
    assert(this.resultType === "table");
    this.resultType = "selection";
    const index = this.resolveQueriedIndex(stage.options?.index);
    const indexVar = `$${SingleIndexField(index)}`;
    const low = await DecodeValue(stage.args[0], this.context);
    const high = await DecodeValue(stage.args[1], this.context);
    this.pipeline.push({
      $match: {
        $expr: { $and: [{ $gte: [indexVar, low] }, { $lt: [indexVar, high] }] },
      },
    });
  }

  protected override isCrossInstance(): boolean {
    return this.instance.kind === "cross";
  }

  protected async stage_insert(stage: QueryStage) {
    assert(this.resultType === "table");
    this.resultType = "insert";
    this._newValue = stage.args[0];
    this._conflictMode = stage.options?.conflict;
  }

  protected async stage_update(stage: QueryStage) {
    assert(this.resultType === "table" || this.resultType === "selection");
    this.resultType = "update";
    if (stage.args[0]?.stage === "func") {
      this._updateValue = await DecodeFunction(
        stage.args[0],
        this.context,
        ["$$ROOT"],
        DecodeLiteralValue,
      );
    } else {
      this._updateValue = { $literal: stage.args[0] };
    }
  }

  protected async stage_replace(stage: QueryStage) {
    assert(this.resultType === "table" || this.resultType === "selection");
    this.resultType = "replace";
    this._newValue = stage.args[0];
  }

  protected stage_delete() {
    assert(this.resultType === "table" || this.resultType === "selection");
    this.resultType = "delete";
  }
}
