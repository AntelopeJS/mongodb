import assert from "node:assert";
import type { QueryStage } from "@antelopejs/interface-database/common";
import { CROSS_INSTANCE } from "@antelopejs/interface-database/schema";
import { v4 as uuidv4 } from "uuid";
import { GetCollection } from "../../connection";
import { AggregationPipeline } from "./pipeline";
import { DecodeFunction, DecodeValue } from "./query";
import {
  DecodingContext,
  INSTANCE_FIELD,
  collectionName,
  normalizeInstanceId,
} from "./utils";

type InstanceContext =
  | { kind: "scoped"; instanceId: string | null }
  | { kind: "cross" };

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
              instance.instanceId,
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
      for (const [key, val] of Object.entries(filterDoc)) {
        filters.push({ $eq: [`$${key}`, val] });
      }
      return { $expr: filters.length === 1 ? filters[0] : { $and: filters } };
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
          filter: { _id: doc._id },
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
          filter: { _id: doc._id },
          replacement: doc,
          upsert: true,
        },
      }),
    };
    const buildOp = CONFLICT_OPERATIONS[this._conflictMode!];
    await collection.bulkWrite(documents.map(buildOp));
    return documents.map((doc) => doc._id);
  }

  private isAggregationOperator(
    value: unknown,
  ): value is Record<string, unknown> {
    return (
      value !== null &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      Object.getPrototypeOf(value) === Object.prototype &&
      Object.keys(value as object).some((k) => k.startsWith("$"))
    );
  }

  private hasExpression(value: unknown): boolean {
    if (typeof value === "string") return value.startsWith("$");
    if (value === null || typeof value !== "object") return false;
    if (Array.isArray(value)) return value.some((v) => this.hasExpression(v));
    if (Object.getPrototypeOf(value) !== Object.prototype) return false;
    if (this.isAggregationOperator(value)) return true;
    return Object.values(value as object).some((v) => this.hasExpression(v));
  }

  private literalizeUpdateValue(value: unknown): unknown {
    if (!this.hasExpression(value)) return { $literal: value };
    if (typeof value === "string") return value;
    if (Array.isArray(value)) {
      return value.map((v) => this.literalizeUpdateValue(v));
    }
    if (this.isAggregationOperator(value)) return value;
    return {
      $arrayToObject: [
        Object.entries(value as object).map(([k, v]) => [
          k,
          this.literalizeUpdateValue(v),
        ]),
      ],
    };
  }

  private async update() {
    const collection = await GetCollection(this.collection);
    const res = await collection.updateMany(this.getFilter(), [
      {
        $replaceWith: {
          $mergeObjects: ["$$ROOT", this.literalizeUpdateValue(this._newValue)],
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

  private needsExpr(value: unknown): boolean {
    if (typeof value === "string" && value.startsWith("$")) return true;
    if (value && typeof value === "object" && !Array.isArray(value))
      return true;
    return false;
  }

  protected async stage_get(stage: QueryStage) {
    assert(this.resultType === "table");
    this.resultType = "selection";
    this.singleElement = true;
    const value = await DecodeValue(stage.args[0], this.context);
    if (this.needsExpr(value)) {
      this.pipeline.push({
        $match: { $expr: { $eq: ["$_id", value] } },
      });
    } else {
      this.pipeline.push({
        $match: { _id: value },
      });
    }
  }

  protected async stage_getAll(stage: QueryStage) {
    assert(this.resultType === "table");
    this.resultType = "selection";
    const index = stage.options?.index ?? "_id";
    const rawValue = stage.args[0];
    if (Array.isArray(rawValue)) {
      const values = await Promise.all(
        rawValue.map((v) => DecodeValue(v, this.context)),
      );
      this.pipeline.push({
        $match: { $expr: { $in: [`$${index}`, values] } },
      });
    } else {
      const value = await DecodeValue(rawValue, this.context);
      if (this.needsExpr(value)) {
        this.pipeline.push({
          $match: { $expr: { $eq: [`$${index}`, value] } },
        });
      } else {
        this.pipeline.push({
          $match: { [index]: value },
        });
      }
    }
  }

  protected stage_between(stage: QueryStage) {
    assert(this.resultType === "table");
    this.resultType = "selection";
    const indexVar = `$${stage.options?.index ?? "_id"}`;
    const low = stage.args[0];
    const high = stage.args[1];
    this.pipeline.push({
      $match: {
        $expr: { $and: [{ $gte: [indexVar, low] }, { $lt: [indexVar, high] }] },
      },
    });
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
      this._newValue = await DecodeFunction(stage.args[0], this.context, [
        "$$ROOT",
      ]);
    } else {
      this._newValue = stage.args[0];
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
