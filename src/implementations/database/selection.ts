import assert from "node:assert";
import type { QueryStage } from "@antelopejs/interface-database/common";
import { v4 as uuidv4 } from "uuid";
import { buildDatabaseName, GetCollection } from "../../connection";
import { AggregationPipeline } from "./pipeline";
import { DecodeFunction, DecodeValue } from "./query";
import {
  CreateInstance,
  DestroyInstance,
  IsRowLevel,
  IsValidInstance,
} from "./schema";
import { DecodingContext } from "./utils";

export class SelectionQuery extends AggregationPipeline {
  private _newValue: any;
  private _conflictMode?: "update" | "replace";
  private readonly rowLevel: boolean;
  public readonly instanceId: string | undefined;
  public readonly tableName: string;

  public constructor(
    schemaId: string,
    instanceId: string | undefined,
    tableName: string,
    isChangeStream: boolean,
    context: DecodingContext,
  ) {
    const rowLevel = IsRowLevel(schemaId);
    if (rowLevel) {
      if (instanceId === undefined) {
        throw new Error(`Row-level schema '${schemaId}' requires a tenant ID`);
      }
      super(
        schemaId,
        schemaId,
        tableName,
        [{ $match: { tenant_id: instanceId } }],
        isChangeStream,
        context,
      );
    } else {
      super(
        schemaId,
        buildDatabaseName(schemaId, instanceId),
        tableName,
        [],
        isChangeStream,
        context,
      );
    }
    this.instanceId = instanceId;
    this.tableName = tableName;
    this.rowLevel = rowLevel;
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
      if (stages[1].stage === "createInstance") {
        const selection = new SelectionQuery(
          schemaId,
          instanceId,
          "",
          false,
          new DecodingContext(),
        );
        selection.resultType = "createInstance";
        return selection;
      }
      if (stages[1].stage === "destroyInstance") {
        const selection = new SelectionQuery(
          schemaId,
          instanceId,
          "",
          false,
          new DecodingContext(),
        );
        selection.resultType = "destroyInstance";
        return selection;
      }
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

  private async createInstance() {
    if (this.rowLevel) {
      return this.instanceId;
    }
    await CreateInstance(this.schemaId, this.instanceId);
    return this.instanceId;
  }

  private async destroyInstance() {
    if (this.rowLevel) {
      return;
    }
    await DestroyInstance(this.schemaId, this.instanceId);
  }

  private async insert() {
    const collection = await GetCollection(this.database, this.collection);
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
    for (const document of documents) {
      document._id = document._id ?? uuidv4();
      if (this.rowLevel) {
        document.tenant_id = this.instanceId;
      }
    }
    return documents;
  }

  private async insertWithConflict(collection: any, documents: any[]) {
    const CONFLICT_OPERATIONS: Record<string, (doc: any) => any> = {
      update: (doc) => ({
        updateOne: {
          filter: { _id: doc._id },
          update: { $set: doc },
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

  private async update() {
    const collection = await GetCollection(this.database, this.collection);
    const isExpression =
      typeof this._newValue === "string" ||
      (typeof this._newValue === "object" &&
        this._newValue !== null &&
        Object.keys(this._newValue).some((k) => k.startsWith("$")));
    const updatePipeline = isExpression
      ? [{ $replaceWith: this._newValue }]
      : [{ $set: this._newValue }];
    const res = await collection.updateMany(this.getFilter(), updatePipeline);
    return res.modifiedCount;
  }

  private async replace() {
    const collection = await GetCollection(this.database, this.collection);
    if (this.rowLevel) {
      this._newValue.tenant_id = this.instanceId;
    }
    const res = await collection.findOneAndReplace(
      this.getFilter(),
      this._newValue,
    );
    return res ? 1 : 0;
  }

  private async delete() {
    const collection = await GetCollection(this.database, this.collection);
    const res = await collection.deleteMany(this.getFilter());
    return res.deletedCount;
  }

  private async ensureInstance() {
    if (!IsValidInstance(this.schemaId, this.instanceId)) {
      throw new Error(
        `Instance '${this.instanceId ?? "(global)"}' does not exist for schema '${this.schemaId}'`,
      );
    }
  }

  public async run(): Promise<any> {
    if (
      this.resultType !== "createInstance" &&
      this.resultType !== "destroyInstance"
    ) {
      await this.ensureInstance();
    }
    switch (this.resultType) {
      case "createInstance":
        return this.createInstance();
      case "destroyInstance":
        return this.destroyInstance();
      case "insert":
        return this.insert();
      case "update":
        return this.update();
      case "replace":
        return this.replace();
      case "delete":
        return this.delete();
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
      const hasExpr = values.some((v) => this.needsExpr(v));
      if (hasExpr) {
        this.pipeline.push({
          $match: { $expr: { $in: [`$${index}`, values] } },
        });
      } else {
        this.pipeline.push({
          $match: { [index]: { $in: values } },
        });
      }
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
