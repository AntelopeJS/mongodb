import assert from "node:assert";
import type { AggregateOptions, AggregationCursor, Document } from "mongodb";
import type { QueryStage } from "@antelopejs/interface-database/common";

import {
  type ResolvedIndex,
  ResolveIndex,
  WarnIfNotCrossInstance,
} from "./indexes";
import { GetCollection } from "../../connection";
import { ToChangeEvent } from "./change-event";
import type { DecodingContext } from "./utils";
import { DecodeFunction, DecodeValue } from "./expression";

/** What a pipeline reads: its table and collection, the stages it starts from, and the context it decodes in. */
export interface PipelineSource {
  schemaId: string;
  tableName: string;
  collection: string;
  initialFilter: Document[];
  isChangeStream: boolean;
  context: DecodingContext;
}

/**
 * The aggregation pipeline of one stream: its state, how it runs, and the stages that filter,
 * project, order or reduce it without rewriting its root.
 */
export class PipelineCore {
  public readonly schemaId: string;
  public readonly tableName: string;
  public readonly collection: string;
  public readonly isChangeStream: boolean;
  public readonly context: DecodingContext;
  public resultType = "stream";

  private cursor?: AggregationCursor<Document>;
  protected options: AggregateOptions = {};
  private isClosed = false;

  /**
   * This query will only return one document, ignore the other
   */
  public singleElement = false;

  public reductionDefault?: number;

  /**
   * MongoDB does not support values other than objects at the root of aggregation pipelines
   *
   * The `wrappedObject` field contains the actual value we want
   */
  public wrappedObject?: string;

  /**
   * Documents contain fields other than `'_id'` and `wrappedObject` that must not be modified
   */
  public inCompoundObject = false;

  public readonly initialFilter: Document[];
  public readonly pipeline: Document[];

  public constructor(source: PipelineSource) {
    this.schemaId = source.schemaId;
    this.tableName = source.tableName;
    this.collection = source.collection;
    this.isChangeStream = source.isChangeStream;
    this.context = source.context;
    this.initialFilter = source.initialFilter;
    this.pipeline = [...source.initialFilter];
    if (source.isChangeStream) {
      this.pipeline.unshift(
        {
          $changeStream: {
            fullDocument: "updateLookup",
            fullDocumentBeforeChange: "whenAvailable",
          },
        },
        {
          $addFields: {
            fullDocumentBeforeChange: {
              $ifNull: ["$fullDocumentBeforeChange", "$documentKey"],
            },
          },
        },
      );
    }
  }

  public async run(): Promise<any> {
    const collection = await GetCollection(this.collection);
    const rows = await collection
      .aggregate(this.pipeline, this.options)
      .toArray();
    const wrappedObject = this.wrappedObject;
    const result: unknown[] = [];
    for (const element of rows) {
      result.push(wrappedObject ? element[wrappedObject] : element);
    }
    if (this.singleElement) {
      return result.length > 0 ? result[0] : this.reductionDefault;
    }
    return result;
  }

  public async runDebug(limit = 10): Promise<any[]> {
    const collection = await GetCollection(this.collection);
    const results = [];
    try {
      for (let i = 0; i <= this.pipeline.length; ++i) {
        results[i] = await collection
          .aggregate([...this.pipeline.slice(0, i), { $limit: limit }])
          .toArray();
      }
    } catch (e) {
      results.push(e);
    }
    return results;
  }

  public async readCursor() {
    if (!this.cursor) {
      const collection = await GetCollection(this.collection);
      if (this.isClosed) {
        return null;
      }
      this.cursor = collection.aggregate(this.pipeline, this.options);
    }
    const change = await this.cursor.next();
    if (change === null) {
      return null;
    }
    if (this.isChangeStream) {
      return ToChangeEvent(change);
    }
    return this.wrappedObject ? change[this.wrappedObject] : change;
  }

  public async closeCursor() {
    this.isClosed = true;
    if (this.cursor) {
      await this.cursor.close();
    }
  }

  protected getRoot() {
    return this.wrappedObject ? `$${this.wrappedObject}` : "$$ROOT";
  }

  protected getField(field?: string) {
    if (this.wrappedObject) {
      return field ? `${this.wrappedObject}.${field}` : this.wrappedObject;
    }
    return field ?? "$ROOT";
  }

  protected makeWrapped() {
    if (!this.wrappedObject) {
      this.wrappedObject = "_wrapped";
    }
    return this.wrappedObject;
  }

  protected isCrossInstance(): boolean {
    return false;
  }

  protected resolveQueriedIndex(indexId: string | undefined): ResolvedIndex {
    const index = ResolveIndex(this.schemaId, this.tableName, indexId);
    if (this.isCrossInstance()) {
      WarnIfNotCrossInstance(index);
    }
    return index;
  }

  protected stage_changes() {
    assert(this.isChangeStream, "Changes call must be last");
    return this;
  }

  protected async stage_filter(stage: QueryStage<"filter">) {
    const filterRoot = this.isChangeStream ? "$fullDocument" : this.getRoot();
    const filterResult = await DecodeFunction(stage.args[0], this.context, [
      filterRoot,
    ]);
    this.pipeline.push({
      $match: { $expr: filterResult },
    });
    return this;
  }

  protected stage_pluck(stage: QueryStage<"pluck">) {
    const pluckObject: Record<string, number> = {};
    for (const field of stage.args[0]) {
      pluckObject[this.getField(field)] = 1;
    }
    if (this.isChangeStream) {
      this.pipeline.push({
        $project: {
          _id: 1,
          operationType: 1,
          fullDocument: pluckObject,
          fullDocumentBeforeChange: pluckObject,
        },
      });
    } else if (this.inCompoundObject && this.wrappedObject) {
      const projectedFields: Record<string, string> = {};
      for (const field of stage.args[0]) {
        projectedFields[field] = `$${this.wrappedObject}.${field}`;
      }
      this.pipeline.push({
        $replaceRoot: {
          newRoot: {
            $mergeObjects: [
              "$$ROOT",
              { [this.wrappedObject]: projectedFields },
            ],
          },
        },
      });
    } else {
      this.pipeline.push({
        $project: pluckObject,
      });
    }
    return this;
  }

  protected stage_without(stage: QueryStage<"without">) {
    const withoutObject: Record<string, number> = {};
    for (const field of stage.args[0]) {
      withoutObject[this.getField(field)] = 0;
    }
    if (this.isChangeStream) {
      this.pipeline.push({
        $project: {
          fullDocument: withoutObject,
          fullDocumentBeforeChange: withoutObject,
        },
      });
    } else {
      this.pipeline.push({
        $project: withoutObject,
      });
    }
    return this;
  }

  protected stage_orderBy(stage: QueryStage<"orderBy">) {
    assert(!this.isChangeStream, "OrderBy not supported in change streams");
    const index = this.resolveQueriedIndex(stage.options.index);
    const direction = stage.options.direction === "desc" ? -1 : 1;
    const fields = index.fields.map((field): [string, number] => [
      this.getField(field),
      direction,
    ]);
    this.pipeline.push({
      $sort: Object.fromEntries(fields),
    });
    return this;
  }

  protected async stage_slice(stage: QueryStage<"slice">) {
    const offset = await DecodeValue(stage.args[0], this.context);
    if (offset !== 0) {
      this.pipeline.push({ $skip: offset });
    }

    const count = await DecodeValue(stage.args[1], this.context);
    if (count) {
      this.pipeline.push({ $limit: count });
    }
  }

  protected async stage_nth(stage: QueryStage<"nth">) {
    this.singleElement = true;
    return this.stage_slice({ stage: "slice", args: [stage.args[0], 1] });
  }

  protected stage_count(stage: QueryStage<"count">) {
    this.singleElement = true;
    this.reductionDefault = 0;
    if (stage.options.field) {
      const field = this.getField(stage.options.field);
      const wrapped = this.makeWrapped();
      this.pipeline.push(
        {
          $group: {
            _id: null,
            [wrapped]: { $addToSet: `$${field}` },
          },
        },
        {
          $project: {
            [wrapped]: { $size: `$${wrapped}` },
          },
        },
      );
    } else {
      this.pipeline.push({
        $group: {
          _id: null,
          [this.makeWrapped()]: { $count: {} },
        },
      });
    }
  }

  protected stage_sum(stage: QueryStage<"sum">) {
    this.singleElement = true;
    this.reductionDefault = 0;
    assert(stage.options.field || this.wrappedObject);
    const field = this.getField(stage.options.field);
    this.pipeline.push({
      $group: {
        _id: null,
        [this.makeWrapped()]: { $sum: `$${field}` },
      },
    });
    return this;
  }

  protected stage_avg(stage: QueryStage<"avg">) {
    this.singleElement = true;
    assert(stage.options.field || this.wrappedObject);
    const field = this.getField(stage.options.field);
    this.pipeline.push({
      $group: {
        _id: null,
        [this.makeWrapped()]: { $avg: `$${field}` },
      },
    });
    return this;
  }

  protected stage_min(stage: QueryStage<"min">) {
    this.singleElement = true;
    assert(stage.options.field || this.wrappedObject);
    const field = this.getField(stage.options.field);
    this.pipeline.push({
      $group: {
        _id: null,
        [this.makeWrapped()]: { $min: `$${field}` },
      },
    });
    return this;
  }

  protected stage_max(stage: QueryStage<"max">) {
    this.singleElement = true;
    assert(stage.options.field || this.wrappedObject);
    const field = this.getField(stage.options.field);
    this.pipeline.push({
      $group: {
        _id: null,
        [this.makeWrapped()]: { $max: `$${field}` },
      },
    });
    return this;
  }

  protected stage_distinct(stage: QueryStage<"distinct">) {
    const field = this.getField(stage.options.field);
    const wrapped = this.makeWrapped();
    this.pipeline.push({
      $group: {
        _id: null,
        [wrapped]: { $addToSet: `$${field}` },
      },
    });
    if (!this.inCompoundObject) {
      this.pipeline.push({
        $unwind: { path: `$${wrapped}` },
      });
    }
  }
}
