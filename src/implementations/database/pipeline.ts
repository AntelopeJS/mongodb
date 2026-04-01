import assert from "node:assert";
import type { Stream } from "@antelopejs/interface-database";
import type { QueryStage } from "@antelopejs/interface-database/common";
import type { AggregationCursor } from "mongodb";
import { GetCollection } from "../../connection";
import { DecodeFunction, DecodeValue } from "./query";
import { GetIndex } from "./schema";
import { SelectionQuery } from "./selection";
import { type DecodingContext, Temporary } from "./utils";

function DefaultConstant(data: any, def: any) {
  if (def) {
    return { $ifNull: [data, { $literal: def }] };
  }
  return data;
}

export class AggregationPipeline {
  public resultType = "stream";

  private cursor?: AggregationCursor;

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

  public constructor(
    public readonly schemaId: string,
    public readonly database: string,
    public readonly collection: string,
    public readonly pipeline: any[],
    public readonly isChangeStream: boolean,
    public readonly context: DecodingContext,
  ) {
    if (isChangeStream) {
      pipeline.unshift(
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

  public static async decode(
    stages: QueryStage[],
    context?: DecodingContext,
    modifier?: (pipeline: AggregationPipeline) => any,
  ): Promise<AggregationPipeline> {
    if (stages[0]?.stage === "arg") {
      assert(context, "Arg query without context?");
      const query = new AggregationPipeline(
        "",
        "$ARG",
        stages[0].args[0],
        [],
        false,
        context,
      );
      if (modifier) {
        await modifier(query);
      }
      await query.addStages(stages.slice(1));
      return query;
    }
    throw new Error("Invalid query");
  }

  private pendingUnset: string[] = [];

  public async addStages(stages: QueryStage[]) {
    const oldSubquery = this.context.subquery;
    this.context.subquery = async (subQuery) => {
      const tmp = Temporary("join");
      const tmpRoot = Temporary("parentRoot");
      const root = this.getRoot();
      const rightStream = await SelectionQuery.decode(
        subQuery,
        this.context.withRoot(`$$${tmpRoot}`),
      );
      this.pipeline.push({
        $lookup: {
          from: rightStream.collection,
          let: { [tmpRoot]: root },
          pipeline: rightStream.pipeline,
          as: tmp,
        },
      });
      this.pendingUnset.push(tmp);
      const resultField = `$${tmp}${rightStream.wrappedObject ? `.${rightStream.wrappedObject}` : ""}`;
      if (rightStream.singleElement) {
        const first = { $first: resultField };
        return { $ifNull: [first, null] };
      }
      return resultField;
    };
    try {
      for (const stage of stages) {
        const stageCallback = `stage_${stage.stage}`;
        if (!(stageCallback in this)) {
          throw new Error(`Unimplemented stage: ${stage.stage}`);
        }
        const callback = this[stageCallback as keyof this];
        if (!(callback instanceof Function)) {
          continue;
        }
        await callback.apply(this, [stage]);
      }
    } finally {
      this.context.subquery = oldSubquery;
    }
  }

  public async run(): Promise<any> {
    const collection = await GetCollection(this.database, this.collection);
    let result = await collection.aggregate(this.pipeline, {}).toArray();
    if (this.wrappedObject) {
      const wrappedObject = this.wrappedObject;
      result = result.map((element: any) => element[wrappedObject]);
    }
    if (this.singleElement) {
      return result.length > 0 ? result[0] : this.reductionDefault;
    }
    return result;
  }

  public async runDebug(limit = 10): Promise<any[]> {
    const collection = await GetCollection(this.database, this.collection);
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
      const collection = await GetCollection(this.database, this.collection);
      this.cursor = collection.aggregate(this.pipeline, {});
    }
    const change = await this.cursor.next();
    if (this.isChangeStream) {
      const operations: Record<string, string> = {
        insert: "added",
        replace: "modified",
        update: "modified",
        delete: "removed",
      };
      return {
        changeType: operations[change.operationType] ?? change.operationType,
        // TODO: Handle undefined fullDocumentBeforeChange better
        oldValue: change.fullDocumentBeforeChange,
        newValue: change.fullDocument,
        _mongo: change,
      };
    } else {
      return this.wrappedObject ? change[this.wrappedObject] : change;
    }
  }

  public async closeCursor() {
    if (this.cursor) {
      await this.cursor.close();
    }
  }

  private getRoot() {
    return this.wrappedObject ? `$${this.wrappedObject}` : "$$ROOT";
  }

  private setRoot(val: unknown) {
    if (this.wrappedObject) {
      this.pipeline.push({
        $addFields: { [this.wrappedObject]: val },
      });
    } else {
      this.pipeline.push({
        $replaceRoot: { newRoot: { [this.makeWrapped()]: val } },
      });
    }
  }

  private getField(field?: string) {
    if (this.wrappedObject) {
      return field ? `${this.wrappedObject}.${field}` : this.wrappedObject;
    }
    return field ?? "$ROOT";
  }

  private makeWrapped() {
    if (!this.wrappedObject) {
      this.wrappedObject = "_wrapped";
    }
    return this.wrappedObject;
  }

  protected stage_changes() {
    assert(this.isChangeStream, "Changes call must be last");
    return this;
  }

  protected stage_key(stage: QueryStage) {
    const fieldName = stage.args[0];
    const defaultValue = stage.args[1];

    this.resultType = "stream";

    if (this.isChangeStream) {
      this.pipeline.push({
        $addFields: {
          fullDocument: DefaultConstant(
            `$fullDocument.${fieldName}`,
            defaultValue,
          ),
          fullDocumentBeforeChange: DefaultConstant(
            `$fullDocumentBeforeChange.${fieldName}`,
            defaultValue,
          ),
        },
      });
    } else {
      const root = this.getRoot();
      this.setRoot(DefaultConstant(`${root}.${fieldName}`, defaultValue));
    }
    return this;
  }

  protected async stage_default(stage: QueryStage) {
    const defaultValue = await DecodeValue(stage.args[0], this.context);
    if (this.isChangeStream) {
      this.pipeline.push({
        $addFields: {
          fullDocument: DefaultConstant(`$fullDocument`, defaultValue),
          fullDocumentBeforeChange: DefaultConstant(
            `$fullDocumentBeforeChange`,
            defaultValue,
          ),
        },
      });
    } else if (this.wrappedObject) {
      this.setRoot(DefaultConstant(`$${this.wrappedObject}`, defaultValue));
    }
    return this;
  }

  private flushPendingUnset() {
    if (this.pendingUnset.length > 0) {
      const fields = this.wrappedObject
        ? this.pendingUnset.map((f) => `${this.wrappedObject}.${f}`)
        : this.pendingUnset;
      this.pipeline.push({ $unset: fields });
      this.pendingUnset = [];
    }
  }

  protected async stage_map(stage: QueryStage) {
    const runmap = (root: string) =>
      DecodeFunction(stage.args[0], this.context, [root]);

    if (this.isChangeStream) {
      this.pipeline.push({
        $addFields: {
          fullDocument: await runmap("$fullDocument"),
          fullDocumentBeforeChange: await runmap("$fullDocumentBeforeChange"),
        },
      });
    } else {
      const root = this.getRoot();
      this.setRoot(await runmap(root));
      this.flushPendingUnset();
    }
    return this;
  }

  protected async stage_filter(stage: QueryStage) {
    const filterRoot = this.isChangeStream ? "$fullDocument" : this.getRoot();
    const filterResult = await DecodeFunction(stage.args[0], this.context, [
      filterRoot,
    ]);
    this.pipeline.push({
      $match: { $expr: filterResult },
    });
    return this;
  }

  protected stage_pluck(stage: QueryStage) {
    const pluckObject: any = {};
    for (const field of stage.args[0]) {
      pluckObject[this.getField(field)] = 1;
    }
    // TODO: inCompoundObject (group with multiple branches): $replaceRoot $mergeObjects $$ROOT
    if (this.isChangeStream) {
      this.pipeline.push({
        $project: {
          _id: 1,
          operationType: 1,
          fullDocument: pluckObject,
          fullDocumentBeforeChange: pluckObject,
        },
      });
    } else {
      this.pipeline.push({
        $project: pluckObject,
      });
    }
    return this;
  }

  protected stage_without(stage: QueryStage) {
    const withoutObject: any = {};
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

  protected async stage_union(stage: QueryStage) {
    assert(!this.isChangeStream, "Union not supported in change streams");
    const rightStream = await SelectionQuery.decode(
      (stage.args[0] as Stream<any>).build(),
      this.context,
    );
    assert(rightStream instanceof AggregationPipeline);
    assert(rightStream.database === this.database);

    if (this.wrappedObject !== rightStream.wrappedObject) {
      if (!this.wrappedObject) {
        this.wrappedObject = rightStream.wrappedObject;
        this.setRoot("$$ROOT");
      } else {
        const root = rightStream.getRoot();
        rightStream.wrappedObject = this.wrappedObject;
        rightStream.setRoot(root);
      }
    }

    this.pipeline.push({
      $unionWith: {
        coll: rightStream.collection,
        pipeline: rightStream.pipeline,
      },
    });
  }

  protected async stage_join(stage: QueryStage) {
    assert(!this.isChangeStream, "Join not supported in change streams");
    const innerOnly = stage.options.innerOnly;
    const rightStream = await SelectionQuery.decode(
      (stage.args[0] as Stream<any>).build(),
      this.context,
    );
    const predicate = stage.args[1];
    const mapper = stage.args[2];
    assert(rightStream instanceof AggregationPipeline);
    assert(rightStream.database === this.database);
    const root = this.getRoot();
    const tmp = Temporary("join");
    this.pipeline.push(
      {
        $lookup: {
          from: rightStream.collection,
          let: { [tmp]: root },
          pipeline: [
            {
              $match: {
                $expr: await DecodeFunction(predicate, this.context, [
                  `$$${tmp}`,
                  "$$ROOT",
                ]),
              },
            },
            ...rightStream.pipeline,
          ],
          as: tmp,
        },
      },
      {
        $unwind: {
          path: `$${tmp}`,
          preserveNullAndEmptyArrays: !innerOnly,
        },
      },
    );
    this.setRoot(await DecodeFunction(mapper, this.context, [root, `$${tmp}`]));
    this.pipeline.push({
      // TODO?: collect obsolete fields and remove them all at the end
      $unset: [tmp],
    });
    return this;
  }

  protected async stage_lookup(stage: QueryStage) {
    assert(!this.isChangeStream, "Lookup not supported in change streams");
    const rightStream = await SelectionQuery.decode(
      (stage.args[0] as Stream<any>).build(),
      this.context,
    );
    assert(rightStream instanceof SelectionQuery);
    assert(rightStream.database === this.database);

    const localField = this.getField(stage.options.localKey);
    const foreignField = stage.options.otherKey;

    const tmp = Temporary("lookup");
    this.pipeline.push(
      {
        $lookup: {
          from: rightStream.collection,
          localField,
          foreignField,
          as: tmp,
          pipeline: rightStream.pipeline,
        },
      },
      {
        $addFields: {
          [localField]: {
            $cond: {
              if: { $isArray: `$${localField}` },
              // biome-ignore lint/suspicious/noThenProperty: MongoDB $cond requires a then branch.
              then: `$${tmp}`,
              else: { $arrayElemAt: [`$${tmp}`, 0] },
            },
          },
          [tmp]: "$$REMOVE",
        },
      },
    );
    return this;
  }

  protected async stage_group(stage: QueryStage) {
    const root = this.getRoot();
    const group = Temporary("group");
    const setupStage: Record<string, unknown> = {
      [group]: `$${this.getField(stage.options.index)}`,
    };
    this.pipeline.push({
      [this.wrappedObject ? "$addFields" : "$project"]: setupStage,
    });
    const groupStage: Record<string, any> = {
      _id: `$${group}`,
      [group]: { $first: `$${group}` },
    };
    const beforeGroup: any[] = [];
    const afterGroup: any[] = [];
    const pipelineInserter = async (stages: QueryStage[]) => {
      const tmp = Temporary("groupstream");
      const subquery = await AggregationPipeline.decode(
        stages,
        this.context,
        (subquery) => {
          subquery.wrappedObject = tmp;
          subquery.inCompoundObject = true;
        },
      );
      let isBeforeGroup = true;
      for (const stage of subquery.pipeline) {
        if ("$group" in stage) {
          if (stage.$group._id) {
            stage.$group._id = `$${tmp}.${stage.$group._id}`;
            // TODO: sub groups are way more complicated than anticipated
          } else {
            delete stage.$group._id;
            Object.assign(groupStage, stage.$group);
            isBeforeGroup = false;
          }
        } else if (isBeforeGroup) {
          beforeGroup.push(stage);
        } else {
          afterGroup.push(stage);
        }
      }
      if (isBeforeGroup) {
        // no group stage found, make stream into an array
        groupStage[tmp] = { $push: `$${tmp}` };
      }
      //TODO: interleave stages?
      setupStage[tmp] = root;
      return `$${tmp}`;
    };
    const result = await DecodeFunction(stage.args[0], this.context, [
      pipelineInserter,
      `$${group}`,
    ]); // TODO: support named indexes by referencing schema
    this.pipeline.push(...beforeGroup, { $group: groupStage }, ...afterGroup);
    this.setRoot(result);
    return this;
  }

  protected stage_orderBy(stage: QueryStage) {
    assert(!this.isChangeStream, "OrderBy not supported in change streams");
    const index = GetIndex(this.schemaId, this.collection, stage.options.index);
    const direction = stage.options.direction === "desc" ? -1 : 1;
    const indexFields = index.fields ?? [stage.options.index];
    const fields = indexFields.map((field) => [
      this.getField(field),
      direction,
    ]);
    this.pipeline.push({
      $sort: Object.fromEntries(fields),
    });
    return this;
  }

  protected async stage_slice(stage: QueryStage) {
    const offset = await DecodeValue(stage.args[0], this.context);
    if (typeof offset !== "number" || offset !== 0) {
      this.pipeline.push({ $skip: offset });
    }

    const count = await DecodeValue(stage.args[1], this.context);
    if (count) {
      this.pipeline.push({ $limit: count });
    }
  }

  protected async stage_nth(stage: QueryStage) {
    this.singleElement = true;
    return this.stage_slice({ stage: "slice", args: [stage.args[0], 1] });
  }

  // Reduction stages

  protected stage_count(stage: QueryStage) {
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

  protected stage_sum(stage: QueryStage) {
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

  protected stage_avg(stage: QueryStage) {
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

  protected stage_min(stage: QueryStage) {
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

  protected stage_max(stage: QueryStage) {
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

  protected stage_distinct(stage: QueryStage) {
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
        $unwind: { path: `$${wrapped}` },
      },
    );
  }
}
