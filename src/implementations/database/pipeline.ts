import assert from "node:assert";
import type { Document } from "mongodb";
import type { Stream } from "@antelopejs/interface-database";
import type { QueryStage } from "@antelopejs/interface-database/common";

import { GetIndex } from "./schema";
// oxlint-disable-next-line import/no-cycle -- base class dispatches to its own subclass decoder
import { SelectionQuery } from "./selection";
import { ArrayLookup } from "./array-lookup";
import {
  type DecodingContext,
  DefaultConstant,
  DefaultExpression,
  hasKey,
  isString,
  Temporary,
} from "./utils";
import { DecodeFunction, DecodeLiteralValue, DecodeValue } from "./expression";
import { PipelineCore } from "./pipeline-core";

function buildRightStages(stage: QueryStage): QueryStage[] {
  return (stage.args[0] as Stream<any>).build();
}

/** The stages that rewrite the root of a pipeline, including those that decode another stream into it. */
export class AggregationPipeline extends PipelineCore {
  private pendingUnset: string[] = [];

  public static async decode(
    stages: QueryStage[],
    context?: DecodingContext,
    modifier?: (pipeline: AggregationPipeline) => any,
  ): Promise<AggregationPipeline> {
    if (stages[0]?.stage === "arg") {
      assert(context, "Arg query without context?");
      const argRef = stages[0].args[0];
      const query = new AggregationPipeline({
        schemaId: "",
        tableName: argRef,
        collection: argRef,
        initialFilter: [],
        isChangeStream: false,
        context,
      });
      if (modifier) {
        await modifier(query);
      }
      await query.addStages(stages.slice(1));
      return query;
    }
    throw new Error("Invalid query");
  }

  public async addStages(stages: QueryStage[]) {
    const oldSubquery = this.context.subquery;
    this.context.subquery = async (subQuery) => {
      const tmp = Temporary("join");
      const tmpRoot = Temporary("parentRoot");
      const rightStream = await SelectionQuery.decode(
        subQuery,
        this.context.withRoot(`$$${tmpRoot}`),
      );

      const getStage = subQuery.find(
        (s) => s.stage === "get" || s.stage === "getAll",
      );
      const fkExpr = getStage?.args?.[0]
        ? await DecodeValue(getStage.args[0], this.context)
        : null;
      const arraySource = isString(fkExpr)
        ? this.context.mapVarSources.get(fkExpr)
        : undefined;

      if (arraySource) {
        const plan = ArrayLookup(
          rightStream,
          getStage!,
          fkExpr,
          arraySource,
          tmp,
        );
        this.pipeline.push(plan.stage);
        this.pendingUnset.push(tmp);
        return plan.match;
      }

      this.pipeline.push({
        $lookup: {
          from: rightStream.collection,
          let: { [tmpRoot]: "$$ROOT" },
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
        const stageCallback: `stage_${string}` = `stage_${stage.stage}`;
        if (!hasKey(this, stageCallback)) {
          throw new Error(`Unimplemented stage: ${stage.stage}`);
        }
        const callback = this[stageCallback];
        if (!(callback instanceof Function)) {
          continue;
        }
        await callback.apply(this, [stage]);
      }
    } finally {
      this.context.subquery = oldSubquery;
    }
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

  protected stage_key(stage: QueryStage) {
    const fieldName = stage.args[0];
    const defaultValue: unknown = stage.args[1];

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
    const defaultValue: unknown =
      stage.args[0] && (await DecodeLiteralValue(stage.args[0], this.context));
    if (this.isChangeStream) {
      this.pipeline.push({
        $addFields: {
          fullDocument: DefaultExpression(`$fullDocument`, defaultValue),
          fullDocumentBeforeChange: DefaultExpression(
            `$fullDocumentBeforeChange`,
            defaultValue,
          ),
        },
      });
    } else if (this.wrappedObject) {
      this.setRoot(DefaultExpression(`$${this.wrappedObject}`, defaultValue));
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

  protected async stage_union(stage: QueryStage) {
    assert(!this.isChangeStream, "Union not supported in change streams");
    const rightStream = await SelectionQuery.decode(
      buildRightStages(stage),
      this.context,
    );
    assert(rightStream instanceof AggregationPipeline);

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
    const innerOnly: unknown = stage.options.innerOnly;
    const rightStream = await SelectionQuery.decode(
      buildRightStages(stage),
      this.context,
    );
    const predicate = stage.args[1];
    const mapper = stage.args[2];
    assert(rightStream instanceof AggregationPipeline);
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
      $unset: [tmp],
    });
    return this;
  }

  protected async stage_lookup(stage: QueryStage) {
    assert(!this.isChangeStream, "Lookup not supported in change streams");
    const rightStream = await SelectionQuery.decode(
      buildRightStages(stage),
      this.context,
    );
    assert(rightStream instanceof SelectionQuery);

    const localField = this.getField(stage.options.localKey);
    const foreignField: unknown = stage.options.otherKey;

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
              // oxlint-disable-next-line unicorn/no-thenable -- MongoDB $cond requires a then branch; this is a pipeline operator, not a promise.
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
    const indexId: string = stage.options.index;
    const index = GetIndex(this.schemaId, this.tableName, indexId);
    const indexFields = index.fields ?? [indexId];

    let groupKey: string | Record<string, string>;
    if (indexFields.length === 1) {
      groupKey = `$${group}`;
    } else {
      const groupKeyObj: Record<string, string> = {};
      for (const field of indexFields) {
        groupKeyObj[field] = `$${group}.${field}`;
      }
      groupKey = groupKeyObj;
    }

    const groupSetup =
      indexFields.length === 1
        ? `$${this.getField(indexFields[0])}`
        : Object.fromEntries(
            indexFields.map((field) => [field, `$${this.getField(field)}`]),
          );
    const setupStage = {
      [group]: groupSetup,
    };
    this.pipeline.push({
      [this.wrappedObject ? "$addFields" : "$project"]: setupStage,
    });
    const groupStage: Document = {
      _id: groupKey,
      [group]: { $first: `$${group}` },
    };
    const beforeGroup: Document[] = [];
    const afterGroup: Document[] = [];
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
            throw new Error(
              "Nested sub-groups with non-null _id are not supported",
            );
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
        groupStage[tmp] = { $push: `$${tmp}` };
      }
      setupStage[tmp] = root;
      return `$${tmp}`;
    };
    const result = await DecodeFunction(stage.args[0], this.context, [
      pipelineInserter,
      `$${group}`,
    ]);
    this.pipeline.push(...beforeGroup, { $group: groupStage }, ...afterGroup);
    this.setRoot(result);
    return this;
  }
}
