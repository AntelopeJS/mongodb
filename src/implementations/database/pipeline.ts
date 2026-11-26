import assert from "node:assert";
import type { Document } from "mongodb";
import { IsValueProxy } from "@antelopejs/interface-database";
import type { QueryStage } from "@antelopejs/interface-database/common";

import { GetIndex } from "./schema";
import { ArrayLookup } from "./array-lookup";
import { type GroupMapperField, SplitGroupFields } from "./group-field";
import {
  type ArgumentProvider,
  type DecodedOperand,
  type DecodingContext,
  DefaultConstant,
  DefaultExpression,
  hasKey,
  isCallable,
  isString,
  ReadField,
  Temporary,
} from "./utils";
import { DecodeFunction, DecodeLiteralValue, DecodeValue } from "./expression";
import { PipelineCore, type PipelineSource } from "./pipeline-core";

/** Decodes a subquery into the pipeline of the table it reads, or undefined when it reads none. */
export type TableReader = (
  stages: QueryStage[],
  context: DecodingContext,
) => Promise<AggregationPipeline | undefined>;

function buildRightStages(
  stage: QueryStage<"union" | "join" | "lookup">,
): QueryStage[] {
  return stage.args[0].build();
}

/** The stages that rewrite the root of a pipeline, including those that decode another stream into it. */
export class AggregationPipeline extends PipelineCore {
  private pendingUnset: string[] = [];
  private readonly readTable: TableReader;

  public constructor(source: PipelineSource, readTable: TableReader) {
    super(source);
    this.readTable = readTable;
  }

  /**
   * Decodes an argument query over the rows of this pipeline's table into the field `wrappedObject`
   * of a compound document, so its indexes resolve on this pipeline's table.
   */
  private async decodeArgument(
    stages: QueryStage[],
    context: DecodingContext,
    wrappedObject: string,
  ): Promise<AggregationPipeline> {
    assert(stages[0]?.stage === "arg", "Invalid query");
    const argRef = String(stages[0].args[0]);
    const query = new AggregationPipeline(
      {
        schemaId: this.schemaId,
        tableName: this.tableName,
        collection: argRef,
        initialFilter: [],
        isChangeStream: false,
        context,
      },
      this.readTable,
    );
    query.wrappedObject = wrappedObject;
    query.inCompoundObject = true;
    await query.addStages(stages.slice(1));
    return query;
  }

  public async addStages(stages: QueryStage[]) {
    const oldSubquery = this.context.subquery;
    this.context.subquery = async (subQuery, scope) => {
      const tmp = Temporary("join");
      const tmpRoot = Temporary("parentRoot");
      const rightStream = await this.readTable(
        subQuery,
        scope.withRoot(`$$${tmpRoot}`),
      );
      assert(rightStream, "Subquery must read a table");

      const getStage = subQuery.find(
        (s) => s.stage === "get" || s.stage === "getAll",
      );
      const fkExpr = getStage?.args?.[0]
        ? await DecodeValue(getStage.args[0], scope)
        : null;
      const arraySource = isString(fkExpr)
        ? scope.mapVarSources.get(fkExpr)
        : undefined;

      if (arraySource && getStage) {
        const plan = ArrayLookup(
          rightStream,
          getStage,
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
        if (!isCallable(callback)) {
          continue;
        }
        await callback.apply(this, [stage]);
      }
    } finally {
      this.context.subquery = oldSubquery;
    }
  }

  /**
   * Writes `val` as the row's value. A wrapped value is written through `$let`, so `$addFields`
   * replaces it instead of merging an object literal into the value before it.
   */
  private setRoot(val: DecodedOperand) {
    if (this.wrappedObject) {
      this.pipeline.push({
        $addFields: { [this.wrappedObject]: { $let: { vars: {}, in: val } } },
      });
    } else {
      this.pipeline.push({
        $replaceRoot: { newRoot: { [this.makeWrapped()]: val } },
      });
    }
  }

  protected async stage_key(stage: QueryStage<"key">) {
    const name: unknown = stage.args[0];
    const field = IsValueProxy(name)
      ? await DecodeValue(name, this.context)
      : undefined;
    const defaultValue = stage.args[1];
    const read = (root: string) =>
      DefaultConstant(
        IsValueProxy(name)
          ? { $getField: { field, input: root } }
          : ReadField(root, String(name)),
        defaultValue,
      );

    this.resultType = "stream";

    if (this.isChangeStream) {
      this.pipeline.push({
        $addFields: {
          fullDocument: read("$fullDocument"),
          fullDocumentBeforeChange: read("$fullDocumentBeforeChange"),
        },
      });
    } else {
      this.setRoot(read(this.getRoot()));
    }
    return this;
  }

  protected async stage_default(stage: QueryStage<"default">) {
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

  protected async stage_map(stage: QueryStage<"map">) {
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

  protected async stage_union(stage: QueryStage<"union">) {
    assert(!this.isChangeStream, "Union not supported in change streams");
    const rightStream = await this.readTable(
      buildRightStages(stage),
      this.context,
    );
    assert(rightStream, "Union must read a table");

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

  protected async stage_join(stage: QueryStage<"join">) {
    assert(!this.isChangeStream, "Join not supported in change streams");
    const { innerOnly } = stage.options;
    const rightStream = await this.readTable(
      buildRightStages(stage),
      this.context,
    );
    const [, predicate, mapper] = stage.args;
    assert(rightStream, "Join must read a table");
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

  protected async stage_lookup(stage: QueryStage<"lookup">) {
    assert(!this.isChangeStream, "Lookup not supported in change streams");
    const rightStream = await this.readTable(
      buildRightStages(stage),
      this.context,
    );
    assert(rightStream);

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
            $cond: [
              { $isArray: `$${localField}` },
              `$${tmp}`,
              { $arrayElemAt: [`$${tmp}`, 0] },
            ],
          },
          [tmp]: "$$REMOVE",
        },
      },
    );
    return this;
  }

  protected async stage_group(stage: QueryStage<"group">) {
    const root = this.getRoot();
    const group = Temporary("group");
    const indexId = stage.options.index;
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
    const fields: GroupMapperField[] = [];
    const pipelineInserter: ArgumentProvider = async (stages, scope) => {
      const tmp = Temporary("groupstream");
      const subquery = await this.decodeArgument(stages, scope, tmp);
      fields.push({
        field: tmp,
        pipeline: subquery.pipeline,
        singleElement: subquery.singleElement,
      });
      setupStage[tmp] = root;
      return `$${tmp}`;
    };
    const result = await DecodeFunction(stage.args[0], this.context, [
      pipelineInserter,
      `$${group}`,
    ]);
    const split = SplitGroupFields(fields);
    Object.assign(groupStage, split.accumulators);
    this.pipeline.push(
      ...split.beforeGroup,
      { $group: groupStage },
      ...split.afterGroup,
    );
    this.setRoot(result);
    return this;
  }
}
