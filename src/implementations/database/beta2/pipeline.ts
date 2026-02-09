import { QueryStage } from '@ajs.local/database/beta2/common';
import { DecodeFunction, DecodeValue } from './query';
import assert from 'assert';
import { SelectionQuery } from './selection';
import { DecodingContext, Temporary } from './utils';
import { GetCollection } from '../../../connection';
import { GetIndex } from './schema';

function DefaultConstant(data: any, def: any) {
  if (def) {
    return { $ifNull: [data, { $literal: def }] };
  }
  return data;
}

export class AggregationPipeline {
  public resultType = 'stream';

  /**
   * This query will only return one document, ignore the other
   */
  public singleElement = false;

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
      pipeline.unshift({
        $changeStream: {
          fullDocument: 'whenAvailable',
          fullDocumentBeforeChange: 'whenAvailable',
        },
      });
    }
  }

  public static async decode(
    stages: QueryStage[],
    context?: DecodingContext,
    modifier?: (pipeline: AggregationPipeline) => any,
  ): Promise<AggregationPipeline> {
    if (stages[0]?.stage === 'arg') {
      assert(context, 'Arg query without context?');
      const query = new AggregationPipeline('', '$ARG', stages[0].args[0], [], false, context);
      if (modifier) {
        await modifier(query);
      }
      await query.addStages(stages.slice(1));
      return query;
    }
    throw new Error('Invalid query');
  }

  public async addStages(stages: QueryStage[]) {
    for (const stage of stages) {
      const stageCallback = `stage_${stage.stage}`;
      if (!(stageCallback in this)) {
        throw new Error('Unimplemented stage: ' + stage.stage);
      }
      const callback = this[stageCallback as keyof this];
      if (!(callback instanceof Function)) {
        continue;
      }
      await callback.apply(this, [stage]);
    }
  }

  public async run(): Promise<any> {
    const collection = await GetCollection(this.database, this.collection);
    let result = await collection.aggregate(this.pipeline, {}).toArray();
    if (this.wrappedObject) {
      result = result.map((element: any) => element[this.wrappedObject!]);
    }
    return this.singleElement ? result[0] : result;
  }

  public async runDebug(limit = 10): Promise<any[]> {
    const collection = await GetCollection(this.database, this.collection);
    const results = [];
    try {
      for (let i = 0; i <= this.pipeline.length; ++i) {
        results[i] = await collection.aggregate([...this.pipeline.slice(0, i), { $limit: limit }]).toArray();
      }
    } catch (e) {
      results.push(e);
    }
    return results;
  }

  private getRoot() {
    return this.wrappedObject ? '$' + this.wrappedObject : '$$ROOT';
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
      return field ? this.wrappedObject + '.' + field : this.wrappedObject;
    }
    return field ?? '$ROOT';
  }

  private makeWrapped() {
    if (!this.wrappedObject) {
      this.wrappedObject = '_wrapped';
    }
    return this.wrappedObject;
  }

  protected stage_changes() {
    assert(this.isChangeStream, 'Changes call must be last');
    return this;
  }

  protected stage_key(stage: QueryStage) {
    const fieldName = stage.args[0];
    const defaultValue = stage.args[1];

    this.resultType = 'stream';

    if (this.isChangeStream) {
      this.pipeline.push({
        $project: {
          fullDocument: DefaultConstant(`$fullDocument.${fieldName}`, defaultValue),
          fullDocumentBeforeChange: DefaultConstant(`$fullDocumentBeforeChange.${fieldName}`, defaultValue),
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
        $project: {
          fullDocument: DefaultConstant(`$fullDocument`, defaultValue),
          fullDocumentBeforeChange: DefaultConstant(`$fullDocumentBeforeChange`, defaultValue),
        },
      });
    } else if (this.wrappedObject) {
      this.setRoot(DefaultConstant('$' + this.wrappedObject, defaultValue));
    }
    return this;
  }

  protected async stage_map(stage: QueryStage) {
    const runmap = (root: string) => DecodeFunction(stage.args[0], this.context, [root]);

    if (this.isChangeStream) {
      this.pipeline.push({
        $project: {
          fullDocument: await runmap('$fullDocument'),
          fullDocumentBeforeChange: await runmap('$fullDocumentBeforeChange'),
        },
      });
    } else {
      const root = this.getRoot();
      this.setRoot(await runmap(root));
    }
    return this;
  }

  protected async stage_filter(stage: QueryStage) {
    const filterRoot = this.isChangeStream ? '$fullDocument' : this.getRoot();
    const filterResult = await DecodeFunction(stage.args[0], this.context, [filterRoot]);
    this.pipeline.push({
      $match: { $expr: filterResult },
    });
    return this;
  }

  protected pluck(stage: QueryStage) {
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

  protected async stage_join(stage: QueryStage) {
    assert(!this.isChangeStream, 'Join not supported in change streams');
    const innerOnly = stage.options.innerOnly;
    const rightStream = SelectionQuery.decode(stage.args[0]);
    const predicate = stage.args[1];
    const mapper = stage.args[2];
    assert(rightStream instanceof SelectionQuery);
    assert(rightStream.database === this.database);
    const root = this.getRoot();
    const tmp = Temporary('join');
    this.pipeline.push(
      {
        from: rightStream.collection,
        let: { [tmp]: root },
        pipeline: [
          {
            $match: { $expr: await DecodeFunction(predicate, this.context, ['$$' + tmp, '$$ROOT']) },
          },
          ...rightStream.pipeline,
        ],
        as: tmp,
      },
      {
        $unwind: {
          path: '$' + tmp,
          preserveNullAndEmptyArrays: !innerOnly,
        },
      },
    );
    this.setRoot(await DecodeFunction(mapper, this.context, [root, '$' + tmp]));
    this.pipeline.push({
      // TODO?: collect obsolete fields and remove them all at the end
      $unset: {
        [tmp]: 1,
      },
    });
    return this;
  }

  protected async stage_lookup(stage: QueryStage) {
    assert(!this.isChangeStream, 'Lookup not supported in change streams');
    const rightStream = SelectionQuery.decode(stage.args[0]);
    assert(rightStream instanceof SelectionQuery);
    assert(rightStream.database === this.database);

    const localField = this.getField(stage.options.localKey);
    const foreignField = stage.options.otherKey;

    const tmp = Temporary('lookup');
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
              if: { $isArray: '$' + localField },
              then: '$' + tmp,
              else: { $arrayElemAt: ['$' + tmp, 0] },
            },
          },
          [tmp]: '$$REMOVE',
        },
      },
    );
    return this;
  }

  protected async stage_group(stage: QueryStage) {
    const root = this.getRoot();
    const group = Temporary('group');
    const setupStage: Record<string, unknown> = { [group]: '$' + this.getField(stage.options.index) };
    this.pipeline.push({
      [this.wrappedObject ? '$addFields' : '$project']: setupStage,
    });
    const groupStage: Record<string, any> = {
      _id: '$' + group,
      [group]: { $first: '$' + group },
    };
    const beforeGroup: any[] = [];
    const afterGroup: any[] = [];
    const pipelineInserter = async (stages: QueryStage[]) => {
      const tmp = Temporary('groupstream');
      const subquery = await AggregationPipeline.decode(stages, this.context, (subquery) => {
        subquery.wrappedObject = tmp;
        subquery.inCompoundObject = true;
      });
      let isBeforeGroup = true;
      for (const stage of subquery.pipeline) {
        if ('$group' in stage) {
          if (stage.$group._id) {
            stage.$group._id = `$${tmp}.${stage.$group._id}`;
            // TODO: sub groups are way more complicated than anticipated
          } else {
            delete stage.$group._id;
            Object.assign(groupStage, stage.$group);
            isBeforeGroup = false;
          }
        } else if (isBeforeGroup) {
          beforeGroup.push(stage)
        } else {
          afterGroup.push(stage);
        }
      }
      if (isBeforeGroup) {
        // no group stage found, make stream into an array
        groupStage[tmp] = { $push: '$' + tmp };
      }
      //TODO: interleave stages?
      setupStage[tmp] = root;
      return '$' + tmp;
    };
    const result = await DecodeFunction(stage.args[0], this.context, [pipelineInserter, '$' + group]); // TODO: support named indexes by referencing schema
    this.pipeline.push(...beforeGroup, { $group: groupStage }, ...afterGroup);
    console.log(result, stage.args[0], group);
    this.setRoot(result);
    return this;
  }

  protected stage_orderBy(stage: QueryStage) {
    assert(!this.isChangeStream, 'OrderBy not supported in change streams');
    const index = GetIndex(this.schemaId, this.collection, stage.options.index);
    const direction = stage.options.direction === 'desc' ? -1 : 1;
    const fields = index.fields!.map((field) => [this.getField(field), direction]);
    this.pipeline.push({
      $sort: Object.fromEntries(fields),
    });
    return this;
  }

  protected async stage_slice(stage: QueryStage) {
    const offset = await DecodeValue(stage.args[0], this.context);
    if (typeof offset !== 'number' || offset !== 0) {
      this.pipeline.push({ $skip: offset });
    }

    const count = await DecodeValue(stage.args[1], this.context);
    if (count) {
      this.pipeline.push({ $limit: count });
    }
  }

  protected async stage_nth(stage: QueryStage) {
    this.singleElement = true;
    return this.stage_slice({ stage: 'slice', args: [stage.args[0], 1] });
  }

  // Reduction stages

  protected stage_count(stage: QueryStage) {
    if (stage.options.field) {
      const field = this.getField(stage.options.field);
      const wrapped = this.makeWrapped();
      this.pipeline.push(
        {
          $group: {
            _id: null,
            [wrapped]: { $addToSet: '$' + field },
          },
        },
        {
          $project: {
            [wrapped]: { $size: '$' + wrapped },
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
    assert(stage.options.field || this.wrappedObject);
    const field = this.getField(stage.options.field);
    this.pipeline.push({
      $group: {
        _id: null,
        [this.makeWrapped()]: { $sum: '$' + field },
      },
    });
    return this;
  }

  protected stage_avg(stage: QueryStage) {
    assert(stage.options.field || this.wrappedObject);
    const field = this.getField(stage.options.field);
    this.pipeline.push({
      $group: {
        _id: null,
        [this.makeWrapped()]: { $avg: '$' + field },
      },
    });
    return this;
  }

  protected stage_min(stage: QueryStage) {
    assert(stage.options.field || this.wrappedObject);
    const field = this.getField(stage.options.field);
    this.pipeline.push({
      $group: {
        _id: null,
        [this.makeWrapped()]: { $min: '$' + field },
      },
    });
    return this;
  }

  protected stage_max(stage: QueryStage) {
    assert(stage.options.field || this.wrappedObject);
    const field = this.getField(stage.options.field);
    this.pipeline.push({
      $group: {
        _id: null,
        [this.makeWrapped()]: { $max: '$' + field },
      },
    });
    return this;
  }

  protected stage_distinct(stage: QueryStage) {
    const field = this.getField(stage.options.field);
    const wrapped = this.makeWrapped();
    if (stage.options.field) {
      this.pipeline.push({
        $group: {
          _id: null,
          [wrapped]: { $addToSet: '$' + field },
        },
      });
    } else {
      this.pipeline.push(
        {
          $group: {
            _id: null,
            [wrapped]: { $addToSet: '$' + field },
          },
        },
        {
          $unwind: { path: '$' + wrapped },
        },
      );
    }
  }
}
