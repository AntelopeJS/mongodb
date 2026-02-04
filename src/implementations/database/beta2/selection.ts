import { QueryStage } from '@ajs.local/database/beta2/common';
import { AggregationPipeline } from './pipeline';
import { assert } from 'console';
import { DecodingContext } from './utils';
import { DecodeValue } from './query';

export class SelectionQuery extends AggregationPipeline {
  private _newValue: any;

  public constructor(
    public readonly schemaId: string,
    public readonly instanceId: string,
    public readonly tableName: string,
    isChangeStream: boolean,
    context: DecodingContext,
  ) {
    // database-level
    super(`${schemaId}-${instanceId}`, tableName, [], isChangeStream, context);

    // row-level
    // super(schemaId, tableName, [{ $match: { tenant_id: instanceId } }]); // TODO: use isChangeStream
    this.resultType = 'table';
  }

  public static async decode(stages: QueryStage[], context?: DecodingContext): Promise<AggregationPipeline> {
    if (stages[0]?.stage === 'schema') {
      const schemaId = stages[0]?.options?.id;
      const instanceId = stages[1]?.options?.id;
      // TODO: assert schema & instance exist
      assert(stages[0]?.stage === 'schema' && schemaId);
      assert(instanceId);
      if (stages[1].stage === 'createInstance') {
        const selection = new SelectionQuery(schemaId, instanceId, '', false, null!);
        selection.resultType = 'createInstance';
        return selection;
      }
      assert(stages[1].stage === 'instance' && stages[2]?.stage === 'table');
      const tableName = stages[2].options.id;
      const selection = new SelectionQuery(
        schemaId,
        instanceId,
        tableName,
        stages[stages.length - 1]?.stage === 'changes',
        context ?? new DecodingContext(),
      );
      await selection.addStages(stages.slice(3));
      return selection;
    }
    return AggregationPipeline.decode(stages, context);
  }

  public async run(): Promise<any> {
    switch (this.resultType) {
      case 'createInstance':
        return;
      case 'insert':
        return;
      case 'update':
        return;
      case 'replace':
        return;
      case 'delete':
        return;
    }
    return super.run();
  }

  protected stage_get(stage: QueryStage) {
    assert(this.resultType === 'table');
    this.resultType = 'selection';
    this.singleElement = true;
    this.pipeline.push({
      $match: { _id: stage.args[0] },
    });
  }

  protected stage_getAll(stage: QueryStage) {
    assert(this.resultType === 'table');
    this.resultType = 'selection';
    const index = stage.options?.index ?? '_id';
    if (Array.isArray(stage.args[0])) {
      this.pipeline.push({
        $match: { $expr: { $in: ['$' + index, DecodeValue(stage.args[0], this.context)] } },
      });
    } else {
      this.pipeline.push({
        $match: { [index]: stage.args[0] }, // TODO: multi value
      });
    }
  }

  protected stage_between(stage: QueryStage) {
    assert(this.resultType === 'table');
    this.resultType = 'selection';
    const indexVar = '$' + (stage.options?.index ?? '_id');
    const low = stage.args[0];
    const high = stage.args[1];
    this.pipeline.push({
      $match: { $expr: { $and: [{ $gte: [indexVar, low] }, { $lt: [indexVar, high] }] } },
    });
  }

  protected stage_insert(stage: QueryStage) {
    assert(this.resultType === 'table');
    this.resultType = 'insert';
    this._newValue = stage.args[0];
  }

  protected stage_update(stage: QueryStage) {
    assert(this.resultType === 'table' || this.resultType === 'selection');
    this.resultType = 'update';
    this._newValue = stage.args[0];
  }

  protected stage_replace(stage: QueryStage) {
    assert(this.resultType === 'table' || this.resultType === 'selection');
    this.resultType = 'replace';
    this._newValue = stage.args[0];
  }

  protected stage_delete() {
    assert(this.resultType === 'table' || this.resultType === 'selection');
    this.resultType = 'delete';
  }
}
