import { QueryStage } from '@ajs.local/database/beta2/common';
import { AggregationPipeline } from './pipeline';
import { assert } from 'console';
import { DecodingContext } from './utils';
import { DecodeValue } from './query';
import { CreateInstance, IsValidInstance } from './schema';
import { GetCollection } from '../../../connection';

export class SelectionQuery extends AggregationPipeline {
  private _newValue: any;

  public constructor(
    schemaId: string,
    public readonly instanceId: string,
    public readonly tableName: string,
    isChangeStream: boolean,
    context: DecodingContext,
  ) {
    // database-level
    super(schemaId, `${schemaId}-${instanceId}`, tableName, [], isChangeStream, context);

    // row-level
    // super(schemaId, tableName, [{ $match: { tenant_id: instanceId } }]); // TODO: use isChangeStream
    this.resultType = 'table';
  }

  public static async decode(stages: QueryStage[], context?: DecodingContext): Promise<AggregationPipeline> {
    if (stages[0]?.stage === 'schema') {
      const schemaId = stages[0]?.options?.id;
      const instanceId = stages[1]?.options?.id;
      assert(IsValidInstance(schemaId, instanceId));
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

  private getFilter() {
    const filters = [];
    const filterDoc = {};
    for (let i = 0; i < this.pipeline.length; ++i) {
      if ('$match' in this.pipeline[i]) {
        if ('$expr' in this.pipeline[i].$match) {
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
        filters.push({ $eq: ['$' + key, val] });
      }
      return { $expr: filters.length === 1 ? filters[0] : { $and: filters } };
    }
  }

  private createInstance() {
    return CreateInstance(this.schemaId, this.instanceId);
  }

  private async insert() {
    const collection = await GetCollection(this.database, this.collection);
    const documents = Array.isArray(this._newValue) ? this._newValue : [this._newValue];
    for (const document of documents) {
      document._id = document._id ?? '';
      // tenant_id = this.instanceId;
    }
    await collection.insertMany(documents);
  }

  private async update() {
    const collection = await GetCollection(this.database, this.collection);
    await collection.updateMany(this.getFilter(), { $set: this._newValue });
  }

  private async replace() {
    const collection = await GetCollection(this.database, this.collection);
    // add tenant_id
    await collection.updateMany(this.getFilter(), { $replaceRoot: { newRoot: this._newValue } });
  }

  private async delete() {
    const collection = await GetCollection(this.database, this.collection);
    await collection.deleteMany(this.getFilter());
  }

  public async run(): Promise<any> {
    switch (this.resultType) {
      case 'createInstance':
        return this.createInstance();
      case 'insert':
        return this.insert();
      case 'update':
        return this.update();
      case 'replace':
        return this.replace();
      case 'delete':
        return this.delete();
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
    this._newValue = DecodeValue(stage.args[0], this.context);
  }

  protected stage_update(stage: QueryStage) {
    assert(this.resultType === 'table' || this.resultType === 'selection');
    this.resultType = 'update';
    this._newValue = DecodeValue(stage.args[0], this.context);
  }

  protected stage_replace(stage: QueryStage) {
    assert(this.resultType === 'table' || this.resultType === 'selection');
    this.resultType = 'replace';
    this._newValue = DecodeValue(stage.args[0], this.context);
  }

  protected stage_delete() {
    assert(this.resultType === 'table' || this.resultType === 'selection');
    this.resultType = 'delete';
  }
}
