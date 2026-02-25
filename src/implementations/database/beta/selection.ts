import { QueryStage } from '@ajs.local/database/beta/common';
import { AggregationPipeline } from './pipeline';
import { assert } from 'console';
import { DecodingContext } from './utils';
import { DecodeValue } from './query';
import { CreateInstance, DestroyInstance, IsValidInstance } from './schema';
import { GetCollection } from '../../../connection';
import { v4 as uuidv4 } from 'uuid';

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
      assert(stages[0]?.stage === 'schema' && schemaId, 'Unknown schema');
      if (stages[1].stage === 'createInstance') {
        const selection = new SelectionQuery(schemaId, instanceId ?? uuidv4(), '', false, null!);
        selection.resultType = 'createInstance';
        return selection;
      }
      if (stages[1].stage === 'destroyInstance') {
        const selection = new SelectionQuery(schemaId, instanceId, '', false, null!);
        selection.resultType = 'destroyInstance';
        return selection;
      }
      assert(instanceId, 'Missing instance');
      assert(IsValidInstance(schemaId, instanceId), 'Unknown instance');
      assert(stages[1].stage === 'instance' && stages[2]?.stage === 'table', 'Invalid request');
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

  private async createInstance() {
    await CreateInstance(this.schemaId, this.instanceId);
    return this.instanceId;
  }

  private async destroyInstance() {
    await DestroyInstance(this.schemaId, this.instanceId);
  }

  private async insert() {
    const collection = await GetCollection(this.database, this.collection);
    const documents = Array.isArray(this._newValue) ? this._newValue : [this._newValue];
    for (const document of documents) {
      document._id = document._id ?? uuidv4();
      // tenant_id = this.instanceId;
    }
    const res = await collection.insertMany(documents);
    return Object.values(res.insertedIds);
  }

  private async update() {
    const collection = await GetCollection(this.database, this.collection);
    const res = await collection.updateMany(this.getFilter(), { $set: this._newValue });
    return res.modifiedCount;
  }

  private async replace() {
    const collection = await GetCollection(this.database, this.collection);
    // add tenant_id
    // TODO: what should we do when there's more than one?
    const res = await collection.replaceOne(this.getFilter(), this._newValue);
    return res.modifiedCount;
  }

  private async delete() {
    const collection = await GetCollection(this.database, this.collection);
    const res = await collection.deleteMany(this.getFilter());
    return res.deletedCount;
  }

  public async run(): Promise<any> {
    switch (this.resultType) {
      case 'createInstance':
        return this.createInstance();
      case 'destroyInstance':
        return this.destroyInstance();
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

  protected async stage_getAll(stage: QueryStage) {
    assert(this.resultType === 'table');
    this.resultType = 'selection';
    const index = stage.options?.index ?? '_id';
    if (Array.isArray(stage.args[0])) {
      this.pipeline.push({
        $match: { $expr: { $in: ['$' + index, await DecodeValue(stage.args[0], this.context)] } },
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

  protected async stage_insert(stage: QueryStage) {
    assert(this.resultType === 'table');
    this.resultType = 'insert';
    this._newValue = stage.args[0];
  }

  protected async stage_update(stage: QueryStage) {
    assert(this.resultType === 'table' || this.resultType === 'selection');
    this.resultType = 'update';
    this._newValue = stage.args[0];
  }

  protected async stage_replace(stage: QueryStage) {
    assert(this.resultType === 'table' || this.resultType === 'selection');
    this.resultType = 'replace';
    this._newValue = stage.args[0];
  }

  protected stage_delete() {
    assert(this.resultType === 'table' || this.resultType === 'selection');
    this.resultType = 'delete';
  }
}
