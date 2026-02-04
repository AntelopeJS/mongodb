import { StagedObject } from '@ajs.local/database/beta2/common';
import { generate as randomstring } from 'randomstring';
import { AggregationPipeline } from './pipeline';
import assert from 'assert';

export function Temporary() {
  return `temporary_${randomstring({ capitalization: 'lowercase', length: 16 })}`;
}

export type QueryStage = StagedObject['stages'][number];

export type ArgumentProvider = (subQuery: QueryStage[]) => string | Promise<string>;

export class DecodingContext {
  public args: Record<number, ArgumentProvider | string> = {};
  public subquery?: ArgumentProvider; // TODO: implement in relevant pipeline stages

  public decodeSubquery(stages: QueryStage[]) {
    if (stages[0]?.stage === 'arg') {
      const num = stages[0].args[0];
      const subquery = this.args[num];
      assert(subquery, 'Unknown arg used');
      assert(typeof subquery !== 'string', 'No query arg for query?');
      return subquery(stages);
    }
    if (this.subquery) {
      return this.subquery(stages);
    }
    throw new Error("TODO: subquery");
  }
}
