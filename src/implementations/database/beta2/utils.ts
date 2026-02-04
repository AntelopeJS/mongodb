import { StagedObject } from '@ajs.local/database/beta2/common';
import { generate as randomstring } from 'randomstring';
import { AggregationPipeline } from './pipeline';

export function Temporary() {
  return `temporary_${randomstring({ capitalization: 'lowercase', length: 16 })}`;
}

export type ArgumentProvider = (subQuery: AggregationPipeline) => string | Promise<string>;

export class DecodingContext {
  public args: Record<number, ArgumentProvider | string> = {};
  //TODO: public subquery?: ArgumentProvider;
}

export type QueryStage = StagedObject['stages'][number];
