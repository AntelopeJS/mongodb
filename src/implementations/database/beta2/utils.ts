import { StagedObject } from '@ajs.local/database/beta2/common';
import { generate as randomstring } from 'randomstring';
import assert from 'assert';

export function Temporary(name?: string) {
  return `temporary_${name ? name + '_' : ''}${randomstring({ capitalization: 'lowercase', length: 16 })}`;
}

export type QueryStage = StagedObject['stages'][number];

export type ArgumentProvider = (
  subQuery: QueryStage[],
) => Record<string, any> | string | Promise<Record<string, any> | string>;

export class DecodingContext {
  public args: Record<string, ArgumentProvider | string> = {};
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
    throw new Error('Subquery with no handler?');
  }

  public withRoot(parentRoot: string) {
    const newContext = new DecodingContext();
    for (const [id, arg] of Object.entries(this.args)) {
      if (typeof arg === 'string') {
        if (arg === '$$ROOT') {
          newContext.args[id] = parentRoot;
        } else if (arg.match(/^\$[^\$]/)) {
          newContext.args[id] = `${parentRoot}.${arg.substring(1)}`;
        } else {
          newContext.args[id] = arg; // variables? this will definitely break.
        }
      } else {
        // TODO: functions, { $first } objects, anything that is passed to an arg
      }
    }
    return newContext;
  }
}
