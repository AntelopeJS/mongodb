import { Value } from '@ajs.local/database/beta2/common';
import assert from 'assert';
import { SelectionQuery } from './selection';
import { Query, ValueProxy } from '@ajs.local/database/beta2';
import { ArgumentProvider, DecodingContext, QueryStage } from './utils';
import { Expression } from './expression';
import { AggregationPipeline } from './pipeline';

export async function DecodeValue(value: Value<unknown>, context: DecodingContext): Promise<unknown> {
  if (value instanceof ValueProxy) {
    return Expression.decode(value.build(), context);
  }

  if (value instanceof Query) {
    return context.decodeSubquery(value.build());
  }

  if (value && typeof value === 'object') {
    if (Array.isArray(value)) {
      return Promise.all(value.map((val) => DecodeValue(val, context)));
    } else if (Object.getPrototypeOf(value) === Object.prototype) {
      return Object.fromEntries(
        await Promise.all(Object.entries(value).map(async ([key, val]) => [key, await DecodeValue(val, context)])),
      );
    }
  }

  if (value === undefined) {
    return undefined;
  }

  return typeof value === 'object' && !(value instanceof Date) ? { $literal: value } : value;
}

export async function DecodeFunction(func: QueryStage, context: DecodingContext, args: (string | ArgumentProvider)[]) {
  const argNumbers = func.args[0];
  for (let i = 0; i < argNumbers.length; ++i) {
    assert(args[i], 'Unexpected argument');
    context.args[argNumbers[i]] = args[i];
  }
  const val = await DecodeValue(func.args[1], context);
  for (let i = 0; i < argNumbers.length; ++i) {
    delete context.args[argNumbers[i]];
  }
  return val;
}

export async function RunQuery(stages: QueryStage[]) {
  const query = await SelectionQuery.decode(stages);
  return await query.run();
}

const openQueries: Record<number, AggregationPipeline> = {};
export async function ReadCursor(reqId: number, stages: QueryStage[]) {
  if (!(reqId in openQueries)) {
    const query = await SelectionQuery.decode(stages);
    openQueries[reqId] = query;
  }
  const next = await openQueries[reqId].readCursor();
  if (next === null) {
    delete openQueries[reqId];
  }

  return { done: next === null, value: next };
}

export async function CloseCursor(reqId: number) {
  if (reqId in openQueries) {
    await openQueries[reqId].closeCursor();
    delete openQueries[reqId];
  }
}
