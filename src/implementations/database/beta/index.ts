import { internal as internalRuntime } from '@ajs.local/database/beta/runtime';
import assert from 'assert';
import { GetCollection, GetDatabase, ListDatabases } from '../../../connection';
import { JoinType, MultiFieldSelector } from '@ajs.local/database/beta';
import { AggregationCursor, Document, ObjectId } from 'mongodb';
import { generate as randomstring } from 'randomstring';
import { v4 as uuidv4 } from 'uuid';
import { IdProvider } from '../../../index';

export let currentIdProvider: IdProvider;

export function setIdProvider(provider: IdProvider) {
  currentIdProvider = provider;
}

export interface TranslationContext {
  vars: { name: string; ref: { $literal: any } }[];
  groupStream?: number;
  args: Record<number, ExpressionValue>;
  autoCoerce?: boolean;
}

type JoinFunction = (
  left: AggregateQuery,
  right: AggregateQuery,
  condition: internalRuntime.QueryArg,
  context: TranslationContext,
) => [string, string];

function temporary() {
  return `temporary_${randomstring({ capitalization: 'lowercase', length: 16 })}`;
}

function root(agg: AggregateQuery, ...fields: string[]) {
  return [agg.single_value ? '$__singleval' : '$$ROOT', ...fields].join('.');
}

export type ExpressionValue =
  | string
  | number
  | boolean
  | undefined
  | { $literal: any }
  | {
      [key: string]: ExpressionValue;
    }
  | ExpressionValue[];

function guaranteeObject(val: ExpressionValue): { [key: string]: ExpressionValue } {
  if (typeof val === 'object') {
    const keys = Object.keys(val);
    if ((keys.length === 1 && keys[0].startsWith('$')) || Array.isArray(val)) {
      return { __singleval: val };
    }
    return val;
  } else {
    return { __singleval: val };
  }
}

interface AggregateQuery {
  database?: string;
  collection?: string;
  mode:
    | 'get'
    | 'insert'
    | 'update'
    | 'replace'
    | 'delete'
    | 'indexCreate'
    | 'indexDrop'
    | 'indexList'
    | 'tableCreate'
    | 'tableDrop'
    | 'tableList'
    | 'dbCreate'
    | 'dbDrop'
    | 'dbList';
  args?: any[];
  pipeline: Document[];
  single_value?: boolean;
  is_datum?: boolean;
}

/*
[
	{ id: 'db', args: [{type: 'value', value: 'MYDB'}] },
	{ id: 'table', args: [{type: 'value', value: 'MYTABLE'}] },
	{ id: 'map', args: [{type: 'func', args: [0], value: {
		type: 'query',
		queryType: 'stream',
		value: [
			{ 'id': 'arg', args: [{type: 'value', value: 0}] },

		]
	}}] },
]

[
	{"id":"db","args":[{"type":"value","value":"db"}]},
	{"id":"table","args":[{"type":"value","value":"t"}]},
	{"id":"map","args":[{"type":"func","args":[0],"value":{"type":"object","value":{
		"var1":{"type":"query","value":[
			{"id":"arg","args":[{"type":"value","value":0}]},
			{"id":"index","args":[{"type":"value","value":"t"}]}
		],"queryType":"valueproxy"},
		"var2":{"type":"query","value":[
			{"id":"arg","args":[{"type":"value","value":0}]},
			{"id":"index","args":[{"type":"value","value":"id"}]}
		],"queryType":"valueproxy"}
	}}}]}
]

db.collection('t').aggregate([
	{ $project: {
		_id: 0,
		var1: '$t',
		var2: '$id',
	} }
])

map:
 - set context.args[0] to '$$ROOT'
 - assert value.type == 'object'
 - foreach key in value.value:
   - recurse on expression stage (ex: index => "$$ROOT.<index>")
   - if query => add $lookup step using _temporary arg:
	 - set context.args[0] to '$$LEFT'
	 - subagg = translateQuery(query, ctx)
	 - set context.args[0] to '$$ROOT'
	 - agg.pipeline.push({ $lookup: {
		from: subagg.collection,
		let: { LEFT: '$$ROOT' },
		pipeline: subagg.pipeline,
		as: '_TEMP_LOOKUP_x',
	 } })
	 - use "$_TEMP_LOOKUP_x"



*/

function join_cross(left: AggregateQuery, right: AggregateQuery): [string, string] {
  const temp_var = temporary();
  left.pipeline.push(
    {
      $lookup: { from: right.collection, pipeline: right.pipeline, as: temp_var },
    },
    {
      $unwind: '$' + temp_var,
    },
  );
  return [root(left), temp_var];
}

function join_leftexcl(
  left: AggregateQuery,
  right: AggregateQuery,
  condition: internalRuntime.QueryArg,
  context: TranslationContext,
): [string, string] {
  const root_var = temporary();
  const temp_var = temporary();
  left.pipeline.push(
    {
      $lookup: {
        from: right.collection,
        let: { [root_var]: root(left) },
        pipeline: [
          ...right.pipeline,
          { $match: processFunction(condition, left, context, true, '$$' + root_var, '$$ROOT') },
        ],
        as: temp_var,
      },
    },
    {
      $match: {
        $expr: { $eq: [{ $size: '$' + temp_var }, 0] },
      },
    },
  );
  return [root(left), temp_var];
}

function join_inner(
  left: AggregateQuery,
  right: AggregateQuery,
  condition: internalRuntime.QueryArg,
  context: TranslationContext,
): [string, string] {
  const root_var = temporary();
  left.pipeline.push(
    {
      $lookup: {
        from: right.collection,
        let: { [root_var]: root(left) },
        pipeline: [
          ...right.pipeline,
          { $match: processFunction(condition, left, context, true, '$$' + root_var, '$$ROOT') },
        ],
        as: root_var,
      },
    },
    {
      $unwind: {
        path: '$' + root_var,
        preserveNullAndEmptyArrays: false,
      },
    },
  );
  return [root(left), root_var];
}

function join_left(
  left: AggregateQuery,
  right: AggregateQuery,
  condition: internalRuntime.QueryArg,
  context: TranslationContext,
): [string, string] {
  const root_var = temporary();
  const temp_var = temporary();
  left.pipeline.push(
    {
      $lookup: {
        from: right.collection,
        let: { [root_var]: root(left) },
        pipeline: [
          ...right.pipeline,
          { $match: processFunction(condition, left, context, true, '$$' + root_var, '$$ROOT') },
        ],
        as: temp_var,
      },
    },
    {
      $unwind: {
        path: '$' + temp_var,
        preserveNullAndEmptyArrays: true,
      },
    },
  );
  return [root(left), temp_var];
}

type MultifieldCallback<R = any> = (key: string, val: number | boolean) => R;

function multifieldRecurse(obj: Record<string, MultiFieldSelector>, path: string[], callback: MultifieldCallback) {
  const res: Record<string, any> = {};
  for (const [key, val] of Object.entries(obj)) {
    const keypath = [...path, key];
    switch (typeof val) {
      case 'number':
      case 'boolean':
        res[key] = callback(keypath.join('.'), val);
        break;
      case 'object':
        if (Array.isArray(val)) {
          const subres: Record<string, any> = {};
          for (const subkey of val) {
            const subkeypath = [...keypath, subkey];
            res[subkey] = callback(subkeypath.join('.'), true);
          }
          res[key] = subres;
        } else {
          res[key] = multifieldRecurse(val, keypath, callback);
        }
        break;
    }
  }
  return res;
}

type MultifieldResult<R = any> = {
  [field: string]: R | MultifieldResult<R>;
};
function multifield<R = any>(fields: internalRuntime.QueryArg[], callback: MultifieldCallback<R>): MultifieldResult<R> {
  const proj: Record<string, any> = {};
  for (const arg of fields) {
    switch (typeof arg.value) {
      case 'string':
        proj[arg.value] = callback(arg.value, true);
        break;
      case 'object':
        if (Array.isArray(arg.value)) {
          for (const key of arg.value) {
            proj[key] = callback(key, true);
          }
        } else {
          Object.assign(proj, multifieldRecurse(arg.value, [], callback));
        }
        break;
    }
  }
  return proj;
}

const aggregationTranslators: Record<
  string,
  (term: internalRuntime.QueryBuilderContext[number], res: AggregateQuery, context: TranslationContext) => void
> = {
  db: (term, res) => {
    res.database = term.args[0].value;
  },
  table: (term, res) => {
    res.collection = term.args[0].value;
  },
  index: (step, agg) => {
    agg.pipeline.push({
      $project: {
        __singleval: root(agg, step.args[0].value),
      },
    });
    agg.single_value = true;
  },
  default: (step, agg, context) => {
    agg.pipeline.push({
      $project: {
        __singleval: { $ifNull: [root(agg), processValue(step.args[1], agg, context)] },
      },
    });
    agg.single_value = true;
  },
  //do: (step, agg, context) => {}, // set = map below
  append: (step, agg, context) => {
    agg.pipeline.push({
      $project: {
        __singleval: {
          $concatArrays: ['$__singleval', step.args.map((val) => processValue(val, agg, context))],
        },
      },
    });
  },
  prepend: (step, agg, context) => {
    agg.pipeline.push({
      $project: {
        __singleval: {
          $concatArrays: [step.args.map((val) => processValue(val, agg, context)), '$__singleval'],
        },
      },
    });
  },
  value: () => {}, // TODO: switch to expression in processQuery()

  changes: () => {},
  join: (step, agg, context) => {
    // right, type, mapper, predicate?
    assert(step.args[0].type === 'query');
    const right = processQuery(step.args[0].value, context);

    const join_functions: Record<number, JoinFunction> = {
      [JoinType.Cross]: join_cross,
      [JoinType.LeftExcl]: join_leftexcl,
      [JoinType.Inner]: join_inner,
      [JoinType.Left]: join_left,
    };

    const join_function = join_functions[step.args[1].value];
    if (!join_function) {
      throw new Error('Unsupported join operation');
    }
    const [root_field_name, joined_field_name] = join_function(agg, right, step.args[3], context);
    const res = guaranteeObject(
      processFunction(step.args[2], agg, context, false, root_field_name, '$' + joined_field_name),
    );

    agg.pipeline.push({ $project: res });
    if ('__singleval' in res) {
      agg.single_value = true;
    }
    if (joined_field_name) {
      let remove: any = { [joined_field_name]: 0 };
      if (agg.single_value) {
        remove = { __singleval: remove };
      }
      agg.pipeline.push({ $project: remove });
    }
  },
  lookup: (step, agg, context) => {
    assert(step.args[0].type === 'query');
    const subagg = processQuery(step.args[0].value as internalRuntime.QueryBuilderContext, context);
    assert(agg.database === subagg.database);
    const from = subagg.collection;
    const localField = step.args[1].value;
    const foreignField = step.args[2].value;
    const tmp = temporary();
    agg.pipeline.push({
      $lookup: {
        from,
        localField,
        foreignField,
        as: tmp,
        pipeline: subagg.pipeline,
      },
    });
    agg.pipeline.push({
      $project: {
        [localField]: {
          $cond: {
            if: {
              $isArray: '$' + localField,
            },
            then: '$' + tmp,
            else: {
              $arrayElemAt: ['$' + tmp, 0],
            },
          },
        },
      },
    });
  },
  union: (step, agg, context) => {
    const subagg = processQuery(step.args[0].value as internalRuntime.QueryBuilderContext, context);
    agg.pipeline.push({
      $unionWith: {
        coll: subagg.collection,
        pipeline: subagg.pipeline,
      },
    });
  },
  map: (step, agg, context) => {
    const res = guaranteeObject(processFunction(step.args[0], agg, context, false, root(agg)));
    agg.pipeline.push({ $project: res });
    if ('__singleval' in res) {
      agg.single_value = true;
    }
  },
  withFields: () => {},
  hasFields: () => {},
  filter: (step, agg, context) => {
    agg.pipeline.push({
      $match: processFunction(step.args[0], agg, context, true, root(agg)),
    });
  },
  orderBy: (step, agg) => {
    agg.pipeline.push({
      $sort: {
        [step.args[0].value]: step.args[1]?.value === 'desc' ? -1 : 1,
      },
    });
  },
  group: (step, agg, context) => {
    agg.pipeline.push({
      $group: {
        _id: processExpression(root(agg), { id: 'index', args: [step.args[0]] }, agg, context),
        stream: { $push: '$$ROOT' },
      },
    });
    const res = guaranteeObject(processAccumulationFunction(step.args[1], agg, context, 0, '$stream', '$_id'));
    agg.pipeline.push({
      $project: res,
    });
    if ('__singleval' in res) {
      agg.single_value = true;
    }
  },
  count: (_step, agg) => {
    agg.pipeline.push({ $group: { _id: 1, __singleval: { $count: {} } } });
    agg.single_value = true;
    agg.is_datum = true;
  },
  sum: (step, agg) => {
    const field = step.args[0]?.value || '__singleval';
    agg.pipeline.push({ $group: { _id: 1, __singleval: { $sum: '$' + field } } });
    agg.single_value = true;
    agg.is_datum = true;
  },
  avg: (step, agg) => {
    const field = step.args[0]?.value || '__singleval';
    agg.pipeline.push({ $group: { _id: 1, __singleval: { $avg: '$' + field } } });
    agg.single_value = true;
    agg.is_datum = true;
  },
  min: (step, agg) => {
    const field = step.args[0]?.value || '__singleval';
    agg.pipeline.push({ $sort: { [field]: 1 } });
    agg.pipeline.push({ $limit: 1 });
    agg.single_value = true;
    agg.is_datum = true;
  },
  max: (step, agg) => {
    const field = step.args[0]?.value || '__singleval';
    agg.pipeline.push({ $sort: { [field]: -1 } });
    agg.pipeline.push({ $limit: 1 });
    agg.single_value = true;
    agg.is_datum = true;
  },
  distinct: (step, agg) => {
    const field = step.args[0]?.value || '__singleval';
    agg.pipeline.push({ $group: { _id: 1, __singleval: { $addToSet: '$' + field } } });
    agg.pipeline.push({ $unwind: { path: '$__singleval' } });
    agg.single_value = true;
  },
  pluck: (step, agg) => {
    const proj = multifield(step.args, (_, val) => (val ? 1 : 0));
    agg.pipeline.push({ $project: agg.single_value ? { __singleval: proj } : proj });
  },
  without: () => {}, // TODO
  slice: (step, agg, context) => {
    if (step.args[0]?.value !== 0) {
      agg.pipeline.push({ $skip: processValue(step.args[0], agg, context) });
    }
    if (step.args[1]) {
      agg.pipeline.push({ $limit: processValue(step.args[1], agg, context) });
    }
  },
  nth: (step, agg, context) => {
    agg.is_datum = true;
    if (step.args[0]?.value !== 0) {
      agg.pipeline.push({ $skip: processValue(step.args[0], agg, context) });
    }
    agg.pipeline.push({ $limit: 1 });
  },

  insert: (step, agg, context) => {
    agg.mode = 'insert';
    agg.args = step.args.map((arg) => processValue(arg, agg, context));
  },
  update: (step, agg, context) => {
    const updateAgg: AggregateQuery = {
      pipeline: [],
      single_value: false,
      is_datum: true,
      mode: 'get',
    };
    const setData = processFunction(step.args[0], updateAgg, context, false, '$$ROOT');
    if (setData && typeof setData === 'object' && !('$literal' in setData) && !Array.isArray(setData)) {
      if (setData._id) {
        if (agg.pipeline.length === 0) {
          agg.pipeline.push({
            $match: { _id: setData._id },
          });
        }
        delete setData._id;
      }
    }
    updateAgg.pipeline.push({
      $set: setData,
    });
    agg.mode = 'replace';
    agg.args = [updateAgg, processValue(step.args[1], agg, context)];
  },
  replace: (step, agg, context) => {
    const updateAgg: AggregateQuery = {
      pipeline: [],
      single_value: false,
      is_datum: true,
      mode: 'get',
    };
    updateAgg.pipeline.push({
      $replaceRoot: {
        newRoot: processFunction(step.args[0], updateAgg, context, false, '$$ROOT'),
      },
    });
    agg.mode = 'replace';
    agg.args = [updateAgg, processValue(step.args[1], agg, context)];
  },
  delete: (step, agg) => {
    agg.mode = 'delete';
    agg.args = step.args;
  },
  get: (step, agg, context) => {
    if (step.args[0].type === 'value') {
      let val = step.args[0].value;
      if (typeof val === 'string' && val.match(/^[0-9a-f]{24}$/)) {
        val = new ObjectId(val);
      }
      agg.pipeline.push({ $match: { _id: val } });
    } else {
      agg.pipeline.push({ $match: { $expr: { $eq: ['$_id', processValue(step.args[0], agg, context)] } } });
    }
    agg.is_datum = true;
  },
  getAll: (step, agg, context) => {
    agg.pipeline.push({
      $match: {
        $expr: {
          $eq: [
            // TODO: more than 1 value
            step.args[0]?.value
              ? processExpression('$$ROOT', { id: 'index', args: [step.args[0]] }, agg, context)
              : '$_id',
            processValue(step.args[1], agg, context),
          ],
        },
      },
    });
  },
  between: (step, agg, context) => {
    const field = processExpression('$$ROOT', { id: 'index', args: [step.args[0]] }, agg, context);
    const left = processValue(step.args[1], agg, context);
    const right = processValue(step.args[2], agg, context);
    agg.pipeline.push({
      $match: { $expr: { $and: [{ $gte: [field, left] }, { $lt: [field, right] }] } },
    });
  },

  indexCreate: (step, agg) => {
    agg.mode = 'indexCreate';
    agg.args = step.args.map((arg) => arg.value);
  },
  indexDrop: (step, agg) => {
    agg.mode = 'indexDrop';
    agg.args = [step.args[0].value];
  },
  indexList: (step, agg, context) => {
    agg.mode = 'indexList';
    agg.args = step.args.map((arg) => processValue(arg, agg, context));
  },

  tableCreate: (step, agg) => {
    agg.mode = 'tableCreate';
    // TODO: check if primary in args[1] ({ primary_key: "..." }) is not _id
    agg.args = [step.args[0].value];
  },
  tableDrop: (step, agg) => {
    agg.mode = 'tableDrop';
    agg.args = [step.args[0].value];
  },
  tableList: (step, agg, context) => {
    agg.mode = 'tableList';
    agg.args = step.args.map((arg) => processValue(arg, agg, context));
  },

  dbCreate: (step, agg) => {
    agg.mode = 'dbCreate';
    agg.args = [step.args[0].value];
  },
  dbDrop: (step, agg) => {
    agg.mode = 'dbDrop';
    agg.args = [step.args[0].value];
  },
  dbList: (step, agg, context) => {
    agg.mode = 'dbList';
    agg.args = step.args.map((arg) => processValue(arg, agg, context));
  },

  expr: (step, agg) => {
    agg.pipeline.push({ $documents: [{ __singleval: { $literal: step.args[0].value } }] });
    agg.single_value = true;
    agg.is_datum = true;
  },
};
aggregationTranslators['do'] = aggregationTranslators['map'];

const expressionTranslators: Record<
  string,
  | ((
      prev: ExpressionValue,
      step: internalRuntime.QueryBuilderContext[number],
      aggregation: AggregateQuery,
      context: TranslationContext,
    ) => ExpressionValue)
  | string
> = {
  arg: (_prev, step, _agg, context) => {
    return context.args[step.args[0].value];
  },
  expr: (_prev, step) => {
    return { $literal: step.args[0].value };
  },
  index: (prev, step, agg, context) => {
    return typeof prev === 'string' && typeof step.args[0].value === 'string'
      ? `${prev}.${step.args[0].value}`
      : { $getField: { field: processValue(step.args[0], agg, context), input: prev } };
  },
  default: '$ifNull',

  and: '$and',
  or: '$or',
  not: '$not',

  during: (prev, step, agg, context) =>
    ({
      $and: [
        { $gte: [prev, processValue(step.args[0], agg, context)] },
        { $lt: [prev, processValue(step.args[1], agg, context)] },
      ],
    }) as ExpressionValue,

  // TODO: composite date object { date: epochtime, timezone: string }
  inTimezone: () => ({}),
  timezone: () => ({}),
  timeOfDay: (prev) =>
    ({
      $add: [{ $mul: [{ $hour: prev }, 3600] }, { $mul: [{ $minute: prev }, 60] }, { $second: prev }],
    }) as ExpressionValue,
  year: '$year',
  month: '$month',
  day: '$dayOfMonth',
  dayOfWeek: '$dayOfWeek',
  dayOfYear: '$dayOfYear',
  hours: '$hour',
  minutes: '$minute',
  seconds: '$second',
  toEpochTime: () => ({}),

  add: '$add',
  sub: '$subtract',
  mul: '$multiply',
  div: '$divide',
  mod: '$mod',
  bitAnd: '$bitAnd',
  bitOr: '$bitOr',
  bitXor: '$bitXor',
  bitNot: '$bitNot',
  bitLShift: (prev, step, agg, context) => ({
    $multiply: [prev, { $pow: [2, processValue(step.args[0], agg, context)] }],
  }),
  bitRShift: (prev, step, agg, context) => ({
    $divide: [prev, { $pow: [2, processValue(step.args[0], agg, context)] }],
  }),
  round: '$round',
  ceil: '$ceil',
  floor: '$floor',

  eq: '$eq',
  ne: '$ne',

  gt: '$gt',
  ge: '$gte',
  lt: '$lt',
  le: '$lte',

  split: '$split',
  upcase: '$toUpper',
  downcase: '$toLower',
  count: (prev) => ({
    $cond: {
      if: { $isArray: prev },
      then: { $size: prev },
      else: { $strLenCP: prev },
    },
  }),
  match: (prev, step, agg, context) => ({
    $regexMatch: {
      input: prev,
      regex: processValue(step.args[0], agg, context),
    },
  }),

  includes: (prev, step, agg, context) => {
    const tmp = temporary();
    return {
      $ne: [
        0,
        {
          $size: {
            $filter: {
              input: prev,
              as: tmp,
              cond: { $eq: ['$$' + tmp, processValue(step.args[0], agg, context)] },
              limit: 1,
            },
          },
        },
      ],
    };
  },
  slice: (prev, step, agg, context) => {
    const start = processValue(step.args[0], agg, context);
    const end = step.args[1] ? processValue(step.args[1], agg, context) : { $size: prev };
    return { $slice: [prev, start, { $subtract: [end, start] }] };
  },
  map: (prev, step, agg, context) => {
    const tmp = temporary();
    return { $map: { input: prev, as: tmp, in: processFunction(step.args[0], agg, context, false, tmp) } };
  },
  filter: (prev, step, agg, context) => {
    const tmp = temporary();
    return { $filter: { input: prev, as: tmp, cond: processFunction(step.args[0], agg, context, false, tmp) } };
  },
  hasFields: () => ({}), // TODO
  isEmpty: (prev) => ({ $eq: [0, { $size: prev }] }),
  sum: '$sum',
  avg: (prev) => ({
    $divide: [
      { $sum: prev },
      { $size: prev }, // TODO: division by 0?
    ],
  }),
  min: (prev) => ({
    $arrayElemAt: [{ $minN: { n: 1, input: prev } }, 0],
  }),
  max: (prev) => ({
    $arrayElemAt: [{ $maxN: { n: 1, input: prev } }, 0],
  }),

  merge: (prev, step, agg, context) => ({ $mergeObjects: [prev, processValue(step.args[0], agg, context)] }),
  keys: (prev) => ({
    $map: {
      input: { $objectToArray: prev },
      as: 'temporary_entries',
      in: '$$temporary_entries.k',
    },
  }),
  values: (prev) => ({
    $map: {
      input: { $objectToArray: prev },
      as: 'temporary_entries',
      in: '$$temporary_entries.v',
    },
  }),
};

const accumulationTranslators: Record<
  string,
  | ((
      prev: ExpressionValue,
      step: internalRuntime.QueryBuilderContext[number],
      aggregation: AggregateQuery,
      context: TranslationContext,
    ) => ExpressionValue)
  | string
> = {
  arg: expressionTranslators.arg,
  index: expressionTranslators.index,
  default: expressionTranslators.default,

  //join: '',
  //union: '',

  map: expressionTranslators.map,

  //withFields: '',
  //hasFields: '',
  filter: expressionTranslators.filter,
  orderBy: (prev, step) => {
    return {
      $sortArray: {
        input: prev,
        sortBy: {
          [step.args[0].value]: step.args[1]?.value === 'desc' ? -1 : 1,
        },
      },
    };
  },
  count: '$size',
  sum: expressionTranslators.sum,
  avg: expressionTranslators.avg,
  min: expressionTranslators.min,
  max: expressionTranslators.max,
  distinct: (prev) => {
    return {
      $setIntersection: [prev],
    };
  },
  pluck: (prev, step) => {
    const tmp = temporary();
    return {
      $map: {
        input: prev,
        as: tmp,
        in: multifield(step.args, (key, val) => (val ? `$${tmp}.${key}` : undefined)),
      },
    };
  },
  //without: '',
  slice: '$slice',
  nth: '$arrayElemAt',
};

export function processExpression(
  prev: ExpressionValue,
  step: internalRuntime.QueryBuilderContext[number],
  aggregation: AggregateQuery,
  context: TranslationContext,
): ExpressionValue {
  const translator = expressionTranslators[step.id];
  if (typeof translator === 'string') {
    return {
      [translator]:
        step.args.length === 0 ? prev : [prev, ...step.args.map((val) => processValue(val, aggregation, context))],
    };
  } else if (typeof translator === 'function') {
    return translator(prev, step, aggregation, context);
  }
  return prev;
}

export function processAccumulatorExpression(
  prev: ExpressionValue,
  step: internalRuntime.QueryBuilderContext[number],
  aggregation: AggregateQuery,
  context: TranslationContext,
): ExpressionValue {
  const translator = accumulationTranslators[step.id];
  if (typeof translator === 'string') {
    return {
      [translator]:
        step.args.length === 0 ? prev : [prev, ...step.args.map((val) => processValue(val, aggregation, context))],
    };
  } else if (typeof translator === 'function') {
    return translator(prev, step, aggregation, context);
  }
  return prev;
}

export function processFunction(
  arg: internalRuntime.QueryArg,
  aggregation: AggregateQuery,
  context: TranslationContext,
  wrapExpr: boolean,
  ...parameters: ExpressionValue[]
): ExpressionValue {
  if (arg.type !== 'func') {
    return processValue(arg, aggregation, context);
  }
  for (let i = 0; i < arg.args.length && i < parameters.length; ++i) {
    context.args[arg.args[i]] = parameters[i];
  }
  const res = processValue(arg, aggregation, context);
  for (let i = 0; i < arg.args.length && i < parameters.length; ++i) {
    delete context.args[arg.args[i]];
  }
  return wrapExpr ? { $expr: res } : res;
}

export function processAccumulationFunction(
  arg: internalRuntime.QueryArg,
  aggregation: AggregateQuery,
  context: TranslationContext,
  streamIndex: number,
  ...parameters: ExpressionValue[]
): ExpressionValue {
  if (arg.type !== 'func') {
    return processValue(arg, aggregation, context);
  }
  for (let i = 0; i < arg.args.length && i < parameters.length; ++i) {
    context.args[arg.args[i]] = parameters[i];
  }
  context.groupStream = arg.args[streamIndex];
  const res = processValue(arg, aggregation, context);
  delete context.groupStream;
  for (let i = 0; i < arg.args.length && i < parameters.length; ++i) {
    delete context.args[arg.args[i]];
  }
  return res;
}

export function processValue(
  arg: internalRuntime.QueryArg,
  aggregation: AggregateQuery,
  context: TranslationContext,
): ExpressionValue {
  if (arg === undefined) {
    return undefined;
  }
  switch (arg.type) {
    case 'value':
      if (typeof arg.value === 'object' && arg.value && Object.getPrototypeOf(arg.value) === Object.prototype) {
        return { $literal: arg.value };
      }
      return arg.value;
    case 'query': {
      if (arg.value[0] && (arg.value[0].id === 'arg' || arg.value[0].id === 'expr')) {
        // First step is always 'arg' or 'expr' which sets prev to a real value.
        let prev: ExpressionValue = undefined;
        if (context.groupStream && arg.value[0].id === 'arg' && arg.value[0].args[0].value === context.groupStream) {
          for (const step of arg.value) {
            prev = processAccumulatorExpression(prev, step, aggregation, context);
          }
        } else {
          for (const step of arg.value) {
            prev = processExpression(prev, step, aggregation, context);
          }
        }
        return prev;
      }
      const old_root = parseInt(Object.entries(context.args).find(([, v]) => v == '$$ROOT')![0]);
      const tmp_root = temporary();
      context.args[old_root] = '$$' + tmp_root;
      const sub_aggregation = processQuery(arg.value, context);
      context.args[old_root] = '$$ROOT';
      const temp_var = temporary();
      aggregation.pipeline.push({
        $lookup: {
          from: sub_aggregation.collection,
          let: { [tmp_root]: '$$ROOT' },
          pipeline: sub_aggregation.pipeline,
          as: temp_var,
        },
      });
      let ret_var: ExpressionValue = '$' + temp_var;
      if (sub_aggregation.single_value) ret_var = ret_var + '.__singleval';
      if (sub_aggregation.is_datum) ret_var = { $arrayElemAt: [ret_var, 0] };
      return ret_var;
    }
    case 'func':
      return processValue(arg.value, aggregation, context);
    case 'array':
      return arg.value.map((val) => processValue(val, aggregation, context));
    case 'object':
      // TODO: deal with keys containing $ and .
      return Object.fromEntries(Object.entries(arg.value).map(([k, v]) => [k, processValue(v, aggregation, context)]));
    case 'var': {
      const container = { $literal: null };
      context.vars.push({ name: arg.value, ref: container });
      return container;
    }
  }
}

export function processQuery(query: internalRuntime.QueryBuilderContext, context: TranslationContext): AggregateQuery {
  const res: AggregateQuery = {
    pipeline: [],
    single_value: false,
    is_datum: false,
    mode: 'get',
  };
  for (const term of query) {
    const translator = aggregationTranslators[term.id];
    if (typeof translator === 'function') {
      translator(term, res, context);
    }
  }
  return res;
}

const modes: Record<string, (dbQuery: AggregateQuery) => Promise<any>> = {};

modes.get = async (dbQuery) => {
  const collection = await GetCollection(dbQuery.database!, dbQuery.collection!);
  const cursor = collection.aggregate(dbQuery.pipeline);
  let res = await cursor.toArray();
  if (dbQuery.single_value) res = res.map((val) => val.__singleval);
  if (dbQuery.is_datum) return res[0];
  return res;
};

modes.insert = async (dbQuery) => {
  const collection = await GetCollection(dbQuery.database!, dbQuery.collection!);
  const values = dbQuery.args?.[0];
  if (Array.isArray(values)) {
    for (const value of values) {
      if (!('_id' in value) && currentIdProvider == 'uuid') {
        value._id = uuidv4();
      }
    }
    const res = await collection.insertMany(values);
    return {
      inserted: res.insertedCount,
      generated_keys: res.insertedIds,
    };
  } else {
    if (!('_id' in values) && currentIdProvider == 'uuid') {
      values._id = uuidv4();
    }
    const res = await collection.insertOne(values);
    return {
      inserted: 1,
      generated_keys: [res.insertedId],
    };
  }
};

function FilterFromQuery(dbQuery: AggregateQuery) {
  let filters = [];
  for (let i = 0; i < dbQuery.pipeline.length; ++i) {
    if ('$match' in dbQuery.pipeline[i]) {
      if ('$expr' in dbQuery.pipeline[i].$match) {
        filters.push(dbQuery.pipeline[i].$match.$expr);
      } else {
        return dbQuery.pipeline[i].$match; // get()
      }
    }
  }
  return filters.length > 0 ? { $expr: filters.length === 1 ? filters[0] : { $and: filters } } : {};
}

modes.replace = async (dbQuery) => {
  const collection = await GetCollection(dbQuery.database!, dbQuery.collection!);

  return await collection.updateMany(FilterFromQuery(dbQuery), dbQuery.args![0].pipeline[0]);
};

modes.delete = async (dbQuery) => {
  const collection = await GetCollection(dbQuery.database!, dbQuery.collection!);

  let filter: ExpressionValue | undefined = undefined;
  if (dbQuery.pipeline.length === 1 && '$match' in dbQuery.pipeline[0]) {
    filter = dbQuery.pipeline[0].$match;
  }
  return await collection.deleteMany(filter as any); // TODO: getAll().delete()
};

modes.indexCreate = async (dbQuery) => {
  const collection = await GetCollection(dbQuery.database!, dbQuery.collection!);

  if (dbQuery.args?.length === 1) {
    return await collection.createIndex(dbQuery.args[0]);
  } else {
    return await collection.createIndex(dbQuery.args!.slice(1), { name: dbQuery.args![0] });
  }
};

modes.indexDrop = async (dbQuery) => {
  const collection = await GetCollection(dbQuery.database!, dbQuery.collection!);

  return await collection.dropIndex(dbQuery.args![0]);
};

modes.indexList = async (dbQuery) => {
  const collection = await GetCollection(dbQuery.database!, dbQuery.collection!);

  return (await collection.indexes()).map((doc) => doc.name);
};

modes.tableCreate = async (dbQuery) => {
  const database = await GetDatabase(dbQuery.database!);

  return await database.createCollection(dbQuery.args![0]);
};

modes.tableDrop = async (dbQuery) => {
  const database = await GetDatabase(dbQuery.database!);

  return await database.dropCollection(dbQuery.args![0]);
};

modes.tableList = async (dbQuery) => {
  const database = await GetDatabase(dbQuery.database!);

  return (await database.collections()).map((collection) => collection.collectionName);
};

modes.dbCreate = async () => {
  // implicit
};

modes.dbDrop = async (dbQuery) => {
  const database = await GetDatabase(dbQuery.database!);

  return await database.dropDatabase();
};

modes.dbList = async () => {
  const databases = await ListDatabases();
  return databases.map((doc) => doc.name);
};

export namespace internal {
  export async function runQuery(query: internalRuntime.QueryBuilderContext) {
    // TODO: make sure return values follow the typing
    const dbQuery = processQuery(query, {
      vars: [],
      args: {},
    });
    console.log('runQuery', JSON.stringify(dbQuery));
    if (dbQuery.mode in modes) {
      return await modes[dbQuery.mode](dbQuery);
    }
  }

  const openedCursors = new Map<number, [AggregationCursor, AsyncIterableIterator<any>, boolean]>();

  export async function readCursor(reqId: number, query: internalRuntime.QueryBuilderContext) {
    if (!openedCursors.has(reqId)) {
      const dbQuery = processQuery(query, {
        vars: [],
        args: {},
      });
      console.log('runQuery', JSON.stringify(dbQuery));
      const collection = await GetCollection(dbQuery.database!, dbQuery.collection!);
      const cursor = collection.aggregate(dbQuery.pipeline);
      assert(cursor, 'Query returned no cursor.');
      openedCursors.set(reqId, [cursor, cursor[Symbol.asyncIterator](), !!dbQuery.single_value]);
      cursor.on('close', () => openedCursors.delete(reqId));
    }
    const [, iterator, single_val] = openedCursors.get(reqId)!;
    let res = await iterator.next();
    if (single_val && typeof res.value === 'object') {
      res.value = res.value.__singleval;
    }
    return res;
  }

  export async function closeCursor(reqId: number) {
    if (openedCursors.has(reqId)) {
      const [cursor] = openedCursors.get(reqId)!;
      openedCursors.delete(reqId);
      return await cursor.close();
    }
  }
}
