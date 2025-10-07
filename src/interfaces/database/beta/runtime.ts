import { InterfaceFunction } from '@ajs/core/beta';

/**
 * @internal
 */
export namespace internal {
  export type QueryArg =
    | { type: 'value'; value: any }
    | { type: 'query'; value: QueryBuilderContext; queryType: string }
    | { type: 'func'; value: QueryArg; args: number[] }
    | { type: 'array'; value: QueryArg[] }
    | { type: 'object'; value: Record<string, QueryArg> }
    | { type: 'var'; value: string };
  export type QueryBuilderContext = { id: string; args: QueryArg[]; opts?: Record<string, any> }[];

  export const runQuery = InterfaceFunction<(query: QueryBuilderContext) => any>();
  export const readCursor =
    InterfaceFunction<(reqId: number, query: QueryBuilderContext) => IteratorResult<any, void>>();
  export const closeCursor = InterfaceFunction<(reqId: number) => void>();
}

interface Callback {
  pos: number;
  args: string[];
}
interface Options {
  pos: number;
  mapper?: (opts: Record<string, any>) => any;
}
type ProxyFunctionDecl = [id: string, ret: string, ...args: (string | Options | Callback)[]];
type ProxyFunctionRaw = (context: internal.QueryBuilderContext, ...args: any[]) => any;
type UniqueProxyFunctionDecl =
  | [funcName: string, id: string, ret: string, ...args: (string | Options | Callback)[]]
  | [funcName: string, id: string, func: ProxyFunctionRaw];

interface ProxyFunction {
  (context: internal.QueryBuilderContext, ...args: any[]): any;
  args: string[];
  ret: string;
}

const proxySymbol = Symbol('databaseProxy');
function isProxy(val: any) {
  return (typeof val === 'object' || typeof val === 'function') && val && val[proxySymbol];
}

let nextReqId = 0;
class IterableCursor implements AsyncGenerator<any, void, unknown> {
  private reqId: number;
  private resolve?: (val: IteratorResult<any, void>) => void;
  private reject?: (err: any) => void;
  constructor(private context: internal.QueryBuilderContext) {
    this.reqId = nextReqId++;
  }
  next(): Promise<IteratorResult<any, void>> {
    return new Promise((resolve, reject) => {
      this.resolve = resolve;
      this.reject = reject;
      internal
        .readCursor(this.reqId, this.context)
        .then(resolve)
        .catch(reject)
        .then(() => {
          this.resolve = undefined;
          this.reject = undefined;
        });
    });
  }
  return(): Promise<IteratorResult<any, void>> {
    const res = { done: true, value: undefined };
    if (this.resolve) {
      this.resolve(res);
      this.resolve = undefined;
      this.reject = undefined;
    }
    return internal.closeCursor(this.reqId).then(() => res);
  }
  throw(e: any): Promise<IteratorResult<any, void>> {
    if (this.reject) {
      this.reject(e);
      this.resolve = undefined;
      this.reject = undefined;
    }
    return internal.closeCursor(this.reqId).then(() => ({ done: true, value: undefined }));
  }
  [Symbol.asyncIterator](): AsyncGenerator<any, void, unknown> {
    return this;
  }
}

/*
	each base level object represents a proxy type with the following properties:
		- _inherit: string name of other type to inherit properties from
		- _call: result of calling the proxy type, see below for details
		- <name>[$<#>]: result of calling property of name <name> (array):
		                $<#> denotes multiple definitions, evaluated in order
			- [0]: return type of function
			- [*]: argument(s): string name of type or object
				- needed to differentiate multiple properies of the same name
				- if object: argument is callback
					pos: argument position (0-indexed)
					args: array of string name of types
*/

const proxyFunctions: Record<string, Record<string | symbol, string | ProxyFunctionDecl | ProxyFunctionRaw>> = {
  query: {
    run: (context) => internal.runQuery(context),
    then: (context, callback) => internal.runQuery(context).then(callback),
    iterator: (context) => new IterableCursor(context),
    [Symbol.asyncIterator]: (context) => new IterableCursor(context),
    build: (context, q) => ({ type: 'query', value: context, queryType: q._type }),
  },
  datum: {
    _inherit: 'query',
    _call: ['index', 'datum'],
    //_index: ['index', 'datum'],
    default: ['default', 'datum', 'datum'],
    lookup: ['lookup', 'datum', 'table', 'string', 'string'],
    do: ['do', 'datum', { pos: 0, args: ['valueproxy'] }],
    append: ['append', 'datum', 'datum'],
    prepend: ['prepend', 'datum', 'datum'],
    pluck: ['pluck', 'datum'],
    value: ['value', 'valueproxy'],
  },
  stream: {
    _inherit: 'query',
    _call: ['index', 'stream'],
    //_index: ['index', 'stream'],
    changes: ['changes', 'feed', { pos: 0 }],
    default: ['default', 'stream', 'datum'],
    innerJoin: [
      'innerJoin',
      'stream',
      'stream',
      { pos: 1, args: ['valueproxy', 'valueproxy'] },
      { pos: 2, args: ['valueproxy', 'valueproxy'] },
    ],
    outerJoin: [
      'outerJoin',
      'stream',
      'stream',
      { pos: 1, args: ['valueproxy', 'valueproxy'] },
      { pos: 2, args: ['valueproxy', 'valueproxy'] },
    ],
    eqJoin: ['eqJoin', 'stream', 'table', { pos: 1, args: ['valueproxy', 'valueproxy'] }, 'string', 'string'],
    join: [
      'join',
      'stream',
      'stream',
      'number',
      { pos: 2, args: ['valueproxy', 'valueproxy'] },
      { pos: 3, args: ['valueproxy', 'valueproxy'] },
    ],
    lookup: ['lookup', 'stream', 'table', 'string', 'string'],
    union$1: ['union', 'stream', 'stream'],
    union$2: ['union', 'feed', 'feed'],
    map: ['map', 'stream', { pos: 0, args: ['valueproxy'] }],
    withFields: ['withFields', 'stream'],
    hasFields: ['hasFields', 'this'],
    filter: ['filter', 'this', { pos: 0, args: ['valueproxy'] }],
    orderBy: ['orderBy', 'this'],
    group: ['group', 'stream', { pos: 1, args: ['stream', 'valueproxy'] }],
    count: ['count', 'datum'],
    sum: ['sum', 'datum'],
    avg: ['avg', 'datum'],
    min: ['min', 'datum'],
    max: ['max', 'datum'],
    distinct$1: ['distinct', 'stream', 'string'],
    distinct$2: ['distinct', 'datum'],
    pluck: ['pluck', 'stream'],
    without: ['without', 'stream'],
    slice: ['slice', 'this', 'datum', 'datum'],
    nth: ['nth', 'datum', 'datum'],
  },
  feed: {
    _inherit: 'query',
    _call: ['index', 'feed'],
    //_index: ['index', 'feed'],
    map: ['map', 'feed', { pos: 0, args: ['valueproxy'] }],
    withFields: ['withFields', 'feed'],
    hasFields: ['hasFields', 'this'],
    filter: ['filter', 'this', { pos: 0, args: ['valueproxy'] }],
    pluck: ['pluck', 'feed'],
    without: ['without', 'feed'],
  },
  single_selection: {
    _inherit: 'datum',
    update$1: ['update', 'query', 'datum', { pos: 1 }],
    update$2: ['update', 'query', { pos: 0, args: ['valueproxy'] }, { pos: 1 }],
    replace$1: ['replace', 'query', 'datum', { pos: 1 }],
    replace$2: ['replace', 'query', { pos: 0, args: ['valueproxy'] }, { pos: 1 }],
    delete: ['delete', 'query', { pos: 0 }],
    changes: ['changes', 'feed', { pos: 0 }],
  },
  selection: {
    _inherit: 'stream',
    update$1: ['update', 'query', 'datum', { pos: 1 }],
    update$2: ['update', 'query', { pos: 0, args: ['valueproxy'] }, { pos: 1 }],
    replace$1: ['replace', 'query', 'datum', { pos: 1 }],
    replace$2: ['replace', 'query', { pos: 0, args: ['valueproxy'] }, { pos: 1 }],
    delete: ['delete', 'query', { pos: 0 }],
    nth: ['nth', 'single_selection'],
  },
  table: {
    _inherit: 'selection',
    indexCreate: ['indexCreate', 'query'],
    indexDrop: ['indexDrop', 'query'],
    indexList: ['indexList', 'query'],
    insert: ['insert', 'query', 'datum', { pos: 1 }],
    get: ['get', 'single_selection'],
    getAll: ['getAll', 'selection'],
    between: ['between', 'selection'],
  },
  database: {
    tableCreate: [
      'tableCreate',
      'query',
      { pos: 1, mapper: (opts) => (opts.primary ? { primary_key: opts.primary } : {}) },
    ],
    tableDrop: ['tableDrop', 'query'],
    tableList: ['tableList', 'query'],
    table: ['table', 'table'],
  },
  root: {
    Database: ['db', 'database'],
    CreateDatabase: ['dbCreate', 'query'],
    DeleteDatabase: ['dbDrop', 'query'],
    ListDatabases: ['dbList', 'query'],
    Var: (_, name) => ({
      [proxySymbol]: true,
      build: () => ({ type: 'var', value: name }),
    }),
    Expr: ['expr', 'valueproxy'],
  },
  valueproxy: {
    build: (context, q) => ({ type: 'query', value: context, queryType: q._type }),
    _call: ['index', 'valueproxy'],
    //_index: ['index', 'valueproxy'],
    and: ['and', 'valueproxy', 'valueproxy'],
    or: ['or', 'valueproxy', 'valueproxy'],
    not: ['not', 'valueproxy'],
    during: ['during', 'valueproxy', 'valueproxy', 'valueproxy'],
    inTimezone: ['inTimezone', 'valueproxy', 'valueproxy'],
    timezone: ['timezone', 'valueproxy'],
    timeOfDay: ['timeOfDay', 'valueproxy'],
    year: ['year', 'valueproxy'],
    month: ['month', 'valueproxy'],
    day: ['day', 'valueproxy'],
    dayOfWeek: ['dayOfWeek', 'valueproxy'],
    dayOfYear: ['dayOfYear', 'valueproxy'],
    hours: ['hours', 'valueproxy'],
    minutes: ['minutes', 'valueproxy'],
    seconds: ['seconds', 'valueproxy'],
    toEpochTime: ['toEpochTime', 'valueproxy'],
    add: ['add', 'valueproxy', 'valueproxy'],
    sub: ['sub', 'valueproxy', 'valueproxy'],
    mul: ['mul', 'valueproxy', 'valueproxy'],
    div: ['div', 'valueproxy', 'valueproxy'],
    mod: ['mod', 'valueproxy', 'valueproxy'],
    bitAnd: ['bitAnd', 'valueproxy', 'valueproxy'],
    bitOr: ['bitOr', 'valueproxy', 'valueproxy'],
    bitXor: ['bitXor', 'valueproxy', 'valueproxy'],
    bitNot: ['bitNot', 'valueproxy'],
    bitLShift: ['bitLShift', 'valueproxy', 'valueproxy'],
    bitRShift: ['bitRShift', 'valueproxy', 'valueproxy'],
    round: ['round', 'valueproxy'],
    ceil: ['ceil', 'valueproxy'],
    floor: ['floor', 'valueproxy'],
    eq: ['eq', 'valueproxy', 'valueproxy'],
    ne: ['ne', 'valueproxy', 'valueproxy'],
    gt: ['gt', 'valueproxy', 'valueproxy'],
    ge: ['ge', 'valueproxy', 'valueproxy'],
    lt: ['lt', 'valueproxy', 'valueproxy'],
    le: ['le', 'valueproxy', 'valueproxy'],
    split: ['split', 'valueproxy', 'valueproxy'],
    upcase: ['upcase', 'valueproxy'],
    downcase: ['downcase', 'valueproxy'],
    match: ['match', 'valueproxy', 'valueproxy'],
    includes: ['includes', 'valueproxy', 'valueproxy'],
    slice: ['slice', 'valueproxy', 'valueproxy', 'valueproxy'],
    map: ['map', 'valueproxy', { pos: 0, args: ['valueproxy'] }],
    filter: ['filter', 'valueproxy', { pos: 0, args: ['valueproxy'] }],
    hasFields: ['hasFields', 'valueproxy'],
    isEmpty: ['isEmpty', 'valueproxy'],
    count: ['count', 'valueproxy'],
    sum: ['sum', 'valueproxy'],
    avg: ['avg', 'valueproxy'],
    min: ['min', 'valueproxy'],
    max: ['max', 'valueproxy'],
    keys: ['keys', 'valueproxy'],
    values: ['values', 'valueproxy'],
    default: ['default', 'valueproxy', 'valueproxy'],
    merge: ['merge', 'valueproxy', 'valueproxy'],
  },
};

const serializable = new Set([
  'query',
  'datum',
  'stream',
  'feed',
  'single_selection',
  'selection',
  'table',
  'valueproxy',
]);
function createFunction(id: string, ret: string, ...argDefs: (string | Options | Callback)[]): ProxyFunction {
  const canSerialize: (boolean | undefined)[] = [];
  const callbacks: (Callback | undefined)[] = [];
  let optionsIndex: number | undefined;
  let optionsMapper: Options['mapper'] | undefined;
  // eslint-disable-next-line @typescript-eslint/no-for-in-array
  for (const i in argDefs) {
    if (serializable.has(<any>argDefs[i])) {
      canSerialize[i] = true;
    }
    if (typeof argDefs[i] === 'object') {
      if ('args' in argDefs[i]) {
        callbacks[argDefs[i].pos] = argDefs[i];
      } else {
        optionsIndex = argDefs[i].pos;
        optionsMapper = argDefs[i].mapper;
      }
    }
  }
  const f = function (context: internal.QueryBuilderContext, ...args: any[]) {
    let opts = optionsIndex !== undefined ? args[optionsIndex] : undefined;
    if (opts && optionsMapper) {
      opts = optionsMapper(opts);
    }
    return createProxyType(ret, [
      ...context,
      {
        id,
        args: args
          .filter((_, i) => i !== optionsIndex)
          .map((a, i) => {
            if (isProxy(a) && 'build' in a) {
              return a.build(a);
            }
            if (typeof a === 'function' && callbacks[i]) {
              return callValueProxy(a, callbacks[i].args);
            }
            return serializeAny(a);
          }),
        opts,
      },
    ]);
  };
  f.args = argDefs.map((a) => (typeof a === 'string' ? a : null));
  f.ret = ret;
  return f as ProxyFunction;
}

const typeRelations: Record<string, Set<string>> = {};
for (const [typeName, info] of Object.entries(proxyFunctions)) {
  const relatedTypes = new Set([typeName]);
  let parent = info;
  while (typeof parent._inherit === 'string') {
    if (relatedTypes.has(parent._inherit)) {
      break; // failsafe
    }
    relatedTypes.add(parent._inherit);
    parent = proxyFunctions[parent._inherit];
  }
  typeRelations[typeName] = relatedTypes;
}
function typeMatches(typeName: string | null, value: any): boolean {
  if (typeName === null || typeName === 'any' || typeof value === typeName) {
    return true;
  }
  if ((typeof value === 'object' || typeof value === 'function') && typeof value._type === 'string') {
    return typeRelations[value._type].has(typeName);
  }
  return false;
}

function createDiscriminator(funcs: ProxyFunction[]): ProxyFunction {
  const f = function (context: internal.QueryBuilderContext, ...args: any[]) {
    for (const func of funcs) {
      let match = true;
      for (let i = 0; i < func.args.length; ++i) {
        if (!typeMatches(func.args[i], args[i])) {
          match = false;
          break;
        }
      }
      if (match) {
        return func(context, ...args);
      }
    }
  };
  f.args = [] as string[];
  f.ret = 'any';
  return f as ProxyFunction;
}

function keys<T extends {}>(object: T): (keyof T)[] {
  return [Object.getOwnPropertySymbols(object), Object.getOwnPropertyNames(object)].flat() as (keyof T)[];
}

function values<T extends {}>(object: T): T[keyof T][] {
  return keys(object).map((key) => object[key]);
}

function entries<T extends {}>(object: T): [keyof T, T[keyof T]][] {
  return keys(object).map((key) => [key, object[key]]);
}

function createFunctionList(typeName: string): UniqueProxyFunctionDecl[] {
  const list = proxyFunctions[typeName];
  return [
    ...(list._inherit ? createFunctionList(<string>list._inherit) : []),
    ...entries(list)
      .filter(([_, v]) => typeof v !== 'string')
      .map(([k, v]) => <UniqueProxyFunctionDecl>(typeof v === 'function' ? [k, k, v] : [k, ...(<ProxyFunctionDecl>v)])),
  ];
}

const proxyFunctionsImpl: Record<string, Record<string, ProxyFunction | ProxyFunctionRaw>> = {};
for (const typeName of Object.keys(proxyFunctions)) {
  const funcs: Record<string, Record<string, ProxyFunction | ProxyFunctionRaw>> = {};
  for (const [funcID, id, ret, ...args] of createFunctionList(typeName)) {
    const funcName = typeof funcID === 'string' ? funcID.match(/^([^$]+)/)![1] : funcID;
    funcs[funcName] = funcs[funcName] ?? {};
    if (typeof ret === 'string') {
      funcs[funcName][funcID] = createFunction(id, ret === 'this' ? typeName : ret, ...args);
    } else {
      funcs[funcName][funcID] = ret;
    }
  }
  proxyFunctionsImpl[typeName] = {};
  for (const funcName of keys(funcs)) {
    const funcImpls = funcs[funcName];
    if (keys(funcImpls).length === 1) {
      proxyFunctionsImpl[typeName][funcName] = values(funcImpls)[0];
    } else {
      proxyFunctionsImpl[typeName][funcName] = createDiscriminator(
        Object.entries(funcImpls)
          .sort(([id1], [id2]) => (id1 < id2 ? -1 : 1))
          .map(([_, f]) => <ProxyFunction>f),
      );
    }
  }
}

function createProxyType(typeName: string, context: internal.QueryBuilderContext) {
  const funcImpls = entries(proxyFunctionsImpl[typeName]).reduce(
    (prev, [k, v]) => {
      prev[k] = (...args: any[]) => v(context, ...args);
      return prev;
    },
    {} as Record<string, (...args: any[]) => any>,
  );
  const p = new Proxy(Object.assign('_call' in funcImpls ? funcImpls._call : {}, funcImpls), {
    get: (_, prop) => {
      if (prop === proxySymbol) {
        return true;
      }
      if (prop in funcImpls) {
        return funcImpls[<string>prop];
      }
      if (prop === '_type') {
        return typeName;
      }
      if ('_index' in funcImpls) {
        return funcImpls._index(prop);
      }
    },
  });
  return p;
}

function serializeAny(value: any): internal.QueryArg {
  if (isProxy(value) && 'build' in value) {
    return value.build(value);
  }
  if (typeof value === 'object' && value && !(value instanceof Date)) {
    if (Array.isArray(value)) {
      return {
        type: 'array',
        value: value.map(serializeAny),
      };
    } else if (Object.getPrototypeOf(value) === Object.prototype) {
      return {
        type: 'object',
        value: Object.entries(value).reduce(
          (prev, [k, v]) => {
            prev[k] = serializeAny(v);
            return prev;
          },
          {} as Record<string, internal.QueryArg>,
        ),
      };
    }
  }
  return { type: 'value', value };
}

let nextArgId = 0;
function callValueProxy(f: (...args: any[]) => any, argTypes: string[]): internal.QueryArg {
  const argIds: number[] = [];
  const args = argTypes.map((typeName, i) => {
    const id = nextArgId++;
    argIds[i] = id;
    return createProxyType(typeName, [{ id: 'arg', args: [{ type: 'value', value: id }] }]);
  });
  const result = f(...args);
  return {
    type: 'func',
    args: argIds,
    value: serializeAny(result),
  };
}

Object.assign(exports, createProxyType('root', []));
