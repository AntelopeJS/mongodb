import assert from "node:assert";
import {
  Query,
  ValueProxy,
  type ValueProxyOrValue,
} from "@antelopejs/interface-database";
import {
  IsQueryStage,
  type IndexKeys,
  type QueryStage,
  type QueryStageName,
  type SelectionKey,
  type Value,
} from "@antelopejs/interface-database/common";

import type { Document } from "mongodb";

import {
  type ArgumentProvider,
  type DecodingContext,
  hasKey,
  isDocument,
  isObjectOrNull,
  isString,
  Temporary,
} from "./utils";

const FIELD_PATH_PREFIX = "$";
const DEFAULT_SEPARATOR = " ";
const MAX_DATE_MILLISECONDS = 8.64e15;

type ValueDecoder = typeof DecodeValue;

/** An operand as {@link DecodeValue} answers it: a literal, or an aggregation expression. */
type DecodedOperand =
  | string
  | number
  | boolean
  | null
  | undefined
  | Date
  | Document
  | DecodedOperand[];

function isPlainObject<Value>(value: Value): value is Value & object {
  return (
    value !== null &&
    typeof value === "object" &&
    Object.getPrototypeOf(value) === Object.prototype
  );
}

function isExpressionMarker(
  value: unknown,
): value is ValueProxy<unknown> | Query<unknown> {
  return value instanceof ValueProxy || value instanceof Query;
}

/**
 * This decorator should be used on stages where this.value is used multiple times.
 */
function CondenseValue(
  _cl: Expression,
  _key: string,
  desc: PropertyDescriptor,
) {
  const prev: unknown = desc.value;
  if (!(prev instanceof Function)) return;
  desc.value = function (this: Expression, ...args: unknown[]) {
    const valueBefore = this.value;
    if (isString(valueBefore)) {
      const result: unknown = prev.apply(this, args);
      return result;
    }
    const tmp = Temporary();
    this.value = `$$${tmp}`;
    const body: unknown = prev.apply(this, args);
    return {
      $let: {
        vars: { [tmp]: valueBefore },
        in: body,
      },
    };
  };
}

/**
 * A bound of `during`. On a date, a number is milliseconds since the UNIX epoch, as `new Date(n)`
 * reads it. Every other receiver or bound compares as sent.
 *
 * `$add` holds only a number `new Date` can read: the server evaluates it ahead of the `$cond` when
 * the bound is a constant, and refuses the whole pipeline on anything else.
 */
function DuringBound(receiverIsDate: Document, bound: DecodedOperand) {
  if (bound instanceof Date) {
    return bound;
  }
  const readable = {
    $and: [
      { $isNumber: [bound] },
      { $gte: [bound, -MAX_DATE_MILLISECONDS] },
      { $lte: [bound, MAX_DATE_MILLISECONDS] },
    ],
  };
  return {
    $cond: [
      { $and: [receiverIsDate, readable] },
      { $add: [new Date(0), { $cond: [readable, bound, 0] }] },
      bound,
    ],
  };
}

class Expression {
  protected currentStage?: QueryStage;

  public value: unknown;

  public constructor(public readonly context: DecodingContext) {}

  public static async decode(stages: QueryStage[], context: DecodingContext) {
    const expr = new Expression(context);
    await expr.addStages(stages);
    return expr.value;
  }

  public async addStages(stages: QueryStage[]) {
    for (const stage of stages) {
      const stageCallback: `stage_${string}` = `stage_${stage.stage}`;
      if (!hasKey(this, stageCallback)) {
        throw new Error(`Unimplemented stage: ${stage.stage}`);
      }
      const callback = this[stageCallback];
      if (isString(callback)) {
        this.value = {
          [callback]:
            stage.args.length === 0
              ? this.value
              : [
                  this.value,
                  ...(await Promise.all(
                    stage.args.map((arg) => DecodeValue(arg, this.context)),
                  )),
                ],
        };
      } else if (callback instanceof Function) {
        this.currentStage = stage;
        this.value = await callback.apply(
          this,
          await Promise.all(
            stage.args.map((arg) => DecodeValue(arg, this.context)),
          ),
        );
      }
    }
  }

  /** The stage whose handler runs, which `addStages` dispatched by this name. */
  private stageNamed<Name extends QueryStageName>(name: Name) {
    const stage = this.currentStage;
    assert(IsQueryStage(stage, name), `Not in a ${name} stage`);
    return stage;
  }

  async stage_arg(num: number) {
    return this.context.args[num];
  }
  stage_constant() {
    const constant: unknown = this.currentStage!.args[0];
    if (isPlainObject(constant)) {
      return DecodeLiteralValue(constant, this.context);
    }
    return DecodeValue(constant, this.context);
  }

  stage_default = "$ifNull";
  stage_and = "$and";
  stage_or = "$or";
  stage_not = "$not";
  stage_eq = "$eq";
  stage_ne = "$ne";
  stage_add(...operands: DecodedOperand[]) {
    if (operands.length === 0) {
      return { $add: [this.value] };
    }
    for (const operand of operands) {
      this.value = Number.isFinite(operand)
        ? { $add: [this.value, operand] }
        : this.addByReceiver(operand);
    }
    return this.value;
  }
  /**
   * `$add` reads numbers and dates only, and an expression's type is known only to the server, so
   * a string or a list goes to its own operator there. Each stage holds one operand: the server
   * evaluates two constants side by side ahead of the `$cond`, and refuses them on any operator
   * they do not suit.
   */
  @CondenseValue
  addByReceiver(operand: DecodedOperand) {
    return {
      $cond: [
        { $eq: [{ $type: this.value }, "string"] },
        { $concat: [this.value, operand] },
        {
          $cond: [
            { $isArray: [this.value] },
            { $concatArrays: [this.value, operand] },
            { $add: [this.value, operand] },
          ],
        },
      ],
    };
  }
  stage_sub = "$subtract";

  @CondenseValue
  stage_date_during(low: DecodedOperand, high: DecodedOperand) {
    const receiverIsDate = { $eq: [{ $type: this.value }, "date"] };
    return {
      $and: [
        { $gte: [this.value, DuringBound(receiverIsDate, low)] },
        { $lt: [this.value, DuringBound(receiverIsDate, high)] },
      ],
    };
  }
  stage_date_with_timezone(timezone?: DecodedOperand) {
    return {
      date: this.value,
      timezone,
    };
  }
  @CondenseValue
  stage_date_tod() {
    return {
      $add: [
        { $multiply: [{ $hour: this.value }, 3600] },
        { $multiply: [{ $minute: this.value }, 60] },
        { $second: this.value },
      ],
    };
  }
  stage_date_year = "$year";
  stage_date_month = "$month";
  stage_date_day = "$dayOfMonth";
  stage_date_dow = "$dayOfWeek";
  stage_date_doy = "$dayOfYear";
  stage_date_hours = "$hour";
  stage_date_minutes = "$minute";
  stage_date_seconds = "$second";
  stage_date_epoch() {
    return { $divide: [{ $toLong: this.value }, 1000] };
  }

  stage_mul = "$multiply";
  stage_div = "$divide";
  stage_mod = "$mod";
  stage_round = "$round";
  stage_ceil = "$ceil";
  stage_floor = "$floor";
  stage_bit_and = "$bitAnd";
  stage_bit_or = "$bitOr";
  stage_bit_xor = "$bitXor";
  stage_bit_not = "$bitNot";
  stage_bit_lshift(shift: DecodedOperand) {
    return {
      $multiply: [this.value, { $pow: [2, shift] }],
    };
  }
  stage_bit_rshift(shift: DecodedOperand) {
    return {
      $floor: {
        $divide: [this.value, { $pow: [2, shift] }],
      },
    };
  }

  stage_cmp_gt = "$gt";
  stage_cmp_ge = "$gte";
  stage_cmp_lt = "$lt";
  stage_cmp_le = "$lte";

  stage_str_split() {
    const { options } = this.stageNamed("str_split");
    const separator = options?.separator ?? DEFAULT_SEPARATOR;
    const split = { $split: [this.value, { $literal: separator }] };
    if (options?.maxSplits) {
      return { $slice: [split, options.maxSplits] };
    }
    return split;
  }
  stage_str_concat = "$concat";
  stage_str_upcase = "$toUpper";
  stage_str_downcase = "$toLower";
  stage_str_len = "$strLenCP";
  stage_str_match(regex: DecodedOperand) {
    return {
      $regexMatch: {
        input: this.value,
        regex: regex,
      },
    };
  }

  stage_arr_index = "$arrayElemAt";
  stage_arr_includes(val: DecodedOperand) {
    return { $in: [val, this.value] };
  }
  @CondenseValue
  stage_arr_slice(start: number, end?: number) {
    if (end) {
      return { $slice: [this.value, start, { $subtract: [end, start] }] };
    } else {
      return { $slice: [this.value, start, { $size: this.value }] };
    }
  }
  async stage_arr_map() {
    const [func] = this.stageNamed("arr_map").args;
    const tmp = Temporary();
    this.context.mapVarSources.set(`$$${tmp}`, this.value);
    const result = {
      $map: {
        input: this.value,
        as: tmp,
        in: await DecodeFunction(func, this.context, [`$$${tmp}`]),
      },
    };
    this.context.mapVarSources.delete(`$$${tmp}`);
    return result;
  }
  async stage_arr_filter() {
    const [func] = this.stageNamed("arr_filter").args;
    const tmp = Temporary();
    return {
      $filter: {
        input: this.value,
        as: tmp,
        cond: await DecodeFunction(func, this.context, [`$$${tmp}`]),
      },
    };
  }
  stage_arr_empty() {
    return { $eq: [0, { $size: this.value }] };
  }
  stage_arr_count = "$size";
  stage_arr_sum = "$sum";
  stage_arr_avg = "$avg";
  stage_arr_min = "$min";
  stage_arr_max = "$max";

  stage_obj_index(key: string, def: DecodedOperand) {
    const index =
      isString(this.value) &&
      isString(key) &&
      this.value.startsWith("$") &&
      !key.startsWith("$")
        ? `${this.value}.${key}`
        : { $getField: { field: key, input: this.value } };
    if (def) {
      return { $ifNull: [index, def] };
    }
    return index;
  }
  stage_obj_merge = "$mergeObjects";
  stage_obj_keys() {
    return {
      $map: {
        input: { $objectToArray: this.value },
        as: "temporary_entries",
        in: "$$temporary_entries.k",
      },
    };
  }
  stage_obj_values() {
    return {
      $map: {
        input: { $objectToArray: this.value },
        as: "temporary_entries",
        in: "$$temporary_entries.v",
      },
    };
  }
  stage_obj_has(fields: DecodedOperand) {
    const keys = {
      $map: {
        input: { $objectToArray: this.value },
        as: "temporary_entry",
        in: "$$temporary_entry.k",
      },
    };
    return { $setIsSubset: [fields, keys] };
  }
}

export async function DecodeValue(
  value: Value<unknown>,
  context: DecodingContext,
): Promise<unknown> {
  if (value instanceof ValueProxy) {
    return Expression.decode(value.build(), context);
  }

  if (value instanceof Query) {
    return context.decodeSubquery(value.build());
  }

  if (isDocument(value)) {
    if (Array.isArray(value)) {
      return Promise.all(value.map((val) => DecodeValue(val, context)));
    } else if (Object.getPrototypeOf(value) === Object.prototype) {
      return Object.fromEntries(
        await Promise.all(
          Object.entries(value).map(async ([key, val]) => [
            key,
            await DecodeValue(val, context),
          ]),
        ),
      );
    }
  }

  if (value === undefined) {
    return undefined;
  }

  const isFieldPath = isString(value) && value.startsWith(FIELD_PATH_PREFIX);
  const isObject = isObjectOrNull(value) && !(value instanceof Date);
  return isFieldPath || isObject ? { $literal: value } : value;
}

/**
 * Tells whether a query value is plain data, i.e. holds no value proxy or
 * subquery anywhere in its tree.
 */
export function IsConstantValue(value: unknown): boolean {
  if (isExpressionMarker(value)) {
    return false;
  }
  if (Array.isArray(value)) {
    return value.every(IsConstantValue);
  }
  if (isPlainObject(value)) {
    return Object.values(value).every(IsConstantValue);
  }
  return true;
}

/**
 * Tells whether `get` or `getAll` keys are plain data, as `IsConstantValue` reads them: no value
 * proxy or subquery in place of a key.
 */
export function IsConstantKeys(
  keys: IndexKeys | ValueProxyOrValue<string>,
): keys is SelectionKey | SelectionKey[] | SelectionKey[][] {
  return IsConstantValue(keys);
}

/**
 * Decodes a value meant to be written as is: plain data subtrees become
 * `$literal` expressions and objects mixing data and expressions are rebuilt
 * with `$arrayToObject`, so that no data key or value is read as a field path
 * or an operator.
 */
export async function DecodeLiteralValue(
  value: Value<unknown>,
  context: DecodingContext,
): Promise<unknown> {
  if (IsConstantValue(value)) {
    return { $literal: value };
  }
  if (Array.isArray(value)) {
    return Promise.all(value.map((val) => DecodeLiteralValue(val, context)));
  }
  if (!isPlainObject(value)) {
    return DecodeValue(value, context);
  }
  const entries = await Promise.all(
    Object.entries(value).map(async ([key, val]) => [
      { $literal: key },
      await DecodeLiteralValue(val, context),
    ]),
  );
  return { $arrayToObject: [entries] };
}

export async function DecodeFunction(
  func: QueryStage<"func">,
  context: DecodingContext,
  args: (string | ArgumentProvider)[],
  decode: ValueDecoder = DecodeValue,
) {
  const argNumbers = func.args[0];
  for (let i = 0; i < argNumbers.length; ++i) {
    assert(args[i], "Unexpected argument");
    context.args[argNumbers[i]] = args[i];
  }
  const val = await decode(func.args[1], context);
  for (let i = 0; i < argNumbers.length; ++i) {
    delete context.args[argNumbers[i]];
  }
  return val;
}
