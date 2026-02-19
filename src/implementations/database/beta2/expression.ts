import { QueryStage } from '@ajs.local/database/beta2/common';
import { DecodingContext, Temporary } from './utils';
import { DecodeFunction, DecodeValue } from './query';

/**
 * This decorator should be used on stages where this.value is used multiple times.
 */
function CondenseValue(cl: Expression, key: string, desc: PropertyDescriptor) {
  const prev = desc.value!;
  desc.value = function (this: Expression, ...args: any[]) {
    const valueBefore = this.value;
    if (typeof valueBefore === 'string') {
      return prev.apply(this, args);
    }
    const tmp = Temporary();
    this.value = '$$' + tmp;
    return {
      $let: {
        vars: { [tmp]: valueBefore },
        in: prev.apply(this, args),
      },
    };
  };
}

export class Expression {
  protected options?: Record<string, any>;

  public constructor(
    public readonly context: DecodingContext,
    public value?: unknown,
  ) {}

  public static async decode(stages: QueryStage[], context: DecodingContext, startValue?: unknown) {
    const expr = new Expression(context, startValue);
    await expr.addStages(stages);
    return expr.value;
  }

  public async addStages(stages: QueryStage[]) {
    for (const stage of stages) {
      const stageCallback = `stage_${stage.stage}`;
      if (!(stageCallback in this)) {
        throw new Error('Unimplemented stage: ' + stage.stage);
      }
      const callback = this[stageCallback as keyof this];
      if (typeof callback === 'string') {
        this.value = {
          [callback]:
            stage.args.length === 0
              ? this.value
              : [this.value, ...(await Promise.all(stage.args.map((arg) => DecodeValue(arg, this.context))))],
        };
      } else if (callback instanceof Function) {
        this.options = stage.options;
        this.value = await callback.apply(
          this,
          await Promise.all(stage.args.map((arg) => DecodeValue(arg, this.context))),
        );
      }
    }
    delete this.options;
  }

  async stage_arg(num: number) {
    return this.context.args[num];
  }
  stage_constant(constant: unknown) {
    return DecodeValue(constant, this.context);
  }

  stage_default = '$ifNull';
  stage_and = '$and';
  stage_or = '$or';
  stage_not = '$not';
  stage_eq = '$eq';
  stage_ne = '$ne';
  stage_add = '$add';
  stage_sub = '$subtract';

  @CondenseValue
  stage_date_during(low: unknown, high: unknown) {
    return {
      $and: [{ $gte: [this.value, low] }, { $lt: [this.value, high] }],
    };
  }
  stage_date_with_timezone(timezone?: unknown) {
    return {
      date: this.value,
      timezone,
    };
  }
  @CondenseValue
  stage_date_tod() {
    return {
      $add: [{ $mul: [{ $hour: this.value }, 3600] }, { $mul: [{ $minute: this.value }, 60] }, { $second: this.value }],
    };
  }
  stage_date_year = '$year';
  stage_date_month = '$month';
  stage_date_day = '$dayOfMonth';
  stage_date_dow = '$dayOfWeek';
  stage_date_doy = '$dayOfYear';
  stage_date_hours = '$hour';
  stage_date_minutes = '$minute';
  stage_date_seconds = '$second';
  stage_date_epoch = '$toLong';

  stage_mul = '$multiply';
  stage_div = '$divide';
  stage_mod = '$mod';
  stage_round = '$round';
  stage_ceil = '$ceil';
  stage_floor = '$floor';
  stage_bit_and = '$bitAnd'; // TODO: bitwise operations only work on Int32() and Long(), integrate with schema?
  stage_bit_or = '$bitOr';
  stage_bit_xor = '$bitXor';
  stage_bit_not = '$bitNot';
  stage_bit_lshift = '$bit_lshift'; // TODO: should we keep bitshifts? { $multiply: [value, { $pow: [2, arg] }], }
  stage_bit_rshift = '$bit_rshift';

  stage_cmp_gt = '$gt';
  stage_cmp_ge = '$gte';
  stage_cmp_lt = '$lt';
  stage_cmp_le = '$lte';

  stage_str_split() {
    const split = { $split: [this.value, this.options?.separator ?? ' '] };
    if (this.options?.maxSplits) {
      return { $slice: [split, this.options.maxSplits] };
    }
    return split;
  }
  stage_str_concat = '$add';
  stage_str_upcase = '$toUpper';
  stage_str_downcase = '$toLower';
  stage_str_len = '$strLenCP';
  stage_str_match(regex: unknown) {
    return {
      $regexMatch: {
        input: this.value,
        regex: regex,
      },
    };
  }

  stage_arr_index = '$arrayElemAt';
  stage_arr_includes(val: unknown) {
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
  stage_arr_map(func: QueryStage) {
    const tmp = Temporary();
    return {
      $map: {
        input: this.value,
        as: tmp,
        in: DecodeFunction(func, this.context, ['$$' + tmp]),
      },
    };
  }
  stage_arr_filter(func: QueryStage) {
    const tmp = Temporary();
    return {
      $filter: {
        input: this.value,
        as: tmp,
        cond: DecodeFunction(func, this.context, ['$$' + tmp]),
      },
    };
  }
  stage_arr_empty() {
    return { $eq: [0, { $size: this.value }] };
  }
  stage_arr_count = '$size';
  stage_arr_sum = '$sum';
  stage_arr_avg = '$avg';
  stage_arr_min = '$min';
  stage_arr_max = '$max';

  stage_obj_index(key: string, def: unknown) {
    const index =
      typeof this.value === 'string' && typeof key === 'string' && this.value.startsWith('$')
        ? `${this.value}.${key}`
        : { $getField: { field: key, input: this.value } };
    if (def) {
      return { $ifNull: [index, def] };
    }
    return index;
  }
  stage_obj_merge = '$mergeObjects';
  stage_obj_keys() {
    return {
      $map: {
        input: { $objectToArray: this.value },
        as: 'temporary_entries',
        in: '$$temporary_entries.k',
      },
    };
  }
  stage_obj_values() {
    return {
      $map: {
        input: { $objectToArray: this.value },
        as: 'temporary_entries',
        in: '$$temporary_entries.v',
      },
    };
  }
  //stage_obj_has = '$obj_has'; // TODO: generate big condition?
}
