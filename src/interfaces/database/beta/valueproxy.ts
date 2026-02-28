import { ExtractType, QueryStage, StagedObject } from './common';

export type ValueProxyOrValue<T> = ValueProxy<T> | T;

export type Is<Left, Right, R> = Left extends Right ? R : never;

export type ArrayValue<T> = T extends (infer V)[] ? V : never;

export type IsArray<Left, Right, R> = Right extends (infer A)[] ? Is<A, Left, R> : never;

export type OnlyObject<T> = T extends Record<any, any> ? T : never;

/**
 * Proxy to an actual value in the database
 *
 * Actions performed on this proxy are not executed right away but instead recorded for the actual query
 */
export class ValueProxy<T> extends StagedObject {
  //@internal
  public static arg<T = unknown>(id: number) {
    return new ValueProxy<T>(QueryStage('arg', undefined, id));
  }

  /**
   * Create a new proxy with a constant value
   *
   * @param value Value
   * @returns Proxy
   */
  public static constant<T = unknown>(value: unknown) {
    return new ValueProxy<T>(QueryStage('constant', undefined, value));
  }

  /**
   * Changes the type of this proxy.
   * This does not actually perform any conversion, it only changes the typescript type.
   *
   * @returns Same proxy with a different type
   */
  public cast<U>() {
    return this as unknown as ValueProxy<U>;
  }

  //#region Any

  /**
   * Returns the parameter if the proxy is null.
   *
   * @param value Value to use in case the proxy is null
   * @returns Non-null value.
   */
  public default<U>(value: ValueProxyOrValue<U>) {
    return this.stage(ValueProxy<Exclude<T, undefined | null> | U>, 'default', undefined, value);
  }

  /**
   * AND operator.
   *
   * @param other Operand B
   * @returns A && B
   */
  public and(value: unknown) {
    return this.stage(ValueProxy<boolean>, 'and', undefined, value);
  }

  /**
   * OR operator.
   *
   * @param other Operand B
   * @returns A || B
   */
  public or(value: unknown) {
    return this.stage(ValueProxy<boolean>, 'or', undefined, value);
  }

  /**
   * NOT operator.
   *
   * @returns !A
   */
  public not() {
    return this.stage(ValueProxy<boolean>, 'not');
  }

  /**
   * Equality operator.
   *
   * @param other Operand B
   * @returns A == B
   */
  public eq(value: unknown) {
    return this.stage(ValueProxy<boolean>, 'eq', undefined, value);
  }

  /**
   * Inequality operator.
   *
   * @param other Operand B
   * @returns A != B
   */
  public ne(value: unknown) {
    return this.stage(ValueProxy<boolean>, 'ne', undefined, value);
  }

  //#endregion

  //#region Date & number arithmethic

  /**
   * Addition operator.
   *
   * @param other Operand B
   * @returns New value
   */
  public add(this: Is<Date | number, T, this>, value: ValueProxyOrValue<number>) {
    return (<ValueProxy<unknown>>this).stage(ValueProxy<T>, 'add', undefined, value);
  }

  /**
   * Subtraction operator.
   *
   * @param other Operand B
   * @returns New value
   */
  public sub<U>(this: Is<Date | number, T, this>, value: ValueProxyOrValue<U>) {
    return (<ValueProxy<unknown>>this).stage(
      ValueProxy<
        Date extends U
          ? (Date extends T ? number : never) | (number extends U ? (T extends number ? number : Date) : never)
          : Extract<Date | number, T>
      >,
      'sub',
      undefined,
      value,
    );
  }

  //#endregion

  //#region Date

  /**
   * Determines whether or not the proxy is between the two bounds.
   *
   * @param left Left bound (inclusive)
   * @param right Right bound (exclusive)
   * @returns True if the date is within the bounds
   */
  public during(this: Is<Date, T, this>, left: ValueProxyOrValue<Date>, right: ValueProxyOrValue<Date>) {
    return this.stage(ValueProxy<boolean>, 'date_during', undefined, left, right);
  }

  //@internal
  private withTimezone(timezone?: ValueProxyOrValue<string>) {
    if (timezone) {
      return this.stage(ValueProxy<Date>, 'date_with_timezone', undefined, timezone);
    }
    return this.cast<Date>();
  }

  /**
   * Number of seconds since the start of the day.
   *
   * @returns Seconds
   */
  public timeofday(this: Is<Date, T, this>, timezone?: ValueProxyOrValue<string>) {
    return this.withTimezone(timezone).stage(ValueProxy<number>, 'date_tod');
  }

  /**
   * Year.
   *
   * @returns Year
   */
  public year(this: Is<Date, T, this>, timezone?: ValueProxyOrValue<string>) {
    return this.withTimezone(timezone).stage(ValueProxy<number>, 'date_year', undefined, timezone);
  }

  /**
   * Month.
   *
   * @returns Month
   */
  public month(this: Is<Date, T, this>, timezone?: ValueProxyOrValue<string>) {
    return this.withTimezone(timezone).stage(ValueProxy<number>, 'date_month', undefined, timezone);
  }

  /**
   * Day of the month.
   *
   * @returns Day of the month
   */
  public day(this: Is<Date, T, this>, timezone?: ValueProxyOrValue<string>) {
    return this.withTimezone(timezone).stage(ValueProxy<number>, 'date_day', undefined, timezone);
  }

  /**
   * Day of the week.
   *
   * @returns Day of the week
   */
  public dayofweek(this: Is<Date, T, this>, timezone?: ValueProxyOrValue<string>) {
    return this.withTimezone(timezone).stage(ValueProxy<number>, 'date_dow', undefined, timezone);
  }

  /**
   * Day of the year.
   *
   * @returns Day of the year
   */
  public dayofyear(this: Is<Date, T, this>, timezone?: ValueProxyOrValue<string>) {
    return this.withTimezone(timezone).stage(ValueProxy<number>, 'date_doy', undefined, timezone);
  }

  /**
   * Hour of the day.
   *
   * @returns Hours
   */
  public hours(this: Is<Date, T, this>, timezone?: ValueProxyOrValue<string>) {
    return this.withTimezone(timezone).stage(ValueProxy<number>, 'date_hours', undefined, timezone);
  }

  /**
   * Minutes.
   *
   * @returns Minutes
   */
  public minutes(this: Is<Date, T, this>, timezone?: ValueProxyOrValue<string>) {
    return this.withTimezone(timezone).stage(ValueProxy<number>, 'date_minutes', undefined, timezone);
  }

  /**
   * Seconds.
   *
   * @returns Seconds
   */
  public seconds(this: Is<Date, T, this>, timezone?: ValueProxyOrValue<string>) {
    return this.withTimezone(timezone).stage(ValueProxy<number>, 'date_seconds', undefined, timezone);
  }

  /**
   * Seconds since the UNIX epoch with millisecond precision.
   *
   * @returns Seconds
   */
  public epoch(this: Is<Date, T, this>) {
    return this.stage(ValueProxy<number>, 'date_epoch');
  }

  //#endregion

  //#region Number

  /**
   * Multiplication operator.
   *
   * @param other Operand B
   * @returns A * B
   */
  public mul(this: Is<number, T, this>, other: ValueProxyOrValue<number>) {
    return this.stage(ValueProxy<number>, 'mul', undefined, other);
  }

  /**
   * Division operator.
   *
   * @param other Operand B
   * @returns A / B
   */
  public div(this: Is<number, T, this>, other: ValueProxyOrValue<number>) {
    return this.stage(ValueProxy<number>, 'div', undefined, other);
  }

  /**
   * Modulo operator.
   *
   * @param other Operand B
   * @returns A % B
   */
  public mod(this: Is<number, T, this>, other: ValueProxyOrValue<number>) {
    return this.stage(ValueProxy<number>, 'mod', undefined, other);
  }

  /**
   * Round to the nearest integer.
   *
   * @returns Integer
   */
  public round(this: Is<number, T, this>) {
    return this.stage(ValueProxy<number>, 'round');
  }

  /**
   * Round to the higher integer.
   *
   * @returns Integer
   */
  public ceil(this: Is<number, T, this>) {
    return this.stage(ValueProxy<number>, 'ceil');
  }

  /**
   * Round to the lower integer.
   *
   * @returns Integer
   */
  public floor(this: Is<number, T, this>) {
    return this.stage(ValueProxy<number>, 'floor');
  }

  /**
   * Bitwise AND operator.
   *
   * @param other Operand B
   * @return A & B
   */
  public band(this: Is<number, T, this>, other: ValueProxyOrValue<number>) {
    return this.stage(ValueProxy<number>, 'bit_and', undefined, other);
  }

  /**
   * Bitwise OR operator.
   *
   * @param other Operand B
   * @return A | B
   */
  public bor(this: Is<number, T, this>, other: ValueProxyOrValue<number>) {
    return this.stage(ValueProxy<number>, 'bit_or', undefined, other);
  }

  /**
   * Bitwise XOR operator.
   *
   * @param other Operand B
   * @return A ^ B
   */
  public bxor(this: Is<number, T, this>, other: ValueProxyOrValue<number>) {
    return this.stage(ValueProxy<number>, 'bit_xor', undefined, other);
  }

  /**
   * Bitwise NOT operator.
   *
   * @return ~A
   */
  public bnot(this: Is<number, T, this>) {
    return this.stage(ValueProxy<number>, 'bit_not');
  }

  /**
   * Bitwise left shift operator.
   *
   * @param other Operand B
   * @returns A << B
   */
  public blshift(this: Is<number, T, this>, other: ValueProxyOrValue<number>) {
    return this.stage(ValueProxy<number>, 'bit_lshift', undefined, other);
  }

  /**
   * Bitwise right shift operator.
   *
   * @param other Operand B
   * @param preserveSign Preserve sign bit
   * @returns A >> B
   */
  public brshift(this: Is<number, T, this>, other: ValueProxyOrValue<number>, preserveSign?: boolean) {
    return this.stage(ValueProxy<number>, 'bit_rshift', { preserveSign }, other);
  }

  //#endregion

  //#region Comparison

  /**
   * Greater-than operator.
   *
   * @param other Operand B
   * @returns A > B
   */
  public gt(this: Is<Date | number | string, T, this>, other: ValueProxyOrValue<T>) {
    return (<ValueProxy<unknown>>this).stage(ValueProxy<boolean>, 'cmp_gt', undefined, other);
  }

  /**
   * Greater-or-equal operator.
   *
   * @param other Operand B
   * @returns A >= B
   */
  public ge(this: Is<Date | number | string, T, this>, other: ValueProxyOrValue<T>) {
    return (<ValueProxy<unknown>>this).stage(ValueProxy<boolean>, 'cmp_ge', undefined, other);
  }

  /**
   * Lesser-than operator.
   *
   * @param other Operand B
   * @returns A < B
   */
  public lt(this: Is<Date | number | string, T, this>, other: ValueProxyOrValue<T>) {
    return (<ValueProxy<unknown>>this).stage(ValueProxy<boolean>, 'cmp_lt', undefined, other);
  }

  /**
   * Lesser-or-equal operator.
   *
   * @param other Operand B
   * @returns A <= B
   */
  public le(this: Is<Date | number | string, T, this>, other: ValueProxyOrValue<T>) {
    return (<ValueProxy<unknown>>this).stage(ValueProxy<boolean>, 'cmp_le', undefined, other);
  }

  //#endregion

  //#region String

  /**
   * Splits the string using a separator.
   *
   * @param separator Separator string
   * @param maxSplits Maximum number of results
   * @returns Array of sub-strings
   */
  public split(this: Is<string, T, this>, separator?: string, maxSplits?: number) {
    return this.stage(ValueProxy<string[]>, 'str_split', { separator, maxSplits });
  }

  /**
   * Concatenate the string with another.
   *
   * @param other Second string
   * @returns Concatenated string
   */
  public concat(this: Is<string, T, this>, other: ValueProxyOrValue<string>) {
    return this.stage(ValueProxy<string>, 'str_concat', undefined, other);
  }

  /**
   * Converts the string to all upper case.
   *
   * @returns New string
   */
  public upcase(this: Is<string, T, this>) {
    return this.stage(ValueProxy<string>, 'str_upcase');
  }

  /**
   * Converts the string to all lower case.
   *
   * @returns New string
   */
  public downcase(this: Is<string, T, this>) {
    return this.stage(ValueProxy<string>, 'str_downcase');
  }

  /**
   * Gets the number of Unicode codepoints in the string.
   *
   * @returns Number of codepoints
   */
  public strlen(this: Is<string, T, this>) {
    return this.stage(ValueProxy<number>, 'str_len');
  }

  /**
   * Checks if the string matches a regex.
   *
   * @param regex Regex
   * @returns True if the string matched
   */
  public match(this: Is<string, T, this>, regex: ValueProxyOrValue<string>) {
    return this.stage(ValueProxy<boolean>, 'str_match', undefined, regex);
  }

  //#endregion

  //#region Array

  /**
   * Indexes the array with a given number position
   *
   * @param key Position in the array
   * @returns Element at the given position
   */
  public index(this: IsArray<unknown, T, this>, key: ValueProxyOrValue<number>) {
    return this.stage(ValueProxy<ArrayValue<T>>, 'arr_index', undefined, key);
  }

  /**
   * Tests if the arrays contains a value.
   *
   * @param val Value to search for.
   * @returns True if the value was found.
   */
  public includes(this: IsArray<unknown, T, this>, val: ValueProxyOrValue<ArrayValue<T>>) {
    return this.stage(ValueProxy<boolean>, 'arr_includes', undefined, val);
  }

  /**
   * Returns a slice of the array.
   *
   * @param start Start index (inclusive, 0-indexed)
   * @param end End index (exclusive)
   * @returns Sub-array
   */
  public slice(this: IsArray<unknown, T, this>, start: ValueProxyOrValue<number>, end?: ValueProxyOrValue<number>) {
    return this.stage(ValueProxy<Array<ArrayValue<T>>>, 'arr_slice', undefined, start, end);
  }

  /**
   * Maps the array values using a mapping function.
   *
   * @param mapper Mapping function
   * @returns New array
   */
  public map<U>(this: IsArray<unknown, T, this>, mapper: (val: ValueProxy<ArrayValue<T>>) => U) {
    return this.stage(
      ValueProxy<ExtractType<U>[]>,
      'arr_map',
      undefined,
      this.callfunc(mapper, ValueProxy<ArrayValue<T>>),
    );
  }

  /**
   * Filters the array using a predicate function.
   *
   * @param predicate Predicate function.
   * @returns Filtered array
   */
  public filter(
    this: IsArray<unknown, T, this>,
    predicate: (val: ValueProxy<ArrayValue<T>>) => ValueProxyOrValue<boolean>,
  ) {
    return this.stage(
      ValueProxy<Array<ArrayValue<T>>>,
      'arr_filter',
      undefined,
      this.callfunc(predicate, ValueProxy<ArrayValue<T>>),
    );
  }

  /**
   * Checks if the array is empty.
   *
   * @returns True if the array is empty
   */
  public isempty(this: IsArray<unknown, T, this>) {
    return this.stage(ValueProxy<boolean>, 'arr_empty');
  }

  /**
   * Gets the length of the array.
   *
   * @returns Length
   */
  public count(this: IsArray<unknown, T, this>) {
    return this.stage(ValueProxy<number>, 'arr_count');
  }

  /**
   * Gets the sum of a number array.
   *
   * @returns Sum
   */
  public sum(this: IsArray<number, T, this>) {
    return (<ValueProxy<unknown>>this).stage(ValueProxy<number>, 'arr_sum');
  }

  /**
   * Gets the average of a number array.
   *
   * @returns Average
   */
  public avg(this: IsArray<number, T, this>) {
    return (<ValueProxy<unknown>>this).stage(ValueProxy<number>, 'arr_avg');
  }

  /**
   * Gets the minimum of a number array.
   *
   * @returns Minimum
   */
  public min(this: IsArray<number, T, this>) {
    return (<ValueProxy<unknown>>this).stage(ValueProxy<number>, 'arr_min');
  }

  /**
   * Gets the maxmimum of a number array.
   *
   * @returns Maximum
   */
  public max(this: IsArray<number, T, this>) {
    return (<ValueProxy<unknown>>this).stage(ValueProxy<number>, 'arr_max');
  }

  //#endregion

  //#region Object

  /**
   * Indexes the object.
   *
   * TODO: Better name?
   *
   * @param key Field name
   * @param def Default value
   * @returns Value of the field
   */
  public key<TO = OnlyObject<T>, K extends keyof TO = keyof TO, U = undefined>(
    this: Is<T, Record<any, any>, this>,
    key: ValueProxyOrValue<K>,
    def?: ValueProxyOrValue<U>,
  ) {
    return this.stage(
      ValueProxy<U extends undefined ? TO[K] : Exclude<TO[K], undefined | null> | U>,
      'obj_index',
      undefined,
      key,
      def,
    );
  }

  /**
   * Merges the object with another.
   *
   * @param value Other object
   * @returns `{...A, ...B}`
   */
  public merge<U, TO = OnlyObject<T>>(this: Is<T, Record<any, any>, this>, other: ValueProxyOrValue<U>) {
    return this.stage(ValueProxy<Omit<TO, keyof U> & ExtractType<U>>, 'obj_merge', undefined, other);
  }

  /**
   * Gets the keys of the object as an array.
   *
   * @returns Array of keys
   */
  public keys<TO = OnlyObject<T>>(this: Is<T, Record<any, any>, this>) {
    return this.stage(ValueProxy<Array<keyof TO>>, 'obj_keys');
  }

  /**
   * Gets the values of the object as an array.
   *
   * @returns Array of values
   */
  public values<TO = OnlyObject<T>>(this: Is<T, Record<any, any>, this>) {
    return this.stage(ValueProxy<Array<TO[keyof TO]>>, 'obj_values');
  }

  /**
   * Tests if the object has the specified fields.
   *
   * @param fields field list
   * @returns True if the object matches
   */
  public hasfields(this: Is<T, Record<any, any>, this>, ...fields: string[]) {
    return this.stage(ValueProxy<boolean>, 'obj_has', undefined, fields);
  }

  //#endregion
}
