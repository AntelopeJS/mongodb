import { QueryStage, StagedObject } from './common';

export type ValueProxyOrValue<T> = ValueProxy<T> | T;

//@internal
export type Is<Left, Right, R> = Left extends Right ? R : never;
export class ValueProxy<T> extends StagedObject {
  public static arg<T = unknown>(id: number) {
    return new ValueProxy<T>(QueryStage('arg', undefined, id));
  }

  public static constant<T = unknown>(value: unknown) {
    return new ValueProxy<T>(QueryStage('constant', undefined, value));
  }

  cast<U>() {
    return this as unknown as ValueProxy<U>;
  }

  //#region Any

  /**
   * Returns the parameter if the proxy is null.
   *
   * @param value Value to use in case the proxy is null
   * @returns Non-null value.
   */
  default<U>(value: ValueProxyOrValue<U>) {
    return this.stage(ValueProxy<Exclude<T, undefined | null> | U>, 'default', undefined, value);
  }

  /**
   * AND operator.
   *
   * @param other Operand B
   * @returns A && B
   */
  and(value: unknown) {
    return this.stage(ValueProxy<boolean>, 'and', undefined, value);
  }

  /**
   * OR operator.
   *
   * @param other Operand B
   * @returns A || B
   */
  or(value: unknown) {
    return this.stage(ValueProxy<boolean>, 'or', undefined, value);
  }

  /**
   * NOT operator.
   *
   * @returns !A
   */
  not() {
    return this.stage(ValueProxy<boolean>, 'not', undefined);
  }

  /**
   * Equality operator.
   *
   * @param other Operand B
   * @returns A == B
   */
  eq(value: unknown) {
    return this.stage(ValueProxy<boolean>, 'eq', undefined, value);
  }

  /**
   * Inequality operator.
   *
   * @param other Operand B
   * @returns A != B
   */
  ne(value: unknown) {
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
  add(this: Is<Date | number, T, this>, value: ValueProxyOrValue<number>) {
    return this.stage(ValueProxy<T>, 'add', undefined, value);
  }

  /**
   * Subtraction operator.
   *
   * @param other Operand B
   * @returns New value
   */
  sub<U>(this: Is<Date | number, T, this>, value: ValueProxyOrValue<U>) {
    return this.stage(
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

  //#endregion

  //#region number

  //#endregion

  //#region Comparison

  //#endregion

  //#region string

  //#endregion

  //#region array

  //#endregion

  //#region object

  //#endregion
}
