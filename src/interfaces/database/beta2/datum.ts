import { ExtractType, Value } from './common';
import { Query } from './query';
import { ValueProxy } from './valueproxy';

export class Datum<T> extends Query<T> {
  /**
   * Changes the type of this datum.
   * This does not actually perform any conversion, it only changes the typescript type.
   *
   * @returns Same datum with a different type
   */
  public cast<U>() {
    return this as unknown as Datum<U>;
  }

  /**
   * Indexes the datum.
   *
   * TODO: Better name?
   * TODO: typing for compound keys (a.b.c)
   *
   * @param key Field name
   * @param def Default value
   * @returns New datum with the value
   */
  public key<K extends keyof NonNullable<T>, U = undefined>(key: K, def?: U) {
    return this.stage(
      Datum<U extends undefined ? NonNullable<T>[K] : NonNullable<NonNullable<T>[K]> | U>,
      'key',
      undefined,
      key,
      def,
    );
  }

  /**
   * Defaults the datum to a given value if it is null.
   *
   * @param value Default value
   * @returns Current datum or given value
   */
  public default<U>(val: Value<U>) {
    return this.stage(Datum<Exclude<T, undefined | null> | U>, 'default', undefined, val);
  }

  /**
   * Run a mapping function on the datum.
   *
   * @param mapper Mapping function
   * @returns New datum with the result of the mapper
   */
  public do<U>(mapper: (val: ValueProxy<T>) => U) {
    return this.stage(Datum<ExtractType<U>>, 'map', undefined, this.callfunc(mapper, ValueProxy<T>));
  }

  /**
   * Perform a foreign key lookup
   *
   * @param other Other table
   * @param localKey Key in local object
   * @param otherKey Key in other table
   */
  public lookup<U = any, TK extends keyof T = keyof T>(
    other: Datum<U>, // TODO: swap to Stream<U>
    localKey: TK,
    otherKey: keyof U,
  ) {
    return this.stage(
      Datum<Omit<T, TK> & Record<TK, T[TK] extends any[] ? U[] : U>>,
      'lookup',
      { localKey, otherKey },
      other,
    );
  }

  /**
   * Plucks fields from the documents.
   *
   * TODO: Better typing
   *
   * @param fields Fields to keep
   * @returns New datum
   */
  public pluck(...fields: string[]) {
    return this.stage(Datum<Partial<T>>, 'pluck', undefined, fields);
  }
}
