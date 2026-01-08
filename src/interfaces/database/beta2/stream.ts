import { Changes, Value } from './common';
import { Query } from './query';
import { ValueProxy, ValueProxyOrValue } from './valueproxy';
import { Datum } from './datum';

export class Stream<T> extends Query<T[]> {
  /**
   * Changes the type of the value in this stream.
   * This does not actually perform any conversion, it only changes the typescript type.
   *
   * @returns Same stream with a different type
   */
  public cast<U>() {
    return this as unknown as Stream<U>;
  }

  /**
   * Indexes the stream value.
   *
   * TODO: Better name?
   *
   * @param key Field name
   * @param def Default value
   * @returns New stream
   */
  public key<K extends keyof T, U = undefined>(key: K, def?: U) {
    return this.stage(
      Stream<U extends undefined ? T[K] : Exclude<T[K], undefined | null> | U>,
      'key',
      undefined,
      key,
      def,
    );
  }

  /**
   * Defaults the stream value to a given value if it is null.
   *
   * @param value Default value
   * @returns Stream with non-null value
   */
  public default<U>(val: Value<U>) {
    return this.stage(Stream<Exclude<T, undefined | null> | U>, 'default', undefined, val);
  }

  /**
   * Maps the array values using a mapping function.
   *
   * @param mapper Mapping function
   * @returns New stream
   */
  public map<U>(mapper: (val: ValueProxy<T>) => ValueProxyOrValue<U>) {
    return this.stage(Stream<U>, 'map', undefined, this.callfunc(mapper, ValueProxy<T>));
  }

  /**
   * Filters the array using a predicate function.
   *
   * @param predicate Predicate function.
   * @returns Filtered stream
   */
  public filter(predicate: (val: ValueProxy<T>) => ValueProxyOrValue<boolean>) {
    return this.stage(undefined, 'filter', undefined, this.callfunc(predicate, ValueProxy<T>));
  }

  /**
   * Selects specific fields in the documents, discarding the rest.
   *
   * @param fields selected fields
   * @returns New stream
   */
  public pluck(...fields: string[]) {
    return this.stage(Stream<Partial<T>>, 'pluck', undefined, fields);
  }

  /**
   * Excludes specific fields in the documents
   *
   * @param fields excluded fields
   * @returns New stream
   */
  public without(...fields: string[]) {
    return this.stage(Stream<Partial<T>>, 'without', undefined, fields);
  }

  public join<U, V>(
    right: Stream<U>,
    predicate: (left: ValueProxy<T>, right: ValueProxy<U>) => ValueProxyOrValue<boolean>,
    mapper: (left: ValueProxy<T>, right: ValueProxy<U | null>) => ValueProxyOrValue<V>,
    innerOnly = false,
  ) {
    return this.stage(
      Stream<T>,
      'join',
      { innerOnly },
      right,
      this.callfunc(predicate, ValueProxy<T>, ValueProxy<U>),
      this.callfunc(mapper, ValueProxy<T>, ValueProxy<U | null>),
    );
  }

  public lookup<U, TK extends keyof T>(right: Stream<U>, localKey: TK, otherKey: keyof U) {
    return this.stage(
      Stream<Omit<T, TK> & Record<TK, T[TK] extends any[] ? U[] : U>>,
      'lookup',
      { localKey, otherKey },
      right,
    );
  }

  public group<U>(index: string, mapper: (stream: Stream<T>, group: ValueProxy<unknown>) => ValueProxyOrValue<U>) {
    return this.stage(Stream<U>, 'group', { index }, this.callfunc(mapper, Stream<T>, ValueProxy<unknown>));
  }

  public orderBy(index: string, direction?: 'asc' | 'desc') {
    return this.stage(undefined, 'orderBy', { index, direction });
  }

  public slice(offset: Value<number>, count?: Value<number>) {
    return this.stage(undefined, 'slice', undefined, offset, count);
  }

  public nth(n: Value<number>) {
    return this.stage(Datum<T | null>, 'nth', undefined, n);
  }

  public count(field?: keyof T) {
    return this.stage(Datum<number>, 'count', { field });
  }

  public sum(field?: keyof T) {
    return this.stage(Datum<number>, 'sum', { field });
  }

  public avg(field?: keyof T) {
    return this.stage(Datum<number>, 'avg', { field });
  }

  public min(field?: keyof T) {
    return this.stage(Datum<number>, 'min', { field });
  }

  public max(field?: keyof T) {
    return this.stage(Datum<number>, 'max', { field });
  }

  public distinct(): Datum<T[]>;
  public distinct(field: undefined): Datum<T[]>;
  public distinct<TK extends keyof T>(field: TK): Stream<T[TK]>;
  public distinct(field?: keyof T) {
    return this.stage<any>(field ? Stream<T> : Datum<T[]>, 'distinct', { field });
  }

  /**
   * Turns this stream into a change feed
   *
   * @returns Change feed
   */
  public changes() {
    return this.stage(Query<Changes<T>[]>, 'changes');
  }
}
