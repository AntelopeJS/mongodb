import { Changes, ExtractType, Value } from './common';
import { Query } from './query';
import { ValueProxy, ValueProxyOrValue } from './valueproxy';
import { Datum } from './datum';
import { Table } from './selection';

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
  public map<U>(mapper: (val: ValueProxy<T>) => U) {
    return this.stage(Stream<ExtractType<U>>, 'map', undefined, this.callfunc(mapper, ValueProxy<T>));
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
   * @param fields Selected fields
   * @returns New stream
   */
  public pluck(...fields: string[]) {
    return this.stage(Stream<Partial<T>>, 'pluck', undefined, fields);
  }

  /**
   * Excludes specific fields in the documents
   *
   * @param fields Excluded fields
   * @returns New stream
   */
  public without(...fields: string[]) {
    return this.stage(Stream<Partial<T>>, 'without', undefined, fields);
  }

  /**
   * Perform a join operation between this stream (left) and another stream (right)
   *
   * @param right Right stream
   * @param predicate Predicate to match elements from the left stream to the right stream
   * @param mapper Mapping function for each pair of documents
   * @param innerOnly Exclude documents in the left stream that have no match in the right stream
   * @returns New stream with results of the mapping function
   */
  public join<U, V>(
    right: Stream<U>,
    predicate: (left: ValueProxy<T>, right: ValueProxy<U>) => ValueProxyOrValue<boolean>,
    mapper: (left: ValueProxy<T>, right: ValueProxy<U | null>) => V,
    innerOnly = false,
  ) {
    return this.stage(
      Stream<ExtractType<V>>,
      'join',
      { innerOnly },
      right,
      this.callfunc(predicate, ValueProxy<T>, ValueProxy<U>),
      this.callfunc(mapper, ValueProxy<T>, ValueProxy<U | null>),
    );
  }

  /**
   * Transform a foreign key or array of foreign keys into a document from another stream
   *
   * @param right Stream containing the other documents
   * @param localKey Key in the local document to search with and replace
   * @param otherKey Key in the other document to match against
   * @returns New stream
   */
  public lookup<U, TK extends keyof T>(right: Table<U>, localKey: TK, otherKey: keyof U) {
    return this.stage(
      Stream<Omit<T, TK> & Record<TK, T[TK] extends any[] ? U[] : U>>,
      'lookup',
      { localKey, otherKey },
      right,
    );
  }

  /**
   * Group the documents using the given index and maps the result using a mapping function
   *
   * The parameters of this function are:
   * - The stream with all the documents inside the group
   * - The index value for this group
   *
   * The result of this function is used as the element in the new stream
   *
   * @param index Index to group on
   * @param mapper Mapping function
   * @returns New stream of grouped data
   */
  public group<U>(index: string, mapper: (stream: Stream<T>, group: ValueProxy<unknown>) => U) {
    return this.stage(
      Stream<ExtractType<U>>,
      'group',
      { index },
      this.callfunc(mapper, Stream<T>, ValueProxy<unknown>),
    );
  }

  /**
   * Sort the stream using the given index and direction
   *
   * @param index Index to sort
   * @param direction Sort direction
   * @returns New (sorted) stream
   */
  public orderBy(index: string, direction?: 'asc' | 'desc') {
    return this.stage(undefined, 'orderBy', { index, direction });
  }

  /**
   * Obtain a slice (subsection) of the stream
   *
   * @param offset Offset into the stream
   * @param count Number of documents to pick
   * @returns New stream
   */
  public slice(offset: Value<number>, count?: Value<number>) {
    return this.stage(undefined, 'slice', undefined, offset, count);
  }

  /**
   * Obtain the Nth document of the stream
   *
   * @param n N
   * @returns Single document
   */
  public nth(n: Value<number>) {
    return this.stage(Datum<T | null>, 'nth', undefined, n);
  }

  /**
   * Gets the count of documents or the count of distinct values of a given field
   *
   * @param field Field to count distinct entries
   * @returns Count
   */
  public count(field?: keyof T) {
    return this.stage(Datum<number>, 'count', { field });
  }

  /**
   * Sum of the values on the given field
   *
   * @param field Field to use
   * @returns Sum
   */
  public sum(field?: keyof T) {
    return this.stage(Datum<number>, 'sum', { field });
  }

  /**
   * Average of the values on the given field
   *
   * @param field Field to use
   * @returns Average
   */
  public avg(field?: keyof T) {
    return this.stage(Datum<number>, 'avg', { field });
  }

  /**
   * Minimum of the values on the given field
   *
   * @param field Field to use
   * @returns Minimum value
   */
  public min(field?: keyof T) {
    return this.stage(Datum<number>, 'min', { field });
  }

  /**
   * Maximum of the values on the given field
   *
   * @param field Field to use
   * @returns Maximum value
   */
  public max(field?: keyof T) {
    return this.stage(Datum<number>, 'max', { field });
  }

  /**
   * Gets an array of distinct documents in the stream
   *
   * @returns Array of documents
   */
  public distinct(): Datum<T[]>;
  public distinct(field: undefined): Datum<T[]>;

  /**
   * Gets a stream of the distinct values of a field
   *
   * @param index Field to use
   * @returns New stream
   */
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
