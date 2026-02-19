import { Changes, ExtractType, Value } from './common';
import { Query } from './query';
import { ValueProxy, ValueProxyOrValue } from './valueproxy';
import { Datum } from './datum';
import { Table } from './selection';
export declare class Stream<T> extends Query<T[]> {
    /**
     * Changes the type of the value in this stream.
     * This does not actually perform any conversion, it only changes the typescript type.
     *
     * @returns Same stream with a different type
     */
    cast<U>(): Stream<U>;
    /**
     * Indexes the stream value.
     *
     * TODO: Better name?
     *
     * @param key Field name
     * @param def Default value
     * @returns New stream
     */
    key<K extends keyof T, U = undefined>(key: K, def?: U): Stream<U extends undefined ? T[K] : U | Exclude<T[K], null | undefined>>;
    /**
     * Defaults the stream value to a given value if it is null.
     *
     * @param value Default value
     * @returns Stream with non-null value
     */
    default<U>(val: Value<U>): Stream<U | Exclude<T, null | undefined>>;
    /**
     * Maps the array values using a mapping function.
     *
     * @param mapper Mapping function
     * @returns New stream
     */
    map<U>(mapper: (val: ValueProxy<T>) => U): Stream<ExtractType<U>>;
    /**
     * Filters the array using a predicate function.
     *
     * @param predicate Predicate function.
     * @returns Filtered stream
     */
    filter(predicate: (val: ValueProxy<T>) => ValueProxyOrValue<boolean>): this;
    /**
     * Selects specific fields in the documents, discarding the rest.
     *
     * @param fields Selected fields
     * @returns New stream
     */
    pluck(...fields: string[]): Stream<Partial<T>>;
    /**
     * Excludes specific fields in the documents
     *
     * @param fields Excluded fields
     * @returns New stream
     */
    without(...fields: string[]): Stream<Partial<T>>;
    /**
     * Perform a join operation between this stream (left) and another stream (right)
     *
     * @param right Right stream
     * @param predicate Predicate to match elements from the left stream to the right stream
     * @param mapper Mapping function for each pair of documents
     * @param innerOnly Exclude documents in the left stream that have no match in the right stream
     * @returns New stream with results of the mapping function
     */
    join<U, V>(right: Stream<U>, predicate: (left: ValueProxy<T>, right: ValueProxy<U>) => ValueProxyOrValue<boolean>, mapper: (left: ValueProxy<T>, right: ValueProxy<U | null>) => V, innerOnly?: boolean): Stream<ExtractType<V>>;
    /**
     * Transform a foreign key or array of foreign keys into a document from another stream
     *
     * @param right Stream containing the other documents
     * @param localKey Key in the local document to search with and replace
     * @param otherKey Key in the other document to match against
     * @returns New stream
     */
    lookup<U, TK extends keyof T>(right: Table<U>, localKey: TK, otherKey: keyof U): Stream<Omit<T, TK> & Record<TK, T[TK] extends any[] ? U[] : U>>;
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
    group<U>(index: string, mapper: (stream: Stream<T>, group: ValueProxy<unknown>) => U): Stream<ExtractType<U>>;
    /**
     * Sort the stream using the given index and direction
     *
     * @param index Index to sort
     * @param direction Sort direction
     * @returns New (sorted) stream
     */
    orderBy(index: string, direction?: 'asc' | 'desc'): this;
    /**
     * Obtain a slice (subsection) of the stream
     *
     * @param offset Offset into the stream
     * @param count Number of documents to pick
     * @returns New stream
     */
    slice(offset: Value<number>, count?: Value<number>): this;
    /**
     * Obtain the Nth document of the stream
     *
     * @param n N
     * @returns Single document
     */
    nth(n: Value<number>): Datum<T | null>;
    /**
     * Gets the count of documents or the count of distinct values of a given field
     *
     * @param field Field to count distinct entries
     * @returns Count
     */
    count(field?: keyof T): Datum<number>;
    /**
     * Sum of the values on the given field
     *
     * @param field Field to use
     * @returns Sum
     */
    sum(field?: keyof T): Datum<number>;
    /**
     * Average of the values on the given field
     *
     * @param field Field to use
     * @returns Average
     */
    avg(field?: keyof T): Datum<number>;
    /**
     * Minimum of the values on the given field
     *
     * @param field Field to use
     * @returns Minimum value
     */
    min(field?: keyof T): Datum<number>;
    /**
     * Maximum of the values on the given field
     *
     * @param field Field to use
     * @returns Maximum value
     */
    max(field?: keyof T): Datum<number>;
    /**
     * Gets an array of distinct documents in the stream
     *
     * @returns Array of documents
     */
    distinct(): Datum<T[]>;
    distinct(field: undefined): Datum<T[]>;
    /**
     * Gets a stream of the distinct values of a field
     *
     * @param index Field to use
     * @returns New stream
     */
    distinct<TK extends keyof T>(field: TK): Stream<T[TK]>;
    /**
     * Turns this stream into a change feed
     *
     * @returns Change feed
     */
    changes(): Query<Changes<T>[]>;
}
