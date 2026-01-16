import { Value } from './common';
import { Query } from './query';
import { ValueProxy, ValueProxyOrValue } from './valueproxy';
export declare class Datum<T> extends Query<T> {
    /**
     * Converts the Datum to a value proxy to use ValueProxy-specific methods.
     *
     * @returns New ValueProxy
     */
    value(): ValueProxy<T>;
    /**
     * Changes the type of this datum.
     * This does not actually perform any conversion, it only changes the typescript type.
     *
     * @returns Same datum with a different type
     */
    cast<U>(): Datum<U>;
    /**
     * Indexes the datum.
     *
     * TODO: Better name?
     *
     * @param key Field name
     * @param def Default value
     * @returns New datum with the value
     */
    key<K extends keyof T, U = undefined>(key: K, def?: U): Datum<U extends undefined ? T[K] : U | Exclude<T[K], null | undefined>>;
    /**
     * Defaults the datum to a given value if it is null.
     *
     * @param value Default value
     * @returns Current datum or given value
     */
    default<U>(val: Value<U>): Datum<U | Exclude<T, null | undefined>>;
    /**
     * Run a mapping function on the datum.
     *
     * @param mapper Mapping function
     * @returns New datum with the result of the mapper
     */
    do<U>(mapper: (val: ValueProxy<T>) => ValueProxyOrValue<U>): Datum<U>;
    /**
     * Perform a foreign key lookup
     *
     * @param other Other table
     * @param localKey Key in local object
     * @param otherKey Key in other table
     */
    lookup<U = any, TK extends keyof T = keyof T>(other: Datum<U>, // TODO: swap to Stream<U>
    localKey: TK, otherKey: keyof U): Datum<Omit<T, TK> & Record<TK, T[TK] extends any[] ? U[] : U>>;
    /**
     * Plucks fields from the documents.
     *
     * TODO: Better typing
     *
     * @param fields Fields to keep
     * @returns New datum
     */
    pluck(...fields: string[]): Datum<Partial<T>>;
}
