import { Changes, DeepPartial } from './common';
import { Datum } from './datum';
import { Query } from './query';
import { Stream } from './stream';
/**
 * Selection containing a single element
 */
export declare class SingleSelection<T> extends Datum<T> {
    /**
     * Update fields of this selection with the given values
     *
     * @param document Partial document with new values
     * @returns Number of modified documents
     */
    update(document: DeepPartial<T>): Query<number>;
    /**
     * Replace documents of this selection
     *
     * @param document Partial document to replace with
     * @returns Number of modified documents
     */
    replace(document: DeepPartial<T>): Query<number>;
    /**
     * Delete selected documents
     *
     * @returns Number of deleted documents
     */
    delete(): Query<number>;
    /**
     * Turns this selection into a change feed
     *
     * @returns Change feed
     */
    changes(): Query<Changes<T>[]>;
}
/**
 * Selection containing any number of elements
 */
export declare class Selection<T> extends Stream<T> {
    /**
     * Update fields of this selection with the given values
     *
     * @param document Partial document with new values
     * @returns Number of modified documents
     */
    update(document: DeepPartial<T>): Query<number>;
    /**
     * Replace documents of this selection
     *
     * @param document Partial document to replace with
     * @returns Number of modified documents
     */
    replace(document: DeepPartial<T>): Query<number>;
    /**
     * Delete selected documents
     *
     * @returns Number of deleted documents
     */
    delete(): Query<number>;
}
/**
 * Database table
 */
export declare class Table<T> extends Selection<T> {
    /**
     * Inserts one or more documents into this table
     *
     * @param obj Document(s) to insert
     * @returns Inserted IDs
     */
    insert(obj: DeepPartial<T> | DeepPartial<T>[]): Query<string[]>;
    /**
     * Gets a document using its primary key
     *
     * @param key Primary key value
     * @returns Single document selection
     */
    get(key: string): SingleSelection<T>;
    /**
     * Gets multiple documents using a secondary index
     *
     * @param keys Key value(s)
     * @param index Secondary index, will use the primary key if undefined
     * @returns Multiple document selection
     */
    getAll(keys: string | number | (string | number)[], index?: string): Selection<T>;
    /**
     * Gets multiple documents using a secondary index and bounding values
     *
     * @param index Secondary index
     * @param low Lowest value of the range
     * @param high Highest value of the range (excluded)
     * @returns Multiple document selection
     */
    between<TK extends keyof T>(index: TK, low: T[TK], high: T[TK]): Selection<T>;
}
