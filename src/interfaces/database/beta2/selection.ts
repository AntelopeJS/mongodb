import { Changes, DeepPartial } from './common';
import { Datum } from './datum';
import { Query } from './query';
import { Stream } from './stream';

/**
 * Selection containing a single element
 */
export class SingleSelection<T> extends Datum<T> {
  /**
   * Update fields of this selection with the given values
   *
   * @param document Partial document with new values
   * @returns Number of modified documents
   */
  public update(document: DeepPartial<T>) {
    return this.stage(Query<number>, 'update', undefined, document);
  }

  /**
   * Replace documents of this selection
   *
   * @param document Partial document to replace with
   * @returns Number of modified documents
   */
  public replace(document: DeepPartial<T>) {
    return this.stage(Query<number>, 'replace', undefined, document);
  }

  /**
   * Delete selected documents
   *
   * @returns Number of deleted documents
   */
  public delete() {
    return this.stage(Query<number>, 'delete');
  }

  /**
   * Turns this selection into a change feed
   *
   * @returns Change feed
   */
  public changes() {
    return this.stage(Query<Changes<T>[]>, 'changes');
  }
}

/**
 * Selection containing any number of elements
 */
export class Selection<T> extends Stream<T> {
  /**
   * Update fields of this selection with the given values
   *
   * @param document Partial document with new values
   * @returns Number of modified documents
   */
  public update(document: DeepPartial<T>) {
    return this.stage(Query<number>, 'update', undefined, document);
  }

  /**
   * Replace documents of this selection
   *
   * @param document Partial document to replace with
   * @returns Number of modified documents
   */
  public replace(document: DeepPartial<T>) {
    return this.stage(Query<number>, 'replace', undefined, document);
  }

  /**
   * Delete selected documents
   *
   * @returns Number of deleted documents
   */
  public delete() {
    return this.stage(Query<number>, 'delete');
  }
}

/**
 * Database table
 */
export class Table<T> extends Selection<T> {
  /**
   * Inserts one or more documents into this table
   *
   * @param obj Document(s) to insert
   * @returns Inserted IDs
   */
  public insert(obj: DeepPartial<T> | DeepPartial<T>[]) {
    return this.stage(Query<string[]>, 'insert', undefined, obj);
  }

  /**
   * Gets a document using its primary key
   *
   * @param key Primary key value
   * @returns Single document selection
   */
  public get(key: string) {
    return this.stage(SingleSelection<T>, 'get', undefined, key);
  }

  /**
   * Gets multiple documents using a secondary index
   *
   * @param keys Key value(s)
   * @param index Secondary index, will use the primary key if undefined
   * @returns Multiple document selection
   */
  public getAll(keys: string | number | (string | number)[], index?: string) {
    return this.stage(Selection<T>, 'getAll', { index }, keys);
  }

  /**
   * Gets multiple documents using a secondary index and bounding values
   *
   * @param index Secondary index
   * @param low Lowest value of the range
   * @param high Highest value of the range (excluded)
   * @returns Multiple document selection
   */
  public between<TK extends keyof T>(index: TK, low: T[TK], high: T[TK]) {
    return this.stage(Selection<T>, 'between', { index }, low, high);
  }
}
