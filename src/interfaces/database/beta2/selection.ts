import { Changes, DeepPartial } from './common';
import { Datum } from './datum';
import { Query } from './query';
import { Stream } from './stream';

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

export class Table<T> extends Selection<T> {
  public insert(obj: DeepPartial<T> | DeepPartial<T>[]) {
    return this.stage(Query<number>, 'insert', undefined, obj);
  }

  public get(key: string) {
    return this.stage(SingleSelection<T>, 'get', undefined, key);
  }

  public getAll(keys: string | number | (string | number)[], index?: string) {
    return this.stage(Selection<T>, 'getAll', { index }, keys);
  }

  public between<TK extends keyof T>(index: TK, low: T[TK], high: T[TK]) {
    return this.stage(Selection<T>, 'between', { index }, low, high);
  }
}
