import { InterfaceFunction } from '@ajs/core/beta';
import { StagedObject } from './common';

//@internal
export const RunQuery = InterfaceFunction<(query: StagedObject['stages']) => any>();

export class Query<T> extends StagedObject implements PromiseLike<T> {
  /**
   * Execute the query
   *
   * @returns Query result
   */
  public run(): Promise<T> {
    return RunQuery(this.stages);
  }

  public then<TResult1 = T, TResult2 = never>(
    onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | null,
    onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | null,
  ): PromiseLike<TResult1 | TResult2> {
    return this.run().then(onfulfilled, onrejected);
  }

  /*
  TODO: core interface function for async generators

  public cursor(): AsyncGenerator<T extends Array<infer T1> ? T1 : T, void, unknown> {}

  [Symbol.asyncIterator]() {
    return this.cursor();
  }
  */
}
