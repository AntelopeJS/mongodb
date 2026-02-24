import { InterfaceFunction } from '@ajs/core/beta';
import { StagedObject } from './common';

//@internal
export const RunQuery = InterfaceFunction<(query: StagedObject['stages']) => any>();
//@internal
export const ReadCursor =
  InterfaceFunction<(reqId: number, stages: StagedObject['stages']) => IteratorResult<any, void>>();
//@internal
export const CloseCursor = InterfaceFunction<(reqId: number) => void>();

let nextReqId = 0;
class IterableCursor implements AsyncGenerator<any, void, unknown> {
  private reqId: number;
  private resolve?: (val: IteratorResult<any, void>) => void;
  private reject?: (err: any) => void;

  public constructor(private stages: StagedObject['stages']) {
    this.reqId = nextReqId++;
  }

  public next(): Promise<IteratorResult<any, void>> {
    return new Promise((resolve, reject) => {
      this.resolve = resolve;
      this.reject = reject;
      ReadCursor(this.reqId, this.stages)
        .then(resolve)
        .catch(reject)
        .then(() => {
          this.resolve = undefined;
          this.reject = undefined;
        });
    });
  }

  public async return(): Promise<IteratorResult<any, void>> {
    const res = { done: true, value: undefined };
    if (this.resolve) {
      this.resolve(res);
      this.resolve = undefined;
      this.reject = undefined;
    }
    await CloseCursor(this.reqId);
    return res;
  }

  public async throw(e: any): Promise<IteratorResult<any, void>> {
    if (this.reject) {
      this.reject(e);
      this.resolve = undefined;
      this.reject = undefined;
    }
    await CloseCursor(this.reqId);
    return { done: true, value: undefined };
  }

  [Symbol.asyncIterator](): AsyncGenerator<any, void, unknown> {
    return this;
  }
}

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

  //TODO: core interface function for async generators

  public cursor(): AsyncGenerator<T extends Array<infer T1> ? T1 : T, void, unknown> {
    return new IterableCursor(this.stages);
  }

  [Symbol.asyncIterator]() {
    return this.cursor();
  }
}
