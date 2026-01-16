import { StagedObject } from './common';
export declare class Query<T> extends StagedObject implements PromiseLike<T> {
    /**
     * Execute the query
     *
     * @returns Query result
     */
    run(): Promise<T>;
    then<TResult1 = T, TResult2 = never>(onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | null, onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | null): PromiseLike<TResult1 | TResult2>;
}
