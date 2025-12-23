import { InterfaceFunction } from '@ajs/core/beta';
import { StagedObject } from './common';

//@internal
export namespace internal {
  export const RunQuery = InterfaceFunction<(query: StagedObject['stages']) => any>();
}

export class Query<T> extends StagedObject {
  // run
  public run(): Promise<T> {
    return internal.RunQuery(this.stages);
  }
  // then
  // iterator
}
