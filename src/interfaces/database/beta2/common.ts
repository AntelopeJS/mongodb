import { Class } from '@ajs/core/beta/decorators';
import { ValueProxyOrValue } from './valueproxy';
import { Datum } from './datum';

export type QueryStage = {
  stage: string;
  options?: any;
  args: any[];
};

export function QueryStage(stage: string, options?: any, ...args: any[]) {
  return {
    stage,
    options,
    args,
  };
}

export class FunctionObject {
  public constructor(public readonly result: StagedObject, public readonly args: number[]) {}
}

export class StagedObject {
  public readonly stages: QueryStage[];

  public constructor(newStage: QueryStage, previous?: StagedObject) {
    this.stages = previous ? [...previous.stages, newStage] : [newStage];
  }

  protected stage<T extends StagedObject>(type: Class<T>, stage: string, options?: any, ...args: any[]): T {
    return new type(
      {
        stage,
        options,
        args,
      },
      this,
    );
  }

  protected call() {}
}

export type Value<T> = Datum<T> | ValueProxyOrValue<T>;
