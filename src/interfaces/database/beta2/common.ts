import { Class } from '@ajs/core/beta/decorators';
import { ValueProxy, ValueProxyOrValue } from './valueproxy';
import { Datum } from './datum';
import { Query } from './query';

/**
 * Recursive Partial generic type
 */
export type DeepPartial<T> = {
  [K in keyof T]?: T[K] extends Array<infer U1>
    ? Array<DeepPartial<U1>>
    : T[K] extends ReadonlyArray<infer U2>
      ? ReadonlyArray<DeepPartial<U2>>
      : DeepPartial<T[K]>;
};

/**
 * Change event
 */
export interface Changes<T> {
  /**
   * The type of change that occured
   *
   * Possible values: added, removed, modified
   */
  changeType: 'added' | 'removed' | 'modified';

  /**
   * Value prior to the change
   */
  oldValue?: T;

  /**
   * New value after the change
   */
  newValue?: T;
}

export interface QueryStage {
  stage: string;
  options?: any;
  args: any[];
}

export function QueryStage(stage: string, options?: any, ...args: any[]) {
  return {
    stage,
    options,
    args,
  };
}

export class StagedObject {
  protected readonly stages: QueryStage[];

  public constructor(newStage: QueryStage, previous?: StagedObject) {
    this.stages = previous ? [...previous.stages, newStage] : [newStage];
  }

  //@internal
  public build() {
    return this.stages;
  }

  protected stage(type: undefined, stage: string, options?: any, ...args: any[]): this;
  protected stage<T extends StagedObject>(type: Class<T>, stage: string, options?: any, ...args: any[]): T;
  protected stage<T extends StagedObject>(type: Class<T> | undefined, stage: string, options?: any, ...args: any[]) {
    return new (type ?? (this.constructor as Class<StagedObject>))(
      {
        stage,
        options,
        args,
      },
      this,
    );
  }

  private static nextargid = 0;
  protected callfunc<T extends StagedObject[]>(
    func: (...args: T) => any,
    ...args: (typeof StagedObject)[]
  ): QueryStage {
    const argNumbers: number[] = [];
    const argValues: StagedObject[] = [];
    for (let i = 0; i < args.length; ++i) {
      const id = StagedObject.nextargid++;
      argNumbers[i] = id;
      argValues[i] = new args[i](QueryStage('arg', undefined, id));
    }
    return {
      stage: 'func', // TODO: using the query stage structure here doesnt make any sense
      args: [argNumbers, func(...(argValues as T))],
    };
  }
}

// TODO: This adds a lot of complexity to implementations, investigate if we should remove it.
export type Value<T> = Datum<T> | ValueProxyOrValue<T>;

type UnknownObject = Record<keyof any, unknown>;
type ExtractTypeObject<T extends UnknownObject> = T extends infer O
  ? {
      [K in keyof O]: ExtractType<O[K]>;
    }
  : never;
export type ExtractType<T> =
  T extends ValueProxy<infer A>
    ? A
    : T extends Query<infer A>
      ? A
      : T extends UnknownObject
        ? ExtractTypeObject<T>
        : T extends Array<infer A>
          ? Array<ExtractType<A>>
          : T;
