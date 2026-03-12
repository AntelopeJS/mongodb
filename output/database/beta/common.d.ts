import type { Class } from "@ajs/core/beta/decorators";
import type { Datum } from "./datum";
import type { Query } from "./query";
import type { ValueProxy, ValueProxyOrValue } from "./valueproxy";
/**
 * Recursive Partial generic type
 */
export type DeepPartial<T> = {
    [K in keyof T]?: T[K] extends Array<infer U1> ? Array<DeepPartial<U1>> : T[K] extends ReadonlyArray<infer U2> ? ReadonlyArray<DeepPartial<U2>> : DeepPartial<T[K]>;
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
    changeType: "added" | "removed" | "modified";
    /**
     * Value prior to the change
     */
    oldValue?: T;
    /**
     * New value after the change
     */
    newValue?: T;
}
export interface InsertOptions {
    conflict?: "update" | "replace";
}
export interface QueryStage {
    stage: string;
    options?: any;
    args: any[];
}
export declare function QueryStage(stage: string, options?: any, ...args: any[]): {
    stage: string;
    options: any;
    args: any[];
};
export declare class StagedObject {
    protected readonly stages: QueryStage[];
    constructor(newStage: QueryStage, previous?: StagedObject);
    protected stage(type: undefined, stage: string, options?: any, ...args: any[]): this;
    protected stage<T extends StagedObject>(type: Class<T>, stage: string, options?: any, ...args: any[]): T;
    private static nextargid;
    protected callfunc<T extends StagedObject[]>(func: (...args: T) => any, ...args: (typeof StagedObject)[]): QueryStage;
}
export type Value<T> = Datum<T> | ValueProxyOrValue<T>;
type UnknownObject = Record<keyof any, unknown>;
type ExtractTypeObject<T extends UnknownObject> = T extends infer O ? {
    [K in keyof O]: ExtractType<O[K]>;
} : never;
export type ExtractType<T> = T extends ValueProxy<infer A> ? A : T extends Query<infer A> ? A : T extends UnknownObject ? ExtractTypeObject<T> : T extends Array<infer A> ? Array<ExtractType<A>> : T;
export {};
