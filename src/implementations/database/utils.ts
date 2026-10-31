import assert from "node:assert";
import type { Document } from "mongodb";
import { generate as randomstring } from "randomstring";
import type { StagedObject } from "@antelopejs/interface-database/common";
import type { InstanceId } from "@antelopejs/interface-database/schema";

export const INSTANCE_FIELD = "_instance";
export const BOOKKEEPING_COLLECTION = "__antelope_instances";
const COLLECTION_NAME_SEPARATOR = "__";
export const INSTANCE_INDEX_SUFFIX = "__i";

export function collectionName(schemaId: string, tableName: string): string {
  if (
    schemaId.includes(COLLECTION_NAME_SEPARATOR) ||
    tableName.includes(COLLECTION_NAME_SEPARATOR)
  ) {
    throw new Error(
      `schemaId and tableName may not contain "${COLLECTION_NAME_SEPARATOR}" (got schemaId="${schemaId}", tableName="${tableName}")`,
    );
  }
  return `${schemaId}${COLLECTION_NAME_SEPARATOR}${tableName}`;
}

export function instanceIndexName(indexId: string): string {
  return `${indexId}${INSTANCE_INDEX_SUFFIX}`;
}

/** Tells whether a value is a string, keeping what is known of its type. */
export function isString<Value>(value: Value): value is Value & string {
  return typeof value === "string";
}

/** Tells whether a value is an object or `null`, as `typeof` reports both. */
export function isObjectOrNull<Value>(
  value: Value,
): value is Value & (object | null) {
  return typeof value === "object";
}

/** Tells whether a value is a non-null object, read as a MongoDB document. */
export function isDocument<Value>(value: Value): value is Value & Document {
  return typeof value === "object" && value !== null;
}

/**
 * Tells whether `table` has the stage handler `key`, a method of its class or a field of its own.
 * The `stage_` prefix keeps the `Object.prototype` members out of the answer.
 */
export function hasKey<Table extends object>(
  table: Table,
  key: `stage_${string}`,
): key is Extract<keyof Table, `stage_${string}`> {
  return key in table;
}

export function normalizeInstanceId(
  id: InstanceId | null | undefined,
): string | null {
  if (id === undefined || id === null || id === "") {
    return null;
  }
  if (!isString(id)) {
    throw new Error(`Invalid instance id: expected string, got ${typeof id}`);
  }
  return id;
}

export function DefaultExpression(data: string, def: any) {
  if (def) {
    return { $ifNull: [data, def] };
  }
  return data;
}

export function DefaultConstant(data: string, def: any) {
  return DefaultExpression(data, def && { $literal: def });
}

export function Temporary(name?: string) {
  return `temporary_${name ? `${name}_` : ""}${randomstring({ capitalization: "lowercase", length: 16 })}`;
}

export type QueryStage = StagedObject["stages"][number];

export type ArgumentProvider = (
  subQuery: QueryStage[],
) => Document | string | Promise<Document | string>;

function isArgumentProvider(
  arg: ArgumentProvider | string,
): arg is ArgumentProvider {
  return typeof arg === "function";
}

export class DecodingContext {
  public args: Record<number, ArgumentProvider | string> = {};
  public subquery?: ArgumentProvider;
  /**
   * The array expression each `$map` variable (`$$var`) iterates, typed as the `source` an
   * `ArrayLookup` takes. A `$lookup` pipeline cannot read `$map` variables, so a join keyed on one
   * is planned from this source instead.
   */
  public mapVarSources = new Map<string, Document[keyof Document]>();

  public decodeSubquery(stages: QueryStage[]) {
    if (stages[0]?.stage === "arg") {
      const num = stages[0].args[0];
      const subquery = this.args[num];
      assert(subquery, "Unknown arg used");
      assert(!isString(subquery), "No query arg for query?");
      return subquery(stages);
    }
    if (this.subquery) {
      return this.subquery(stages);
    }
    throw new Error("Subquery with no handler?");
  }

  /**
   * Rebases the outer references, which are document paths that include the
   * wrapped field (`$_wrapped.key`), onto `parentRoot`. The variable named by
   * `parentRoot` must therefore hold the whole document, not the wrapped value.
   */
  public withRoot(parentRoot: string) {
    const remapString = (value: string): string => {
      if (value === "$$ROOT") {
        return parentRoot;
      } else if (value.match(/^\$[^$]/)) {
        return `${parentRoot}.${value.substring(1)}`;
      }
      return value;
    };

    const remapValue = (value: Document | string): Document | string => {
      if (isString(value)) {
        return remapString(value);
      }
      if (isDocument(value) && "$first" in value) {
        const inner: unknown = value.$first;
        return {
          $first:
            isString(inner) || isDocument(inner) ? remapValue(inner) : inner,
        };
      }
      return value;
    };

    const newContext = new DecodingContext();
    for (const [id, arg] of Object.entries(this.args)) {
      if (isArgumentProvider(arg)) {
        newContext.args[Number(id)] = async (subQuery: QueryStage[]) => {
          const result = await arg(subQuery);
          return remapValue(result);
        };
      } else {
        newContext.args[Number(id)] = remapString(arg);
      }
    }
    newContext.mapVarSources = new Map(this.mapVarSources);
    return newContext;
  }
}
