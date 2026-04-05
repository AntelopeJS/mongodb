import assert from "node:assert";
import type { StagedObject } from "@antelopejs/interface-database/common";
import { generate as randomstring } from "randomstring";

export function Temporary(name?: string) {
  return `temporary_${name ? `${name}_` : ""}${randomstring({ capitalization: "lowercase", length: 16 })}`;
}

export type QueryStage = StagedObject["stages"][number];

export type ArgumentProvider = (
  subQuery: QueryStage[],
) => Record<string, any> | string | Promise<Record<string, any> | string>;

export class DecodingContext {
  public args: Record<string, ArgumentProvider | string> = {};
  public subquery?: ArgumentProvider;
  public mapVarSources: Record<string, unknown> = {}; // maps $$var → array source expression

  public decodeSubquery(stages: QueryStage[]) {
    if (stages[0]?.stage === "arg") {
      const num = stages[0].args[0];
      const subquery = this.args[num];
      assert(subquery, "Unknown arg used");
      assert(typeof subquery !== "string", "No query arg for query?");
      return subquery(stages);
    }
    if (this.subquery) {
      return this.subquery(stages);
    }
    throw new Error("Subquery with no handler?");
  }

  public withRoot(parentRoot: string) {
    const remapString = (value: string): string => {
      if (value === "$$ROOT") {
        return parentRoot;
      } else if (value.match(/^\$[^$]/)) {
        return `${parentRoot}.${value.substring(1)}`;
      }
      return value;
    };

    const remapValue = (
      value: Record<string, any> | string,
    ): Record<string, any> | string => {
      if (typeof value === "string") {
        return remapString(value);
      }
      if (typeof value === "object" && value !== null && "$first" in value) {
        return { $first: remapValue(value.$first) };
      }
      return value;
    };

    const newContext = new DecodingContext();
    for (const [id, arg] of Object.entries(this.args)) {
      if (typeof arg === "function") {
        newContext.args[id] = async (subQuery: QueryStage[]) => {
          const result = await arg(subQuery);
          return remapValue(result);
        };
      } else {
        newContext.args[id] = remapValue(arg) as string;
      }
    }
    newContext.mapVarSources = { ...this.mapVarSources };
    return newContext;
  }
}
