import type { Document } from "mongodb";
import type { QueryStage } from "@antelopejs/interface-database/common";

import { ResolveIndex, SingleIndexField } from "./indexes";
import { RefuseExternalArrayLookup } from "./external-refusals";
import { Temporary } from "./utils";

/** The other side of a `get` or `getAll` inside a `map` over an array. */
interface ArrayLookupSide {
  collection: string;
  schemaId: string;
  tableName: string;
  initialFilter: Document[];
  singleElement: boolean;
}

/** The `$lookup` stage over a whole array, and the expression picking each element's match. */
export interface ArrayLookupPlan {
  stage: Document;
  match: Document;
}

/**
 * A `get` or `getAll` inside a `map` over an array: `$lookup` variables do not reach a `$map`, so
 * one `$lookup` reads every element's match with `$in`, and a `$filter` picks each element's.
 * Refused on an external collection, whose raw documents this would answer.
 */
export function ArrayLookup(
  right: ArrayLookupSide,
  getStage: QueryStage<"get" | "getAll">,
  key: Document[keyof Document],
  source: Document[keyof Document],
  as: string,
): ArrayLookupPlan {
  RefuseExternalArrayLookup(right);
  const matchField =
    getStage.stage === "get"
      ? "_id"
      : SingleIndexField(
          ResolveIndex(
            right.schemaId,
            right.tableName,
            getStage.options?.index,
          ),
        );
  const array = Temporary("arr");
  const picked = {
    $filter: {
      input: `$${as}`,
      as: "bdoc",
      cond: { $eq: [`$$bdoc.${matchField}`, key] },
    },
  };
  return {
    stage: {
      $lookup: {
        from: right.collection,
        let: { [array]: source },
        pipeline: [
          ...right.initialFilter,
          { $match: { $expr: { $in: [`$${matchField}`, `$$${array}`] } } },
        ],
        as,
      },
    },
    match: right.singleElement
      ? { $ifNull: [{ $first: picked }, null] }
      : picked,
  };
}
