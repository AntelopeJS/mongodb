import type { Document } from "mongodb";

import { isDocument, Temporary } from "./utils";

/** Where the stages of one mapper field go around the `$group` its sibling fields share. */
export interface GroupFieldStages {
  /** Row-wise stages that only write the field's own temporaries, so every row reaches the `$group`. */
  beforeGroup: Document[];
  /** The field's accumulators, merged into the shared `$group`. */
  accumulators: Document;
  /** Stages that rewrite the field's value once the `$group` has reduced it. */
  afterGroup: Document[];
}

/** A step that orders or slices the rows a field collected, applied to its array after the `$group`. */
type ArrayStep =
  | { kind: "sort"; sortBy: Document }
  | { kind: "skip"; count: unknown }
  | { kind: "limit"; count: unknown };

const ROW_STAGES = new Set([
  "$addFields",
  "$replaceRoot",
  "$project",
  "$unset",
  "$lookup",
]);

const STAGE_NAMES = new Map([
  ["$group", "a reduction"],
  ["$match", "a filter"],
  ["$sort", "an orderBy"],
  ["$skip", "a slice or nth"],
  ["$limit", "a slice or nth"],
  ["$unwind", "a join"],
  ["$unionWith", "a union"],
]);

function Describe(operator: string): string {
  return STAGE_NAMES.get(operator) ?? `the ${operator} stage`;
}

function Refuse(reason: string): never {
  throw new Error(`Unsupported group mapper: ${reason}`);
}

function Entries(document: Document): [string, unknown][] {
  return Object.entries(document);
}

function Single(document: Document): [string, unknown] {
  return Entries(document)[0] ?? Refuse("an empty stage");
}

function Body(stage: Document, operator: string): Document {
  const body: unknown = stage[operator];
  return isDocument(body) ? body : Refuse(`a ${operator} without a document`);
}

/**
 * `value` cut down to what `$sort` reads on the path `fields` below it, so `$sort` on the copy ranks
 * as it ranks `value`: an array keeps every element, a document keeps only the next field of the
 * path, and any other value stays whole.
 */
function SortCopy(
  value: string | Document,
  fields: string[],
): string | Document {
  const [field, ...rest] = fields;
  if (field === undefined) {
    return value;
  }
  const narrow = (input: string): Document => ({
    $cond: [
      { $eq: [{ $type: input }, "object"] },
      { [field]: SortCopy({ $getField: { field, input } }, rest) },
      input,
    ],
  });
  return {
    $let: {
      vars: { value },
      in: {
        $cond: [
          { $isArray: "$$value" },
          { $map: { input: "$$value", in: narrow("$$this") } },
          narrow("$$value"),
        ],
      },
    },
  };
}

function ApplyStep(step: ArrayStep, field: string): Document[] {
  const array = `$${field}`;
  switch (step.kind) {
    case "sort":
      return [
        {
          $lookup: {
            let: { rows: array },
            pipeline: [{ $documents: "$$rows" }, { $sort: step.sortBy }],
            as: field,
          },
        },
      ];
    case "skip":
      return [
        {
          $addFields: {
            [field]: {
              $slice: [array, step.count, { $max: [{ $size: array }, 1] }],
            },
          },
        },
      ];
    case "limit":
      return [{ $addFields: { [field]: { $slice: [array, step.count] } } }];
  }
}

/** Walks the compiled stages of one mapper field and sorts each to its place around the `$group`. */
class GroupFieldSplit {
  private readonly keepName = Temporary("groupkeep");
  private readonly keep = `$${this.keepName}`;
  private readonly fieldRef: string;
  private filtered = false;
  private sliced = false;
  private readonly captures: string[] = [];
  private readonly steps: ArrayStep[] = [];
  private readonly beforeGroup: Document[] = [];
  private readonly afterGroup: Document[] = [];
  private accumulators?: Document;

  public constructor(private readonly field: string) {
    this.fieldRef = `$${field}`;
  }

  public add(stage: Document) {
    const [operator, value] = Single(stage);
    if (this.accumulators) {
      if (!ROW_STAGES.has(operator)) {
        Refuse(`${Describe(operator)} after a reduction`);
      }
      this.afterGroup.push(stage);
      return;
    }
    switch (operator) {
      case "$group":
        return this.reduce(Body(stage, operator));
      case "$match":
        return this.filter(Body(stage, operator));
      case "$sort":
        return this.sort(Body(stage, operator));
      case "$skip":
      case "$limit":
        this.steps.push({
          kind: operator === "$skip" ? "skip" : "limit",
          count: value,
        });
        this.sliced = true;
        return;
      default:
        if (!ROW_STAGES.has(operator)) {
          Refuse(`${Describe(operator)} in a mapper field`);
        }
        this.beforeGroup.push(this.filtered ? this.gateRowStage(stage) : stage);
    }
  }

  private reduce(group: Document) {
    if (group._id !== null) {
      Refuse("a nested group with a non-null key");
    }
    if (this.sliced) {
      Refuse("a reduction after slice or nth");
    }
    const reductions = Entries(group).filter(([name]) => name !== "_id");
    this.accumulators = Object.fromEntries(
      reductions.map(([name, accumulator]) => [
        name,
        this.filtered && isDocument(accumulator)
          ? this.gateAccumulator(accumulator)
          : accumulator,
      ]),
    );
  }

  private filter(match: Document) {
    if (this.sliced) {
      Refuse("a filter after slice or nth");
    }
    const condition: unknown = match.$expr;
    if (condition === undefined) {
      Refuse("a filter that is not an expression");
    }
    this.beforeGroup.push({
      $addFields: {
        [this.keepName]: this.filtered
          ? { $and: [this.keep, condition] }
          : condition,
      },
    });
    this.filtered = true;
  }

  private sort(order: Document) {
    const capture = Temporary("groupsort");
    const keys = Entries(order);
    this.beforeGroup.push({
      $addFields: {
        [capture]: Object.fromEntries(
          keys.map(([path], i) => [
            `k${i}`,
            SortCopy("$$ROOT", path.split(".")),
          ]),
        ),
      },
    });
    this.captures.push(capture);
    this.steps.push({
      kind: "sort",
      sortBy: Object.fromEntries(
        keys.map(([path, direction], i) => [
          `${capture}.k${i}.${path}`,
          direction,
        ]),
      ),
    });
  }

  private gateRowStage(stage: Document): Document {
    if ("$addFields" in stage) {
      return {
        $addFields: Object.fromEntries(
          Entries(Body(stage, "$addFields")).map(([name, value]) => [
            name,
            { $cond: [this.keep, value, null] },
          ]),
        ),
      };
    }
    if ("$replaceRoot" in stage) {
      const newRoot: unknown = Body(stage, "$replaceRoot").newRoot;
      return {
        $replaceRoot: { newRoot: { $cond: [this.keep, newRoot, "$$ROOT"] } },
      };
    }
    return stage;
  }

  private gateAccumulator(accumulator: Document): Document {
    const [operator, argument] = Single(accumulator);
    if (operator === "$count") {
      return { $sum: { $cond: [this.keep, 1, 0] } };
    }
    return { [operator]: { $cond: [this.keep, argument, "$$REMOVE"] } };
  }

  public finish(singleElement: boolean): GroupFieldStages {
    const { beforeGroup, afterGroup, accumulators } = this;
    if (accumulators) {
      return { beforeGroup, accumulators, afterGroup };
    }
    const sorted = this.captures.length > 0;
    const element = sorted
      ? {
          value: this.fieldRef,
          ...Object.fromEntries(
            this.captures.map((capture) => [capture, `$${capture}`]),
          ),
        }
      : this.fieldRef;
    const conditions: unknown[] = [];
    if (this.filtered) {
      conditions.push(this.keep);
    }
    if (sorted) {
      conditions.push({ $ne: [{ $type: this.fieldRef }, "missing"] });
    }
    const rewrites: Document[] = [];
    if (sorted) {
      rewrites.push({ $map: { input: this.fieldRef, in: "$$this.value" } });
    }
    if (singleElement) {
      rewrites.push({ $ifNull: [{ $first: this.fieldRef }, null] });
    }
    return {
      beforeGroup,
      accumulators: {
        [this.field]: {
          $push:
            conditions.length > 0
              ? { $cond: [{ $and: conditions }, element, "$$REMOVE"] }
              : element,
        },
      },
      afterGroup: [
        ...this.steps.flatMap((step) => ApplyStep(step, this.field)),
        ...rewrites.map((rewrite) => ({
          $addFields: { [this.field]: rewrite },
        })),
      ],
    };
  }
}

/**
 * Splits the compiled pipeline of one mapper field around the `$group` the mapper's fields share, so
 * the field's filters, orders and slices apply to that field only. A filter marks the rows it keeps
 * in a temporary flag that gates the field's later row stages and accumulators. An order copies the
 * values its keys read (`SortCopy`) into each row the field collects, and after the `$group` it ranks
 * the field's rows with `$sort` on those copies, in a `$lookup` over `$documents` of that array, so
 * the stream its sibling fields read keeps its order. A slice runs on the field's array of rows. A
 * filter or a reduction after a slice, anything but a row stage after a reduction, and a join or a
 * union are refused.
 */
export function SplitGroupField(
  pipeline: Document[],
  field: string,
  singleElement: boolean,
): GroupFieldStages {
  const split = new GroupFieldSplit(field);
  for (const stage of pipeline) {
    split.add(stage);
  }
  return split.finish(singleElement);
}
