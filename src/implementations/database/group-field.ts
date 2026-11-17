import type { Document } from "mongodb";

import { isDocument, Temporary } from "./utils";

/** One field of a group mapper: the pipeline its stream compiled to, over the temporary `field`. */
export interface GroupMapperField {
  field: string;
  pipeline: Document[];
  singleElement: boolean;
}

/** The stages of a group mapper around the `$group` its fields share. */
export interface GroupMapperStages {
  /** Row-wise stages that only write their field's temporaries, so every row reaches the `$group`. */
  beforeGroup: Document[];
  /** The fields' accumulators, merged into the shared `$group`. */
  accumulators: Document;
  /** Stages that rewrite each field's value once the `$group` has reduced it. */
  afterGroup: Document[];
}

/**
 * A step that orders or slices the rows a field collected. `at` is the length of the field's
 * row-wise stages when the order came, so the order reads the field's value as it was then.
 */
type ArrayStep =
  | { kind: "sort"; order: Document; at: number }
  | {
      kind: "slice";
      operator: "$skip" | "$limit";
      stage: Document;
      constant: boolean;
    };

/**
 * One step on a field's array of rows after the `$group`: a `stage` that runs in a `$lookup` over
 * `$documents` of the rows, or a `rewrite` expression of the array.
 */
type ArrayOperation = { stage: Document } | { rewrite: Document };

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

const VALUE = "v";

function IsNumber<Value>(value: Value): value is Value & number {
  return typeof value === "number";
}

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
 * path, and any other value stays whole. From a numeric field on, which `$sort` reads as an array
 * index, the value stays whole.
 */
function SortCopy(
  value: string | Document,
  fields: string[],
): string | Document {
  const [field, ...rest] = fields;
  if (field === undefined || /^\d+$/.test(field)) {
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

/** Walks the compiled stages of one mapper field and sorts each to its place around the `$group`. */
class GroupFieldSplit {
  private readonly keepName = Temporary("groupkeep");
  private readonly keep = `$${this.keepName}`;
  private readonly fieldRef: string;
  private filtered = false;
  private sliced = false;
  private readonly steps: ArrayStep[] = [];
  private readonly beforeGroup: Document[] = [];
  private readonly afterGroup: Document[] = [];
  private accumulators?: Document;

  public constructor(
    private readonly field: string,
    private readonly singleElement: boolean,
  ) {
    this.fieldRef = `$${field}`;
  }

  /** Whether the field answers its rows in an order, rather than reducing them to one value. */
  public get readsOrder(): boolean {
    return this.accumulators === undefined;
  }

  /** Whether the only order of the field comes before its slices, so it can rank the rows before the `$group`. */
  public get leadingOrder(): boolean {
    const orders = this.steps.filter((step) => step.kind === "sort");
    return orders.length === 1 && this.steps[0]?.kind === "sort";
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
        this.steps.push({
          kind: "sort",
          order: Body(stage, operator),
          at: this.beforeGroup.length,
        });
        return;
      case "$skip":
      case "$limit":
        this.steps.push({
          kind: "slice",
          operator: operator === "$skip" ? "$skip" : "$limit",
          stage,
          constant: IsNumber(value),
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

  /** The path of an order key below the field's value, which `getField` prefixes with the field. */
  private keyPath(path: string): string[] {
    if (path === this.field) {
      return [];
    }
    return path.startsWith(`${this.field}.`)
      ? path.slice(this.field.length + 1).split(".")
      : Refuse("an orderBy on a value outside its field");
  }

  /**
   * The field's stages once the mapper is known. With `rankFirst`, the field's one order runs as a
   * `$sort` among its row-wise stages, so the rows reach the `$group` in that order. Otherwise each
   * order copies the values its keys read into the row the field collects, and runs on those copies
   * after the `$group`.
   */
  public finish(rankFirst: boolean): GroupMapperStages {
    const { beforeGroup, afterGroup, accumulators } = this;
    if (accumulators) {
      return { beforeGroup, accumulators, afterGroup };
    }
    const element: Document = { [VALUE]: this.fieldRef };
    const operations: ArrayOperation[] = [];
    const rows = [...beforeGroup];
    for (const [index, step] of this.steps.entries()) {
      if (step.kind === "slice") {
        operations.push(this.slice(step));
        continue;
      }
      const at = step.at + rows.length - beforeGroup.length;
      if (rankFirst) {
        rows.splice(at, 0, { $sort: step.order });
        continue;
      }
      const copy = this.copyOrder(step.order, `s${index}`);
      rows.splice(at, 0, copy.capture);
      element[`s${index}`] = copy.value;
      operations.push({ stage: copy.sort });
    }
    const copies = Object.keys(element).length > 1;
    const conditions: unknown[] = [];
    if (this.filtered) {
      conditions.push(this.keep);
    }
    if (copies) {
      conditions.push({ $ne: [{ $type: this.fieldRef }, "missing"] });
    }
    const collected = copies ? element : this.fieldRef;
    const rewrites: Document[] = [];
    if (operations.length > 0) {
      rewrites.push(...this.arrayStages(operations, copies));
    }
    if (this.singleElement) {
      rewrites.push({
        $addFields: {
          [this.field]: { $ifNull: [{ $first: this.fieldRef }, null] },
        },
      });
    }
    return {
      beforeGroup: rows,
      accumulators: {
        [this.field]: {
          $push:
            conditions.length > 0
              ? { $cond: [{ $and: conditions }, collected, "$$REMOVE"] }
              : collected,
        },
      },
      afterGroup: rewrites,
    };
  }

  /**
   * The stages of an order that runs after the `$group`: `capture` copies the values `order` ranks
   * on into a temporary of each row, the collected row holds `value` under `name`, and `sort` ranks
   * the collected rows on it.
   */
  private copyOrder(order: Document, name: string) {
    const capture = Temporary("groupsort");
    const keys = Entries(order).map(
      ([path, direction], at): [string[], unknown, string] => [
        this.keyPath(path),
        direction,
        `k${at}`,
      ],
    );
    return {
      capture: {
        $addFields: {
          [capture]: Object.fromEntries(
            keys.map(([path, , key]) => [key, SortCopy(this.fieldRef, path)]),
          ),
        },
      },
      value: `$${capture}`,
      sort: {
        $sort: Object.fromEntries(
          keys.map(([path, direction, key]) => [
            [name, key, ...path].join("."),
            direction,
          ]),
        ),
      },
    };
  }

  /**
   * A slice whose count is a number runs as the `$skip` or `$limit` stage it compiled to, so it
   * reads the count as a top-level query does. A count computed per group, such as the group key,
   * slices the array with `$slice`, since those stages take only a constant.
   */
  private slice(step: Extract<ArrayStep, { kind: "slice" }>): ArrayOperation {
    const { operator, stage, constant } = step;
    if (constant) {
      return { stage };
    }
    const count: unknown = stage[operator];
    const array = this.fieldRef;
    return {
      rewrite:
        operator === "$skip"
          ? { $slice: [array, count, { $max: [{ $size: array }, 1] }] }
          : { $slice: [array, count] },
    };
  }

  /**
   * Runs the field's orders and slices on its array of rows, each row as `{ v: row }` unless it
   * carries order copies. Consecutive stages share one `$lookup` over `$documents` of the rows.
   */
  private arrayStages(
    operations: ArrayOperation[],
    copies: boolean,
  ): Document[] {
    const array = this.fieldRef;
    const result: Document[] = copies
      ? []
      : [
          {
            $addFields: {
              [this.field]: {
                $map: { input: array, in: { [VALUE]: "$$this" } },
              },
            },
          },
        ];
    let stages: Document[] = [];
    const flush = () => {
      if (stages.length > 0) {
        result.push({
          $lookup: {
            let: { rows: array },
            pipeline: [{ $documents: "$$rows" }, ...stages],
            as: this.field,
          },
        });
        stages = [];
      }
    };
    for (const operation of operations) {
      if ("stage" in operation) {
        stages.push(operation.stage);
        continue;
      }
      flush();
      result.push({ $addFields: { [this.field]: operation.rewrite } });
    }
    flush();
    result.push({
      $addFields: {
        [this.field]: { $map: { input: array, in: `$$this.${VALUE}` } },
      },
    });
    return result;
  }
}

/**
 * Splits the compiled pipelines of a group mapper's fields around the `$group` they share, so each
 * field's filters, orders and slices apply to that field only. A filter marks the rows it keeps in a
 * temporary flag that gates the field's later row stages and accumulators. When one field alone
 * answers its rows in an order and that order comes before its slices, the order runs as a `$sort`
 * before the `$group`, as a top-level `orderBy` does, and the field collects only its values.
 * Otherwise an order copies the values its keys read (`SortCopy`) into each row the field collects,
 * so the rows its sibling fields read keep their order. After the `$group`, a field's remaining
 * orders and its slices run in a `$lookup` over `$documents` of its rows. A filter or a reduction
 * after a slice, anything but a row stage after a reduction, and a join or a union are refused.
 */
export function SplitGroupFields(
  fields: GroupMapperField[],
): GroupMapperStages {
  const splits = fields.map(({ field, pipeline, singleElement }) => {
    const split = new GroupFieldSplit(field, singleElement);
    for (const stage of pipeline) {
      split.add(stage);
    }
    return split;
  });
  const ordered = splits.filter((split) => split.readsOrder);
  const rankFirst =
    ordered.length === 1 && ordered[0]?.leadingOrder ? ordered[0] : undefined;
  const merged: GroupMapperStages = {
    beforeGroup: [],
    accumulators: {},
    afterGroup: [],
  };
  for (const split of splits) {
    const stages = split.finish(split === rankFirst);
    merged.beforeGroup.push(...stages.beforeGroup);
    Object.assign(merged.accumulators, stages.accumulators);
    merged.afterGroup.push(...stages.afterGroup);
  }
  return merged;
}
