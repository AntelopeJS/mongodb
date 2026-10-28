import assert from "node:assert";
import type { Document } from "mongodb";
import type { Stream } from "@antelopejs/interface-database";
import type { QueryStage } from "@antelopejs/interface-database/common";

import type { DecodingContext } from "./utils";
import type { ExternalCollection } from "./external-descriptor";
import { DecodeFunction, DecodeValue, IsConstantValue } from "./expression";
import {
  type BsonValue,
  NativeFilter,
  NormalizingStages,
  ComparesNatively,
  SortsNatively,
} from "./external-read";
import {
  KeyGetMatch,
  KeyRange,
  KeysFilter,
  LookupCandidates,
  NumericReduction,
  RunsOnRawDocuments,
} from "./external-selection";
import { Temporary } from "./utils";
import { ExternalMapperRefusal } from "./external-refusals";

type StageArgument = QueryStage["args"][number];

/** What a lookup needs of the read on the other side. */
interface ExternalRead {
  external?: ExternalStages;
  pipeline: Document[];
}

const SINGLE_FIELD = 1;
const ROW_RESULTS = new Set(["table", "selection", "stream"]);
const NOT_NATIVE = new Error("the filter reads a subquery");

/**
 * The stages of a read on an external collection, over the query's own pipeline: those that come
 * first run on the raw documents, so indexes serve them, and the one normalizing projection goes in
 * before the first stage that needs the declared row.
 */
export class ExternalStages {
  private isNormalized = false;

  public constructor(
    public readonly collection: ExternalCollection,
    private readonly pipeline: Document[],
    private readonly context: DecodingContext,
  ) {}

  public get isRaw(): boolean {
    return !this.isNormalized;
  }

  public normalize() {
    if (!this.isNormalized) {
      this.isNormalized = true;
      this.pipeline.push(...NormalizingStages(this.collection));
    }
  }

  /** The values an update writes; an update computed from the row is refused. */
  public updateValue(value: StageArgument): StageArgument {
    if (value?.stage === "func") {
      throw ExternalMapperRefusal(this.collection);
    }
    return value;
  }

  /** Marks the rows as consumed by a reduction, after which nothing is normalized. */
  public reduced() {
    this.isNormalized = true;
  }

  public beforeStage(stage: QueryStage) {
    if (!RunsOnRawDocuments(stage)) {
      this.normalize();
    }
  }

  /** Ends a read on normalized rows, unless a reduction already consumed them. */
  public finish(resultType: string) {
    if (ROW_RESULTS.has(resultType)) {
      this.normalize();
    }
  }

  public async get(key: StageArgument) {
    const decoded = IsConstantValue(key)
      ? undefined
      : await DecodeValue(key, this.context);
    this.pipeline.push({
      // SAFETY: a decoded query value is plain data, BSON values and expression documents.
      $match: KeyGetMatch(this.collection, key, decoded as BsonValue),
    });
  }

  /** Pushes `getAll`'s native match and answers true, or normalizes and answers false. */
  public getAll(fields: string[], keys: StageArgument): boolean {
    const native =
      fields.length === SINGLE_FIELD
        ? KeysFilter(this.collection, fields[0], keys)
        : undefined;
    if (!native) {
      this.normalize();
      return false;
    }
    this.pipeline.push({ $match: native });
    return true;
  }

  /** Pushes `between`'s native match and answers true, or leaves the range to the owned stage. */
  public between(field: string, stage: QueryStage): boolean {
    const range = KeyRange(
      this.collection,
      field,
      stage.args[0],
      stage.args[1],
    );
    if (range) {
      this.pipeline.push({ $match: range });
      return true;
    }
    if (!ComparesNatively(this.collection, field)) {
      this.normalize();
    }
    return false;
  }

  private async nativeFilter(
    predicate: QueryStage,
  ): Promise<Document | undefined> {
    const subquery = this.context.subquery;
    this.context.subquery = () => {
      throw NOT_NATIVE;
    };
    try {
      const compiled = await DecodeFunction(predicate, this.context, [
        "$$ROOT",
      ]);
      // SAFETY: a compiled filter is plain data, BSON values and expression documents.
      return NativeFilter(this.collection, compiled as BsonValue);
    } catch (error) {
      if (error === NOT_NATIVE) {
        return undefined;
      }
      throw error;
    } finally {
      this.context.subquery = subquery;
    }
  }

  /** Pushes a filter's native match and answers true, or normalizes and answers false. */
  public async filter(predicate: QueryStage): Promise<boolean> {
    if (this.isRaw) {
      const native = await this.nativeFilter(predicate);
      if (native) {
        this.pipeline.push({ $match: native });
        return true;
      }
    }
    this.normalize();
    return false;
  }

  public orderBy(fields: string[]) {
    if (!SortsNatively(this.collection, fields)) {
      this.normalize();
    }
  }

  public count(stage: QueryStage) {
    if (stage.options.field) {
      this.normalize();
    }
  }

  /**
   * Reduces a declared number field over its native values, converting the one result, and
   * answers true; a sum of Decimal128 stays exact until then. Anything else normalizes first.
   */
  public reduce(
    stage: QueryStage,
    operator: string,
    wrapped: () => string,
  ): boolean {
    const field: string | undefined = stage.options.field;
    const isNumberField =
      field !== undefined && this.collection.fields[field]?.kind === "number";
    if (!this.isRaw || !isNumberField) {
      this.normalize();
      return false;
    }
    this.pipeline.push(...NumericReduction(field, operator, wrapped()));
    this.reduced();
    return true;
  }

  /**
   * Appends a lookup from a local field to another external collection: the local value converted
   * the way the other side stores it, matched natively, then the other side's rows.
   */
  public async lookup(
    stage: QueryStage,
    localField: string,
    decode: (stages: QueryStage[]) => Promise<ExternalRead>,
  ) {
    // SAFETY: lookup's first argument is the stream the interface built it from.
    const stream = stage.args[0] as Stream<unknown>;
    const other = await decode(stream.build());
    assert(
      other.external,
      "lookup reads another collection of the same external instance",
    );
    const otherKey: string = stage.options.otherKey;
    const candidates = Temporary("candidates");
    const matches = Temporary("lookup");
    this.pipeline.push(
      {
        $set: {
          [candidates]: LookupCandidates(
            other.external.collection,
            otherKey,
            `$${localField}`,
          ),
        },
      },
      {
        $lookup: {
          from: other.external.collection.collection,
          localField: candidates,
          foreignField: otherKey,
          as: matches,
          pipeline: other.pipeline,
        },
      },
      {
        $set: {
          [localField]: {
            $cond: [
              { $isArray: `$${localField}` },
              `$${matches}`,
              { $arrayElemAt: [`$${matches}`, 0] },
            ],
          },
        },
      },
      { $unset: [candidates, matches] },
    );
  }
}
