import { type Document, MongoBulkWriteError, MongoServerError } from "mongodb";
import { DuplicateKeyError } from "@antelopejs/interface-database";

import { isDocument, isString } from "./utils";

/** What went wrong with a write, in terms a caller can act on without reading MongoDB codes. */
export type ExternalWriteErrorKind = "invalid" | "forbidden";

/**
 * A write to an external collection the database, or the adapter on its behalf, refused.
 *
 * It carries what a later mapping to an HTTP status needs, the kind and the fields, without
 * importing anything an interface package defines; the message is written to be read as it is.
 */
export class ExternalWriteError extends Error {
  public readonly kind: ExternalWriteErrorKind;
  public readonly fields: string[];
  public readonly code?: number;

  public constructor(
    message: string,
    kind: ExternalWriteErrorKind,
    fields: string[] = [],
    code?: number,
  ) {
    super(message);
    this.name = "ExternalWriteError";
    this.kind = kind;
    this.fields = fields;
    this.code = code;
  }
}

/** One refusal of the validation before a write, naming its field. */
export interface InvalidValue {
  field: string;
  reason: string;
}

/** The write a validation refused before it ran, naming every field that failed. */
export function InvalidValues(
  collection: string,
  failures: InvalidValue[],
): ExternalWriteError {
  return new ExternalWriteError(
    failures
      .map(
        (failure) =>
          `Field '${failure.field}' of ${collection} ${failure.reason}`,
      )
      .join("; "),
    "invalid",
    failures.map((failure) => failure.field),
  );
}

const DUPLICATE_KEY_CODE = 11000;
const VALIDATION_CODE = 121;
const UNAUTHORIZED_CODE = 13;
const PRIMARY_INDEX = "_id_";
const PRIMARY_KEY = "_id";
const DUPLICATE_INDEX_PATTERN = / index: (\S+) dup key/;

/** What a refusal says, from the server error or, for a bulk write, its first write error. */
interface ServerFailure {
  code: number;
  message: string;
  errInfo?: Document;
  /** The fields of the index a duplicate key broke. */
  keyFields: string[];
}

function FailureOf(error: MongoServerError): ServerFailure {
  const [first] =
    error instanceof MongoBulkWriteError ? [error.writeErrors].flat() : [];
  // SAFETY: the server's write error carries keyPattern on a duplicate key; the driver's type omits it.
  const written = first?.err as Document | undefined;
  const keyPattern: unknown = first ? written?.keyPattern : error.keyPattern;
  const keyFields = isDocument(keyPattern) ? Object.keys(keyPattern) : [];
  return first
    ? {
        code: Number(first.code),
        message: first.errmsg ?? error.message,
        errInfo: first.errInfo,
        keyFields,
      }
    : {
        code: Number(error.code),
        message: error.message,
        errInfo: error.errInfo,
        keyFields,
      };
}

function ValidatorFields(error: ServerFailure): string[] {
  const details: unknown = error.errInfo?.details;
  const rules: unknown = isDocument(details)
    ? details.schemaRulesNotSatisfied
    : undefined;
  const ruleList: unknown[] = Array.isArray(rules) ? rules : [];
  const fields: string[] = [];
  for (const rule of ruleList) {
    if (!isDocument(rule)) {
      continue;
    }
    const properties: unknown = rule.propertiesNotSatisfied;
    const propertyList: unknown[] = Array.isArray(properties) ? properties : [];
    for (const property of propertyList) {
      const name: unknown = isDocument(property)
        ? property.propertyName
        : undefined;
      if (isString(name) && name) {
        fields.push(name);
      }
    }
    const missing: unknown = rule.missingProperties;
    const missingList: unknown[] = Array.isArray(missing) ? missing : [];
    fields.push(...missingList.filter(isString));
  }
  return fields;
}

const MAPPINGS = new Map<
  number,
  (error: ServerFailure, collection: string) => Error
>([
  [
    DUPLICATE_KEY_CODE,
    (error) => {
      const index = DUPLICATE_INDEX_PATTERN.exec(error.message)?.[1];
      return new DuplicateKeyError(
        index === undefined || index === PRIMARY_INDEX ? PRIMARY_KEY : index,
        error.keyFields,
      );
    },
  ],
  [
    VALIDATION_CODE,
    (error, collection) => {
      const fields = ValidatorFields(error);
      return new ExternalWriteError(
        `The document breaks the validator of ${collection}${fields.length > 0 ? ` on ${fields.join(", ")}` : ""}`,
        "invalid",
        fields,
        Number(error.code),
      );
    },
  ],
  [
    UNAUTHORIZED_CODE,
    (error, collection) =>
      new ExternalWriteError(
        `The connected account may not write to ${collection}`,
        "forbidden",
        [],
        Number(error.code),
      ),
  ],
]);

/**
 * A server error from writing an external collection, as a readable error: a duplicate key is
 * interface-database's `DuplicateKeyError`, naming the native index and quoting no value.
 */
export function ExternalWriteFailure(error: Error, collection: string): Error {
  if (!(error instanceof MongoServerError)) {
    return error;
  }
  const failure = FailureOf(error);
  const mapping = MAPPINGS.get(failure.code);
  return mapping ? mapping(failure, collection) : error;
}
