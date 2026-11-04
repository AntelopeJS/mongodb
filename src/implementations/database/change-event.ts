import type { Document } from "mongodb";

import { isString } from "./utils";

const CHANGE_TYPES = new Map([
  ["insert", "added"],
  ["replace", "modified"],
  ["update", "modified"],
  ["delete", "removed"],
]);

/** A change stream event in the shape the database interface streams. */
export function ToChangeEvent(change: Document) {
  const op: unknown = change.operationType;
  const before: unknown = change.fullDocumentBeforeChange;
  const after: unknown = change.fullDocument;
  return {
    changeType: isString(op) ? (CHANGE_TYPES.get(op) ?? op) : op,
    oldValue: op === "insert" ? undefined : before,
    newValue: op === "delete" ? undefined : after,
    _mongo: change,
  };
}
