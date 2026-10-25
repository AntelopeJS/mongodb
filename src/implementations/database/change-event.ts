import type { Document } from "mongodb";

const CHANGE_TYPES = new Map([
  ["insert", "added"],
  ["replace", "modified"],
  ["update", "modified"],
  ["delete", "removed"],
]);

/** A change stream event in the shape the database interface streams. */
export function ToChangeEvent(change: Document) {
  const op = change.operationType;
  return {
    changeType: CHANGE_TYPES.get(op) ?? op,
    oldValue: op === "insert" ? undefined : change.fullDocumentBeforeChange,
    newValue: op === "delete" ? undefined : change.fullDocument,
    _mongo: change,
  };
}
