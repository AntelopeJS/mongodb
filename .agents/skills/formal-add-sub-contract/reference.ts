
/**
 * What `add` and `sub` answer by the interface-database contract, computed
 * in plain JavaScript: `add` moves a date by seconds, adds two numbers and
 * concatenates two strings or two lists, and answers null when its operand
 * is null; `sub` moves a date back by seconds, answers the seconds between
 * two dates and subtracts two numbers.
 */
export const TYPES = ["date", "number", "string", "list", "null"] as const;

export type Type = (typeof TYPES)[number];

/** A value a field or a constant holds. */
export type Value = Date | number | string | number[] | null;

export type Operation = "add" | "sub";

const MILLISECONDS_PER_SECOND = 1000;

function isNumber<Operand>(value: Operand): value is Operand & number {
  return typeof value === "number";
}

function isString<Operand>(value: Operand): value is Operand & string {
  return typeof value === "string";
}

/**
 * The answer the contract gives, or `undefined` when the contract does not cover the combination:
 * a null receiver, a null operand of `sub`, and operands of another type than the receiver's.
 */
export function reference(operation: Operation, receiver: Value, operand: Value): Value | undefined {
  if (receiver === null) {
    return undefined;
  }
  if (operand === null) {
    return operation === "add" ? null : undefined;
  }
  if (receiver instanceof Date) {
    return onDate(operation, receiver, operand);
  }
  if (isNumber(receiver) && isNumber(operand)) {
    return operation === "add" ? receiver + operand : receiver - operand;
  }
  if (operation === "add" && isString(receiver) && isString(operand)) {
    return receiver + operand;
  }
  if (operation === "add" && Array.isArray(receiver) && Array.isArray(operand)) {
    return [...receiver, ...operand];
  }
  return undefined;
}

function onDate(operation: Operation, receiver: Date, operand: Value): Value | undefined {
  if (isNumber(operand)) {
    const seconds = operand * MILLISECONDS_PER_SECOND;
    return new Date(receiver.getTime() + (operation === "add" ? seconds : -seconds));
  }
  if (operation === "sub" && operand instanceof Date) {
    return (receiver.getTime() - operand.getTime()) / MILLISECONDS_PER_SECOND;
  }
  return undefined;
}
