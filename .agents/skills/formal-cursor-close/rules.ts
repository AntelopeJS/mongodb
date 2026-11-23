import type { Server, Source } from "./fake-driver";

/**
 * One cursor's life: its source, the reads its consumer makes in a row, and
 * the closes that race them. The close lands at any step of the cursor's
 * life, which the explorer enumerates.
 */
export interface Scenario extends Source {
  reads: number;
  closes: number;
}

/** What one consumer call left. */
export interface Call {
  name: string;
  state: "waiting" | "resolved" | "rejected";
  detail?: string;
}

/** One run of a scenario in one order of its steps. */
export interface Run {
  scenario: Scenario;
  server: Server;
  calls: Call[];
}

/** The largest values a pass draws; `--full` draws more. */
export interface Limits {
  items: number;
  changeItems: number;
  reads: number;
}

function forKind(kind: Source["kind"], most: number, reads: number): Scenario[] {
  const all: Scenario[] = [];
  const sizes = kind === "table" ? [1, 2] : [1];
  for (let items = 0; items <= most; items += 1) {
    for (const batchSize of sizes) {
      for (let read = 1; read <= reads; read += 1) {
        all.push({ kind, items, batchSize, reads: read, closes: 1 });
        all.push({ kind, items, batchSize, reads: read, closes: 2 });
      }
    }
  }
  return all;
}

/** Every scenario within the limits, in a fixed order. */
export function scenarios(limits: Limits): Scenario[] {
  return [
    ...forKind("table", limits.items, limits.reads),
    ...forKind("changes", limits.changeItems, limits.reads),
  ];
}

/** A rule returns what it saw break, one line each, or nothing. */
export type Rule = (run: Run) => string[];

export const RULES = {
  CallsSettle: (run) =>
    run.calls
      .filter((call) => call.state === "waiting")
      .map((call) => `${call.name} never resolved or rejected`),

  NoServerCursorAfterClose: (run) =>
    [...run.server.cursors.keys()].map(
      (id) => `server cursor ${id} is still open after every step ran and the cursor was closed`,
    ),

  NoRequestAfterClose: (run) =>
    run.server.afterClose.map((request) => `${request} was sent after the close was called`),
} satisfies Record<string, Rule>;
