import fc from "fast-check";
import { IsDuplicateKeyError } from "@antelopejs/interface-database";

import { type Event, FOREIGN, OWN, type Store } from "./fake-collection";

const IDS = ["a", "b", "c"] as const;
const MAX_WRITER_OPERATIONS = 5;

/**
 * The work of one scenario; every actor runs at once. It races on one
 * collection: writers that each run one conflict insert of their ids, a
 * deleter that removes rows of the writers' instance, and another instance
 * that inserts rows under the same `_id`s.
 */
export interface Scenario {
  mode: "update" | "replace";
  /** The ids each writer inserts, in the order of its batch. */
  writers: string[][];
  /** The rows of the writers' instance a deleter removes, one step each. */
  deletes: string[];
  /** The rows another instance inserts, one step each, refused when the `_id` is held. */
  foreign: string[];
  /** Rows of the writers' instance that exist before the run. */
  seeded: string[];
}

const idArb = fc.constantFrom(...IDS);

/** The scenarios, drawn with a fixed seed: up to three writers, four operations in all. */
export const scenarioArb: fc.Arbitrary<Scenario> = fc
  .record({
    mode: fc.constantFrom<Scenario["mode"]>("update", "replace"),
    writers: fc
      .array(fc.uniqueArray(idArb, { minLength: 1, maxLength: IDS.length }), {
        minLength: 1,
        maxLength: 3,
      })
      .filter(
        (writers) =>
          writers.reduce((total, ids) => total + ids.length, 0) <= MAX_WRITER_OPERATIONS,
      ),
    deletes: fc.array(idArb, { maxLength: 2 }),
    foreign: fc.array(idArb, { maxLength: 1 }),
    seeded: fc.uniqueArray(idArb, { maxLength: 1 }),
  });

/** What a writer's `insert(..., { conflict })` left. */
export interface Outcome {
  writer: string;
  ids: string[];
  state: "waiting" | "resolved" | "rejected";
  /** Whether the rejection was a `DuplicateKeyError`; other errors are `rejected` with `other`. */
  error?: "duplicate key" | "other";
  detail?: string;
}

/** One run of a scenario in one order of its steps. */
export interface Run {
  scenario: Scenario;
  store: Store;
  outcomes: Outcome[];
}

/** Describes a rejection as the rule that names it prints it. */
export function describeError(error: Error): Pick<Outcome, "error" | "detail"> {
  return {
    error: IsDuplicateKeyError(error) ? "duplicate key" : "other",
    detail: `${error.name}: ${error.message}`,
  };
}

function lastIndex(events: Event[], matches: (event: Event) => boolean): number {
  for (let at = events.length - 1; at >= 0; at -= 1) {
    if (matches(events[at])) {
      return at;
    }
  }
  return -1;
}

function foreignHeld(run: Run, id: string): boolean {
  return run.store.find(id)?._instance === FOREIGN;
}

function refusals(run: Run): string[] {
  return run.outcomes
    .filter((outcome) => outcome.state === "rejected")
    .flatMap((outcome) => {
      const id = run.store.refused.get(outcome.writer);
      if (outcome.error === "duplicate key" && id !== undefined && foreignHeld(run, id)) {
        return [];
      }
      return [
        outcome.error === "duplicate key"
          ? `${outcome.writer} was refused as a duplicate for ${id ?? "an unknown _id"}, which no other instance holds`
          : `${outcome.writer} rejected with ${outcome.detail ?? "an error"}`,
      ];
    });
}

/** A rule returns what it saw break, one line each, or nothing. */
export type Rule = (run: Run) => string[];

export const RULES = {
  WriterSettles: (run) =>
    run.outcomes
      .filter((outcome) => outcome.state === "waiting")
      .map((outcome) => `${outcome.writer} never resolved or rejected`),

  ForeignRowKept: (run) =>
    run.store.events.flatMap((event) => {
      if (event.kind !== "foreign") {
        return [];
      }
      const row = run.store.find(event.id);
      const kept = JSON.stringify(row) === JSON.stringify({ _id: event.id, _instance: FOREIGN, foreign: true });
      return kept ? [] : [`the row the other instance inserted under ${event.id} is now ${JSON.stringify(row)}`];
    }),

  DuplicateOnlyForForeignId: (run) =>
    run.scenario.deletes.length === 0 ? refusals(run) : [],

  DuplicateOnlyForForeignIdUnderDeletes: (run) =>
    run.scenario.deletes.length > 0 ? refusals(run) : [],

  AppliedOnce: (run) => {
    const seen = new Set<string>();
    return run.store.events.flatMap((event) => {
      if (event.kind !== "apply") {
        return [];
      }
      const key = `${event.writer} ${event.id}`;
      const again = seen.has(key);
      seen.add(key);
      return again ? [`${event.writer} wrote ${event.id} twice`] : [];
    });
  },

  ConflictInsertsApply: (run) =>
    run.outcomes
      .filter((outcome) => outcome.state === "resolved")
      .flatMap((outcome) =>
        outcome.ids.flatMap((id) => {
          const events = run.store.events;
          const applied = lastIndex(events, (e) => e.kind === "apply" && e.writer === outcome.writer && e.id === id);
          if (applied === -1) {
            return [`${outcome.writer} resolved without writing ${id}`];
          }
          const removed = lastIndex(events, (e) => e.kind === "remove" && e.id === id);
          const row = run.store.find(id, OWN);
          if (removed > applied) {
            return [];
          }
          if (!row) {
            return [`${outcome.writer} wrote ${id} and no row of its instance remains`];
          }
          return run.scenario.mode === "update" && row[outcome.writer] !== true
            ? [`${outcome.writer} wrote ${id} and its field is not in the row`]
            : [];
        }),
      ),
} satisfies Record<string, Rule>;
