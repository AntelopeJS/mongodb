/**
 * Runs a scenario under every order of its steps, depth first. A step is a
 * driver call of `fake-mongo.ts` or the issue of a registration or a query.
 * Once Node has nothing else to run, the explorer releases one pending step,
 * and Node runs the code it unblocks up to that code's next step. An order
 * is the list of picks, so a failing order replays exactly.
 */
interface Pending {
  resolve: () => void;
}

interface Choice {
  pick: number;
  options: number;
}

/** How many orders of one scenario ran, and whether they were all of them. */
export interface Exploration {
  orders: number;
  exhausted: boolean;
}

function settle(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

/** One run of a scenario: it releases steps in the order `prefix` names, then oldest first. */
export class Schedule {
  readonly choices: Choice[] = [];
  private readonly pending: Pending[] = [];
  private readonly prefix: number[];

  constructor(prefix: number[]) {
    this.prefix = prefix;
  }

  /** Resolves when the explorer releases this step. */
  step(): Promise<void> {
    return new Promise((resolve) => this.pending.push({ resolve }));
  }

  /** Releases steps until none is pending. */
  async drain(): Promise<void> {
    for (;;) {
      await settle();
      if (this.pending.length === 0) {
        return;
      }
      const pick = this.prefix[this.choices.length] ?? 0;
      this.choices.push({ pick, options: this.pending.length });
      this.pending.splice(pick, 1)[0]?.resolve();
    }
  }
}

function nextPrefix(choices: Choice[]): number[] | undefined {
  for (let depth = choices.length - 1; depth >= 0; depth -= 1) {
    const { pick, options } = choices[depth] ?? { pick: 0, options: 0 };
    if (pick + 1 < options) {
      return [...choices.slice(0, depth).map((choice) => choice.pick), pick + 1];
    }
  }
  return undefined;
}

/** Calls `run` once per order of a scenario, up to `limit` orders. */
export async function exploreOrders(
  run: (schedule: Schedule) => Promise<void>,
  limit: number,
): Promise<Exploration> {
  let prefix: number[] | undefined = [];
  let orders = 0;
  while (prefix && orders < limit) {
    const schedule: Schedule = new Schedule(prefix);
    await run(schedule);
    orders += 1;
    prefix = nextPrefix(schedule.choices);
  }
  return { orders, exhausted: prefix === undefined };
}
