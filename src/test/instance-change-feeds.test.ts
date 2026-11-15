import { expect } from "chai";
import { CROSS_INSTANCE, Schema } from "@antelopejs/interface-database/schema";

interface Row {
  _id: string;
  n: number;
}

const SCHEMA_ID = "instance-change-feeds";
const NAMED = "tenant-a";
const FEED_TIMEOUT_MS = 5000;
const PROBE_INTERVAL_MS = 200;

const schema = new Schema<{ rows: Row }>(SCHEMA_ID, {
  rows: { fields: {}, indexes: {} },
});

type Instance = string | undefined;

function Table(instance: Instance) {
  return schema.instance(instance).table("rows");
}

type Feed = ReturnType<
  ReturnType<ReturnType<typeof Table>["changes"]>["cursor"]
>;
type Step = Awaited<ReturnType<Feed["next"]>>;
type Change = Exclude<Step["value"], void>;

/** What a test reads of an event: its type, its row id, and the row before and after. */
interface Seen {
  type: unknown;
  id: unknown;
  old: unknown;
  new: unknown;
}

function SeenOf(change: Change): Seen {
  return {
    type: change.changeType,
    id: change.newValue?._id ?? change.oldValue?._id,
    old: change.oldValue,
    new: change.newValue,
  };
}

function SettlesWithin(
  pending: Promise<unknown>,
  ms: number,
): Promise<boolean> {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(false), ms);
    const settle = () => {
      clearTimeout(timer);
      resolve(true);
    };
    pending.then(settle, settle);
  });
}

/**
 * One instance's change feed of `rows`. It opens and drains by updating a probe row, never by an
 * insert, so a feed that drops inserts still opens and the test fails on the missing insert.
 */
class Reader {
  private readonly feed: Feed;
  private pending: Promise<Step>;
  private readonly deadline = Date.now() + FEED_TIMEOUT_MS;
  private probes = 0;
  private readonly probeId: string;

  public constructor(private readonly instance: Instance) {
    this.probeId = `probe-${instance ?? "default"}`;
    this.feed = Table(instance).changes().cursor();
    this.pending = this.feed.next();
    this.pending.catch(() => undefined);
  }

  public async open(): Promise<void> {
    await Table(this.instance).insert({ _id: this.probeId, n: 0 }).run();
    while (!(await SettlesWithin(this.pending, PROBE_INTERVAL_MS))) {
      expect(Date.now(), "the feed opens").to.be.below(this.deadline);
      await this.probe();
    }
    await this.drain();
  }

  /** Every event other than the probe's, up to an update of the probe written after them. */
  public async drain(): Promise<Seen[]> {
    const marker = await this.probe();
    const seen: Seen[] = [];
    for (;;) {
      const change = await this.next();
      const read = SeenOf(change);
      if (read.id !== this.probeId) {
        seen.push(read);
      } else if (change.newValue?.n === marker) {
        return seen;
      }
    }
  }

  public async close(): Promise<void> {
    await this.feed.return();
    await Table(this.instance).get(this.probeId).delete().run();
  }

  private async probe(): Promise<number> {
    this.probes += 1;
    await Table(this.instance)
      .get(this.probeId)
      .update({ n: this.probes })
      .run();
    return this.probes;
  }

  private async next(): Promise<Change> {
    const remaining = Math.max(this.deadline - Date.now(), 0);
    expect(
      await SettlesWithin(this.pending, remaining),
      "a change event before the deadline",
    ).to.equal(true);
    const step = await this.pending;
    this.pending = this.feed.next();
    this.pending.catch(() => undefined);
    if (step.done) {
      expect.fail("the feed ended");
    }
    return step.value;
  }
}

/**
 * Inserts two rows of `instance`, updates the first and deletes the second, and answers the events
 * a feed of it reads, with each row as reads returned it. The updated row stays, because an update
 * event carries the row as it is when the event is read.
 */
async function Write(instance: Instance) {
  const [kept, removed] = ["kept", "removed"].map(
    (name) => `${name}-${instance ?? "default"}`,
  );
  const table = Table(instance);
  await table.insert({ _id: kept, n: 1 }).run();
  const inserted = await table.get(kept).run();
  await table.get(kept).update({ n: 2 }).run();
  const updated = await table.get(kept).run();
  await table.insert({ _id: removed, n: 3 }).run();
  const doomed = await table.get(removed).run();
  await table.get(removed).delete().run();
  return {
    events: [
      { type: "added", id: kept, old: undefined, new: inserted },
      { type: "modified", id: kept, old: inserted, new: updated },
      { type: "added", id: removed, old: undefined, new: doomed },
      { type: "removed", id: removed, old: doomed, new: undefined },
    ],
  };
}

describe("the change feed of an instance", () => {
  beforeEach(async () => {
    await schema.instance(CROSS_INSTANCE).table("rows").delete().run();
  });

  it("delivers the default instance and a named one their own inserts, updates and deletes, and none of the other's", async () => {
    const readers = [new Reader(undefined), new Reader(NAMED)];
    try {
      for (const reader of readers) {
        await reader.open();
      }
      const ownDefault = await Write(undefined);
      const ownNamed = await Write(NAMED);
      const [onDefault, onNamed] = [
        await readers[0].drain(),
        await readers[1].drain(),
      ];
      expect(onDefault, "the default instance's feed").to.deep.equal(
        ownDefault.events,
      );
      expect(onNamed, "the named instance's feed").to.deep.equal(
        ownNamed.events,
      );
    } finally {
      for (const reader of readers) {
        await reader.close();
      }
    }
  });
});
