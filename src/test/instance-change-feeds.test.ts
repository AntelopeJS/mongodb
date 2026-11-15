import { expect } from "chai";
import { CROSS_INSTANCE, Schema } from "@antelopejs/interface-database/schema";

import { GetCollection } from "../connection";
import { collectionName } from "../implementations/database/utils";

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
 * One instance's change feed of `rows`. It opens and drains by replacing a probe row, never by an
 * insert, so a feed that drops inserts still opens and the test fails on the missing insert.
 */
class Reader {
  private readonly feed: Feed;
  private pending: Promise<Step>;
  private deadline = 0;
  private probes = 0;
  private readonly probeId: string;

  public constructor(private readonly instance: Instance) {
    this.probeId = `probe-${instance ?? "default"}`;
    this.feed = Table(instance).changes().cursor();
    this.pending = this.feed.next();
    this.pending.catch(() => undefined);
  }

  public async open(): Promise<void> {
    this.arm();
    await Table(this.instance).insert({ _id: this.probeId, n: 0 }).run();
    while (!(await SettlesWithin(this.pending, PROBE_INTERVAL_MS))) {
      expect(Date.now(), "the feed opens").to.be.below(this.deadline);
      await this.probe();
    }
    await this.drain();
  }

  /** Every event other than the probe's, up to a replace of the probe written after them. */
  public async drain(): Promise<Seen[]> {
    this.arm();
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

  /**
   * Waits until the feed has read an event and leaves no read in flight, so the feed reads the
   * events written next, and looks up their documents, only once the test drains it.
   */
  public async settle(): Promise<void> {
    await this.probe();
    expect(
      await SettlesWithin(this.pending, FEED_TIMEOUT_MS),
      "the feed reads the settling probe",
    ).to.equal(true);
  }

  public async close(): Promise<void> {
    await this.feed.return();
    await Table(this.instance).get(this.probeId).delete().run();
  }

  private arm(): void {
    this.deadline = Date.now() + FEED_TIMEOUT_MS;
  }

  private async probe(): Promise<number> {
    this.probes += 1;
    await Table(this.instance)
      .get(this.probeId)
      .replace({ _id: this.probeId, n: this.probes })
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

/** Opens a feed of each instance, runs `work`, and answers what each feed read of it. */
async function Feeds(
  instances: Instance[],
  work: (readers: Reader[]) => Promise<void>,
): Promise<Seen[][]> {
  const readers = instances.map((instance) => new Reader(instance));
  try {
    for (const reader of readers) {
      await reader.open();
    }
    await work(readers);
    const seen: Seen[][] = [];
    for (const reader of readers) {
      seen.push(await reader.drain());
    }
    return seen;
  } finally {
    for (const reader of readers) {
      await reader.close();
    }
  }
}

async function SetPreImages(enabled: boolean): Promise<void> {
  const collection = await GetCollection(collectionName(SCHEMA_ID, "rows"));
  await collection.db.command({
    collMod: collection.collectionName,
    changeStreamPreAndPostImages: { enabled },
  });
}

describe("the change feed of an instance", () => {
  beforeEach(async () => {
    await schema.instance(CROSS_INSTANCE).table("rows").delete().run();
  });

  it("delivers the default instance and a named one their own inserts, updates and deletes, and none of the other's", async () => {
    let ownDefault: Seen[] = [];
    let ownNamed: Seen[] = [];
    const [onDefault, onNamed] = await Feeds([undefined, NAMED], async () => {
      ownDefault = (await Write(undefined)).events;
      ownNamed = (await Write(NAMED)).events;
    });
    expect(onDefault, "the default instance's feed").to.deep.equal(ownDefault);
    expect(onNamed, "the named instance's feed").to.deep.equal(ownNamed);
  });

  it("delivers no instance the update of a row another instance re-created under its _id before the feed read it", async () => {
    const id = "reused";
    const named = Table(NAMED);
    const fallback = Table(undefined);
    let expected: Seen[][] = [];
    const seen = await Feeds([undefined, NAMED], async (readers) => {
      for (const reader of readers) {
        await reader.settle();
      }
      await named.insert({ _id: id, n: 1 }).run();
      const inserted = await named.get(id).run();
      await named.get(id).update({ n: 2 }).run();
      const updated = await named.get(id).run();
      await named.get(id).delete().run();
      await fallback.insert({ _id: id, n: 3 }).run();
      const recreated = await fallback.get(id).run();
      expected = [
        [{ type: "added", id, old: undefined, new: recreated }],
        [
          { type: "added", id, old: undefined, new: inserted },
          { type: "removed", id, old: updated, new: undefined },
        ],
      ];
    });
    expect(seen).to.deep.equal(expected);
  });

  it("delivers no instance an update or a delete that recorded no earlier document, even when its _id is re-created", async () => {
    const id = "unseen";
    const named = Table(NAMED);
    const fallback = Table(undefined);
    let expected: Seen[][] = [];
    try {
      const seen = await Feeds([undefined, NAMED], async (readers) => {
        for (const reader of readers) {
          await reader.settle();
        }
        await SetPreImages(false);
        await named.insert({ _id: id, n: 1 }).run();
        const inserted = await named.get(id).run();
        await named.get(id).update({ n: 2 }).run();
        await named.get(id).delete().run();
        await fallback.insert({ _id: id, n: 3 }).run();
        const recreated = await fallback.get(id).run();
        expected = [
          [{ type: "added", id, old: undefined, new: recreated }],
          [{ type: "added", id, old: undefined, new: inserted }],
        ];
      });
      expect(seen).to.deep.equal(expected);
    } finally {
      await SetPreImages(true);
    }
  });

  it("refuses to open the feed of an instance on a collection that records no pre-images, and says how to enable them", async () => {
    await SetPreImages(false);
    try {
      for (const instance of [undefined, NAMED]) {
        const feed = Table(instance).changes().cursor();
        const refusal = await feed.next().then(
          () => "opened",
          (error: unknown) => String(error),
        );
        expect(refusal, `the feed of ${instance ?? "the default instance"}`)
          .to.contain("pre-images")
          .and.to.contain(`collMod: "${collectionName(SCHEMA_ID, "rows")}"`);
      }
    } finally {
      await SetPreImages(true);
    }
  });
});
