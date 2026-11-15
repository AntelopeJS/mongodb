import { expect } from "chai";
import {
  type Categories,
  Profile,
  type RelationProfile,
} from "@antelopejs/interface-database-catalog";
import {
  type CommandStartedEvent,
  type CommandSucceededEvent,
  type Document,
  BSON,
} from "mongodb";

import { GetDatabase } from "../connection";
import {
  MeasuredCollection,
  type Source,
} from "../implementations/catalog/measures";
import { CategoriesOf } from "../implementations/catalog/profile";
import {
  AdminClient,
  CLIENT_DATABASE,
  RANK_COUNT,
  RankLabel,
  RankRows,
  TALLIES,
} from "./fixture";

const CATEGORY_LIMIT = 25;
const CATEGORY_MIN_ROWS = 5;
const SAMPLE_SIZE = 50_000;
const GROUP_STAGE = '"rows":{"$sum":1}';

interface ReferenceRow {
  _id: string;
  rows: number;
}

/** The categories the profile answered when the fold ran in the driver, from the same documents. */
async function Reference(
  collection: string,
  field: string,
): Promise<Categories> {
  const admin = await AdminClient();
  try {
    const rows = await admin
      .db(CLIENT_DATABASE)
      .collection(collection)
      .aggregate<ReferenceRow>([
        { $match: { [field]: { $ne: null } } },
        { $group: { _id: `$${field}`, rows: { $sum: 1 } } },
        { $sort: { rows: -1, _id: 1 } },
        { $limit: CATEGORY_LIMIT + 1 },
      ])
      .toArray();
    return {
      values: rows
        .filter((row) => row.rows >= CATEGORY_MIN_ROWS)
        .slice(0, CATEGORY_LIMIT)
        .map((row) => ({ value: row._id, rows: row.rows })),
      other: rows
        .filter(
          (row, index) =>
            row.rows < CATEGORY_MIN_ROWS || index >= CATEGORY_LIMIT,
        )
        .reduce((total, row) => total + row.rows, 0),
    };
  } finally {
    await admin.close();
  }
}

/** Every reply the module's client receives to a category measure while `work` runs, as EJSON. */
async function Received<T>(
  work: () => Promise<T>,
): Promise<{ answer: T; replies: string }> {
  const client = (await GetDatabase()).client;
  const measures = new Set<number>();
  const replies: string[] = [];
  const started = (event: CommandStartedEvent) => {
    const pipeline: unknown = event.command.pipeline;
    if (
      event.commandName === "aggregate" &&
      JSON.stringify(pipeline).includes(GROUP_STAGE)
    ) {
      measures.add(event.requestId);
    }
  };
  const succeeded = (event: CommandSucceededEvent) => {
    if (measures.has(event.requestId)) {
      replies.push(BSON.EJSON.stringify(event.reply));
    }
  };
  client.on("commandStarted", started);
  client.on("commandSucceeded", succeeded);
  try {
    const answer = await work();
    expect(replies, "category measures received").to.not.deep.equal([]);
    return { answer, replies: replies.join("\n") };
  } finally {
    client.off("commandStarted", started);
    client.off("commandSucceeded", succeeded);
  }
}

function ProfiledCategories(
  profile: RelationProfile,
  column: string,
): Categories {
  const categories = profile.columns.find(
    (profiled) => profiled.name === column,
  )?.categories;
  expect(categories?.status, column).to.equal("ok");
  return categories?.status === "ok"
    ? categories.value
    : { values: [], other: 0 };
}

async function Measure(
  collection: string,
  stages: Document[],
): Promise<Categories> {
  const source: Source = {
    collection: await MeasuredCollection(collection),
    stages,
    sample: null,
    estimatedRows: null,
  };
  const measured = await CategoriesOf(source, {
    $project: { v: "$label" },
  });
  expect(measured).to.include({ status: "ok" });
  return measured.status === "ok" ? measured.value : { values: [], other: 0 };
}

describe("the categories of an external MongoDB column", () => {
  for (const [collection, column] of [
    ["orders", "status"],
    ["tallies", "label"],
  ]) {
    it(`answers ${collection}.${column} as it did when the fold ran in the driver`, async () => {
      const profile = await Profile({
        relation: `${CLIENT_DATABASE}.${collection}`,
        columns: [column],
      });
      expect(ProfiledCategories(profile, column)).to.deep.equal(
        await Reference(collection, column),
      );
    });
  }

  it("names a value seen 5 times and folds those seen fewer times in the database", async () => {
    const { answer, replies } = await Received(() =>
      Profile({ relation: `${CLIENT_DATABASE}.tallies`, columns: ["label"] }),
    );
    expect(ProfiledCategories(answer, "label")).to.deep.equal({
      values: [
        { value: "many", rows: TALLIES.many },
        { value: "five", rows: TALLIES.five },
      ],
      other: TALLIES.four + TALLIES.solo,
    });
    expect(replies).to.include('"five"');
    expect(replies).to.not.include("four");
    expect(replies).to.not.include("solo");
  });

  it("folds the 26th most frequent value in the database, as it did in the driver", async () => {
    const { answer, replies } = await Received(() => Measure("ranks", []));
    expect(answer).to.deep.equal(await Reference("ranks", "label"));
    expect(answer.values.map((category) => category.value)).to.deep.equal(
      Array.from({ length: CATEGORY_LIMIT }, (_, index) =>
        RankLabel(index + 1),
      ),
    );
    expect(answer.other).to.equal(RankRows(CATEGORY_LIMIT + 1));
    expect(replies).to.include(`"${RankLabel(CATEGORY_LIMIT)}"`);
    expect(replies).to.not.include(RankLabel(CATEGORY_LIMIT + 1));
    expect(replies).to.not.include(RankLabel(RANK_COUNT));
  });

  it("folds a rare value of a sample in the database, as it did in the driver", async () => {
    const { answer, replies } = await Received(() =>
      Measure("tallies", [{ $sample: { size: SAMPLE_SIZE } }]),
    );
    expect(answer).to.deep.equal(await Reference("tallies", "label"));
    expect(replies).to.include('"five"');
    expect(replies).to.not.include("four");
    expect(replies).to.not.include("solo");
  });
});
