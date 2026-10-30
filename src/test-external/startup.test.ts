import { expect } from "chai";
import type { Document, MongoClient } from "mongodb";
import { internal as mongoInternal } from "@antelopejs/interface-mongodb";

import { GetOptions, type Options, SetOptions } from "../config";
import { AdminClient, CLIENT_DATABASE, WRITER_USER } from "./fixture";

const BOOKKEEPING_COLLECTION = "__antelope_instances";
const STRUCTURE_COMMANDS = [
  "create",
  "collMod",
  "createIndexes",
  "dropIndexes",
  "drop",
  "dropDatabase",
];
const PROFILE_ALL = 2;
const UNTYPED_FOREIGN_MODE = '{ "mode": "foreign" }';
const WRITE_OPERATIONS = ["insert", "update", "remove"];

function ExternalWith(overrides: Options): Options {
  return { ...GetOptions(), mode: "external", ...overrides };
}

function ModuleCommands(admin: MongoClient): Promise<Document[]> {
  return admin
    .db(CLIENT_DATABASE)
    .collection("system.profile")
    .find({ user: `${WRITER_USER}@${CLIENT_DATABASE}` })
    .toArray();
}

describe("an external instance at startup", () => {
  let admin: MongoClient;

  before(async () => {
    admin = await AdminClient();
  });

  after(async () => {
    await admin.close();
  });

  it("creates no bookkeeping collection in the client's database", async () => {
    const names = await admin
      .db(CLIENT_DATABASE)
      .listCollections({}, { nameOnly: true })
      .toArray();
    expect(names.map((collection) => collection.name)).to.not.include(
      BOOKKEEPING_COLLECTION,
    );
  });

  it("sends the client's database no write and no structure command", async () => {
    const profiler = await admin.db(CLIENT_DATABASE).command({ profile: -1 });
    const commands = await ModuleCommands(admin);
    const offending = commands.filter(
      (entry) =>
        WRITE_OPERATIONS.some((operation) => operation === entry.op) ||
        STRUCTURE_COMMANDS.some((name) => entry.command?.[name] !== undefined),
    );
    expect(profiler.was).to.equal(PROFILE_ALL);
    expect(offending).to.deep.equal([]);
  });

  it("does not publish its client through interface-mongodb", () => {
    expect(mongoInternal.connected).to.equal(false);
  });
});

describe("external configuration", () => {
  let configured: Options;

  beforeEach(() => {
    configured = { ...GetOptions() };
  });

  afterEach(() => {
    SetOptions(configured);
  });

  for (const option of ["url", "database"] as const) {
    it(`refuses to resolve without ${option}, rather than falling back to localhost`, () => {
      for (const missing of [undefined, "", "  "]) {
        expect(() => SetOptions(ExternalWith({ [option]: missing }))).to.throw(
          `mode "external" needs a non-empty config.${option}`,
        );
      }
    });
  }

  it("refuses an unknown mode by name", () => {
    const foreign: Options = {
      ...configured,
      ...JSON.parse(UNTYPED_FOREIGN_MODE),
    };
    expect(() => SetOptions(foreign)).to.throw("received foreign");
  });

  it("refuses options.retryWrites by name", () => {
    expect(() =>
      SetOptions(ExternalWith({ options: { retryWrites: true } })),
    ).to.throw('options.retryWrites cannot be set with mode "external"');
  });

  it("refuses a statementTimeoutMs that is not a positive safe integer", () => {
    for (const refused of [0, -1, 1.5, Number.NaN]) {
      expect(() =>
        SetOptions(ExternalWith({ statementTimeoutMs: refused })),
      ).to.throw("statementTimeoutMs must be a positive safe integer");
    }
  });
});
