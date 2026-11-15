import { execFile } from "node:child_process";
import path from "node:path";
import { promisify } from "node:util";

import type { ProbeAnswer } from "@antelopejs/interface-database-catalog";
import { expect } from "chai";
import { type Db, MongoClient } from "mongodb";
import { MongoMemoryServer } from "mongodb-memory-server-core";

import { probe } from "../probe";
import {
  CLOSE_SETTLE_MS,
  ClosedPort,
  SecretOf,
  StartRelay,
  type Stall,
  WatchClientSockets,
} from "./probe-servers";

const run = promisify(execFile);
const MONGODB_VERSION = "8.0.8";
const START_TIMEOUT_MS = 180_000;
const TIMEOUT_MS = 1000;
const ABORT_AFTER_MS = 200;
const ABORTED = { outcome: "unreachable", message: "The probe was aborted." };
const ROOT = "root";
const DATABASE = "shop";
const NAMED_WITH_AT = "ana@shop";
const OVERLONG_DATABASE = "d".repeat(100);
const INTERNAL_ERROR = 1;
const FAILING_APP = "probe-failing-listing";
const TEST_COMMANDS = ["--setParameter", "enableTestCommands=1"];

let secured: MongoMemoryServer;
let open: MongoMemoryServer;

function PortOf(server: MongoMemoryServer): string {
  return new URL(server.getUri()).port;
}

function UrlOf(
  user: string,
  password = SecretOf(user),
  port = PortOf(secured),
  database = DATABASE,
): string {
  return `mongodb://${encodeURIComponent(user)}:${encodeURIComponent(password)}@127.0.0.1:${port}/${database}?authSource=${DATABASE}`;
}

function RootUrl(): string {
  return `mongodb://${ROOT}:${SecretOf(ROOT)}@127.0.0.1:${PortOf(secured)}/admin`;
}

function OpenUrl(database: string, port = PortOf(open)): string {
  return `mongodb://127.0.0.1:${port}/${database}`;
}

function Probe(url: string, timeoutMs = TIMEOUT_MS): Promise<ProbeAnswer> {
  return probe({ url, timeoutMs, signal: new AbortController().signal });
}

/** The answer without its measured latency, which must be a whole number of milliseconds. */
function Measured(answer: ProbeAnswer): Omit<ProbeAnswer, "latencyMs"> {
  if (answer.outcome !== "signed-in") {
    return answer;
  }
  const { latencyMs, ...rest } = answer;
  expect(latencyMs).to.satisfy(Number.isInteger).and.be.at.least(1);
  return rest;
}

async function Seed(db: Db) {
  await db.collection("orders").insertOne({ total: 1 });
  await db.collection("notes").insertOne({ text: "a" });
}

async function CreateRole(
  db: Db,
  role: string,
  privileges: { resource: object; actions: string[] }[],
  roles: string[] = [],
) {
  await db.command({ createRole: role, privileges, roles });
}

async function CreateUser(db: Db, user: string, roles: unknown[]) {
  await db.command({ createUser: user, pwd: SecretOf(user), roles });
}

async function CreateAccounts(db: Db) {
  await CreateUser(db, "reader", [{ role: "read", db: DATABASE }]);
  await CreateUser(db, "writer", [{ role: "readWrite", db: DATABASE }]);
  await CreateRole(db, "listOnly", [
    {
      resource: { db: DATABASE, collection: "" },
      actions: ["listCollections"],
    },
  ]);
  await CreateUser(db, "lister", ["listOnly"]);
  await CreateRole(db, "insertOnly", [
    {
      resource: { db: DATABASE, collection: "orders" },
      actions: ["insert", "find"],
    },
  ]);
  await CreateUser(db, "inserter", ["insertOnly"]);
}

/** A role that inherits another, defined in `admin`, the one database whose roles reach others. */
async function CreateInheritingAccount(shop: Db, admin: Db) {
  await CreateRole(admin, "finder", [
    { resource: { db: DATABASE, collection: "orders" }, actions: ["find"] },
    { resource: { db: "archive", collection: "orders" }, actions: ["insert"] },
  ]);
  await CreateRole(
    admin,
    "auditor",
    [
      {
        resource: { db: DATABASE, collection: "" },
        actions: ["listCollections"],
      },
    ],
    ["finder"],
  );
  await CreateUser(shop, NAMED_WITH_AT, [{ role: "auditor", db: "admin" }]);
}

async function StartServers() {
  [secured, open] = await Promise.all([
    MongoMemoryServer.create({
      auth: {
        enable: true,
        customRootName: ROOT,
        customRootPwd: SecretOf(ROOT),
      },
      instance: { args: TEST_COMMANDS },
      binary: { version: MONGODB_VERSION },
    }),
    MongoMemoryServer.create({
      instance: { args: TEST_COMMANDS },
      binary: { version: MONGODB_VERSION },
    }),
  ]);
  const root = await MongoClient.connect(RootUrl());
  const shop = root.db(DATABASE);
  await Seed(shop);
  await CreateAccounts(shop);
  await CreateInheritingAccount(shop, root.db("admin"));
  await root.close();
  const unsecured = await MongoClient.connect(open.getUri());
  await Seed(unsecured.db(DATABASE));
  await unsecured
    .db("viewsonly")
    .createCollection("report", { viewOn: "nothing", pipeline: [] });
  await unsecured.close();
}

async function DatabaseNames(server: MongoMemoryServer): Promise<string[]> {
  const client = await MongoClient.connect(server.getUri());
  try {
    const { databases } = await client.db().admin().listDatabases();
    return databases.map(({ name }) => name);
  } finally {
    await client.close();
  }
}

/** Runs `work` while the server at `adminUrl` fails the next `listCollections` of {@link FAILING_APP}. */
async function WithFailingListing(
  adminUrl: string,
  work: () => Promise<ProbeAnswer>,
): Promise<ProbeAnswer> {
  const admin = await MongoClient.connect(adminUrl);
  const failListing = (mode: { times: number } | "off") =>
    admin.db("admin").command({
      configureFailPoint: "failCommand",
      mode,
      data: {
        failCommands: ["listCollections"],
        errorCode: INTERNAL_ERROR,
        appName: FAILING_APP,
      },
    });
  try {
    await failListing({ times: 1 });
    return await work();
  } finally {
    await failListing("off");
    await admin.close();
  }
}

function AccountCases() {
  it("answers read access and no write access to an account that may only find", async () => {
    expect(Measured(await Probe(UrlOf("reader")))).to.deep.equal({
      outcome: "signed-in",
      user: "reader",
      canRead: true,
      canWrite: false,
    });
  });

  it("answers write access to an account that may write", async () => {
    expect(Measured(await Probe(UrlOf("writer")))).to.deep.equal({
      outcome: "signed-in",
      user: "writer",
      canRead: true,
      canWrite: true,
    });
  });

  it("signs in with the URL's password when authMechanismProperties names an OIDC environment without authMechanism", async () => {
    const saved = process.env.OIDC_TOKEN_FILE;
    process.env.OIDC_TOKEN_FILE = path.join(__dirname, "no-such-token");
    try {
      const answer = await Probe(
        `${UrlOf("writer")}&authMechanismProperties=ENVIRONMENT:test`,
      );
      expect(Measured(answer)).to.deep.equal({
        outcome: "signed-in",
        user: "writer",
        canRead: true,
        canWrite: true,
      });
    } finally {
      if (saved === undefined) {
        delete process.env.OIDC_TOKEN_FILE;
      } else {
        process.env.OIDC_TOKEN_FILE = saved;
      }
    }
  });

  it("signs in with the URL's user name and password when the URL names SCRAM-SHA-256", async () => {
    const answer = await Probe(
      `${UrlOf("writer")}&authMechanism=SCRAM-SHA-256`,
    );
    expect(Measured(answer)).to.deep.equal({
      outcome: "signed-in",
      user: "writer",
      canRead: true,
      canWrite: true,
    });
  });

  it("answers find through an inherited custom role to a user named with @, and no write from a grant on another database", async () => {
    expect(Measured(await Probe(UrlOf(NAMED_WITH_AT)))).to.deep.equal({
      outcome: "signed-in",
      user: NAMED_WITH_AT,
      canRead: true,
      canWrite: false,
    });
  });

  it("answers no access to an account that lists the collections and finds in none", async () => {
    expect(Measured(await Probe(UrlOf("lister")))).to.deep.equal({
      outcome: "signed-in",
      user: "lister",
      canRead: false,
      canWrite: false,
    });
  });

  it("answers no access to an account that may not list the collections", async () => {
    expect(Measured(await Probe(UrlOf("inserter")))).to.deep.equal({
      outcome: "signed-in",
      user: "inserter",
      canRead: false,
      canWrite: false,
      unreadable: "cannot-list",
    });
  });

  it("answers no access to a signed-in account whose listing the server fails", async () => {
    const answer = await WithFailingListing(RootUrl(), () =>
      Probe(`${UrlOf("writer")}&appName=${FAILING_APP}`),
    );
    expect(Measured(answer)).to.deep.equal({
      outcome: "signed-in",
      user: "writer",
      canRead: false,
      canWrite: false,
      unreadable: "cannot-list",
    });
  });

  it("answers no access to a URL that names no database", async () => {
    const url = `mongodb://reader:${SecretOf("reader")}@127.0.0.1:${PortOf(secured)}/?authSource=${DATABASE}`;
    expect(Measured(await Probe(url))).to.deep.equal({
      outcome: "signed-in",
      user: "reader",
      canRead: false,
      canWrite: false,
      unreadable: "no-database-named",
    });
  });
}

function RefusalCases() {
  it("answers a wrong password as refused credentials", async () => {
    const answer = await Probe(UrlOf("reader", SecretOf("wrong")));
    expect(answer).to.include({ outcome: "refused", refusal: "credentials" });
  });

  it("answers no account on a server that asks for one as sign-in required", async () => {
    const answer = await Probe(OpenUrl(DATABASE, PortOf(secured)));
    expect(answer).to.include({
      outcome: "refused",
      refusal: "sign-in-required",
    });
  });

  it("answers another server error as an other refusal", async () => {
    const answer = await Probe(
      UrlOf("reader", SecretOf("reader"), PortOf(secured), OVERLONG_DATABASE),
    );
    expect(answer).to.include({ outcome: "refused", refusal: "other" });
  });

  it("answers a port nothing listens on as unreachable, within the timeout", async () => {
    const started = performance.now();
    const answer = await Probe(OpenUrl(DATABASE, String(await ClosedPort())));
    expect(answer.outcome).to.equal("unreachable");
    expect(performance.now() - started).to.be.lessThan(TIMEOUT_MS * 3);
  });
}

function OpenServerCases() {
  it("answers no access and no user when a server without access control fails the listing", async () => {
    const answer = await WithFailingListing(open.getUri(), () =>
      Probe(`${OpenUrl(DATABASE)}?appName=${FAILING_APP}`),
    );
    expect(Measured(answer)).to.deep.equal({
      outcome: "signed-in",
      canRead: false,
      canWrite: false,
      unreadable: "cannot-list",
    });
  });

  it("answers full access and no user on a server without access control", async () => {
    expect(Measured(await Probe(OpenUrl(DATABASE)))).to.deep.equal({
      outcome: "signed-in",
      canRead: true,
      canWrite: true,
    });
  });

  it("answers full access on an empty database and on one of views only, and creates neither", async () => {
    for (const database of ["empty", "viewsonly"]) {
      expect(Measured(await Probe(OpenUrl(database))), database).to.deep.equal({
        outcome: "signed-in",
        canRead: true,
        canWrite: true,
      });
    }
    expect(await DatabaseNames(open)).to.not.include("empty");
  });
}

async function ProbeThroughRelay(
  stall: Stall | undefined,
  abortAfterMs?: number,
): Promise<{ answer: ProbeAnswer; elapsedMs: number; leftOpen: number }> {
  const relay = await StartRelay(Number(PortOf(open)), stall);
  const sockets = WatchClientSockets(relay.port);
  const controller = new AbortController();
  try {
    if (abortAfterMs !== undefined) {
      setTimeout(() => controller.abort(), abortAfterMs);
    }
    const started = performance.now();
    const answer = await probe({
      url: OpenUrl(DATABASE, String(relay.port)),
      timeoutMs: abortAfterMs === undefined ? TIMEOUT_MS : START_TIMEOUT_MS,
      signal: controller.signal,
    });
    const elapsedMs = performance.now() - started;
    expect(sockets.opened()).to.be.at.least(1);
    return {
      answer,
      elapsedMs,
      leftOpen: await sockets.leftOpenAfter(CLOSE_SETTLE_MS),
    };
  } finally {
    sockets.stop();
    await relay.close();
  }
}

function ClosingCases() {
  it("closes its sockets once it has answered", async () => {
    const { answer, leftOpen } = await ProbeThroughRelay(undefined);
    expect(answer.outcome).to.equal("signed-in");
    expect(leftOpen).to.equal(0);
  });

  it("answers, and shuts its sockets, when the server ignores the goodbye", async () => {
    const { answer, leftOpen } = await ProbeThroughRelay(
      (chunk, fromClient) => fromClient && chunk.includes("endSessions"),
    );
    expect(answer.outcome).to.equal("signed-in");
    expect(leftOpen).to.equal(0);
  });

  it("answers within one timeout, without retrying, when the listing never comes back", async () => {
    const { answer, elapsedMs, leftOpen } = await ProbeThroughRelay(
      (chunk, fromClient) => fromClient && chunk.includes("listCollections"),
    );
    expect(answer.outcome).to.equal("unreachable");
    expect(elapsedMs).to.be.lessThan(TIMEOUT_MS * 1.5);
    expect(leftOpen).to.equal(0);
  });

  for (const [moment, stall] of [
    ["during the handshake", () => true],
    [
      "after the sign-in",
      (chunk: Buffer, fromClient: boolean) =>
        fromClient && chunk.includes("connectionStatus"),
    ],
  ] satisfies [string, Stall][]) {
    it(`answers aborted at once, and shuts its sockets, when the signal aborts ${moment}`, async () => {
      const { answer, elapsedMs, leftOpen } = await ProbeThroughRelay(
        stall,
        ABORT_AFTER_MS,
      );
      expect(answer).to.deep.equal(ABORTED);
      expect(elapsedMs).to.be.lessThan(ABORT_AFTER_MS + TIMEOUT_MS);
      expect(leftOpen).to.equal(0);
    });
  }

  it("opens nothing when the signal has aborted already", async () => {
    const sockets = WatchClientSockets(Number(PortOf(open)));
    try {
      const answer = await probe({
        url: OpenUrl(DATABASE),
        timeoutMs: TIMEOUT_MS,
        signal: AbortSignal.abort(),
      });
      expect(answer).to.deep.equal(ABORTED);
      expect(sockets.opened()).to.equal(0);
    } finally {
      sockets.stop();
    }
  });
}

describe("probing a MongoDB connection", function () {
  before(async function () {
    this.timeout(START_TIMEOUT_MS);
    await StartServers();
  });

  after(async function () {
    this.timeout(START_TIMEOUT_MS);
    await Promise.all([secured?.stop(), open?.stop()]);
  });

  AccountCases();
  RefusalCases();
  OpenServerCases();
  ClosingCases();

  it("loads and answers in a Node process without the AntelopeJS runtime", async () => {
    const script = `require(${JSON.stringify(path.join(__dirname, "..", "probe.js"))}).probe({ url: ${JSON.stringify(OpenUrl(DATABASE))}, timeoutMs: ${TIMEOUT_MS}, signal: new AbortController().signal }).then((answer) => process.stdout.write(answer.outcome))`;
    const { stdout } = await run(process.execPath, ["-e", script]);
    expect(stdout).to.equal("signed-in");
  });
});
