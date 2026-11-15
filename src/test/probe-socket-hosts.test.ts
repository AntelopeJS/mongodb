import { execFile } from "node:child_process";
import { chmod, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";

import type { ProbeAnswer } from "@antelopejs/interface-database-catalog";
import { expect } from "chai";
import { MongoMemoryServer } from "mongodb-memory-server-core";

import { probe } from "../probe";
import {
  ClosedPort,
  StartSilentSocket,
  StartSocketRelay,
} from "./probe-servers";

const run = promisify(execFile);
const MONGODB_VERSION = "8.0.8";
const START_TIMEOUT_MS = 180_000;
const TIMEOUT_MS = 500;
const LOCKED = 0o000;
const UNLOCKED = 0o700;
const NO_SERVER = {
  outcome: "unreachable",
  message: "No MongoDB server answered at the hosts of the URL.",
};
/** Socket files exist on POSIX systems only; on Windows a path host is a named pipe. */
const posixDescribe = process.platform === "win32" ? describe.skip : describe;

function Probe(hosts: string): Promise<ProbeAnswer> {
  return probe({
    url: `mongodb://${hosts}/shop`,
    timeoutMs: TIMEOUT_MS,
    signal: new AbortController().signal,
  });
}

/** Leaves a socket file nothing listens on: its server is killed before it can remove it. */
async function DeadSocket(socketPath: string) {
  const script = `require("node:net").createServer().listen(${JSON.stringify(socketPath)}, () => process.kill(process.pid, "SIGKILL"))`;
  await run(process.execPath, ["-e", script]).catch(() => undefined);
}

posixDescribe("probing a socket file named as a host", function () {
  let server: MongoMemoryServer;
  let directory: string;
  const closers: (() => Promise<void>)[] = [];
  const at = (name: string) => path.join(directory, name);

  before(async function () {
    this.timeout(START_TIMEOUT_MS);
    server = await MongoMemoryServer.create({
      binary: { version: MONGODB_VERSION },
    });
    directory = await mkdtemp(path.join(os.tmpdir(), "probe-sockets-"));
    const port = Number(new URL(server.getUri()).port);
    closers.push(await StartSocketRelay(port, at("mongod.sock")));
    closers.push(await StartSilentSocket(at("silent.sock")));
    await writeFile(at("file.sock"), "");
    await mkdir(at("locked"));
    await chmod(at("locked"), LOCKED);
    await DeadSocket(at("dead.sock"));
  });

  after(async function () {
    this.timeout(START_TIMEOUT_MS);
    await Promise.all(closers.map((close) => close()));
    await chmod(at("locked"), UNLOCKED);
    await rm(directory, { recursive: true, force: true });
    await server.stop();
  });

  it("connects to a socket file a MongoDB server listens on", async () => {
    const answer = await Probe(encodeURIComponent(at("mongod.sock")));
    expect(answer).to.include({
      outcome: "signed-in",
      canRead: true,
      canWrite: true,
    });
  });

  it("answers one sentence, with nothing of the path, wherever no MongoDB server answers", async () => {
    const closedPort = await ClosedPort();
    const hosts = {
      missing: encodeURIComponent(at("missing.sock")),
      "regular file": encodeURIComponent(at("file.sock")),
      "locked directory": encodeURIComponent(at(path.join("locked", "x.sock"))),
      "socket nothing listens on": encodeURIComponent(at("dead.sock")),
      "socket that never answers": encodeURIComponent(at("silent.sock")),
      "beside a closed port": `127.0.0.1:${closedPort},${encodeURIComponent(at("missing.sock"))}`,
    };
    for (const [kind, host] of Object.entries(hosts)) {
      expect(await Probe(host), kind).to.deep.equal(NO_SERVER);
    }
  });
});
