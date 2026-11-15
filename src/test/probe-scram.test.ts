import crypto from "node:crypto";

import type { ProbeAnswer } from "@antelopejs/interface-database-catalog";
import { expect } from "chai";

import { probe } from "../probe";
import { SCRAM_ITERATION_LIMIT } from "../probe-scram";
import { StartScramServer } from "./probe-scram-server";

const TIMEOUT_MS = 1000;
const AT_ONCE_MS = 500;
/** Enough iterations to freeze the event loop for seconds if the client derived them. */
const HOSTILE_ITERATIONS = 30_000_000;
const KEY_BYTES = 32;

async function ProbeAsking(iterations: number): Promise<ProbeAnswer> {
  const server = await StartScramServer(iterations);
  try {
    return await probe({
      url: `mongodb://user:secret@127.0.0.1:${server.port}/shop`,
      timeoutMs: TIMEOUT_MS,
      signal: new AbortController().signal,
    });
  } finally {
    await server.close();
  }
}

describe("probing a server that sets the SCRAM iteration count", () => {
  it("refuses at once a server that asks for more iterations than the limit", async () => {
    const started = performance.now();
    const answer = await ProbeAsking(HOSTILE_ITERATIONS);
    expect(performance.now() - started).to.be.lessThan(AT_ONCE_MS);
    expect(answer).to.include({ outcome: "refused", refusal: "other" });
    expect(answer)
      .to.have.property("message")
      .that.includes(String(HOSTILE_ITERATIONS));
  });

  it("derives the iterations a server asks for up to the limit", async () => {
    const answer = await ProbeAsking(SCRAM_ITERATION_LIMIT);
    expect(answer).to.include({ outcome: "refused", refusal: "credentials" });
  });

  it("leaves pbkdf2Sync unbounded outside a probe", async () => {
    await ProbeAsking(HOSTILE_ITERATIONS);
    const key = crypto.pbkdf2Sync(
      "password",
      "salt",
      SCRAM_ITERATION_LIMIT * 2,
      KEY_BYTES,
      "sha256",
    );
    expect(key).to.have.length(KEY_BYTES);
  });
});
