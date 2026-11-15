import crypto from "node:crypto";

import { InProbe } from "./probe-sockets";

/** The most SCRAM iterations a probe derives a password with, as `pg` allows. */
export const SCRAM_ITERATION_LIMIT = 100_000;

/** A server asked a probe to derive the password with more than {@link SCRAM_ITERATION_LIMIT} iterations. */
export class ScramIterationLimit extends Error {
  constructor(iterations: number) {
    super(
      `The server asks to derive the password with ${iterations} SCRAM iterations, and the probe derives at most ${SCRAM_ITERATION_LIMIT}.`,
    );
  }
}

let limited = false;

/**
 * Bounds the SCRAM key derivation of the probes' clients. The driver derives with
 * `crypto.pbkdf2Sync` on the event loop, with the iteration count the server sends and no upper
 * bound, so a hostile server freezes the probing process for minutes, out of reach of `timeoutMs`
 * and `signal`. Inside a probe, a count above {@link SCRAM_ITERATION_LIMIT} throws
 * {@link ScramIterationLimit}; everywhere else in the process, `pbkdf2Sync` is unchanged.
 */
export function LimitScramIterations(): void {
  if (limited) {
    return;
  }
  limited = true;
  const derive = crypto.pbkdf2Sync;
  const bounded: typeof crypto.pbkdf2Sync = (
    password,
    salt,
    iterations,
    keylen,
    digest,
  ) => {
    if (InProbe() && iterations > SCRAM_ITERATION_LIMIT) {
      throw new ScramIterationLimit(iterations);
    }
    return derive(password, salt, iterations, keylen, digest);
  };
  Object.defineProperty(crypto, "pbkdf2Sync", { value: bounded });
}
