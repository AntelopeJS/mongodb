import type {
  Probe,
  ProbeAnswer,
  ProbeUnreadable,
} from "@antelopejs/interface-database-catalog";
import { type Db, MongoClient, MongoServerError } from "mongodb";

import {
  AuthenticatedRole,
  type ListedRelation,
  ReadRelations,
  Reads,
  Writes,
} from "./implementations/catalog/discovery";

const AUTHENTICATION_FAILED = 18;
const UNAUTHORIZED = 13;
const NOT_AN_ERROR = "The probe failed with a value that is not an Error.";
const DATABASE_IN_PATH = /^[^:]+:\/\/[^/?#]*\/[^?#]/;

type SignedInAnswer = Extract<ProbeAnswer, { outcome: "signed-in" }>;
type Access = Pick<SignedInAnswer, "canRead" | "canWrite">;

const FULL_ACCESS: Access = { canRead: true, canWrite: true };
const NO_ACCESS: Access = { canRead: false, canWrite: false };

/** What the account may do, from what the catalog decides for each relation it lists. */
function AccessTo(relations: ListedRelation[]): Access {
  return {
    canRead: relations.length === 0 || relations.some(Reads),
    canWrite: relations.some((relation) =>
      Object.values(Writes(relation)).includes(true),
    ),
  };
}

function SignedIn(
  latencyMs: number,
  user: string,
  access: Access,
  unreadable?: ProbeUnreadable,
): ProbeAnswer {
  const answer: SignedInAnswer = { outcome: "signed-in", latencyMs, ...access };
  if (user !== "") {
    answer.user = user;
  }
  if (unreadable !== undefined) {
    answer.unreadable = unreadable;
  }
  return answer;
}

/** The relations of `db` as the catalog lists them, or the server's refusal to list them. */
async function Listed(db: Db): Promise<ListedRelation[] | MongoServerError> {
  try {
    return await ReadRelations(db);
  } catch (error) {
    if (error instanceof MongoServerError && error.code === UNAUTHORIZED) {
      return error;
    }
    throw error;
  }
}

/**
 * Signs in, reads the account with `connectionStatus`, the query `latencyMs` times, and lists the
 * relations. A connection no user signed in to is on a server without access control once its
 * listing succeeds, and has full access there, whatever the database holds.
 */
async function Answer(
  client: MongoClient,
  named: boolean,
): Promise<ProbeAnswer> {
  await client.connect();
  const db = client.db();
  const started = performance.now();
  const status = await db.command({ connectionStatus: 1 });
  const latencyMs = Math.ceil(performance.now() - started);
  const user = AuthenticatedRole(status);
  if (user !== "" && !named) {
    return SignedIn(latencyMs, user, NO_ACCESS, "no-database-named");
  }
  const relations = await Listed(db);
  if (relations instanceof MongoServerError) {
    return user === ""
      ? {
          outcome: "refused",
          refusal: "sign-in-required",
          message: relations.message,
        }
      : SignedIn(latencyMs, user, NO_ACCESS, "cannot-list");
  }
  if (!named) {
    return SignedIn(latencyMs, user, NO_ACCESS, "no-database-named");
  }
  return SignedIn(
    latencyMs,
    user,
    user === "" ? FULL_ACCESS : AccessTo(relations),
  );
}

function Failure(error: unknown): ProbeAnswer {
  const message = error instanceof Error ? error.message : NOT_AN_ERROR;
  if (!(error instanceof MongoServerError)) {
    return { outcome: "unreachable", message };
  }
  return {
    outcome: "refused",
    refusal: error.code === AUTHENTICATION_FAILED ? "credentials" : "other",
    message,
  };
}

/**
 * Checks a MongoDB connection on the database the URL's path names, with its own client: what the
 * account may do there is decided per relation as the catalog decides it. It runs no command that
 * writes, never rejects, and closes its client once it has answered, or at once when `signal`
 * aborts. The answer does not wait for the close.
 */
export const probe: Probe = async ({ url, timeoutMs, signal }) => {
  let client: MongoClient | undefined;
  const close = () => {
    client?.close().catch(() => undefined);
  };
  signal.addEventListener("abort", close, { once: true });
  try {
    signal.throwIfAborted();
    client = new MongoClient(url, {
      serverSelectionTimeoutMS: timeoutMs,
      connectTimeoutMS: timeoutMs,
      socketTimeoutMS: timeoutMs,
    });
    return await Answer(client, DATABASE_IN_PATH.test(url));
  } catch (error) {
    return Failure(error);
  } finally {
    signal.removeEventListener("abort", close);
    close();
  }
};
