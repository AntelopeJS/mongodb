import type { MongoClientOptions } from "mongodb";

/**
 * Whose collections an instance serves.
 *
 * - `owned`: the adapter creates the collections and indexes it is given, scopes documents to
 *   instances, and keeps its own bookkeeping beside them.
 * - `external`: the collections already exist in a database the adapter does not own. Collection
 *   and field names are the database's own, read verbatim; registration reads what exists and
 *   never creates, alters or drops anything; documents are read and written, never the structure.
 *   `url` and `database` are required. See `docs/external-mode.md`.
 */
export type Mode = "owned" | "external";

/** Whether an external instance answers `@antelopejs/interface-database-catalog`. */
export interface CatalogOptions {
  /**
   * Force the catalog on or off. Unset, it follows the project's development mode: on under
   * `ajs project dev` and `ajs project run`, off under `ajs project start`. The catalog describes
   * and profiles the client's database for a generator, which is development work.
   */
  enabled?: boolean;
}

/**
 * Module configuration, as accepted by `construct`.
 */
export interface Options {
  /** Whose collections this instance serves. Defaults to `owned`. */
  mode?: Mode;
  /** MongoDB connection string. */
  url?: string;
  /** The database every collection of this instance lives in. */
  database?: string;
  /** Additional `MongoClient` options. */
  options?: MongoClientOptions;
  /**
   * Server-side time limit of each read and write, sent as `maxTimeMS`. Refused unless it is a
   * positive safe integer.
   */
  statementTimeoutMs?: number;
  /** The catalog an external instance answers. See {@link CatalogOptions}. */
  catalog?: CatalogOptions;
}

export interface ResolvedOptions {
  mode: Mode;
  url: string;
  database: string;
  options: MongoClientOptions;
  statementTimeoutMs?: number;
  catalog: CatalogOptions;
}

const DEFAULT_URL = "mongodb://localhost:27017";
const DEFAULT_DATABASE = "antelopejs";
const DEFAULT_MODE: Mode = "owned";
const MODES: readonly Mode[] = ["owned", "external"];
const SMALLEST_POSITIVE_INTEGER = 1;

const NOT_CONFIGURED =
  "MongoDB adapter is not configured: construct() has not run yet";

let resolved: ResolvedOptions | undefined;

function ResolveMode(options?: Options): Mode {
  const mode = options?.mode ?? DEFAULT_MODE;
  if (!MODES.includes(mode)) {
    throw new Error(
      `mode must be one of ${MODES.join(", ")}, received ${String(mode)}`,
    );
  }
  if (mode === "external" && options?.options?.retryWrites === true) {
    throw new Error(
      `options.retryWrites cannot be set with mode "external": an external instance runs no atomic mutation, so it keeps no client for them`,
    );
  }
  return mode;
}

function Required(
  mode: Mode,
  option: "url" | "database",
  supplied: string | undefined,
  fallback: string,
): string {
  if (supplied !== undefined && supplied.trim() !== "") {
    return supplied;
  }
  if (mode === "external") {
    throw new Error(
      `mode "external" needs a non-empty config.${option}; set it through the project's envOverrides`,
    );
  }
  return fallback;
}

function PositiveSafeInteger(
  option: string,
  supplied: number | undefined,
): number | undefined {
  if (supplied === undefined) {
    return undefined;
  }
  if (!Number.isSafeInteger(supplied) || supplied < SMALLEST_POSITIVE_INTEGER) {
    throw new Error(
      `${option} must be a positive safe integer, received ${String(supplied)}`,
    );
  }
  return supplied;
}

export function SetOptions(options?: Options): ResolvedOptions {
  const mode = ResolveMode(options);
  resolved = {
    mode,
    url: Required(mode, "url", options?.url, DEFAULT_URL),
    database: Required(mode, "database", options?.database, DEFAULT_DATABASE),
    options: options?.options ?? {},
    statementTimeoutMs: PositiveSafeInteger(
      "statementTimeoutMs",
      options?.statementTimeoutMs,
    ),
    catalog: options?.catalog ?? {},
  };
  return resolved;
}

export function GetOptions(): ResolvedOptions {
  if (!resolved) {
    throw new Error(NOT_CONFIGURED);
  }
  return resolved;
}

/** Whether the configured instance serves collections it does not own. */
export function IsExternal(): boolean {
  return GetOptions().mode === "external";
}
