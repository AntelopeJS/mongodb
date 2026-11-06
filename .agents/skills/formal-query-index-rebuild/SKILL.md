---
name: formal-query-index-rebuild
description: Exhaustive interleaving check, with fast-check and a step scheduler, of a query on a schema racing that schema's index builds in @antelopejs/mongodb (Schemas.register, the readiness barrier and superseded builds in src/schema-initialization.ts, the index sync in InitializeSchema, the barrier awaits in RunQuery and SelectionQuery.decode). Use when a change touches src/schema-initialization.ts, InitializeSchema or its index helpers in src/connection.ts, Schemas.register in src/implementations/database/schema.ts, or the barrier awaits in src/implementations/database/query.ts or selection.ts, or needs proof that a query never runs against half-built or stale indexes.
---

# Formal check: a query against a replacement index build

`Schemas.register` starts an index build in the background. Every query on the schema first awaits `SchemaInitialized(schemaId)`, the readiness barrier, which opens once the build has made every declared index (#59). Registering the schema again supersedes the build under way; the replacement starts only once the superseded attempt has settled, so two builds never sync one collection at once (#61). A break here lets a write land before its unique index exists, so racing inserts of one key all succeed, or leaves a collection with the wrong uniqueness after two builds interleave their `dropIndex` and `createIndex` calls.

The check runs the real code of `src/`, compiled with this folder, against `fake-mongo.ts`, an in-memory database that answers the driver calls `InitializeSchema` makes with the error codes mongod 8.0.8 returns. Queries go through the real `RunQuery`: a read of the table reaches the fake's `aggregate`, and the `listInstances` lifecycle stage reaches its `find`. Each driver call is one indivisible step, as one round trip to mongod is. fast-check draws the scenarios with a fixed seed, and `explore.ts` runs each scenario under every order of its steps: every way the builds, the registrations and the queries can interleave.

- `check.mjs` compiles this folder and the `src/` files it imports into a scratch folder with the repository's `tsc`, runs `check.ts` there, and deletes the folder.
- `check.ts` runs a scenario: it registers the schema, issues the later registrations and queries as steps, and judges each run by the rules.
- `rules.ts` holds the scenarios and the rules. `explore.ts` is the order explorer. `fake-mongo.ts` is the database. `server.ts` points `MongoClient.connect` at it, so `Connect` runs unchanged.

## Run it

From the repository root, with Node 24 and `pnpm install` done:

```sh
node .agents/skills/formal-query-index-rebuild/check.mjs          # CI bounds
node .agents/skills/formal-query-index-rebuild/check.mjs --full   # full bounds
```

`--scenarios <n>`, `--orders <n>` and `--seed <n>` override the bounds. No service is needed.

| Bounds | Scenarios | Orders per scenario, at most | Orders run | Scenarios cut short | Wall time |
| --- | --- | --- | --- | --- | --- |
| CI (default) | 1,000 | 5,000 | 143,048 | 0 | 13 s |
| Full (`--full`) | 3,000 | 20,000 | 421,956 | 0 | 32 s |

Times are one run each on a shared 16-core machine, `tsc` included. No scenario reaches the cap at HEAD, so every order of every drawn scenario runs.

- Exit 0 means every rule that held still holds, and each finding still breaks.
- Exit 1 means a rule changed. `BROKEN` names a rule that held and now breaks, followed by the shortest counterexample of the scenario that broke it. `HOLDS` names a finding that no longer breaks. The check stops after the first scenario that breaks a rule outside the findings, so a failing run ends in seconds; the rules it did not get to judge print on a `not judged` line.
- Exit 2 means the check could not run: `tsc` failed or the check threw.

A line that names a changed rule matches `^(?:BROKEN|HOLDS) +(\w+)`, and a recorded finding prints as `finding <rule>` with its shortest counterexample. Rules that hold print on the lowercase `holds` line, with the scenarios and orders the run covered.

## When to run it

Run `--full` when a change touches a file under "Coverage record" or this folder. CI runs the CI bounds on every push, through `extra-commands` in `.github/workflows/ci.yml`. The verification skill runs `--full` with `pnpm exec antelopejs-verify formal <run>` when the diff touches a covered file: the `formal` list of `.agents/skills/verify-module/verify.config.ts` names this check.

## The rules

A query is superseded when the schema is registered again while the query waits on the barrier.

| Rule | Plain sentence | Harm when it breaks | Today |
| --- | --- | --- | --- |
| `QueryNeverFails` | A query that is not superseded runs; it is never refused. | A caller gets an error from a schema whose declaration is valid. | holds |
| `ReadSeesDeclaredIndexes` | A query that is not superseded runs only once every index its schema declared exists with the declared uniqueness, and no other index on the same key enforces the other uniqueness. | A write lands before its unique index exists, or a stale unique index refuses writes the declaration allows. | holds |
| `SettledMatchesLatest` | Once every step has run, the barrier is open and the collection holds the indexes of the latest registration. | The schema refuses queries until a retry, or keeps the wrong uniqueness for good. | holds |
| `SupersededQueryWaits` | A query that waits while the schema is registered again runs once the new build ends. | A query refused for a re-registration it had no part in. | finding F1 |

The read of a query is its `aggregate` or `find` step, so `ReadSeesDeclaredIndexes` judges the indexes at the moment the query reaches the database.

## Finding at 2a777b1

**F1, a re-registration refuses the queries already waiting (`SupersededQueryWaits`).** `StartSchemaInitialization` calls `CancelSchemaInitialization`, which halts the old task and rejects its readiness promise with `its initialization was halted` (schema-initialization.ts:61, 155-159). A query that was awaiting that promise is refused, though the schema stays registered and the new build will open a new barrier. The shortest counterexample: build 1 starts, query 1 awaits the barrier, the schema is registered again, and query 1 is rejected. The replay ran `dist/` against a mongod 8.0.8: `Schemas.register("shop", { users: { indexes: { email: { unique: false } } } })`, then `SchemaInitialized("shop")`, then a second `register` with `email` unique. It printed `query 1 rejected: Schema "shop" is not initialized: its initialization was halted`; a `SchemaInitialized` called after the second register resolved once `email__i` was unique. The `SchemaInitialized` TSDoc names an unregister or a stop as the reasons to reject; whether a re-registration should reject too is the owner's call. The finding is not filed as an issue yet.

## Real bug or modeling error

What the check treats as one step is one `await` on the driver in `InitializeSchema`, or the issue of one registration or query. Code between two awaits runs in one piece in Node, so the check never splits it. mongod runs each command atomically and the fake does too, so the check leaves out what happens inside one command: a `dropIndex` that waits behind a concurrent `createIndex`, as observed for #61, takes effect in one piece here.

The fake answers like mongod 8.0.8 only for the calls `InitializeSchema` makes: `listCollections`, `createCollection`, `collMod`, `indexes`, `createIndex` and `dropIndex`. A query's `aggregate` or `find` returns no documents; the check judges only when it ran and which indexes it saw. `createIndex` returns error 86 for a same-name index with another spec, error 85 for a same-key index with the same options under another name, and keeps a same-key index that differs in uniqueness. These were probed on mongod 8.0.8 when the check was written. A counterexample that needs another answer from the server is a modeling error until a replay shows mongod gives it.

Replay a counterexample on `dist/`, built by `pnpm run build`, against a real mongod, in the order of the trace, and assert only what a caller sees: whether the query resolves or rejects and with what, and the output of `collection.indexes()`. If the replay does not reproduce it, treat the check as wrong until you show it is not.

## Changing the check

- For an intended change to covered code, change the fake or the scenarios only where the code now does something they do not cover, then run both bounds.
- Never weaken or drop a rule to make the check pass. A fixed finding leaves `FINDINGS` in `check.ts` once a replay confirms the fix. A new finding goes into `FINDINGS` and into this file with its replay.
- When you change the bounds or the scenarios, rerun the scratch changes below. The CI bounds must still catch each one.
- After you confirm that the check describes the code at a newer commit, move the sync commit forward. Do it only then.

## Coverage record

| Part of the check | Code |
| --- | --- |
| registration | `Schemas.register` (implementations/database/schema.ts:33-42), `StartSchemaInitialization`, `CancelSchemaInitialization` (schema-initialization.ts:54-80) |
| build | `runAttempt`, `handleSuccess`, the failure path and its retry (schema-initialization.ts:161-239), `InitializeSchema` and its index helpers (connection.ts:181-312) |
| query | `RunQuery` with a table read and with `listInstances` (implementations/database/query.ts:49-89): the barrier awaits in `tryHandleLifecycle` (query.ts:63) and `SelectionQuery.decode` (implementations/database/selection.ts:123), over `SchemaInitialized` (schema-initialization.ts:91-93) |
| driver | `Connect` and `GetDatabase` (connection.ts:39-104) over `fake-mongo.ts` |

The sync commit is `2a777b1` on AntelopeJS/mongodb `main`. The covered files last changed in `7ca8e4e` (`schema-initialization.ts`, `connection.ts`, `implementations/database/query.ts`, `implementations/database/selection.ts`) and `4ea9ffc` (`implementations/database/schema.ts`).

## Bounds and what is left out

A pass covers the drawn scenarios under every order of their steps, at the bounds of the table under "Run it". A scenario is one table, `users`, with up to two declared indexes, each absent or present with any `unique` and `crossInstance`: one on `email`, named `email` or `mail` so a later registration can rename an index over the same key, and `byOwner` on `owner` and `email`. The collection starts empty or with the indexes a former declaration built. Then the schema is registered, followed by one to three registrations of it and queries on it (table reads or `listInstances`), in any order. The check leaves out:

- atomic mutations: `RunQuery` awaits the barrier for them at query.ts:68, then runs them on the separate atomic client, which the check does not fake. Dropping that await passes here; `schema-readiness.test.ts` catches it on a real mongod.
- what a query returns and writes: the fake holds no documents, so the check judges when a query reaches the database and which indexes it sees, not its result.
- more than one table, and the per-table loop of `InitializeSchema` across tables.
- `stop`, `start`, `destroy` and `unregister` while a build runs. `schema-initialization-stop.test.ts` and `schema-readiness.test.ts` cover them on a real mongod.
- driver failures other than the index conflicts above: network errors, timeouts, and the retry they start.
- the external mode, which registers through `RegisterExternalSchema` and builds no index.
- two module instances building the same collection.

## Confirm the check can fail

Each scratch change was applied to `src/` in a scratch worktree at the sync commit. Every run exited 1, with both bounds, in under 5 s. With the change dropped, both bounds exit 0.

| Scratch change in the code | Result, both bounds |
| --- | --- |
| Revert #61 (`git revert 23982e7`): the replacement build starts at once | `BROKEN SettledMatchesLatest` |
| Remove the barrier of #59: `SchemaInitialized` returns `Promise.resolve()` | `BROKEN ReadSeesDeclaredIndexes` |
| Drop the `isCurrentTask` check in `handleSuccess`: a superseded build's success opens the new build's barrier | `BROKEN ReadSeesDeclaredIndexes` |
| Drop the barrier await in `tryHandleLifecycle` (query.ts:63) | `BROKEN ReadSeesDeclaredIndexes` |
| Drop the barrier await in `SelectionQuery.decode` (selection.ts:123) | `BROKEN ReadSeesDeclaredIndexes` |
| `ensurePhysicalIndex` returns instead of dropping a same-key index under another name (connection.ts:256) | `BROKEN SettledMatchesLatest` |

Before the check stopped at the first broken rule, the #61 revert also broke `QueryNeverFails` and `ReadSeesDeclaredIndexes` under the CI bounds. Its counterexample under the full bounds: build 2 and build 3 read the same indexes, build 2 creates `email__i` unique, and build 3, which declares `email` not unique, gets error 86 creating `email__i`. The barrier rejects with `An existing index has the same name as the requested index`, and the collection keeps build 2's indexes. Through the verification skill, `pnpm exec antelopejs-verify formal <run>` on a scratch commit reverting #61 prints `FAIL formal-query-index-rebuild`.
