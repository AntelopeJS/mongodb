---
name: formal-query-index-rebuild
description: Exhaustive interleaving check, with fast-check and a step scheduler, of a query on a schema racing that schema's index builds in @antelopejs/mongodb (Schemas.register, the readiness barrier and superseded builds in src/schema-initialization.ts, the index sync in InitializeSchema, the barrier awaits in RunQuery and SelectionQuery.decode). Use when a change touches src/schema-initialization.ts, InitializeSchema or its index helpers in src/connection.ts, Schemas.register in src/implementations/database/schema.ts, or the barrier awaits in src/implementations/database/query.ts or selection.ts, or needs proof that a query never runs against half-built or stale indexes.
---

# Formal check: a query against a replacement index build

`Schemas.register` starts an index build in the background. Every query on the schema first awaits `SchemaInitialized(schemaId)`, the readiness barrier, which opens once the build has made every declared index (#59). Registering the schema again supersedes the build under way; the replacement starts only once the superseded attempt has settled, so two builds never sync one collection at once (#61). A break here lets a write land before its unique index exists, so racing inserts of one key all succeed, or leaves a collection with the wrong uniqueness after two builds interleave their `dropIndex` and `createIndexes` calls.

The check runs the real code of `src/`, compiled with this folder, against `fake-mongo.ts`, an in-memory database that answers the driver calls `InitializeSchema` makes with the error codes mongod 8.0.8 returns. Queries go through the real `RunQuery`: a read of the table reaches the fake's `aggregate`, and the `listInstances` lifecycle stage reaches its `find`. Each driver call is one indivisible step, as one round trip to mongod is. fast-check draws the scenarios with a fixed seed, and `explore.ts` runs each scenario under every order of its steps: every way the builds, the registrations and the queries can interleave.

- `check.mjs` compiles this folder and the `src/` files it imports into a scratch folder with the repository's `tsc`, runs `check.ts` there, and deletes the folder.
- `check.ts` runs a scenario: it registers the schema, issues the later registrations and queries as steps, and judges each run by the rules.
- `shards.ts` splits the drawn scenarios across worker threads and merges what they report. Each thread is its own isolate with its own `Schemas` registry and fake server, so scenarios never share state. The merge gives each rule the counterexample of the smallest trace length, the lowest scenario on a tie, and cuts after the first scenario that breaks a rule outside the findings, so the output is the same for any `--jobs`.
- `rules.ts` holds the scenarios and the rules. `explore.ts` is the order explorer. `fake-mongo.ts` is the database. `server.ts` points `MongoClient.connect` at it, so `Connect` runs unchanged.

## Run it

From the repository root, with Node 24 and `pnpm install` done:

```sh
node .agents/skills/formal-query-index-rebuild/check.mjs          # CI bounds
node .agents/skills/formal-query-index-rebuild/check.mjs --full   # full bounds
```

`--scenarios <n>`, `--orders <n>` and `--seed <n>` override the bounds; each takes a positive integer. `--jobs <n>` sets how many worker threads explore the scenarios; it defaults to the number of available cores, at most 4, and `--jobs 1` explores them in the main thread. The printed result does not depend on `--jobs`. No service is needed. SIGINT, SIGTERM and SIGHUP stop the compile or the check, workers included, and remove the scratch folder.

| Bounds | Scenarios | Orders per scenario, at most | Orders run | Scenarios cut short | Wall time |
| --- | --- | --- | --- | --- | --- |
| CI (default) | 1,000 | 5,000 | 66,775 | 0 | 7 s |
| Full (`--full`) | 3,000 | 20,000 | 216,100 | 0 | 15 s |

Times are the median of four runs with the default `--jobs` (4) on a shared 16-core machine at a load of 11 to 20, `tsc` included. With `--jobs 1` the same runs took 14 s and 37 s; the four workers spend about a third more CPU time than one thread. No scenario reaches the cap at HEAD, so every order of every drawn scenario runs.

- Exit 0 means every rule that held still holds, and each finding still breaks.
- Exit 1 means a rule changed. `BROKEN` names a rule that held and now breaks, followed by the shortest counterexample of the scenario that broke it. `HOLDS` names a finding that no longer breaks. The check stops after the first scenario that breaks a rule outside the findings, so a failing run ends in seconds; the rules it did not get to judge print on a `not judged` line.
- Exit 2 means the check could not run: `tsc` failed, an argument was not a positive integer, or the check crashed before it judged the rules.

A line that names a changed rule matches `^(?:BROKEN|HOLDS) +(\w+)`, and a recorded finding prints as `finding <rule>` with its shortest counterexample. Rules that hold print on the lowercase `holds` line, with the scenarios and orders the run covered.

## When to run it

Run `--full` when a change touches a file under "Coverage record" or this folder. CI runs the CI bounds on every push, through `extra-commands` in `.github/workflows/ci.yml`. The verification skill runs `--full` with `pnpm exec antelopejs-verify formal <run>` when the diff touches a covered file: the `formal` list of `.agents/skills/verify-module/verify.config.ts` names this check.

## The rules

A query is superseded when the schema is registered again while the query still waits on the barrier, and in flight when that registration comes after the query passed the barrier but before its driver call. The check tells the two apart by whether the readiness promise the query awaits had resolved. A read is consistent when the indexes it saw are complete for one declaration made between the query's issue and its read: every declared index exists with the declared uniqueness, and no other index on its key enforces the other uniqueness. A mix of two declarations, or one half built, is not consistent.

| Rule | Plain sentence | Harm when it breaks | Today |
| --- | --- | --- | --- |
| `QuerySettles` | Every query resolves or rejects once every step has run. | A caller hangs for good. | holds |
| `QueryNeverFails` | A query that is neither superseded nor in flight runs; it is never refused. | A caller gets an error from a schema whose declaration is valid. | holds |
| `ReadSeesDeclaredIndexes` | Every read of a query that is not in flight is consistent. | A write lands before its unique index exists, or a stale unique index refuses writes the declaration allows. | holds |
| `SettledMatchesLatest` | Once every step has run, the barrier is open and the collection holds the indexes of the latest registration. | The schema refuses queries until a retry, or keeps the wrong uniqueness for good. | holds |
| `InFlightReadKeepsUnchangedIndexes` | An in-flight read sees every index that all declarations between the query's issue and its read agree on. | A build drops an index nobody changed, so a query that passed the barrier loses it on every module start. | holds |
| `SupersededQueryWaits` | A superseded query is not refused. | A query refused for a re-registration it had no part in. | finding F1 |
| `InFlightReadSeesDeclaredIndexes` | Every read of an in-flight query is consistent. | A query that passed the barrier runs while the replacement build has dropped an index the new declaration changes, so a write can land without its unique index. | finding F2 |

The read of a query is its `aggregate` or `find` step, so the read rules judge the indexes at the moment the query reaches the database. A finding is judged by its rule name alone, so a finding that gets worse in the same rule stays green. The other rules bound each finding: F1 turning into a hang breaks `QuerySettles`, and F2 reaching an index the declarations did not change breaks `InFlightReadKeepsUnchangedIndexes`. A different rejection message, or another changed index dropped in the same window, does not change the result.

## Findings at 2a777b1

**F1, a re-registration refuses the queries already waiting (`SupersededQueryWaits`).** `StartSchemaInitialization` calls `CancelSchemaInitialization`, which halts the old task and rejects its readiness promise with `its initialization was halted` (schema-initialization.ts:61, 155-159). A query that was awaiting that promise is refused, though the schema stays registered and the new build will open a new barrier. The shortest counterexample: build 1 starts, query 1 awaits the barrier, the schema is registered again, and query 1 is rejected. The replay ran `dist/` against a mongod 8.0.8: `Schemas.register("shop", { users: { indexes: { email: { unique: false } } } })`, then `SchemaInitialized("shop")`, then a second `register` with `email` unique. It printed `query 1 rejected: Schema "shop" is not initialized: its initialization was halted`; a `SchemaInitialized` called after the second register resolved once `email__i` was unique. The `SchemaInitialized` TSDoc names an unregister or a stop as the reasons to reject; whether a re-registration should reject too is the owner's call.

**F2, a query in flight runs against the replacement build's half-built indexes (`InFlightReadSeesDeclaredIndexes`).** The barrier gates when a query starts, not when its driver call reaches mongod. A registration that comes in between starts a build that drops an index before recreating it (connection.ts:243-294), and the query runs in that gap. The shortest counterexample: build 1 ends, query 1 passes the barrier, the schema is registered again with `byOwner` unique, build 2 drops `byOwner__i`, and query 1 reaches the database before build 2's `createIndexes` creates it again. The replay ran `dist/` against a mongod 8.0.8, with the query's `aggregate` held until the first `dropIndex` returned, as a slow round trip would: register `shop` with `email` unique, await `SchemaInitialized`, call `RunQuery` on `shop.users`, and register `shop` again with `email` not unique. It printed `indexes when query 1 reached mongod: [["_id_","_id",false]]`; after build 2, `email__i` existed, not unique. A write in that gap can store a key the old declaration made unique.

Neither finding is filed as an issue yet.

## Real bug or modeling error

What the check treats as one step is one `await` on the driver in `InitializeSchema`, or the issue of one registration or query. Code between two awaits runs in one piece in Node, so the check never splits it. mongod runs each command atomically and the fake does too, so the check leaves out what happens inside one command: a `dropIndex` that waits behind a concurrent `createIndex`, as observed for #61, takes effect in one piece here.

The fake answers like mongod 8.0.8 only for the calls `InitializeSchema` makes: `listCollections`, `createCollection`, `collMod`, `indexes`, `createIndex`, `createIndexes` and `dropIndex`. A query's `aggregate` or `find` returns no documents; the check judges only when it ran and which indexes it saw. `createCollection` with the module's options succeeds on an existing collection. `indexes` on a missing collection, and `dropIndex` on one, return error 26; `dropIndex` of a missing index returns error 27. `createIndex` returns error 86 for a same-name index with another spec, error 85 for a same-key index with the same options under another name, and keeps a same-key index that differs in uniqueness. `createIndexes` applies its batch as one step and, like mongod, creates none of it when one index of the batch gets error 85 or 86. These were probed on mongod 8.0.8 when the check was written. A counterexample that needs another answer from the server is a modeling error until a replay shows mongod gives it.

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
| build | `runAttempt` and `handleSuccess` (schema-initialization.ts:161-207), `InitializeSchema` and its index helpers (connection.ts:183-375) |
| query | `RunQuery` with a table read and with `listInstances` (implementations/database/query.ts:49-89): the barrier awaits in `tryHandleLifecycle` (query.ts:63) and `SelectionQuery.decode` (implementations/database/selection.ts:123), over `SchemaInitialized` (schema-initialization.ts:91-93) |
| driver | `Connect` and `GetDatabase` (connection.ts:39-104) over `fake-mongo.ts` |

The sync commit is `2a777b1` on AntelopeJS/mongodb `main`. The covered files last changed in `7ca8e4e` (`schema-initialization.ts`, `connection.ts`, `implementations/database/query.ts`, `implementations/database/selection.ts`) and `4ea9ffc` (`implementations/database/schema.ts`).

## Bounds and what is left out

A pass covers the drawn scenarios under every order of their steps, at the bounds of the table under "Run it". A scenario is one table, `users`. A declaration has no index one time in four, which builds only the `_instance` index; otherwise it has up to two declared indexes, each absent or present with any `unique` and `crossInstance`: one on `email`, named `email` or `mail` so a later registration can rename an index over the same key, and `byOwner` on `owner` and `email`. The collection starts empty or with the indexes a former declaration built. Then the schema is registered, followed by one to three registrations of it and queries on it (table reads or `listInstances`), in any order. The check leaves out:

- atomic mutations: `RunQuery` awaits the barrier for them at query.ts:68, then runs them on the separate atomic client, which the check does not fake. Dropping that await passes here; `schema-readiness.test.ts` catches it on a real mongod.
- what a query returns and writes: the fake holds no documents, so the check judges when a query reaches the database and which indexes it sees, not its result.
- more than one table, and the per-table loop of `InitializeSchema` across tables.
- `stop`, `start`, `destroy` and `unregister` while a build runs. `schema-initialization-stop.test.ts` and `schema-readiness.test.ts` cover them on a real mongod.
- the failure path of `runAttempt` and its retry (schema-initialization.ts:180-183, 209-239). No scenario makes a build fail at HEAD: the fake fails only on index conflicts, which the code avoids. Network errors, timeouts and the retry they start are left to `schema-initialization.test.ts` and `schema-readiness.test.ts`.
- the external mode, which registers through `RegisterExternalSchema` and builds no index.
- two module instances building the same collection.

## Confirm the check can fail

Each scratch change was applied to `src/` in a scratch worktree at the sync commit. Every run exited 1 with both bounds. With the change dropped, both bounds exit 0.

| Scratch change in the code | Result, both bounds |
| --- | --- |
| Revert #61 (`git revert 23982e7`): the replacement build starts at once | `BROKEN QueryNeverFails`, `BROKEN ReadSeesDeclaredIndexes`, `BROKEN SettledMatchesLatest` |
| Remove the barrier of #59: `SchemaInitialized` returns `Promise.resolve()` | `BROKEN ReadSeesDeclaredIndexes` |
| Drop the `isCurrentTask` check in `handleSuccess`: a superseded build's success opens the new build's barrier | `BROKEN ReadSeesDeclaredIndexes` |
| Drop the barrier await in `tryHandleLifecycle` (query.ts:63) | `BROKEN ReadSeesDeclaredIndexes` |
| Drop the barrier await in `SelectionQuery.decode` (selection.ts:123) | `BROKEN ReadSeesDeclaredIndexes` |
| `dropStaleIndexes` returns instead of dropping a same-key index under another name (connection.ts:260) | `BROKEN ReadSeesDeclaredIndexes`, `BROKEN SettledMatchesLatest` |
| `StartSchemaInitialization` resolves the old readiness promise before it cancels the task, so waiting queries run during the replacement build | `BROKEN ReadSeesDeclaredIndexes` |
| `StartSchemaInitialization` drops the old task without rejecting its readiness promise, so waiting queries hang | `BROKEN QuerySettles` |
| `dropStaleIndexes` drops and recreates an index that already serves the declaration (connection.ts:249-251) | `BROKEN InFlightReadKeepsUnchangedIndexes` |
| `ensureInstanceIndex` drops and recreates the `_instance` index it already has (connection.ts:325) | `BROKEN InFlightReadKeepsUnchangedIndexes` |

The rules a run reports depend on the first scenario that breaks one, so a table row lists what that scenario broke. The #61 revert's counterexample under the CI bounds: build 1 reads the indexes, the schema is registered again with `mail` not unique, build 1 drops `email__i`, build 2 reads the indexes while build 1 runs, build 1 creates `mail__i` unique, and build 2 gets error 86 creating `mail__i`. The barrier rejects with `An existing index has the same name as the requested index`, and the collection keeps build 1's indexes. Through the verification skill, `pnpm exec antelopejs-verify formal <run>` on a scratch commit reverting #61 prints `FAIL formal-query-index-rebuild`.
