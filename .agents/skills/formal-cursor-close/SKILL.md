---
name: formal-cursor-close
description: Exhaustive interleaving check, with a step scheduler, of closing a query cursor at every step of its life in @antelopejs/mongodb (ReadCursor and CloseCursor in src/implementations/database/query.ts, nextRow, firstRow, KillServerCursor and closeCursor in pipeline-core.ts): before or during the first aggregate, during a getMore, while a change stream waits. Use when a change touches those functions, or needs proof that a closed cursor leaves no server cursor open and no read runs after the close.
---

# Formal check: closing a cursor during its read

A caller reads a query cursor with `ReadCursor(reqId, stages)` and ends it with `CloseCursor(reqId)`, which can land at any moment: with the first `aggregate` in flight, between two reads, during a `getMore`, while a change stream waits for an event. The mongodb driver (6.21) ends a cursor's session when `close()` runs before the first `aggregate` has answered, and the cursor id is still unknown, so it kills nothing; the `aggregate` then opens a server cursor that stays until MongoDB's idle timeout (#116). `firstRow` kills that cursor once the answer names it. A break here leaks server cursors, or runs a read after the caller closed.

The check runs the real `ReadCursor` and `CloseCursor` of `src/`, compiled with this folder, against `fake-driver.ts`: an in-memory server and the driver cursor in front of it. Each request is two steps, which the explorer of `formal-query-index-rebuild` (`../formal-query-index-rebuild/explore.ts`) releases in any order against the others: the server runs it, then its answer reaches the client. A `close()` that lands between the two finds the cursor id unknown and kills nothing. The consumer reads in a row and stops reading once it has called the close, as `IterableCursor` of interface-database does (`return()` ends the cursor and never reads again). The scenarios are enumerated, not drawn.

- `check.mjs` compiles this folder and the `src/` files it imports into a scratch folder with the repository's `tsc`, runs `check.ts` there, and deletes the folder.
- `check.ts` runs a scenario: it starts the consumer, the closer and the change events at once and judges each order by the rules.
- `rules.ts` holds the scenarios and the rules. `fake-driver.ts` is the server, the cursor, the collection and the database. `server.ts` points `MongoClient.connect` at them, so `Connect` runs unchanged.

## Run it

From the repository root, with Node 24 and `pnpm install` done:

```sh
node .agents/skills/formal-cursor-close/check.mjs          # CI bounds
node .agents/skills/formal-cursor-close/check.mjs --full   # full bounds
```

`--orders <n>` overrides the cap of orders per scenario; it takes a positive integer. No service is needed.

| Bounds | Scenarios | Orders per scenario, at most | Orders run | Scenarios cut short | Wall time |
| --- | --- | --- | --- | --- | --- |
| CI (default) | 66 | 20,000 | 8,191 | 0 | 3 s |
| Full (`--full`) | 128 | 200,000 | 58,021 | 0 | 4 s |

Times are one run on a shared machine, `tsc` included. No scenario reaches the cap at HEAD, so every order of every scenario runs.

- Exit 0 means every rule that held still holds, and each finding still breaks.
- Exit 1 means a rule changed. `BROKEN` names a rule that held and now breaks, followed by the shortest counterexample. `HOLDS` names a finding that no longer breaks. The check stops after the first scenario that breaks a rule outside the findings.
- Exit 2 means the check could not run: `tsc` failed, an argument was not a positive integer, or the check crashed.

A line that names a changed rule matches `^(?:BROKEN|HOLDS) +(\w+)`, and a recorded finding prints as `finding <rule>` with its shortest counterexample.

## When to run it

Run `--full` when a change touches a file under "Coverage record" or this folder. CI runs the CI bounds on every push, through `extra-commands` in `.github/workflows/ci.yml`.

## The rules

A scenario is a table of 0 to 3 rows read in batches of 1 or 2, or a change stream that receives 0 to 2 events; the consumer makes 1 to 3 reads in a row and the caller closes once or twice. A change stream on one instance also runs the pre-image check of `RequirePreImages`, a step of its own.

| Rule | Plain sentence | Harm when it breaks | Today |
| --- | --- | --- | --- |
| `CallsSettle` | Every read and every close resolves or rejects once every step has run. | A caller hangs, as a change stream that waits for an event whose close never reaches it. | holds |
| `NoServerCursorAfterClose` | Once every step has run, the server holds no open cursor of the query. | A live server cursor stays until MongoDB's idle timeout. | holds |
| `NoRequestAfterClose` | No `aggregate` or `getMore` is sent after the close was called. | A read opens a cursor that nothing closes. | holds |

## Findings

None. Every rule holds at the sync commit.

## Real bug or modeling error

The fake answers like the driver and mongod 8.0.8 for the calls a query cursor makes. The driver facts it relies on were probed on a mongod 8.0.8 replica set with driver 6.21, with a `failCommand` failpoint that held the first `aggregate` for 400 ms:

- a `close()` during the first `aggregate` leaves `cursor.id` undefined, and the `aggregate` then opens a server cursor: one open cursor in `$currentOp` with `idleCursors`;
- the `next()` that was waiting then resolves the first row of a table, and `null` for a change stream, and `cursor.id` names the live cursor; a `killCursors` for that id closes it;
- a `next()` after the close resolves `null`;
- a `close()` while a change stream waits in a `getMore` kills the server cursor, and the waiting `next()` rejects with `MongoServerError: Executor error during getMore :: caused by :: operation was interrupted`.

The fake models those answers and a `getMore` that reaches a killed cursor as `CursorNotFound`. A counterexample that needs another answer from the driver or the server is a modeling error until a replay shows it gives it. Replay a counterexample on `dist/`, built by `pnpm run build`, against a real mongod with the failpoint above, and assert what a caller sees plus the open cursors in `$currentOp`. If the replay does not reproduce it, treat the check as wrong until you show it is not.

## Changing the check

- For an intended change to covered code, change the fake or the scenarios only where the code now does something they do not cover, then run both bounds.
- Never weaken or drop a rule to make the check pass. A fixed finding leaves `FINDINGS` in `check.ts` once a replay confirms the fix. A new finding goes into `FINDINGS` and into this file with its replay.
- When you change the bounds or the scenarios, rerun the scratch changes below. The CI bounds must still catch each one.
- After you confirm that the check describes the code at a newer commit, move the sync commit forward. Do it only then.

## Coverage record

| Part of the check | Code |
| --- | --- |
| read | `ReadCursor` and `openQuery` (implementations/database/query.ts:99-147), `readCursor`, `nextRow` and `firstRow` (implementations/database/pipeline-core.ts:172-223) |
| close | `CloseCursor` (implementations/database/query.ts:149-153), `closeCursor` and `KillServerCursor` (implementations/database/pipeline-core.ts:44-55, 225-230) |
| pre-images | `RequirePreImages` (implementations/database/instance-scope.ts:63) |
| driver | `Connect` and `GetCollection` (connection.ts) over `fake-driver.ts` |

The sync commit is `5a4e815` on AntelopeJS/mongodb `next`.

## Bounds and what is left out

A pass covers every scenario of the table under "The rules" under every order of its steps. The check leaves out:

- a read after the close: the consumer never does it, so `ReadCursor` reopening a cursor for a closed `reqId` is not judged.
- two cursors on one `reqId`, and `ReadCursor` calls that overlap.
- a cursor whose `aggregate` fails, and the driver's session pool.
- `$currentOp` as the server answers it: the check counts the cursors its own server holds.
- the external mode, which reads through `external-read.ts`.

## Confirm the check can fail

Each scratch change was applied to `src/` at the sync commit. Every run exited 1. With the change dropped, the check exits 0.

| Scratch change in the code | Result |
| --- | --- |
| `firstRow` returns `cursor.next()` without the kill after a close, the code before #116 | `BROKEN NoServerCursorAfterClose` |
| `nextRow` drops the `isClosed` test before the `aggregate` | `BROKEN CallsSettle`, `BROKEN NoServerCursorAfterClose`, `BROKEN NoRequestAfterClose` |
| `closeCursor` drops its cursor without calling `close()` | `BROKEN NoServerCursorAfterClose` |
| `CloseCursor` closes the query twice | none: a second close changes nothing, and the check exits 0 |
