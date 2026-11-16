---
name: formal-conflict-insert-race
description: Exhaustive interleaving check, with fast-check and a step scheduler, of concurrent conflict inserts (insert with conflict "update" or "replace") racing on one _id in @antelopejs/mongodb: ordered bulk upserts of several writers, deletes of the writers' rows, and another instance inserting the same _id. Use when a change touches UpsertAll in src/implementations/database/conflict-upsert.ts, PrimaryKeyDuplicateAt in indexes.ts, insertWithConflict or identityFilter in selection.ts, or needs proof that a conflict insert resolves and applies unless another instance holds its _id.
---

# Formal check: conflict inserts racing on one `_id`

An owned instance upserts a conflict insert with the filter `{ _id, _instance }`. MongoDB retries a duplicate-key upsert on the server only when the filter covers the key of the unique index that refused it, which is `_id` alone, so a writer that loses a race for a new `_id` gets E11000 (#135). `UpsertAll` runs the ordered upserts and, when operation `k` is refused on `_id_`, resumes the batch from `k`; an operation refused twice in a row throws. The filter keeps `_instance` so a row another instance holds is never overwritten. A break here makes callers see `DuplicateKeyError` for rows nobody holds, loses a writer's update, or overwrites another instance's row.

The check runs the real `RunQuery` and `UpsertAll` of `src/`, compiled with this folder, against `fake-collection.ts`, an in-memory collection that answers the `bulkWrite` of ordered upserts as mongod 8.0.8 does. An upsert is two steps: it looks for a row of its filter, then writes. A write that found none inserts, and meets E11000 when any row holds the `_id` by then. A write that found its row updates it, or inserts when a delete took the row in between. fast-check draws the scenarios with a fixed seed, and the explorer of `formal-query-index-rebuild` (`../formal-query-index-rebuild/explore.ts`) runs each scenario under every order of its steps, up to a bound.

- `check.mjs` compiles this folder and the `src/` files it imports into a scratch folder with the repository's `tsc`, runs `check.ts` there, and deletes the folder.
- `check.ts` runs a scenario: it starts every writer, the deleter and the other instance at once and judges each order by the rules.
- `rules.ts` holds the scenarios and the rules. `fake-collection.ts` is the collection. `server.ts` points `MongoClient.connect` at it, so `Connect` runs unchanged.

## Run it

From the repository root, with Node 24 and `pnpm install` done:

```sh
node .agents/skills/formal-conflict-insert-race/check.mjs          # CI bounds
node .agents/skills/formal-conflict-insert-race/check.mjs --full   # full bounds
```

`--scenarios <n>`, `--orders <n>` and `--seed <n>` override the bounds; each takes a positive integer. No service is needed.

| Bounds | Scenarios | Orders per scenario, at most | Orders run | Scenarios cut short | Wall time |
| --- | --- | --- | --- | --- | --- |
| CI (default) | 200 | 3,000 | 230,543 | 69 | 16 s |
| Full (`--full`) | 400 | 8,000 | 1,003,579 | 101 | 66 s |

Times are one run on a shared machine, `tsc` included. A cut scenario is explored from the first order up to the cap, so its later orders are not covered.

- Exit 0 means every rule that held still holds, and each finding still breaks.
- Exit 1 means a rule changed. `BROKEN` names a rule that held and now breaks, followed by the shortest counterexample. `HOLDS` names a finding that no longer breaks. The check stops after the first scenario that breaks a rule outside the findings.
- Exit 2 means the check could not run: `tsc` failed, an argument was not a positive integer, the code sent an operation the fake does not read, or the check crashed.

A line that names a changed rule matches `^(?:BROKEN|HOLDS) +(\w+)`, and a recorded finding prints as `finding <rule>` with its shortest counterexample.

## When to run it

Run `--full` when a change touches a file under "Coverage record" or this folder. CI runs the CI bounds on every push, through `extra-commands` in `.github/workflows/ci.yml`.

## The rules

A scenario is 1 to 3 writers that each insert one to three of the ids `a`, `b` and `c` as one batch (five operations at most), with `update` or `replace`; a deleter that removes up to two rows of the writers' instance; another instance that inserts up to one row under an id; and up to one row of the writers' instance that exists at the start. Each writer sets its own boolean field.

| Rule | Plain sentence | Harm when it breaks | Today |
| --- | --- | --- | --- |
| `WriterSettles` | Every writer resolves or rejects once every step has run. | A caller hangs for good. | holds |
| `ForeignRowKept` | A row another instance inserted keeps its instance and its fields. | A conflict insert overwrites another instance's row. | holds |
| `DuplicateOnlyForForeignId` | With no delete in the scenario, a writer is refused only for an `_id` another instance holds, and only with `DuplicateKeyError`. | A caller sees `DuplicateKeyError` for a row nobody holds. | holds |
| `AppliedOnce` | A writer applies each of its ids at most once. | A resume re-runs operations it already ran. | holds |
| `ConflictInsertsApply` | A writer that resolved wrote every id; unless a delete took the row later, the row exists, and under `update` carries the writer's field. | A caller's write is lost or skipped. | holds |
| `DuplicateOnlyForForeignIdUnderDeletes` | The refusal rule, in scenarios with a deleter. | A caller sees `DuplicateKeyError` for a row nobody holds. | finding F1 |

The fake enforces the unique `_id_` index, so one row per `_id` holds by construction and `ConflictInsertsApply` judges that the rows are there. A finding is judged by its rule name alone, so a finding that gets worse in the same rule stays green. `DuplicateOnlyForForeignId` bounds F1: the same refusal with no delete breaks it.

## Findings at 5a4e815

**F1, a delete and a re-insert between two attempts refuse a writer for its own `_id` (`DuplicateOnlyForForeignIdUnderDeletes`).** `UpsertAll` throws when operation `k` is refused twice in a row, to tell an `_id` another instance holds from a lost race (conflict-upsert.ts:30). A delete that lands between the two attempts breaks that test: the shortest counterexample starts with a row `c` of the writers' instance, `w1` and `w3` inserting `c`, `w2` inserting `b,c`, and a deleter removing `c` twice. The first removal clears `c`; `w1` and `w3` look for `c` and find none; `w1` writes it; `w3` is refused. The second removal clears it again; `w2` and `w3` look and find none; `w2` writes `c`; `w3` is refused a second time at the same operation and throws. `w3` rejects with `DuplicateKeyError` although `c` belongs to its own instance. The code's TSDoc names this case. The replay ran `dist/` against a mongod 8.0.8: eight writers each called `UpsertAll` with one `updateOne` upsert of `{ _id: "x", _instance: "tenant" }` while a loop deleted that row, for 3,000 rounds. It printed `343 writers refused as a duplicate on an _id no other instance holds`. Whether a conflict insert should then retry until it applies, or keep the documented refusal, is the owner's call.

Neither finding is filed as an issue yet.

## Real bug or modeling error

What the check treats as one step is the lookup of an upsert, the write of an upsert, the removal by the deleter, or the insert by the other instance. mongod runs each command atomically, and a single upsert runs its lookup and write as one unit when the row exists, so the check models only the gap that matters: a lookup that finds none, then an insert that meets a row that arrived in between. The check leaves out what happens inside one document write.

The fake answers like mongod 8.0.8 only for `bulkWrite` of `updateOne` and `replaceOne` upserts with the filter `{ _id, _instance }`: it never retries a duplicate key on the server, stops an ordered batch at the first refused operation, and reports the refused operation's position in the batch it received, as the driver does. Any other operation or filter stops the check with exit 2. A counterexample that needs another answer from the server is a modeling error until a replay shows mongod gives it.

Replay a counterexample on `dist/`, built by `pnpm run build`, against a real mongod, and assert only what a caller sees: which writers reject and with what, and the rows left. If the replay does not reproduce it, treat the check as wrong until you show it is not.

## Changing the check

- For an intended change to covered code, change the fake or the scenarios only where the code now does something they do not cover, then run both bounds.
- Never weaken or drop a rule to make the check pass. A fixed finding leaves `FINDINGS` in `check.ts` once a replay confirms the fix. A new finding goes into `FINDINGS` and into this file with its replay.
- When you change the bounds or the scenarios, rerun the scratch changes below. The CI bounds must still catch each one.
- After you confirm that the check describes the code at a newer commit, move the sync commit forward. Do it only then.

## Coverage record

| Part of the check | Code |
| --- | --- |
| conflict insert | `insertWithConflict` and `identityFilter` (implementations/database/selection.ts:69, 197-230), `RunQuery` (implementations/database/query.ts) |
| resume | `UpsertAll` (implementations/database/conflict-upsert.ts:15-37), `PrimaryKeyDuplicateAt` (implementations/database/indexes.ts:91) |
| driver | `Connect` and `GetCollection` (connection.ts) over `fake-collection.ts` |

The sync commit is `5a4e815` on AntelopeJS/mongodb `next`.

## Bounds and what is left out

A pass covers the drawn scenarios under every order of their steps, up to the cap of the table under "Run it". The check leaves out:

- a plain `insert` with no `conflict`, which runs `insertMany` and is not retried.
- a duplicate on a unique index other than `_id_`, which `UpsertAll` does not retry.
- the external mode, whose filter is `{ _id: { $eq } }` and does not reach `UpsertAll`.
- more than three ids, more than three writers, or a second deleter or foreign inserter.
- network errors and timeouts of the driver.

## Confirm the check can fail

Each scratch change was applied to `src/` at the sync commit. Every run exited 1 with the CI bounds. With the change dropped, the check exits 0.

| Scratch change in the code | Result |
| --- | --- |
| `UpsertAll` runs one `bulkWrite` of the whole batch, the code before #135 | `BROKEN DuplicateOnlyForForeignId` |
| `identityFilter` drops `_instance` from the filter | `BROKEN ForeignRowKept` |
| `UpsertAll` reads the driver's slice-relative error index as an absolute one | `BROKEN AppliedOnce` |
| `UpsertAll` resumes from the first operation instead of operation `k`, the single rerun of #135's first commit | `BROKEN AppliedOnce` |
| `UpsertAll` drops the test of a second refusal, so a held `_id` retries for ever | `BROKEN DuplicateOnlyForForeignId` (a writer exceeds 200 steps) |
