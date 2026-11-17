---
name: formal-group-mapper
description: Property check, with fast-check and a real mongod, of group(key, mapper) in @antelopejs/mongodb against the interface-database contract when a field of the mapper filters, orders or picks rows before it reduces them (src/implementations/database/pipeline.ts: stage_group). Use when a change touches stage_group or the pipeline stages a group mapper builds, or needs proof that one mapper field never changes its sibling fields or the group keys.
---

# Formal check: `group` mappers whose fields filter or order

The interface-database contract says `group(key, mapper)` calls the mapper with the stream of the rows of one group, and that the result of the mapper is one element of the new stream. Each field of the result reads the rows its own stages leave: a `filter`, an `orderBy` and an `nth` of one field never change a sibling field or the group key, and every key that has a row appears once. `stage_group` in `pipeline.ts` builds one `$group` for the whole mapper, so it has to keep the stages of each field inside that field (ANT-3422). A break here answers a sibling field from the rows another field kept, or drops a group whose rows all fail one filter.

The check builds each query with the real `Stream` of interface-database, runs it through the real `RunQuery` of `src/` against a mongod 8.0.8 that `mongodb-memory-server-core` starts, and compares the answer with `reference.ts`, the contract in plain JavaScript. fast-check draws the tables and the mappers with a fixed seed.

- `check.mjs` compiles this folder and the `src/` files it imports into a scratch folder with the repository's `tsc`, runs `check.ts` there, and deletes the folder.
- `check.ts` runs each case on a fresh table and judges it by the rules.
- `reference.ts` is the contract. `values.ts` draws the tables and the mappers.

## Run it

From the repository root, with Node 24 and `pnpm install` done:

```sh
node .agents/skills/formal-group-mapper/check.mjs          # CI bounds
node .agents/skills/formal-group-mapper/check.mjs --full   # full bounds
```

`--cases <n>` and `--seed <n>` override the bounds; each takes a positive integer. The check starts a mongod, so the first run downloads the server binary and needs network access; later runs reuse the cache that `pnpm test` uses. SIGINT, SIGTERM and SIGHUP stop the mongod.

| Bounds | Cases | Wall time |
| --- | --- | --- |
| CI (default) | 200 | 3 s |
| Full (`--full`) | 2,000 | 4 s |

Times are one run on a machine that had the binary cached, `tsc` included.

- Exit 0 means every rule that held still holds, and each finding still breaks.
- Exit 1 means a rule changed. `BROKEN` names a rule that held and now breaks, followed by the first failing cases. `HOLDS` names a finding that no longer breaks.
- Exit 2 means the check could not run. `tsc` failed, the mongod did not start, an argument was not a positive integer, the drawn cases left out something the rules need (see "Bounds and what is left out"), or the check crashed.

A line that names a changed rule matches `^(?:BROKEN|HOLDS) +(\w+)`, and a recorded finding prints as `finding <rule>` with the number of cases that break it and the first four.

## When to run it

Run `--full` when a change touches a file under "Coverage record" or this folder. CI runs the CI bounds on every push, through `extra-commands` in `.github/workflows/ci.yml`.

## The rules

A case is a table of 1 to 8 rows and a mapper of 2 to 4 fields. Each row has a group key (`a`, `b` or `c`), an integer `n` between -2 and 2, and an integer `t`, distinct per row, that orders the rows. The mapper always returns the group key, and each field is one of:

- an unfiltered reduction of `n`: `count()`, `sum`, `avg`, `min`, `max` or `distinct`.
- the same reduction behind a `filter` on `n` (`gt`, `lt`, `eq` or `ne` against a drawn threshold), or `filter`, `map` to `n`, then `count()`.
- `orderBy("t", "asc" | "desc").nth(0).key("t")`.

A reduction of no rows answers 0 for `count` and `sum`, null for `avg`, `min` and `max`, and an empty list for `distinct`. The group order is unspecified, so the check compares by key. It compares `distinct` as a sorted set and `avg` within 1e-9.

| Rule | Plain sentence | Harm when it breaks | Today |
| --- | --- | --- | --- |
| `AnswersReference` | Every drawn case answers what `reference.ts` answers, one row per group that has a row. | A caller reads a count, a sum or a key from the rows of another field's filter, or loses a group. | holds |
| `NeverFailsPipeline` | Every drawn case runs without an error. | MongoDB refuses the whole pipeline for a valid mapper. | holds |

No finding is recorded. A finding is judged by its rule name alone, so a finding that gets worse in the same rule stays green.

## Real bug or modeling error

The check runs the translation on a real mongod, so the server answers are real; the model is `reference.ts`. A failing case in `AnswersReference` is a bug of the code or a wrong reading of the contract. Read the TSDoc of `group`, `filter` and `nth` in `@antelopejs/interface-database` (`dist/staged-query/stream.d.ts`) before you change `reference.ts`. The check stores the rows of each case in the collection of the table, and reads them through `RunQuery` with the `group` stage the interface builds.

## Changing the check

- For an intended change to covered code, change `reference.ts` only where the contract changed, then run both bounds.
- Never weaken or drop a rule to make the check pass. A new finding goes into `FINDINGS` in `check.ts` and into this file with its first cases.
- When you change the bounds or the draws, rerun the scratch changes below. The CI bounds must still catch each one.
- After you confirm that the check describes the code at a newer commit, move the sync commit forward. Do it only then.

## Coverage record

| Part of the check | Code |
| --- | --- |
| group | `stage_group` (implementations/database/pipeline.ts) and the `pipelineInserter` it passes to `DecodeFunction` |
| fields around the group | `SplitGroupFields` (implementations/database/group-field.ts): a field's one order ranked before the `$group`, or copied and ranked after it, and its slices in a `$lookup` over `$documents` |
| stages of a field | `stage_filter`, `stage_orderBy`, `stage_nth`, `stage_count`, `stage_sum`, `stage_avg`, `stage_min`, `stage_distinct` (implementations/database/pipeline-core.ts), `stage_map` (implementations/database/pipeline.ts) and their selection overrides (implementations/database/selection.ts) |
| query | `RunQuery` (implementations/database/query.ts) over a real mongod |

The sync commit is `ac61a11`, the fix of ANT-3422 (AntelopeJS/mongodb#150).

## Bounds and what is left out

A pass draws the number of cases above from one seed. The draw puts one unfiltered and one filtered reduction in every mapper, with up to two more fields of any kind, in any order. The check exits 2 when the drawn cases hold no mapper with both kinds, no group that has rows but none that pass a filter of the mapper, or no `orderBy` field beside a filtered one, so a change of the bounds or the draws cannot hide them.

The check leaves out:

- `slice` and a second `orderBy` on one field.
- a `filter` on a field other than `n`, and a `map` that computes a value.
- a mapper that returns a nested object, a field built from two streams, and a mapper over a cross-instance table.
- a table with a compound index, and a group key that is not a plain field.
- a field of the mapper that holds a whole document: `nth(0)` without `.key("t")`.
- tables of more than 8 rows, and an empty table.

## Confirm the check can fail

Each change was applied to `src/` and the check ran with the CI bounds. With the change dropped, the check exits 0.

| Change in the code | Result |
| --- | --- |
| `stage_group` before ANT-3422 hoists the field's stages ahead of the `$group` | exit 1, `BROKEN AnswersReference`: 168 of 200 cases, such as `answers 0 rows for group b, the contract says 1` |

No change tried so far breaks `NeverFailsPipeline`. Before the fix of ANT-3422 the pipeline runs and answers wrong rows, so that rule guards a fix that builds a pipeline MongoDB refuses.
