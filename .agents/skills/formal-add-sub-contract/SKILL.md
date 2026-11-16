---
name: formal-add-sub-contract
description: Property check, with fast-check and a real mongod, of ValueProxy add and sub in @antelopejs/mongodb against the interface-database contract over every receiver kind (field, constant, $let-bound), receiver type and operand type (src/implementations/database/expression.ts: stage_add, addNumber, addByReceiver, stage_sub, SecondsOnDate, CondenseValue). Use when a change touches those functions, or needs proof that add and sub answer seconds on dates, concatenate strings and lists, and never fail the pipeline.
---

# Formal check: `add` and `sub` over every operand type

The interface-database contract says `add` adds seconds to a date, adds two numbers, or concatenates two strings or two lists, and answers null when its operand is null; `sub` moves a date back by seconds, answers the seconds between two dates and subtracts two numbers. MongoDB reads a number on a date as milliseconds, and the server folds the constant branches of a `$cond` ahead of choosing one, so the translation in `expression.ts` branches on `$type` and moves receivers into a `$let` (#119, #120). A break here answers the wrong date, the wrong number of seconds, or makes MongoDB refuse the whole pipeline.

The check builds each query with the real `ValueProxy` of interface-database, runs it through the real `RunQuery` of `src/` against a mongod 8.0.8 that `mongodb-memory-server-core` starts, and compares the answer with `reference.ts`, the contract in plain JavaScript. It covers every combination of operation (`add`, `sub`), receiver kind (a field, a constant, a `$let`-bound expression: `field.default(value)`), receiver type, operand kind (a field or a constant) and operand type, over the types date, number, string, list and null. fast-check draws the values of each type with a fixed seed.

- `check.mjs` compiles this folder and the `src/` files it imports into a scratch folder with the repository's `tsc`, runs `check.ts` there, and deletes the folder.
- `check.ts` runs each case on a one-row collection and judges it by the rules.
- `reference.ts` is the contract. `values.ts` draws the values.

## Run it

From the repository root, with Node 24 and `pnpm install` done:

```sh
node .agents/skills/formal-add-sub-contract/check.mjs          # CI bounds
node .agents/skills/formal-add-sub-contract/check.mjs --full   # full bounds
```

`--values <n>` and `--seed <n>` override the bounds; each takes a positive integer. The check starts a mongod, so the first run downloads the server binary and needs network access; later runs reuse the cache that `pnpm test` uses. SIGINT, SIGTERM and SIGHUP stop the mongod.

| Bounds | Values per type | Cases | Wall time |
| --- | --- | --- | --- |
| CI (default) | 2 | 600 | 3 s |
| Full (`--full`) | 8 | 2,400 | 4 s |

Times are one run on a machine that had the binary cached, `tsc` included.

- Exit 0 means every rule that held still holds, and each finding still breaks.
- Exit 1 means a rule changed. `BROKEN` names a rule that held and now breaks, followed by the first failing cases. `HOLDS` names a finding that no longer breaks.
- Exit 2 means the check could not run: `tsc` failed, the mongod did not start, an argument was not a positive integer, or the check crashed.

A line that names a changed rule matches `^(?:BROKEN|HOLDS) +(\w+)`, and a recorded finding prints as `finding <rule>` with the number of cases that break it and the first four.

## When to run it

Run `--full` when a change touches a file under "Coverage record" or this folder. CI runs the CI bounds on every push, through `extra-commands` in `.github/workflows/ci.yml`.

## The rules

The contract covers a receiver that is a date, a number, a string or a list, with an operand of the receiver's type, a number of seconds on a date, a date for `sub` on a date, or null for `add`. A null receiver, a null operand of `sub` and an operand of another type are outside the contract: the types of `ValueProxy` forbid them, and an untyped caller can still send them.

| Rule | Plain sentence | Harm when it breaks | Today |
| --- | --- | --- | --- |
| `AnswersContract` | Every case the contract covers answers what `reference.ts` answers. | A caller gets a date moved by milliseconds, or seconds that are milliseconds. | holds |
| `NeverFailsPipeline` | Every case the contract covers runs without an error. | MongoDB refuses the whole pipeline for a valid `add` or `sub`. | holds |
| `OutsideContractNeverFailsPipeline` | A case outside the contract runs without an error, with any answer. | One ill-typed row stops the whole query. | finding F1 |

A finding is judged by its rule name alone, so a finding that gets worse in the same rule stays green. The two rules that hold bound F1: a valid combination that starts failing breaks `NeverFailsPipeline`.

## Findings at 5a4e815

**F1, a combination outside the contract fails the whole pipeline (`OutsideContractNeverFailsPipeline`).** Under the CI bounds 324 of the 600 cases are outside the contract and fail. MongoDB raises an error on the first row, so a query that maps one ill-typed row returns nothing. The failing combinations: `add` on a date, number or string receiver with an operand of another type (`$multiply only supports numeric types, not date`, `$concat only supports strings, not array`, `$add only supports numeric or date types, not string`), `add` on a list with an operand of another type, and `sub` on a string, list or number receiver, or on a date receiver with a string or list operand (`can't $subtract int from string`, `$multiply only supports numeric types, not array`). A null receiver fails only for `sub` with a string or a list operand. The types of `ValueProxy` reject these calls at compile time, so a typed caller never sends them. Whether an untyped caller, such as a query a user composes at run time, should get null or a typed error is the owner's call.

Neither finding is filed as an issue yet.

## Real bug or modeling error

The check runs the translation on a real mongod, so the server answers are real; the model is `reference.ts` and the list of what the contract covers. A failing case in `AnswersContract` or `NeverFailsPipeline` is a bug of the code or a wrong reading of the contract: read the TSDoc of `add` and `sub` in `@antelopejs/interface-database` (`dist/staged-query/valueproxy.d.ts`) before you change `reference.ts`. The check stores one document per case, with the receiver and the operand as fields, and reads it through `RunQuery` with the `map` stage the interface builds.

## Changing the check

- For an intended change to covered code, change `reference.ts` only where the contract changed, then run both bounds.
- Never weaken or drop a rule to make the check pass. A fixed finding leaves `FINDINGS` in `check.ts` once a case confirms the fix. A new finding goes into `FINDINGS` and into this file with its first cases.
- When you change the bounds or the types, rerun the scratch changes below. The CI bounds must still catch each one.
- After you confirm that the check describes the code at a newer commit, move the sync commit forward. Do it only then.

## Coverage record

| Part of the check | Code |
| --- | --- |
| add | `stage_add`, `addNumber` and `addByReceiver` (implementations/database/expression.ts:222-262) |
| sub | `stage_sub` (implementations/database/expression.ts:265-293) |
| seconds and receivers | `IsDate` and `SecondsOnDate` (implementations/database/expression.ts:98-120), `CondenseValue` (implementations/database/expression.ts:73-96) |
| query | `RunQuery` (implementations/database/query.ts) over a real mongod |

The sync commit is `5a4e815` on AntelopeJS/mongodb `next`.

## Bounds and what is left out

A pass covers every combination of the table under "The rules" with 2 values per type, or 8 with `--full`. The values are integers for numbers, dates between 1970 and 2096, strings of up to 6 characters and lists of up to 3 integers. The check leaves out:

- an operand that is itself a `$let`-bound or computed expression: the operand is a field or a constant.
- fractional numbers, `NaN`, infinities and dates out of range.
- the operands a call chains (`add(a, b)` with several operands) and `add` with no operand.
- `during`, `timeofday` and the other date operators.
- a document where the field is missing.

## Confirm the check can fail

Each scratch change was applied to `src/` at the sync commit. Every run exited 1 with the CI bounds. With the change dropped, the check exits 0.

| Scratch change in the code | Result |
| --- | --- |
| `MILLISECONDS_PER_SECOND` is 1: a number on a date reads as milliseconds, the code before #119 | `BROKEN AnswersContract` |
| `CondenseValue` keeps a constant string receiver inline, the code before #120 | `BROKEN NeverFailsPipeline` |
