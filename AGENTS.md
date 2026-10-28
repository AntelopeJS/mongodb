# mongodb

The MongoDB backend of AntelopeJS, published as `@antelopejs/mongodb`. It implements `@antelopejs/interface-database`, `@antelopejs/interface-database-catalog` and `@antelopejs/interface-mongodb` (`antelopeJs.implements` in `package.json`) with the official `mongodb` driver.

## Layout

- `src/index.ts` holds the module lifecycle (`construct`, `start`, `stop`, `destroy`) and registers the implementations. `src/config.ts` resolves the module config, including `mode`: an `owned` instance creates and scopes its collections, an `external` one serves a client database it did not create.
- `src/connection.ts` owns two clients: the `MongoClient`, registered for `@antelopejs/interface-mongodb` only by an owned instance, and, for an owned instance, a separate client with `retryWrites` and `retryReads` off that `GetAtomicCollection` returns for atomic mutations. Keep them separate. It also owns the bookkeeping collection; `src/schema-initialization.ts` gates schema initialization across the lifecycle.
- `src/implementations/database/` implements the database interface: `query.ts` and `schema.ts` are the entry points; `pipeline.ts`, `expression.ts`, `selection.ts`, `atomic.ts`, `indexes.ts` and `instances.ts` translate queries, mutations, indexes and instances to MongoDB. The `external-*.ts` files are the external mode, which reads and writes collections the module did not create; `instance-scope.ts` is the owned mode's instance filter.
- `src/implementations/catalog/` implements the catalog interface for an external instance. `docs/external-mode.md` is the user documentation of both.
- `src/test/` holds the mocha suites; `src/test/antelope.test.ts` (`antelopeJs.test` in `package.json`) is the test project config, not a suite. `src/test-external/` holds the external-mode suites and the client database fixture they read.

## Versioning

- The `@antelopejs/interface-*` packages are runtime dependencies with ranges in `package.json`, listed in the compatibility table in `README.md`.
- The release commit writes `CHANGELOG.md`. Upgrade notes for that version, including raised interface minimums and behavior changes visible to callers (for example the error a write throws), follow in a separate `docs(changelog)` pull request, under `### Upgrade notes`.

## Tests

- `pnpm test` builds `dist`, then runs `ajs module test .`, which loads this module with the config from `src/test/antelope.test.ts` and runs the compiled suites in `dist/test` plus the conformance suites shipped in `@antelopejs/interface-database/dist/tests` (most of the tests). Changing the installed interface-database version changes that suite.
- The test project starts a single-node MongoDB 8.0.8 replica set with `mongodb-memory-server-core` (change streams need a replica set). The first run downloads the server binary, so it needs network access; later runs reuse the cache.
- `pnpm test` then runs the external-mode suites in `dist/test-external` with `MONGO_TEST_MODE=external`: the test project starts a replica set with authentication on, seeds the client database as its owner, turns its profiler on, and constructs the module as the `erp_writer` account. `pnpm test:external` runs only those suites.
- `pnpm run checks` runs every test suite; nothing else needs to run separately.

<!-- antelopejs-guardrails:start -->
<!-- Managed by @antelopejs/tooling-configs. `antelopejs-sync` rewrites everything between these markers; write repository-specific guidance outside them. -->

## Guardrails

Finish every change with `pnpm run checks`. It runs typecheck, lint, format, knip, tests, size, ratchet, suppressions, and sync, and prints only what failed. The change is done when it prints no failure.

The tools enforce most rules, listed in the table at the end. The sections below hold the rules no tool checks.

### Comments

Say it in code. A constraint belongs in a type, a lint rule, or a test. A public item's contract belongs in TSDoc (`/** */`) on that item. The only other comments are lint directives that name their rules, `@ts-expect-error <reason>`, `SAFETY: <reason>` above a type assertion, `/*!` license headers, and bundler or formatter directives.

Before suppressing a type rule, try inference or a type derived from its owner with `Parameters`, `ReturnType`, or indexed access. A private alias can still appear in a published declaration. Keep a suppression only after checking the published contract and the unchecked boundary it protects.

Check each directive reason against the current line and its callers. Returning a value is not the same as awaiting and discarding it. A reason must describe what the code does now.

### Tests

Hold every test you write or keep to this checklist:

- Name the wrong implementation this test fails on. A test with no answer is deleted, not kept.
- No test reads the file it tests. Drive the behaviour through the public interface.
- An invariant over many files is a lint rule, not a test.

### Cleanup proof

A behaviour-preserving cleanup PR shows `antelopejs-api-diff <base-ref>` passing. Published declarations and built runtime exports stay unchanged; pass `--allow-added <glob>` (repeatable, e.g. `'**/internal/**'`) to let added entries under that glob pass when their module is `@internal`, and copy its "Added (allowed)" list. A comment-only PR shows `antelopejs-comment-proof <base-ref>` passing instead of a verifier. Both compare committed HEAD.

Copy counts from the proof's base and HEAD table. Never type counts by hand. Keep the command and the committed head beside the copied output. The table counts a lint directive once per rule it names, inline directives included.

Pass `--probe <file.cjs>` to api-diff when changing receivers, positional calls, or modules that no published entry reaches. The same captured script runs against both worktree roots, supplied as `argv[2]`, and prints JSON observations. Matching metadata alone does not prove those behaviours.

An entry-only rule exempts only its own module. Add a second module entry rather than exempting another importer. A no-upward rule must reject peers in the same tier as well as higher tiers. Probe the rank decision with an import, not just a reading of the configuration.

Moving a method body into a function keeps how it is called: the same receiver, argument count, read order and laziness.

Verify a boundary against the imports a developer or agent would plausibly write: static and dynamic imports, re-exports, Vue `src`, CSS `@import`, and worker or child-process entries in normally named files. A construction only an evasion would write, an edit to resolution configuration (`package.json` `browser`, `exports`, `main` or `imports`, bundler aliases, tsconfig `paths`), and a difference observable only by patching private fields or internal methods go under Attention and never block. Public API and ordinary subclass behaviour stay part of the contract.

### Suppression budget

`guardrails.suppressions.json` caps the suppressions in authored code: lint directives per rule they name (`*` for none), `@ts-expect-error`, `@ts-ignore` and `SAFETY:` comments. A suppression over its ceiling fails: fix the code, or remove another suppression of the same kind. A rule that keeps needing suppressions should be changed in the preset. Never raise or remove a number in that file: the check fails a ceiling higher than on the pull request base, and only the operator approves a raise. A directive disabling `anti-slop/require-safety-comment-for-type-assertion` is burn-down debt: remove the assertion, or, where it holds, replace the directive with `SAFETY: <why it holds>`, which takes over its room without a raise. After you remove suppressions, run `pnpm run suppressions:update` and commit the lowered file.

### Debt

`guardrails.ratchet.json` records the lint findings and oversize files this repository already had. Its numbers only go down. After you remove debt, run `pnpm run ratchet:update` and commit the lowered file. A rule listed there is off for `oxlint`. The ratchet counts it instead, and the pre-commit hook rejects it on every line you add.

### What enforces what

| Rule                                                                           | Enforced by                                         |
| ------------------------------------------------------------------------------ | --------------------------------------------------- |
| Formatting                                                                     | oxfmt                                               |
| Lint findings are errors; no rule warns                                        | oxlint preset                                       |
| Comments are TSDoc, directives, or `SAFETY:` only                              | `antelopejs/no-comments`                            |
| Every test asserts                                                             | `antelopejs/require-assertion`                      |
| Tests read no files outside fixtures, helpers, locales                         | `antelopejs/no-fs-read-in-tests`                    |
| No assertion that restates a constant                                          | `antelopejs/no-constant-value-assertion`            |
| No test whose only check is "is a function"                                    | `antelopejs/no-typeof-function-only-test`           |
| Params ≤ 5, function ≤ 120 lines, file ≤ 500 lines, depth ≤ 4, complexity ≤ 20 | `eslint/max-*`, `eslint/complexity`                 |
| Cleanup preserves declarations and built runtime export metadata               | `antelopejs-api-diff <base-ref>`                    |
| Comment-only changes preserve JavaScript emit                                  | `antelopejs-comment-proof <base-ref>`               |
| PR-body counts come from tracked code files                                    | api-diff and comment-proof tables                   |
| Receiver, positional and unpublished-module behaviour                          | `antelopejs-api-diff <base-ref> --probe <file.cjs>` |
| Directive and `SAFETY:` reasons are true of the code                           | review; no tool                                     |
| Evasion-only and patched-internal findings go under Attention                  | review; no tool                                     |
| No-upward rules reject same-tier peers                                         | review and import probes                            |
| No `@ts-ignore`                                                                | `typescript/ban-ts-comment`                         |
| Files ≤ 20,000 bytes, tests included                                           | `antelopejs-check-size` (pre-commit)                |
| Suppressions stay within `guardrails.suppressions.json`                        | `antelopejs-suppressions` (pre-commit, checks)      |
| No added directive disables the `SAFETY:`, comment or test rules               | `antelopejs-suppressions` (pre-commit, checks)      |
| An added suppression gives a specific reason                                   | `antelopejs-suppressions` (pre-commit, checks)      |
| A block disable is closed in its file                                          | `antelopejs-suppressions` (pre-commit, checks)      |
| Types check                                                                    | `tsc` (pre-push)                                    |
| No dead files, exports, or dependencies                                        | knip                                                |
| Managed files match the pack                                                   | `antelopejs-sync --check`                           |
| Entry-only rules exempt no tracked outsider                                    | `antelopejs-sync --check`                           |
| A test names the wrong implementation it catches                               | review; no tool                                     |
| Existing debt only shrinks                                                     | `antelopejs-ratchet`                                |

<!-- antelopejs-guardrails:end -->
