# mongodb

The MongoDB backend of AntelopeJS, published as `@antelopejs/mongodb`. It implements `@antelopejs/interface-database` and `@antelopejs/interface-mongodb` (`antelopeJs.implements` in `package.json`) with the official `mongodb` driver.

## Layout

- `src/index.ts` holds the module lifecycle (`construct`, `start`, `stop`, `destroy`) and registers the implementations.
- `src/connection.ts` owns the shared `MongoClient` and the bookkeeping collection; `src/schema-initialization.ts` gates schema initialization across the lifecycle.
- `src/implementations/database/` implements the database interface: `query.ts` and `schema.ts` are the entry points; `pipeline.ts`, `expression.ts`, `selection.ts`, `atomic.ts`, `indexes.ts` and `instances.ts` translate queries, mutations, indexes and instances to MongoDB.
- `src/test/` holds the mocha suites; `src/test/antelope.test.ts` (`antelopeJs.test` in `package.json`) is the test project config, not a suite.

## Versioning

- The `@antelopejs/interface-*` packages are runtime dependencies with ranges. When you raise a minimum, update the range in `package.json` and the compatibility table in `README.md` together, and say so under "Upgrade notes" in `CHANGELOG.md`.
- A behavior change visible to callers (for example the error a write throws) goes under "Upgrade notes" in `CHANGELOG.md` with the migration step.

## Tests

- `pnpm test` builds `dist`, then runs `ajs module test .`, which loads this module with the config from `src/test/antelope.test.ts` and runs the compiled suites in `dist/test`.
- The test project starts a single-node MongoDB 8.0.8 replica set with `mongodb-memory-server-core` (change streams need a replica set). The first run downloads the server binary, so it needs network access; later runs reuse the cache.
- `pnpm run checks` runs every test suite; nothing else needs to run separately.

<!-- antelopejs-guardrails:start -->
<!-- Managed by @antelopejs/tooling-configs. `antelopejs-sync` rewrites everything between these markers; write repository-specific guidance outside them. -->

## Guardrails

Finish every change with `pnpm run checks`. It runs typecheck, lint, format, knip, tests, size, ratchet, and sync, and prints only what failed. The change is done when it prints no failure.

The tools enforce most rules, listed in the table at the end. The sections below hold the rules no tool checks.

### Comments

Say it in code. A constraint belongs in a type, a lint rule, or a test. A public item's contract belongs in TSDoc (`/** */`) on that item. The only other comments are lint directives that name their rules, `@ts-expect-error <reason>`, `SAFETY: <reason>` above a type assertion, `/*!` license headers, and bundler or formatter directives.

### Tests

Hold every test you write or keep to this checklist:

- Name the wrong implementation this test fails on. A test with no answer is deleted, not kept.
- No test reads the file it tests. Drive the behaviour through the public interface.
- An invariant over many files is a lint rule, not a test.

### Debt

`guardrails.ratchet.json` records the lint findings and oversize files this repository already had. Its numbers only go down. After you remove debt, run `pnpm run ratchet:update` and commit the lowered file. A rule listed there is off for `oxlint`. The ratchet counts it instead, and the pre-commit hook rejects it on every line you add.

### What enforces what

| Rule                                                                           | Enforced by                               |
| ------------------------------------------------------------------------------ | ----------------------------------------- |
| Formatting                                                                     | oxfmt                                     |
| Lint findings are errors; no rule warns                                        | oxlint preset                             |
| Comments are TSDoc, directives, or `SAFETY:` only                              | `antelopejs/no-comments`                  |
| Every test asserts                                                             | `antelopejs/require-assertion`            |
| Tests read no files outside fixtures, helpers, locales                         | `antelopejs/no-fs-read-in-tests`          |
| No assertion that restates a constant                                          | `antelopejs/no-constant-value-assertion`  |
| No test whose only check is "is a function"                                    | `antelopejs/no-typeof-function-only-test` |
| Params ≤ 5, function ≤ 120 lines, file ≤ 500 lines, depth ≤ 4, complexity ≤ 20 | `eslint/max-*`, `eslint/complexity`       |
| Files ≤ 20,000 bytes, tests included                                           | `antelopejs-check-size` (pre-commit)      |
| Existing debt only shrinks                                                     | `antelopejs-ratchet`                      |
| Types check                                                                    | `tsc` (pre-push)                          |
| No dead files, exports, or dependencies                                        | knip                                      |
| Managed files match the pack                                                   | `antelopejs-sync --check`                 |
| A test names the wrong implementation it catches                               | review; no tool                           |

<!-- antelopejs-guardrails:end -->
