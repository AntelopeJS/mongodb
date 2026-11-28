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

CI runs `pnpm run checks` in full on every pull request and reports what failed. The commit hook formats and lints the staged files. Locally, check only what you touched, one test file and not the suite, as the [`verify-changes`](.agents/skills/verify-changes/SKILL.md) skill says.

### Comments

Say it in code: a constraint in a type, a lint rule or a test, a public item's contract in TSDoc. Other comments are lint directives naming their rules, `@ts-expect-error <reason>`, `SAFETY: <reason>` above a type assertion, and license or tool directives. A reason states what the code does now.

### Tests

Each test names the wrong implementation it fails on, or is deleted. Drive behaviour through the public interface. An invariant over many files is a lint rule.

### Review

No tool checks these:

- A type guard checks the nested elements and values it claims.
- A no-upward import rule rejects same-tier peers; probe it with an import.
- A finding only an evasion or a patched internal can trigger goes under Attention and never blocks.

### Suppressions

`guardrails.suppressions.json` caps lint directives, `@ts-expect-error`, `SAFETY:` comments and rules a lint config turns off. Over a cap, fix the code or remove another suppression of that kind; only the operator raises a number. After removing suppressions, run `pnpm run suppressions:update` and commit the file.

<!-- antelopejs-guardrails:end -->
