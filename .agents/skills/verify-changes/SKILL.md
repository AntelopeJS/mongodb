---
name: verify-changes
description: How to check a change in this repository before you push it, scoped to what you touched. The command for each kind of change, the services a test needs, the worker cap, and what CI and the merge queue run instead of you. Use after you edit code, tests, configs or docs, before you commit or push.
---

# Verify changes

CI runs every check on every pull request, and the merge queue runs them again on the merged result. You check only what you changed, so a wrong change fails in seconds on your machine instead of after a CI round.

## Read the repository's own facts first

This skill names no script or path the repository can change. Read them from the repository:

- The `scripts` of the `package.json` of the package you changed. In a workspace (`pnpm-workspace.yaml`), that is the package's own `package.json`, not the root one.
- The `## Tests` section of `AGENTS.md`, or of the package's `README.md`, when one exists. It names the test runner, the fixtures and the services. Where it and this skill differ, follow it.
- `.github/compose.yml`, when it exists. It lists the services the tests need.

## Run the check for each kind of change

| You changed | Run |
| --- | --- |
| Any file | `pnpm exec oxlint <files>` and `pnpm exec oxfmt --check <files>` on the files you changed. |
| A type, a signature or an export | The `typecheck` script of the package you changed, such as `pnpm --filter <package> run typecheck` in a workspace. |
| Behaviour | The one test file that covers it. See [Run one test file](#run-one-test-file). |
| A `.vue` file | ESLint on the files you changed, `pnpm exec eslint <files>`, when the package lints with ESLint. |
| A lint directive, `@ts-expect-error` or `SAFETY:` comment | `pnpm run suppressions`. After you remove one, run `pnpm run suppressions:update` and commit `guardrails.suppressions.json`. |
| A file `antelopejs-sync` writes | Nothing by hand. Change `@antelopejs/tooling-configs` instead, then run `pnpm run sync:check`. |

The commit hook formats and lints the staged files again. A change of Markdown only needs the lint and format rows.

## Run one test file

Pass the path of one test file, never a pattern that matches the suite, and never run the `test` script alone: it runs the suite. Pick the command from the runner the `test` script calls:

- `ajs module test`: tests run from the build. Run `pnpm run build`, then `pnpm exec ajs module test . --timeout 30000 -f dist/<path>.test.js`.
- `node --test`: `node --test --test-concurrency=4 <file>`.
- `vitest`: `pnpm exec vitest run <file> --maxWorkers=4`.
- `mocha`: `pnpm exec mocha --timeout 30000 <file>`. Add `--parallel --jobs 4` only when the file is slow.
- `playwright`: `pnpm exec playwright test <file> --workers=4`.

Run at most 4 test workers. Other agents share the machine, and a runner that takes every core slows their checks and yours.

When a change touches several packages, run one test file in each package you changed.

## Start the services a test needs

Start a service only when the test file you run needs it. A test that needs a service it cannot reach fails on its connection, not on your change.

- MongoDB: run the test through a lease, such as `pnpm exec antelopejs-lease run -- pnpm exec ajs module test . --timeout 30000 -f dist/<path>.test.js`. The lease starts a mongod for this worktree on its own port and sets `MONGO_URL` and `MONGO_DATABASE`. When the test reads another variable, set it from these. Run `pnpm exec antelopejs-lease down --stop` when you are done.
- Another service of `.github/compose.yml`: start it under a project name of your own, such as `docker compose -p <worktree> -f .github/compose.yml up -d --wait <service>`. Remove it after the test with `docker compose -p <worktree> -f .github/compose.yml down`. When its port is taken, another worktree runs it: leave that test to CI.

Stop only what you started. Never stop a process or a container by a pattern match: it stops the work of other agents too.

## Leave the rest to CI

Do not run these locally. CI runs them on every pull request, and the merge queue runs them on the merged result:

- `pnpm run checks` and the full test suite.
- `knip`, `size`, `sync:check` when you changed no synced file, and the lockfile and dependency checks.
- The checks that read the npm registry, such as `published:types` and `readme:version`.
- Windows and the browser suites.

When CI reports a failure, reproduce that one check or test file locally, fix it, and push.

## Report what you ran

Name each command you ran and its result in your reply or pull request, and say which checks you left to CI.
