# Changelog

## v1.0.3

[compare changes](https://github.com/AntelopeJS/mongodb/compare/v1.0.2...v1.0.3)

### 🩹 Fixes

- **database:** Support cross-database $lookup for join and lookup stages ([695363f](https://github.com/AntelopeJS/mongodb/commit/695363f))
- **database:** Prevent $unwind after distinct in group callback context ([76f54c1](https://github.com/AntelopeJS/mongodb/commit/76f54c1))

### 🎨 Styles

- Apply linter formatting ([bb3af9b](https://github.com/AntelopeJS/mongodb/commit/bb3af9b))

### ❤️ Contributors

- Antony Rizzitelli <upd4ting@gmail.com>

## v1.0.2

[compare changes](https://github.com/AntelopeJS/mongodb/compare/v1.0.1...v1.0.2)

### 🩹 Fixes

- **database:** Return null for unresolved single-element subqueries ([881de10](https://github.com/AntelopeJS/mongodb/commit/881de10))
- **database:** Allow join/lookup/union in arg-based pipeline contexts ([1ffb969](https://github.com/AntelopeJS/mongodb/commit/1ffb969))

### ❤️ Contributors

- Antony Rizzitelli <upd4ting@gmail.com>

## v1.0.1

[compare changes](https://github.com/AntelopeJS/mongodb/compare/v1.0.0...v1.0.1)

### 🩹 Fixes

- **database:** Decode args in stage_get/stage_getAll for subquery context ([d18b3a4](https://github.com/AntelopeJS/mongodb/commit/d18b3a4))
- **database:** Clean up temporary $lookup fields from subquery results ([b82f0fb](https://github.com/AntelopeJS/mongodb/commit/b82f0fb))

### 🏡 Chore

- Update package.json and remove unused test file ([46424f7](https://github.com/AntelopeJS/mongodb/commit/46424f7))
- Update dependencies ([816127a](https://github.com/AntelopeJS/mongodb/commit/816127a))

### 🎨 Styles

- Apply linter formatting ([6a38b42](https://github.com/AntelopeJS/mongodb/commit/6a38b42))

### ❤️ Contributors

- Antony Rizzitelli <upd4ting@gmail.com>

## v1.0.0

[compare changes](https://github.com/AntelopeJS/mongodb/compare/v0.1.1...v1.0.0)

### 🚀 Enhancements

- Aql2 implementation and tests ([#12](https://github.com/AntelopeJS/mongodb/pull/12))
- AQL2 implementation with row-level operations and schema improvements ([#13](https://github.com/AntelopeJS/mongodb/pull/13))
- Better group typing ([bfed8f1](https://github.com/AntelopeJS/mongodb/commit/bfed8f1))
- Support function-based .update() for expression-based field updates ([8dad4ff](https://github.com/AntelopeJS/mongodb/commit/8dad4ff))
- Add multi-key .getAll() tests and fix $in query ([9451e43](https://github.com/AntelopeJS/mongodb/commit/9451e43))
- Add boolean to .getAll() key types ([40bcc90](https://github.com/AntelopeJS/mongodb/commit/40bcc90))
- Add union operation for streams ([e1ec7f2](https://github.com/AntelopeJS/mongodb/commit/e1ec7f2))
- Use Selection in lookup instead of Table ([3000cc4](https://github.com/AntelopeJS/mongodb/commit/3000cc4))
- **database:** Add conflict resolution options to insert ([bf22440](https://github.com/AntelopeJS/mongodb/commit/bf22440))

### 🩹 Fixes

- Handles literal values in aggregation expressions ([7de7e95](https://github.com/AntelopeJS/mongodb/commit/7de7e95))
- Datum.lookup ([65dcd5e](https://github.com/AntelopeJS/mongodb/commit/65dcd5e))
- ValueProxy.constant ([3e94913](https://github.com/AntelopeJS/mongodb/commit/3e94913))
- Relax update typing ([df38464](https://github.com/AntelopeJS/mongodb/commit/df38464))
- SingleSelection.update ([386a3ef](https://github.com/AntelopeJS/mongodb/commit/386a3ef))
- Union with different wrappedObjects ([c35873e](https://github.com/AntelopeJS/mongodb/commit/c35873e))
- **database:** Preserve existing instances on schema re-registration ([acea183](https://github.com/AntelopeJS/mongodb/commit/acea183))
- Remove incorrect timezone parameter ([3e660e9](https://github.com/AntelopeJS/mongodb/commit/3e660e9))
- **database:** Return epoch in seconds and add date operation tests ([1aee99b](https://github.com/AntelopeJS/mongodb/commit/1aee99b))
- Resolve biome lint warnings ([#14](https://github.com/AntelopeJS/mongodb/pull/14))
- **database:** Use $replaceWith for aggregation expressions in update ([c195d87](https://github.com/AntelopeJS/mongodb/commit/c195d87))

### 💅 Refactors

- Split boolean-option methods into dedicated variants ([afc5af9](https://github.com/AntelopeJS/mongodb/commit/afc5af9))
- **database:** Require explicit instance creation ([e07e46b](https://github.com/AntelopeJS/mongodb/commit/e07e46b))

### 📖 Documentation

- Improved shields ([#5](https://github.com/AntelopeJS/mongodb/pull/5))

### 📦 Build

- Replace rm -rf with rimraf ([#10](https://github.com/AntelopeJS/mongodb/pull/10))

### 🏡 Chore

- Remove ci publish adopt guidelines strict ts interface tests ([#11](https://github.com/AntelopeJS/mongodb/pull/11))
- Simplify CI workflow triggers, update AGENTS.md, and add @internal tag ([c1d0dfc](https://github.com/AntelopeJS/mongodb/commit/c1d0dfc))
- Update exports ([74f258e](https://github.com/AntelopeJS/mongodb/commit/74f258e))
- Lint & update exports ([494c6bc](https://github.com/AntelopeJS/mongodb/commit/494c6bc))
- Migrate from local beta interfaces to published @antelopejs packages ([d626a5c](https://github.com/AntelopeJS/mongodb/commit/d626a5c))

### ✅ Tests

- Add getAll().orderBy() chaining test ([82de8b1](https://github.com/AntelopeJS/mongodb/commit/82de8b1))
- **database:** Update tests for explicit instance lifecycle ([164df84](https://github.com/AntelopeJS/mongodb/commit/164df84))
- **database:** Add do operation sub-query get test ([e4e02c7](https://github.com/AntelopeJS/mongodb/commit/e4e02c7))
- **database:** Add do operation sub-query get test" ([7700f8c](https://github.com/AntelopeJS/mongodb/commit/7700f8c))

### 🤖 CI

- Remove test:coverage step from CI workflow ([cda2ba9](https://github.com/AntelopeJS/mongodb/commit/cda2ba9))

### ❤️ Contributors

- Antony Rizzitelli <upd4ting@gmail.com>
- Thomasims <thomas@antelopejs.com>
- Glastis ([@Glastis](http://github.com/Glastis))

## v0.1.1

[compare changes](https://github.com/AntelopeJS/mongodb/compare/v0.1.0...v0.1.1)

### 🚀 Enhancements

- Changelog generation is now using changelogen ([#8](https://github.com/AntelopeJS/mongodb/pull/8))
- Lookup ([3a261d6](https://github.com/AntelopeJS/mongodb/commit/3a261d6))

### 🩹 Fixes

- ImplementInterface no longer throw an error ([#6](https://github.com/AntelopeJS/mongodb/pull/6))
- Filter with object ([c5e8587](https://github.com/AntelopeJS/mongodb/commit/c5e8587))
- Single index names ([0279b19](https://github.com/AntelopeJS/mongodb/commit/0279b19))
- ListDatabases() ([13e92a5](https://github.com/AntelopeJS/mongodb/commit/13e92a5))
- Linting ([b61eb8a](https://github.com/AntelopeJS/mongodb/commit/b61eb8a))
- Revert index names ([a7337df](https://github.com/AntelopeJS/mongodb/commit/a7337df))
- Linting ([b688fe9](https://github.com/AntelopeJS/mongodb/commit/b688fe9))
- Linting ([6de9027](https://github.com/AntelopeJS/mongodb/commit/6de9027))
- Dont remove all fields in lookup ([99c6dc3](https://github.com/AntelopeJS/mongodb/commit/99c6dc3))

### 📦 Build

- Update prepare command ([7680f84](https://github.com/AntelopeJS/mongodb/commit/7680f84))
- Command 'build' that remove previous one before building ([#7](https://github.com/AntelopeJS/mongodb/pull/7))
- Update changelog config ([4c9da26](https://github.com/AntelopeJS/mongodb/commit/4c9da26))

### 🏡 Chore

- Update tsconfig.json paths ([ca9b86c](https://github.com/AntelopeJS/mongodb/commit/ca9b86c))
- Lint ([19d6c5d](https://github.com/AntelopeJS/mongodb/commit/19d6c5d))

### 🤖 CI

- Add GitHub Workflow to validate interface export ([#9](https://github.com/AntelopeJS/mongodb/pull/9))

### ❤️ Contributors

- Antony Rizzitelli <upd4ting@gmail.com>
- Thomas ([@Thomasims](http://github.com/Thomasims))
- Thomasims <thomas@antelopejs.com>
- Fabrice Cst <fabrice@altab.be>
- Glastis ([@Glastis](http://github.com/Glastis))

## [0.1.0](https://github.com/AntelopeJS/mongodb/compare/v0.0.1...v0.1.0) (2025-05-29)

### Features

- default config ([#4](https://github.com/AntelopeJS/mongodb/issues/4)) ([65b7174](https://github.com/AntelopeJS/mongodb/commit/65b7174ae0a091a9b7d496a49bbf45a7df9654f5))

## 0.0.1 (2025-05-08)
