---
covers:
docs:
  - README.md
journeys:
  - module-dev
  - write-through-mongo
---

# Smoke

The fixed feature every verdict runs: load the module as `local` against the run's MongoDB, register a schema, insert one row through the database interface and find it with mongosh.

## Sub-features

- `module-dev` is the pack's journey: the project answers, the consumer loads the database, catalog and MongoDB interfaces, Ctrl+C exits 130 and the port refuses afterwards.
- `write-through-mongo`: `Schemas.register` and one `insert` through `RunQuery` end in the collection `smoke__notes`, with the row's `_instance`.

## How to get to it (user POV)

- A project with `@antelopejs/mongodb` configured with `url` and `database`, and a module that imports `@antelopejs/interface-database`, declares a `Schema` and awaits a query on it.

## Driving it with antelopejs-verify

```bash
pnpm exec antelopejs-verify journey "$RUN" smoke
```

## Gotchas

- The smoke feature covers no glob, so it never marks a file as mapped.
- A journey writes AQL with the builder of `@antelopejs/interface-database/staged-query` and sends `query.build()` to `RunQuery` through core, which is what `await query` does in a module. The driver module cannot construct a `Schema` or chain an AQL query itself: `Schema` is a class, and every step of a query is a thenable that would run.
