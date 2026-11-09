---
covers:
  - src/connection.ts
  - src/schema-initialization.ts
  - src/implementations/database/schema.ts
  - src/implementations/database/indexes.ts
docs:
  - README.md
journeys:
  - index-layout
  - index-rebuild
---

# Schema and indexes

What `Schemas.register` leaves in the database of an owned instance: one `<schema>__<table>` collection per table, each declared index as `<index>__i` led by `_instance`, and the rebuild of an index whose definition changed.

## Sub-features

- `index-layout`: a plain, a compound, a cross-instance and a unique index; a cross-instance index also gets `<index>` without `_instance`; `getAll` and `between` read through an index; a unique index refuses a duplicate within one instance with `DuplicateKeyError` naming the declared index, not `<index>__i`, and accepts it in another instance; an index the operator created before registration stays.
- `index-rebuild`: a second `ajs project dev` registers the schema with `unique` added to an index and `crossInstance` removed from another; the index is rebuilt unique and the `<index>` the schema no longer declares stays.

## How to get to it (user POV)

- `new Schema(id, { table: { fields, indexes } })` in a module, then a query on it, which waits for registration.

## Driving it with antelopejs-verify

```bash
pnpm exec antelopejs-verify journey "$RUN" schema-indexes
```

## Gotchas

- The service's `reset` drops both databases before each journey, so `index-layout` creates the operator's index on a fresh collection.
- The readiness barrier between a query and an index build is proven by the formal check `formal-query-index-rebuild`, not by a journey: its interleavings are not reachable from outside.
