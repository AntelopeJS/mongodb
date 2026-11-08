---
covers:
  - src/implementations/database/query.ts
  - src/implementations/database/pipeline.ts
  - src/implementations/database/pipeline-core.ts
  - src/implementations/database/expression.ts
  - src/implementations/database/array-lookup.ts
  - src/implementations/database/change-event.ts
  - src/implementations/database/selection.ts
  - src/implementations/database/atomic.ts
  - src/implementations/database/instance-scope.ts
  - src/implementations/database/instances.ts
  - src/implementations/database/utils.ts
docs:
  - README.md
journeys:
  - rows-across-instances
  - change-feed
---

# Queries

AQL on an owned instance: reads, writes and atomic mutations scoped to their instance, the instance lifecycle and its bookkeeping, and change feeds.

## Sub-features

- `rows-across-instances`: two instances of one schema; `get`, `getAll`, `orderBy`, `slice`, `key`, `pluck`, `count`, `sum`, a `filter` expression, a `get` inside `map` that joins each order to its product, `update`, `delete`, an `atomicMutation` applied then refused as stale, `listInstances` and `destroyInstance`, read back with mongosh: each row carries its `_instance`, the destroyed instance's rows are gone from every table and only the other instance is left in `__antelope_instances`.
- `change-feed`: `changes()` on one instance streams that instance's insert, update and delete in order, with the row before and after, and skips another instance's insert; `CloseCursor` closes the server's change stream.

## How to get to it (user POV)

- `await schema.instance(id).table(name)...` in a module, and `for await (const change of table.changes())`.

## Driving it with antelopejs-verify

```bash
pnpm exec antelopejs-verify journey "$RUN" queries
```

## Gotchas

- A journey sends `query.build()` to `RunQuery`, and a change feed's stages to `ReadCursor`, which is what `await query` and `for await` do in a module.
- `change-feed` polls `$currentOp` until the change stream is open before it writes, since a write before that never reaches the stream.
- The `unknown` outcome of `atomicMutation` needs a lost acknowledgement and is not reachable from a journey.
