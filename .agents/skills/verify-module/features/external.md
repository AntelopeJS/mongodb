---
covers:
  - src/implementations/database/external-*.ts
  - docs/external-mode.md
docs:
  - docs/external-mode.md
journeys:
  - external-reads-writes
---

# External mode

An instance configured with `mode: "external"` serves a client's database it did not create: collections and fields by their own names, values read and written as their stored native types, and refusals for what would change the structure.

## Sub-features

- `external-reads-writes`: mongosh seeds `customers` with ObjectId keys, Long credits, a validator, an index of its own and a view. The module reads `_id` as hex, the declared fields only, Long values as numbers and the view's rows; it inserts with a new ObjectId, updates a Long as a Long, stamps no `_instance` and keeps undeclared fields; it refuses an undeclared key, a write to the view and `createInstance`; and the database ends with no new collection or index.

## How to get to it (user POV)

- `mode: "external"` with `url` and `database` in the module config, and a `Schema` whose table and field names are the client's.

## Driving it with antelopejs-verify

```bash
pnpm exec antelopejs-verify journey "$RUN" external
```

## Gotchas

- The run's MongoDB has no authentication, so the account rules of `docs/external-mode.md` are left to the `pnpm test:external` suites, which run as the `erp_writer` account.
