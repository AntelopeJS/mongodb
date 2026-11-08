---
covers:
  - src/implementations/catalog/**
  - src/implementations/database/external-descriptor.ts
  - src/implementations/database/external-probe.ts
docs:
  - docs/external-mode.md
journeys:
  - catalog-describes
  - catalog-owned-refusal
---

# Catalog

`@antelopejs/interface-database-catalog` on an external instance: what a generator reads about the client's database before it writes a schema.

## Sub-features

- `catalog-describes`: `Info` names the engine, server version and database; `ListRelations` lists the collection keyed by `_id` and the view with no key; `DescribeRelations` lists each sampled field's native types, the declarations that read it with the lossy note on Long, the validator's description and enum as comment and labels, and the collection's indexes; `Profile` counts every row exactly and names no category counted fewer than 5 times; `SampleRows` answers each value as text.
- `catalog-owned-refusal`: an owned instance refuses `Info` with the reason.

## How to get to it (user POV)

- A generator module that calls the catalog interface under `ajs project dev` on a project whose mongodb module has `mode: "external"`.

## Driving it with antelopejs-verify

```bash
pnpm exec antelopejs-verify journey "$RUN" catalog
```

## Gotchas

- `estimatedRows` comes from the collection's metadata and matches the 2 seeded rows only because nothing else wrote to it.
- `catalog.enabled` under `ajs project start` is not driven: `context.dev` only runs `ajs project dev`.
