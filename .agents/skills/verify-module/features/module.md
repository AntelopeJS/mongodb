---
covers:
  - src/index.ts
  - src/config.ts
  - src/connection.ts
docs:
  - README.md
  - docs/external-mode.md
journeys:
  - module-dev
  - config-refused
  - raw-client
---

# Module

How the module is configured and what it registers: the config keys it refuses to start on, and the `MongoClient` an owned instance hands to `@antelopejs/interface-mongodb`.

## Sub-features

- `module-dev` is the pack's journey: the project answers, the consumer loads every implemented interface, Ctrl+C exits 130 and the port refuses afterwards.
- `config-refused`: an unknown `mode`, an external instance without `database` or `url`, `options.retryWrites` on an external instance, and a zero or fractional `statementTimeoutMs` each stop `ajs project dev` with the reason.
- `raw-client`: `GetClient()` answers the owned instance's client, and it counts the rows the database interface wrote in `<schema>__<table>`; an external instance registers no client, so `GetClient()` never answers.

## How to get to it (user POV)

- `modules.mongodb.config` in `antelope.config.ts`, and a module that imports `GetClient` from `@antelopejs/interface-mongodb`.

## Driving it with antelopejs-verify

```bash
pnpm exec antelopejs-verify journey "$RUN" module
```

## Gotchas

- `config-refused` runs `ajs project dev` with `context.command`, since `context.dev` waits for a project that comes up. The configs point at port 1, so a refusal that did not happen would still not reach a server.
- The external half of `raw-client` waits for the driver's call timeout, since an unregistered interface call waits rather than throws.
- Which of the two clients `GetClient` answers (the retrying one, not the atomic client with `retryWrites` off) is not checked by any journey.
- `raw-client` reads the client through the call's `chain`: `GetClient()`, then `db`, `collection` and `countDocuments` on the awaited results. The client itself cannot cross the wire.
