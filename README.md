# @antelopejs/mongodb

<div align="center">
<a href="https://www.npmjs.com/package/@antelopejs/mongodb"><img alt="npm" src="https://img.shields.io/npm/v/@antelopejs/mongodb?style=for-the-badge&labelColor=000000&color=000000"></a>
<a href="./LICENSE"><img alt="License" src="https://img.shields.io/npm/l/@antelopejs/mongodb?style=for-the-badge&labelColor=000000&color=000000"></a>
<a href="https://discord.gg/g4pFt6eXqu"><img src="https://img.shields.io/badge/Discord-18181B?logo=discord&style=for-the-badge&color=000000" alt="Discord"></a>
<a href="https://interfaces.antelopejs.com/docs/database"><img src="https://img.shields.io/badge/Docs-18181B?style=for-the-badge&color=000000" alt="Documentation"></a>
</div>

The MongoDB backend of AntelopeJS: it runs the database interface's queries on MongoDB with the official [`mongodb`](https://github.com/mongodb/node-mongodb-native) driver, and exposes that client to modules that need it directly.

## Interfaces

| Interface                                                                                            | Documentation                                                          |
| ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| [`@antelopejs/interface-database`](https://github.com/AntelopeJS/interface-database)                 | [Database](https://interfaces.antelopejs.com/docs/database)            |
| [`@antelopejs/interface-mongodb`](https://github.com/AntelopeJS/interface-mongodb)                   | [MongoDB](https://interfaces.antelopejs.com/docs/mongodb/introduction) |
| [`@antelopejs/interface-database-catalog`](https://github.com/AntelopeJS/interface-database-catalog) | [External mode](docs/external-mode.md#the-catalog)                     |

Schemas, tables, queries, change feeds and atomic mutations are documented on the database interface pages.

## Install

```bash
ajs project modules add @antelopejs/mongodb
```

The command writes the module's default configuration: `mongodb://localhost:27017`, database `antelopejs`.

## Configuration

```typescript [antelope.config.ts]
import { defineConfig } from "@antelopejs/interface-core/config";

export default defineConfig({
  name: "my-project",
  modules: {
    mongodb: {
      source: {
        type: "package",
        package: "@antelopejs/mongodb",
        version: "^1.4.2",
      },
      config: {
        url: "mongodb://localhost:27017/?replicaSet=rs0",
        database: "my-project",
        options: { maxPoolSize: 10 },
      },
    },
  },
  envOverrides: {
    MONGODB_URL: "modules.mongodb.config.url",
  },
});
```

| Key                  | Description                                                                                                                                                           |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`                | MongoDB connection string.                                                                                                                                            |
| `database`           | Database that holds every schema's collections.                                                                                                                       |
| `options`            | Optional [`MongoClientOptions`](https://mongodb.github.io/node-mongodb-native/6.21/interfaces/MongoClientOptions.html) passed to the driver.                          |
| `mode`               | `owned` (default): the module creates and scopes its own collections. `external`: it serves a database it did not create, see [External mode](docs/external-mode.md). |
| `statementTimeoutMs` | Optional server-side time limit of each read and write of an external instance, sent as `maxTimeMS`.                                                                  |
| `catalog`            | `{ enabled?: boolean }`: whether an external instance answers the catalog outside development.                                                                        |

An external instance requires `url` and `database`, and reads and writes the client's own collections without changing their structure: see [External mode](docs/external-mode.md).

Change feeds rely on MongoDB change streams, which need a replica set. A single-node replica set is enough in development.

## Quick start

In the module that stores data, add the database interface:

```bash
pnpm add @antelopejs/interface-database
```

Then follow the [database interface guide](https://framework.antelopejs.com/docs/guides/database) to declare a schema and query it. The same code runs on [`@antelopejs/postgres`](https://github.com/AntelopeJS/postgres) and [`@antelopejs/rethinkdb`](https://github.com/AntelopeJS/rethinkdb).

To call the driver directly instead, add `@antelopejs/interface-mongodb`:

```typescript
import { GetClient } from "@antelopejs/interface-mongodb";

const client = await GetClient();
const users = client.db("my-project").collection("users");
```

## Storage layout

- Each schema table is one collection, `<schemaId>__<table>`, shared by every instance of the schema. Each document stores its instance in `_instance`.
- `_id` stays unique across instances: use globally unique record identities.
- Each declared index `<name>` becomes a `<name>__i` MongoDB index led by `_instance`. An index declared with `crossInstance: true` also gets an unprefixed `<name>` index for `CROSS_INSTANCE` queries; without it, those queries still work but log a one-time warning and cannot use an index.
- An index declared with `unique: true` makes its `<name>__i` index unique, so a key is unique per instance; the unprefixed cross-instance index never is. Changing the flag rebuilds the index. Creating a unique index fails while the instance already holds duplicates: schema initialization logs the error and retries until they are removed.
- A write refused for a duplicate key (MongoDB code 11000), on `_id` or on a unique index, throws the interface's `DuplicateKeyError` with the declared index name and fields.
- Atomic mutations use a second client with `retryWrites` and `retryReads` disabled.

Versions before 1.4 created each index as `<name>` only. The module creates the `<name>__i` indexes next to them and never drops an index: drop the old ones you no longer need by hand, for example `db.getCollection("<schemaId>__<table>").dropIndex("<name>")`.

## Documentation

- [Database](https://interfaces.antelopejs.com/docs/database): the `@antelopejs/interface-database` contract this module implements
- [MongoDB](https://interfaces.antelopejs.com/docs/mongodb/introduction): the `@antelopejs/interface-mongodb` contract this module implements
- [External mode](docs/external-mode.md): serving a database the module did not create, and its catalog
- [Configuration](https://framework.antelopejs.com/docs/concepts/configuration): module sources, `config` blocks and config variables in `antelope.config.ts`

## Compatibility

<!-- antelopejs-docs-compat:start -->

| Package                                  | Supported        | Tested with |
| ---------------------------------------- | ---------------- | ----------- |
| `@antelopejs/interface-core`             | `>=0.2.4 <1.0.0` | `0.2.4`     |
| `@antelopejs/interface-database`         | `>=0.2.0 <0.3.0` | `0.2.0`     |
| `@antelopejs/interface-database-catalog` | `>=0.0.3 <0.1.0` | `0.0.3`     |
| `@antelopejs/interface-mongodb`          | `>=0.0.6 <1.0.0` | `0.0.6`     |

<!-- antelopejs-docs-compat:end -->

| Package                                  | Supported range  |
| ---------------------------------------- | ---------------- |
| `@antelopejs/interface-database`         | `>=0.2.0 <0.3.0` |
| `@antelopejs/interface-mongodb`          | `>=0.0.6 <1.0.0` |
| `@antelopejs/interface-database-catalog` | `>=0.0.3 <0.1.0` |
| `@antelopejs/interface-core`             | `>=0.2.4 <1.0.0` |

The module is developed and tested against `@antelopejs/core` `^1.13.4` on Node.js 24, with the `mongodb` driver `^6.21.0` and a MongoDB 8.0 replica set.

## Contributing

```bash
pnpm install
pnpm lint
pnpm format:check
pnpm knip
pnpm test           # build, then ajs module test against an in-memory replica set
```

The tests start MongoDB with `mongodb-memory-server`, which downloads the server binary on the first run. They also run the shared conformance suite of `@antelopejs/interface-database`.

Read the [contribution guidelines](CONTRIBUTING.md) before opening a pull request.

## License

[Apache-2.0](LICENSE)
