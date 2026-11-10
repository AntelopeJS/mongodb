# `@antelopejs/mongodb`

<div align="center">
<a href="https://www.npmjs.com/package/@antelopejs/mongodb"><img data-illustration alt="npm" src="https://img.shields.io/npm/v/@antelopejs/mongodb?style=for-the-badge&labelColor=000000&color=000000"></a>
<a href="./LICENSE"><img data-illustration alt="License" src="https://img.shields.io/npm/l/@antelopejs/mongodb?style=for-the-badge&labelColor=000000&color=000000"></a>
<a href="https://discord.gg/g4pFt6eXqu"><img data-illustration alt="Discord" src="https://img.shields.io/badge/Discord-18181B?logo=discord&style=for-the-badge&color=000000"></a>
<a href="https://interfaces.antelopejs.com/docs/database"><img data-illustration alt="Documentation" src="https://img.shields.io/badge/Docs-18181B?style=for-the-badge&color=000000"></a>
</div>

The MongoDB backend of AntelopeJS. It runs the queries of the database interface on MongoDB with the official [`mongodb`](https://github.com/mongodb/node-mongodb-native) driver, and gives that driver's client to modules that need it directly.

Part of [AntelopeJS](https://github.com/AntelopeJS/antelopejs). Used by [AntelopeJS DMS](https://github.com/AntelopeJS/dms), the open-source back office for your PostgreSQL, MongoDB or RethinkDB database.

## Interfaces

| Interface                                                                                            | Documentation                                                          |
| ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| [`@antelopejs/interface-database`](https://github.com/AntelopeJS/interface-database)                 | [Database](https://interfaces.antelopejs.com/docs/database)            |
| [`@antelopejs/interface-mongodb`](https://github.com/AntelopeJS/interface-mongodb)                   | [MongoDB](https://interfaces.antelopejs.com/docs/mongodb/introduction) |
| [`@antelopejs/interface-database-catalog`](https://github.com/AntelopeJS/interface-database-catalog) | [External mode](docs/external-mode.md#the-catalog)                     |

The database interface pages document schemas, tables, queries, change feeds, and atomic mutations.

## Install

```bash
ajs project modules add @antelopejs/mongodb
```

The command writes the default configuration of the module. It connects to `mongodb://localhost:27017` and uses the database `antelopejs`.

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

| Key                  | Description                                                                                                                                                                                                            |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`                | MongoDB connection string.                                                                                                                                                                                             |
| `database`           | The database that holds the collections of every schema.                                                                                                                                                               |
| `options`            | Optional [`MongoClientOptions`](https://mongodb.github.io/node-mongodb-native/6.21/interfaces/MongoClientOptions.html) that the module passes to the driver.                                                           |
| `mode`               | `owned` (the default) or `external`. An owned instance creates its own collections and scopes them to instances. An external instance serves a database it did not create. See [External mode](docs/external-mode.md). |
| `statementTimeoutMs` | Optional server-side time limit of each read and write of an external instance, sent as `maxTimeMS`. It must be a positive safe integer.                                                                               |
| `catalog`            | `{ enabled?: boolean }`. Sets whether an external instance answers the catalog outside development.                                                                                                                    |

An external instance requires a non-empty `url` and `database`. It reads and writes the documents of the client's own collections and never changes their structure. See [External mode](docs/external-mode.md).

Change feeds use MongoDB change streams, which need a replica set. A single-node replica set is enough in development.

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

- Each schema table is one collection, `<schemaId>__<table>`, that every instance of the schema shares. Each document stores its instance in `_instance`.
- `_id` is unique across instances, so use globally unique record identities.
- Each declared index `<name>` becomes a MongoDB index `<name>__i` whose first field is `_instance`. An index declared with `crossInstance: true` also gets an index `<name>` without the prefix, for `CROSS_INSTANCE` queries. Without it, those queries still work, but they cannot use an index and log a warning once per index.
- An index declared with `unique: true` makes its `<name>__i` index unique, so a key is unique per instance. The cross-instance index is never unique. A change of the flag rebuilds the index. The creation of a unique index fails while the instance holds duplicates. Schema initialization then logs the error and retries until you remove them.
- When MongoDB refuses a write for a duplicate key (code 11000), on `_id` or on a unique index, the module throws the `DuplicateKeyError` of the interface with the declared index name and fields.
- Atomic mutations use a second client with `retryWrites` and `retryReads` turned off.

Schema initialization drops an index only in two cases. The index has the name the module gives a declared index in MongoDB (`<name>__i`, or `<name>` for a cross-instance index) but other fields or another uniqueness. Or the index has the fields of such an index but another uniqueness. Any other index stays, such as an index `<name>` that leads with the declared field instead of `_instance`. To remove an index that no schema declares, drop it by hand, for example `db.getCollection("<schemaId>__<table>").dropIndex("<name>")`.

## Documentation

- [Database](https://interfaces.antelopejs.com/docs/database) documents the `@antelopejs/interface-database` contract that this module implements.
- [MongoDB](https://interfaces.antelopejs.com/docs/mongodb/introduction) documents the `@antelopejs/interface-mongodb` contract that this module implements.
- [External mode](docs/external-mode.md) explains how the module serves a database it did not create, and its catalog.
- [Configuration](https://framework.antelopejs.com/docs/concepts/configuration) documents module sources, `config` blocks, and config variables in `antelope.config.ts`.

## Compatibility

<!-- antelopejs-docs-compat:start -->

| Package                                  | Supported        | Tested with |
| ---------------------------------------- | ---------------- | ----------- |
| `@antelopejs/interface-core`             | `>=0.2.4 <1.0.0` | `0.4.0`     |
| `@antelopejs/interface-database`         | `>=0.3.1 <0.4.0` | `0.3.1`     |
| `@antelopejs/interface-database-catalog` | `>=0.0.3 <0.1.0` | `0.0.3`     |
| `@antelopejs/interface-mongodb`          | `>=0.0.7 <1.0.0` | `0.0.7`     |

<!-- antelopejs-docs-compat:end -->

`.nvmrc` sets the Node.js release for development. The `dependencies` of `package.json` set the range of the `mongodb` driver. `MONGODB_VERSION` in `src/test/antelope.test.ts` sets the MongoDB server release of the test replica set.

## Contributing

```bash
pnpm install
pnpm lint
pnpm format:check
pnpm knip
pnpm test           # build, then run the owned and external suites against an in-memory replica set
```

The tests start MongoDB with `mongodb-memory-server`, which downloads the server binary on the first run. They also run the shared conformance suite of `@antelopejs/interface-database`.

Read the [contribution guidelines](CONTRIBUTING.md) before you open a pull request.

## License

[Apache-2.0](LICENSE)
