# @antelopejs/mongodb

<div align="center">
<a href="https://www.npmjs.com/package/@antelopejs/mongodb"><img alt="NPM version" src="https://img.shields.io/npm/v/@antelopejs/mongodb.svg?style=for-the-badge&labelColor=000000"></a>
<a href="./LICENSE"><img alt="License" src="https://img.shields.io/npm/l/@antelopejs/mongodb.svg?style=for-the-badge&labelColor=000000"></a>
<a href="https://discord.gg/sjK28QHrA7"><img src="https://img.shields.io/badge/Discord-18181B?logo=discord&style=for-the-badge&color=000000" alt="Discord"></a>
<a href="https://antelopejs.com/modules/mongodb"><img src="https://img.shields.io/badge/Docs-18181B?style=for-the-badge&color=000000" alt="Documentation"></a>
</div>

A full-featured MongoDB client module that implements both the MongoDB interface and the Database interface for AntelopeJS.

## Installation

```bash
ajs project modules add @antelopejs/mongodb
```

## Interfaces

This module implements two key interfaces:

- **MongoDB Interface**: Provides direct MongoDB operations and connection management
- **Database Interface**: Offers a standardized database abstraction layer

Both interfaces can be used independently or together depending on your application's needs. The interfaces are installed separately to maintain modularity and minimize dependencies.

| Name     | Install command                   |                                                                   |
| -------- | --------------------------------- | ----------------------------------------------------------------- |
| MongoDB  | `ajs module imports add mongodb`  | [Documentation](https://github.com/AntelopeJS/interface-mongodb)  |
| Database | `ajs module imports add database` | [Documentation](https://github.com/AntelopeJS/interface-database) |

## Overview

The AntelopeJS MongoDB module provides functionality for interacting with MongoDB:

- MongoDB client connection management through the MongoDB interface
- Common database operations through the Database interface

## Configuration

The MongoDB module supports connection using the native MongoDB driver with the following options:

```typescript
// MongoDB connection options
{
    url: "mongodb://localhost:27017",     // The MongoDB connection string
    id_provider: "uuid",                  // ID generation strategy: "uuid" (default) or "objectid"
    options: {                            // Optional MongoDB client options
        useNewUrlParser: true,
        useUnifiedTopology: true,
        maxPoolSize: 10,                  // Maximum number of connections in the pool
        connectTimeoutMS: 30000,          // Connection timeout in milliseconds
        socketTimeoutMS: 30000            // Socket timeout in milliseconds
    }
}
```

### Configuration Details

The module uses the official MongoDB Node.js driver to establish connections to your MongoDB servers:

- Connection using `MongoClient.connect()` from the mongodb package
- Support for standard MongoDB connection options
- Built-in connection pooling through the MongoDB driver
- ID generation strategies:
  - `uuid` (default): Uses UUID v4 for generating unique identifiers
  - `objectid`: Uses MongoDB's native ObjectId for document identifiers

## Atomic single-record mutations

`Table.atomicMutation(id, request)` implements the shared database interface contract with native `updateOne` and `deleteOne` commands. Each command matches one schema, table, instance, record identity, and condition. It never upserts. `CROSS_INSTANCE`, selections, and query-expression inputs are not supported.

Revision updates replace the supplied top-level fields, including whole nested objects, and install a required new revision in the same command. Patch values are literal data, not MongoDB expressions. Patches cannot change `id`, `_id`, `_instance`, or the revision field. Callers must use fresh revision tokens and must not reuse a deleted record's identity for a different incarnation.

A string revision matches exactly. `{ kind: "missing" }` matches only an existing record with an absent revision field; stored `null` does not match. `deleteIfEqual` deletes only when one field equals the supplied string, finite number, boolean, or valid `Date`. It rejects arrays and missing fields as matches and does not provide revision-based protection against a value changing away and back.

Acknowledged matches return `applied`; acknowledged misses return `not-applied`, including a wrong instance or missing record. An unacknowledged result or uncertain driver error returns `unknown`, which must not be interpreted as failure to write or automatically retried. Input validation errors throw before dispatch; known server validation failures also throw. The adapter uses a separate lazy MongoDB client with `retryWrites` and `retryReads` disabled, preserving the existing client's retry configuration. The additional client uses the configured connection and pool options and closes when the adapter disconnects.

### Identity uniqueness spans instances

All instances of a schema/table share a collection. MongoDB's existing `_id` unique index therefore applies across those instances, not separately within each tenant. A normal insert without a conflict mode throws a duplicate-key error instead of overwriting an existing record, including when another instance owns that identity. Use globally unique record identities within each schema/table. This change does not migrate identities or alter indexes.

### Interface prerequisite

This implementation requires `@antelopejs/interface-database` version `0.1.8` or later within the supported range. It includes the atomic mutation API, the `crossInstance` index flag, and shared real-backend conformance tests, automatically discovered by `ajs module test`. Backend-specific command, storage, and fault tests remain in this provider.

## Secondary indexes

All instances of a schema/table share one collection, and every document carries its instance in the `_instance` field. Queries scoped to one instance filter on `_instance` first, so the module builds every declared secondary index led by that field:

| Declared index                                    | Physical MongoDB indexes                                                             |
| ------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `email: {}`                                       | `email__i` on `{ _instance: 1, email: 1 }`                                           |
| `fullName: { fields: ["lastName", "firstName"] }` | `fullName__i` on `{ _instance: 1, lastName: 1, firstName: 1 }`                       |
| `category: { crossInstance: true }`               | `category__i` on `{ _instance: 1, category: 1 }` and `category` on `{ category: 1 }` |

A table without declared indexes gets a single-field `_instance` index instead, which serves its scoped queries. Every `<name>__i` index already starts with `_instance`, so tables with declared indexes do not need it; an existing `_instance` index is left in place.

Scoped `getAll`, `between`, and `orderBy` queries are served by the `<name>__i` index. Set `crossInstance: true` on the indexes that `CROSS_INSTANCE` queries rely on: the module then also maintains the unprefixed `<name>` index, which serves those queries. Cross-instance queries that name an index without `crossInstance` still return the same results, but cannot use an index and may be slow; the module logs a warning once per schema, table, and index when that happens. Plain filters never log this warning. The flag never changes query results.

`getAll` resolves an index through its declaration, so an index whose `fields` differ from its name matches on those fields. A compound index takes keys that hold one value per field, in declaration order. A flat list of values is one key:

```typescript
table.getAll(["Doe", "John"], "fullName");
```

A list of lists holds several keys:

```typescript
table.getAll(
  [
    ["Doe", "John"],
    ["Roe", "Jane"],
  ],
  "fullName",
);
```

The interface types `getAll` keys as scalars (`string | number | boolean`), so TypeScript rejects the list of lists form; pass each key as a proxy instead, for example `ValueProxy.constant(["Doe", "John"]).cast<string | number | boolean>()`. A scalar key, or a key whose length differs from the number of fields, throws. `between` requires a single-field index and throws on a compound one.

A query on a schema whose definition this process does not hold (for example a schema registered by another module that has not started yet) uses the index name as the field name, as earlier versions did, and never logs the cross-instance warning.

Indexes are created online with `createIndex` when a schema is registered. Several processes may register the same schema at once: an index that another process already created, or is creating, does not fail the initialization. A failed build is retried with the schema initialization.

Schema initialization runs in the background and does not delay a stop. Stopping the module interrupts it between two MongoDB operations: the operation in flight gets up to 2 seconds to finish before the connection closes, and the next start creates the collections and indexes that are still missing.

### Upgrading from earlier versions

Earlier versions created each declared index as `<name>` on its fields only. After the upgrade, the module creates the new `<name>__i` indexes next to them and never drops an index. An old `<name>` index stays in place and keeps serving queries: for an index declared with `crossInstance: true` it is reused as is, and for any other index it is no longer needed. It is not detected, logged, or dropped automatically. Once the `<name>__i` indexes are built, drop the unneeded ones by hand, for example from `mongosh`:

```javascript
db.getCollection("<schemaId>__<tableName>").dropIndex("<name>");
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
