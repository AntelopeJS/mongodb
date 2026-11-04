---
title: External mode
description: How an external instance of the MongoDB module reads and writes collections of a database it does not own, what it refuses, and what its catalog answers.
mode: reference
---

With `mode: "external"`, the module serves collections that already exist in a database it does not own, and reads and writes their documents. Use it to describe an existing database to AntelopeJS, often with a schema written from the catalog of the module. The module never changes the structure of that database. It creates, alters, and drops no collection, option, or index.

```typescript [antelope.config.ts]
import { defineConfig } from "@antelopejs/interface-core/config";

export default defineConfig({
  name: "erp-project",
  modules: {
    "mongodb-erp": {
      source: {
        type: "package",
        package: "@antelopejs/mongodb",
        version: "^1.6.0",
      },
      config: {
        mode: "external",
        url: "mongodb://erp_reader:secret@client-db:27017/?authSource=erp",
        database: "erp",
      },
      disabledExports: ["@antelopejs/interface-mongodb"],
    },
  },
});
```

`url` and `database` are required. An external instance without one of them, or with an empty one, refuses to construct. It does not fall back to the defaults of the owned mode. Projects usually set both through `envOverrides`, so the credentials stay out of the repository. A staging copy of the same data under another database name then needs only another variable:

```typescript [antelope.config.ts]
import { defineConfig } from "@antelopejs/interface-core/config";

export default defineConfig({
  name: "erp-project",
  modules: {
    "mongodb-erp": {
      source: {
        type: "package",
        package: "@antelopejs/mongodb",
        version: "^1.6.0",
      },
      config: { mode: "external" },
      disabledExports: ["@antelopejs/interface-mongodb"],
    },
  },
  envOverrides: {
    ERP_MONGODB_URL: "modules.mongodb-erp.config.url",
    ERP_MONGODB_DATABASE: "modules.mongodb-erp.config.database",
  },
});
```

One external instance serves one database. A project that needs both kinds of collection runs two instances of this module, one owned and one external, and pins each consumer to one of them through `importOverrides`.

An external instance does not implement `@antelopejs/interface-mongodb`, so no module gets its raw client. The project lists that interface in the `disabledExports` of the module, as in the examples above. The module depends on that interface only optionally, so the project still loads when no other module provides it. An external instance refuses `options.retryWrites: true`, because it keeps no second client for atomic mutations.

## The schema mirrors the database

The module maps nothing. The declaration uses the names of the database, verbatim:

- The table id is the collection or view, as in `@RegisterTable("orders", "erp")`.
- A field is a top-level key of the documents, as in `@Field("string") declare customerId: string`.

The schema id is a label. The module always reads the collections from the configured `database`. A field name with a dot or a leading `$` cannot be declared. An embedded document is one field of type `object`, and its subfields are not fields of their own.

## What registration does

Registration reads, once per declared table, and writes nothing. It reads the listing entry and the validator of the collection, its indexes, the two ends of its `_id` index, and the native types of a `$sample` of 1000 documents. A failure here fails the registration of the schema, so every query on that schema fails with the same message. Registration fails on these declarations:

- A declared collection that the database does not have, or that the connected account cannot read.
- `_id` declared on a collection whose documents have no usable `_id` (see [`_id`](#_id)).
- A field type that the module does not know.

Registration accepts some declarations and logs a warning:

- A field with sampled values of a type that the declaration does not read. Those values read as null, and the warning gives their share.
- A declared index that no index of the collection leads with. Queries stay correct and may be slow.
- A field where no native type covers 80% of the sampled values, when the module falls back to the default type for new values (see [Writing](#writing)).

## Values

A document answers `_id` and its declared fields, nothing else. The module reads each field as the type that its declaration names. A value of a type that the declaration does not read is null:

| Declared  | Reads                                    | Read as                                                                      |
| --------- | ---------------------------------------- | ---------------------------------------------------------------------------- |
| `string`  | string, ObjectId, UUID, Decimal128, Long | the text. ObjectId as hex, UUID canonical, numbers exact                     |
| `number`  | int, long, double, decimal               | a double. A Long beyond 2^53 and a Decimal128's digits beyond 15 are rounded |
| `boolean` | bool                                     | itself                                                                       |
| `date`    | date                                     | itself                                                                       |
| `object`  | embedded document                        | as the driver answers it                                                     |
| `T[]`     | array                                    | each element read as `T`                                                     |

The module never parses a string, so `"12.50"` in a `number` field reads as null. To read a Long or a Decimal128 field exactly, declare it `string`.

Comparisons use the stored value, so the indexes of the collection serve them. `get`, `getAll`, `between`, and `eq` or `ne` between a declared field and a constant compare natively. A string compared with a field that holds ObjectIds matches both forms, so a field that holds ids as ObjectIds and as strings answers both. A sort on a field whose values all read unchanged runs natively. `sum`, `avg`, `min`, and `max` over a `number` field reduce the stored values and round once, so a sum of Decimal128 values stays exact until that rounding.

## `_id`

Every document of a keyed collection answers `_id` as a string. Declare it as `@Field("string") declare _id: string`. Registration reads the type of the first and the last entries of the `_id` index. BSON sorts each type class into one run, so when both ends share a type class, every document does.

| `_id` type        | `_id` answered             | A key addresses     | Inserted without `_id` |
| ----------------- | -------------------------- | ------------------- | ---------------------- |
| ObjectId          | 24 hexadecimal characters  | the ObjectId        | a new ObjectId         |
| string            | itself                     | itself              | refused                |
| number            | its text, exact for a Long | the number          | refused                |
| UUID              | canonical text             | the UUID            | a new UUID             |
| embedded document | canonical Extended JSON    | the parsed document | refused                |
| date              | ISO text                   | the date            | refused                |

A key that does not parse as the type of the collection, such as `"nope"` for ObjectIds, matches nothing, and `get` answers not found.

The module reads a collection without a usable `_id` without one. These collections have no usable `_id`:

- A view.
- A time-series collection.
- A collection whose `_id` index ends hold two type classes.
- A collection whose embedded `_id` values have integer-like subfield names, which JavaScript reorders.

Queries, counts, and aggregations work on such a collection. `get`, `getAll` by `_id`, and writes are refused, and a declaration of `_id` on it fails registration.

## Writing

`insert`, `update`, `replace`, and `delete` work on every keyed collection. A collection without a usable `_id` is read-only.

- A payload names declared fields only. The module refuses any other key by name, `_internal` included, and drops `_instance`.
- `object` and array fields are read-only. `insert` refuses a value for one, and `update` and `replace` ignore it, so a document read and written back whole works.
- `_id` says which document is written, and is never assigned. An `update` whose `_id` names another document writes nothing and answers 0.
- The module writes a new value as the native type of the field. It picks that type among the types that the validator of the collection gives the field, or among the types the declared kind writes when the validator gives none. A `number` field takes the narrowest numeric type that holds every sampled number. Another field takes the type of at least 80% of the sampled values. When no type qualifies, the field takes `double` if the validator allows it, else the first type of the validator, else the default of the declared kind (string, double, bool, date). An update writes the value of each document as the type that this document already stores, so a field that holds two types keeps both.
- A value that does not convert, such as a non-hexadecimal string for an ObjectId field or a fraction for a Long, refuses the write before anything is sent, and the error names the field. On `update` and `replace`, the module accepts such a value only when every targeted document already reads as that value.
- A field keeps its stored value when the written value equals what a read of it answers. A document read and written back whole therefore loses nothing that a read could not spell, such as a Long beyond 2^53, the trailing zeros of a Decimal128, or an absent field written back as null. A write that changes nothing answers 0.
- `replace` writes every declared scalar field and removes those that the payload leaves out. Undeclared fields stay, because the module uses neither `replaceOne` nor any other replacement of the whole document.
- `insert` answers the `_id` of each new document, in input order.
- A duplicate key raises the `DuplicateKeyError` of the interface. The error names the MongoDB index and quotes no value.
- A validator refusal raises an error of kind `invalid` that names the properties. A write that the account may not make raises an error of kind `forbidden`. Both errors have the `name` `"ExternalWriteError"` and carry `kind` and `fields`. The package does not export the class, so check `name` and `kind` instead of `instanceof`.

The module does not report what other writers on the client's side change.

## What is refused

Each of these fails with a message that names what was refused:

- Writes to a collection without a usable `_id`, and `atomicMutation`.
- `update` with an expression mapper (`update(row => ...)`).
- `changes()`.
- A `get` or `getAll` on an external collection inside a `map` over an array. Use a lookup or a join instead.
- A named instance, `createInstance`, and `destroyInstance`. `listInstances` answers an empty list. External collections have no instances, and the default instance and `CROSS_INSTANCE` both read every document.

## The account to connect with

Give the module an account that can read, that can write the collections the module should write, and nothing else:

```javascript
db.getSiblingDB("erp").createRole({
  role: "dms_writer",
  privileges: [
    {
      resource: { db: "erp", collection: "orders" },
      actions: ["insert", "update", "remove"],
    },
    {
      resource: { db: "erp", collection: "customers" },
      actions: ["insert", "update", "remove"],
    },
  ],
  roles: [{ role: "read", db: "erp" }],
});
db.getSiblingDB("erp").createUser({
  user: "dms",
  pwd: "…",
  roles: ["dms_writer"],
});
```

MongoDB has no read-only session. The module sends only reads, apart from the writes it is asked for, and an account with the built-in `read` role is the guarantee. A write to a collection that the account was not granted fails as `forbidden`. Also set `statementTimeoutMs`, because these are someone else's production collections. The module sends it as `maxTimeMS` with every read and write.

## The catalog

An external instance also implements `@antelopejs/interface-database-catalog`. The catalog describes and profiles the database for a generator. It lists collections, fields, keys, and row estimates, and computes aggregates over the data. It answers with the registration rules of this module. A collection that it lists with a key registers with `_id`, and the declarations that it lists for a field are the ones that registration reads.

The catalog is a development tool. It answers under `ajs project dev` and `ajs project run`. Under `ajs project start`, it refuses unless `catalog: { enabled: true }` turns it on. `catalog: { enabled: false }` turns it off everywhere. An owned instance refuses every catalog call. Consumers reach the catalog through `importOverrides`, as for any provider.

**Fields come from a sample.** MongoDB declares no fields. The catalog describes each top-level key of the sample, and each property that the validator names, with its native types and their shares (`objectId 98% | string 2%`), the declarations that read it, and what those declarations lose. A field with no type that covers 80% of its values gets no proposed declaration. The `enum` and `description` of the validator become the labels and the comment of the field. The catalog declares no reference. It leaves references to the generator, which checks them with `CheckReference`.

**Profiles never write and never wait long.**

- Every measure is one aggregation with a `maxTimeMS` of 5 seconds and no disk use. It reads from a secondary when the deployment has one.
- A measure that times out answers `{ status: "timeout" }`, and the rest of the profile stands.
- The catalog reads a collection estimated above one million documents through a `$sample` of 50,000, and the answer says so. Two profiles of such a collection may differ slightly, because `$sample` takes no seed.
- Calls run one at a time.

**Profiles answer aggregates, not documents.**

- Text and ObjectId fields answer no minimum or maximum.
- A profile names a value only as a category of a field with at most 25 distinct values, and only if the value is counted at least 5 times. ObjectId and UUID fields have no categories.

**A sample of rows is for a person.** `SampleRows` is the one catalog function that answers values. It is for a person to look at, never for a generator or a model. It answers at most 20 documents, with every value as text cut at 200 characters, an embedded document as JSON text, and a binary value as its length.
