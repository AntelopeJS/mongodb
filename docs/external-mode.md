# External mode

`mode: "external"` points the module at collections that already exist, in a database it does not
own, and reads and writes their documents. It is for describing an existing database to
AntelopeJS, typically a schema written from the module's catalog. It never changes that database's
structure: no collection, option or index is created, altered or dropped.

```typescript
{
    mode: "external",
    url: "mongodb://erp_reader:…@client-db:27017/?authSource=erp",
    database: "erp",
}
```

`url` and `database` are required: an external instance without one, or with an empty one, refuses
to construct, rather than falling back to the owned mode's defaults. Projects normally set both
through `envOverrides`, so the credentials stay out of the repository, and a staging copy of the
same data under another database name needs only another variable:

```typescript
envOverrides: {
    ERP_MONGODB_URL: "modules.mongodb-erp.config.url",
    ERP_MONGODB_DATABASE: "modules.mongodb-erp.config.database",
},
```

One external instance serves one database. A project that needs both kinds of collection runs two
instances of this module, one owned and one external, and pins each consumer to one through
`importOverrides`.

An external instance does not implement `@antelopejs/interface-mongodb`: no module gets its raw
client. The project lists that interface in the module's `disabledExports`. The module depends on it
only optionally, so the project still loads when no other module provides it. `options.retryWrites: true` is refused, since an external instance keeps no second client
for atomic mutations.

## The schema mirrors the database

Nothing is mapped. The declaration uses the database's own names, verbatim:

- the table id is the collection or view: `@RegisterTable("orders", "erp")`
- a field is a top-level key of the documents: `@Field("string") declare customerId: string`

The schema id is a label: the collections are always read from the configured `database`. A field
name with a dot or a leading `$` cannot be declared. An embedded document is one field of type
`object`; its subfields are not fields of their own.

## What registration does

It reads, once per declared table, and writes nothing: the collection's listing entry and
validator, its indexes, the two ends of its `_id` index, and the native types of a `$sample` of
1000 documents. A failure here fails the schema's registration, so every query on it fails with
the same message:

- a declared collection the database does not have, or the connected account cannot read
- `_id` declared on a collection whose documents have no usable `_id` (below)
- a field type the module does not know

Some declarations are accepted with a warning: a field some of whose sampled values are of a type
the declaration does not read (they read as null, and the warning gives their share), and a
declared index that no index of the collection leads with (queries stay correct, and may be slow).

## Values

A document answers `_id` and its declared fields, nothing else. Each field is read as the type its
declaration names, and a value of a type the declaration does not read is null:

| Declared  | Reads                                    | Read as                                                                      |
| --------- | ---------------------------------------- | ---------------------------------------------------------------------------- |
| `string`  | string, ObjectId, UUID, Decimal128, Long | the text: ObjectId as hex, UUID canonical, numbers exact                     |
| `number`  | int, long, double, decimal               | a double: a Long beyond 2^53 and a Decimal128's digits beyond 15 are rounded |
| `boolean` | bool                                     | itself                                                                       |
| `date`    | date                                     | itself                                                                       |
| `object`  | embedded document                        | as the driver answers it                                                     |
| `T[]`     | array                                    | each element read as `T`                                                     |

A string is never parsed: `"12.50"` in a `number` field reads as null. Declare a Long or a
Decimal128 field `string` to read it exactly.

Comparisons stay on the stored value, so the collection's indexes serve them: `get`, `getAll`,
`between`, and `eq` or `ne` between a declared field and a constant compare natively. A string
compared with a field that holds ObjectIds matches both forms, so a field holding ids as ObjectIds
and as strings answers both. Sorting a field whose values all read unchanged sorts natively.
`sum`, `avg`, `min` and `max` over a `number` field reduce the stored values and round once, so a
sum of Decimal128 is exact until then.

## `_id`

Every document of a keyed collection answers `_id` as a string. Declare it as
`@Field("string") declare _id: string`. Registration reads the type of the first and last entries
of the `_id` index; BSON sorts each type class into one run, so when both ends share one, every
document does.

| `_id` type        | `_id` answered             | A key addresses     | Inserted without `_id` |
| ----------------- | -------------------------- | ------------------- | ---------------------- |
| ObjectId          | 24 hexadecimal characters  | the ObjectId        | a new ObjectId         |
| string            | itself                     | itself              | refused                |
| number            | its text, exact for a Long | the number          | refused                |
| UUID              | canonical text             | the UUID            | a new UUID             |
| embedded document | canonical Extended JSON    | the parsed document | refused                |
| date              | ISO text                   | the date            | refused                |

A key that does not parse as the collection's type, such as `"nope"` for ObjectIds, matches
nothing: `get` answers not found.

A collection without a usable `_id` is read without one: a view, a time-series collection, a
collection whose `_id` index ends hold two type classes, or one whose embedded `_id`s have
integer-like subfield names (JavaScript reorders them). Queries, counts and aggregations work;
`get`, `getAll` by `_id` and writes are refused, and declaring `_id` on it fails registration.

## Writing

`insert`, `update`, `replace` and `delete` work on every keyed collection. A collection without a
usable `_id` is read-only.

- A payload names declared fields only: any other key, `_internal` included, is refused by name.
  `_instance` is dropped
- `object` and array fields are read-only: a value for one is refused by `insert` and ignored by
  `update` and `replace`, so a document read and written back whole works
- `_id` says which document is written, and is never assigned: an `update` whose `_id` names
  another document writes nothing and answers 0
- A new value is written as the field's native type: the type the collection's validator gives
  it, else the type of at least 80% of the sampled values, else the declared kind's default
  (string, double, bool, date). An update writes each document's value as the type that document
  already stores, so a field holding two types keeps both
- A value that does not convert, such as a non-hexadecimal string for an ObjectId field or a
  fraction for a Long, refuses the write before anything is sent, naming the field
- A field keeps its stored value when the written value equals what a read of it answers, so a
  document read and written back whole loses nothing a read could not spell: a Long beyond 2^53, a
  Decimal128's trailing zeros, an absent field written back as null. A write that changes nothing
  answers 0
- `replace` writes every declared scalar field and removes those the payload leaves out. Undeclared
  fields stay: neither `replaceOne` nor any whole-document replace is used
- `insert` answers each new document's `_id` in input order
- A duplicate key raises the interface's `DuplicateKeyError`, naming the MongoDB index and quoting
  no value. A validator refusal raises an `ExternalWriteError` of kind `invalid` naming the
  properties, and a write the account may not make one of kind `forbidden`

Other writers on the client's side are theirs: nothing reports what they change.

## What is refused

Every one of these fails with a message naming what was refused:

- writes to a collection without a usable `_id`, and `atomicMutation`
- `update` with an expression mapper (`update(row => …)`)
- `changes()`
- a named instance, `createInstance` and `destroyInstance`; `listInstances` is empty. External
  collections have no instances: the default instance and `CROSS_INSTANCE` both read every document

## The account to connect with

Give the module an account that can read, and write the collections it should, and nothing else:

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

MongoDB has no read-only session: the module sends reads only, apart from the writes it is asked
for, and an account with the built-in `read` role is the guarantee. A write to a collection the
account was not granted fails as `forbidden`. Set `statementTimeoutMs` too: it is sent as
`maxTimeMS` with every read and write, and these are someone else's production collections.

## The catalog

An external instance also implements `@antelopejs/interface-database-catalog`, which describes and
profiles the database for a generator: collections, fields, keys, row estimates, and aggregates
over the data. It answers with this module's own registration rules, so a collection it lists with
a key registers with `_id`, and the declarations it lists for a field are the ones registration
reads.

It is a development tool. It answers under `ajs project dev` and `ajs project run`, and refuses
under `ajs project start` unless enabled explicitly with `catalog: { enabled: true }`. An owned
instance refuses it. Consumers reach it through `importOverrides`, as for any provider.

**Fields come from a sample.** MongoDB declares no fields, so each top-level key of the sample, and
each property the validator names, is described with its native types and their shares
(`objectId 98% | string 2%`), the declarations that read it, and what they lose. A field whose
values have no type covering 80% of them gets no proposed declaration. The validator's `enum` and
`description` become the field's labels and comment. No reference is declared: references are left
to the generator, which checks them with `CheckReference`.

**Profiles never write and never wait long.**

- Every measure is one aggregation with `maxTimeMS` of 5 s and no disk use, read from a secondary
  when the deployment has one.
- A measure that times out answers `{ status: "timeout" }`, and the rest of the profile stands.
- A collection estimated above one million documents is read through `$sample` of 50,000, and the
  answer says so. Two profiles of such a collection may differ slightly: `$sample` takes no seed.
- Calls run one at a time.

**Profiles answer aggregates, not documents.**

- Text and ObjectId fields answer no minimum or maximum.
- A value is named only as a category of a field with at most 25 distinct values, and only if it
  is counted at least 5 times. ObjectId and UUID fields have no categories.

**A sample of rows is for a person.** `SampleRows` is the one catalog function that answers
values, for a person to look at, never for a generator or a model: at most 20 documents, every
value as text cut at 200 characters, an embedded document as JSON text, a binary value as its
length.
