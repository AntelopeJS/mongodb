# Features

- `smoke`: the module loads as `local` and one row written through the database interface is found with mongosh. Runs on every verdict.
- `module`: the config the module starts on or refuses, and the raw client of `@antelopejs/interface-mongodb`.
- `schema-indexes`: collections and `<index>__i` indexes created at registration, and their rebuild across a restart.
- `queries`: reads, writes, atomic mutations, instances and change feeds of an owned instance.
- `external`: an external instance reading and writing a client's database it did not create.
- `catalog`: the catalog an external instance answers, and its refusal on an owned one.
