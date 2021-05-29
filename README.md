# @yumcc/sharedb-postgres

感谢原先的 `sharedb-postgres` 和 `@plotdb/sharedb-postgre`，由于读操作是直接从数据库获取，业务庞大时对数据库并不友好，所以我这里基于redis加多一层缓存，具体可查看文档，维护在`@yumcc/sharedb-postgre`.

Thanks to the original `sharedb-postgres` and `@plotdb/sharedb-postgre`, since the read operation is obtained directly from the database, it is not friendly to the database when the business is large, so I add an extra layer of cache based on redis here, which can be viewed for details Documentation, maintained in `@yumcc/sharedb-postgre`.

# @plotdb/sharedb-postgres

PostgreSQL database adapter for [sharedb](https://github.com/share/sharedb). This driver can be used both as a snapshot store and oplog.

Doesn't support queries (yet?).

Moderately experimental. (This drives [Synaptograph](https://www.synaptograph.com)'s backend, and [@nornagon](https://github.com/nornagon) hasn't noticed any issues so far.)


## Note about versioning

This is a fork from the [original `sharedb-postgres`](https://github.com/share/sharedb-postgres) and its relative forks (see [billwashere](https://github.com/billwashere/sharedb-postgres-jsonb), [zbryikt](https://github.com/zbryikt/sharedb-postgres-jsonb). It seems to have been not maintained for a long time since 2018, Thus we decide to fork it and maintain it as `@plotdb/sharedb-postgre`.


## Requirements

Due to the fix to resolve [high concurency issues](https://github.com/share/sharedb-postgres/issues/1) Postgres 9.5+ is now required.

## Migrating older versions

Older versions of this adaptor used the data type json. You will need to alter the data type prior to using if you are upgrading. 

```PLpgSQL
ALTER TABLE ops
  ALTER COLUMN operation
  SET DATA TYPE jsonb
  USING operation::jsonb;

ALTER TABLE snapshots
  ALTER COLUMN data
  SET DATA TYPE jsonb
  USING data::jsonb;
```

## Usage

`sharedb-postgres-jsonb` wraps native [node-postgres](https://github.com/brianc/node-postgres), and it supports the same configuration options.

To instantiate a sharedb-postgres wrapper, invoke the module and pass in your
PostgreSQL configuration as an argument or use environmental arguments. 

For example using environmental arugments:

```js
var db = require('sharedb-postgres')();
var backend = require('sharedb')({db: db})
```

Then executing via the command line 

```
PGUSER=dbuser  PGPASSWORD=secretpassword PGHOST=database.server.com PGDATABASE=mydb PGPORT=5433 npm start
```

Example using an object

```js
var db = require('sharedb-postgres')({host: 'localhost', database: 'mydb'});
var backend = require('sharedb')({db: db})
```

## Error codes

PostgreSQL errors are passed back directly.

## Changelog

Note that version 3.0.0 introduces breaking changes in how you specify
connection parameters. See the [changelog](CHANGELOG.md) for more info.
