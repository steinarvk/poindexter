# poindexter

## hobbyist-grade document database for small and medium data

Poindexter is an experimental hobbyist "document database" built on top of
Postgres. It ingests schemaless JSON data and indexes it every
which way to make queries, including for nested data, fast, fun,
and easy.

## Hobbyist project disclaimer

Poindexter is a personal hobby project. It's written mostly for fun and
tailored to my individual whims, wants, and tastes. If you are
not the author of this software, you certainly should not use it in
production. Use a mainstream production-grade [document-oriented database]
or, depending on your needs, just use Postgres.

[document-oriented database]: https://en.wikipedia.org/wiki/Document-oriented_database

## Ingesting log-like data

Though individual items can be inserted with the simple HTTP
API directly, the main way to bulk-insert "log-like" data into
Poindexter is to create JSONL files in a directory on a (possibly
ephemeral) local disk and periodically sync the directory.
The CLI client, while also ultimately using the HTTP API for
uploading, makes this easy and efficient by deduplicating locally.

The "syncing a local directory" pattern allows scripts emitting
records to stay both simple and fast: all they need to do is
write to a file in JSONL format.

## Persistent entities

Conceptually, Poindexter considers records immutable.

However, by the use of special top-level attributes `entity_id`
and `supersedes_id`, individual records can be considered as part
of the change history of one persistent entity. Poindexter
enforces that for each entity, only one one record is *active* at
any one time and will refuse conflicting updates.

## Task queue

The top-level property `locked_until` is treated specially and,
if present, should be a timestamp, indicating that the record
is "locked" until a certain time.

The lock or lease it describes is entirely advisory.
All the special treatment amounts to is that `omit_locked`
queries are supported.

This allows Poindexter to work as a minimalist (or maximalist,
depending on your perspective) task queue. Cooperating distributed
workers can use it to fetch open tasks, lease them, and schedule
tasks for other workers. The entire history of the tasks scheduled,
leased, and completed will be captured and made searchable in
Poindexter as a natural side effect of orchestrating tasks in
this way.

## Usage examples

### Ingestion

The following example uses the poindexter CLI to watch and upload records
from JSONL files in a given directory as they appear (and grow).

```
$ poindexter client watch data/my-blog-records/
```

The CLI uses client-side deduplication to avoid unnecessarily re-reading
or re-uploading the same files or records. For the actual upload it uses
the HTTP API (configured by an in-directory configuration file).

### Querying

The following example uses the HTTP API directly to get all records from
the "demoblog" namespace where author="Jane Doe" and tags contains
"hello-world".

```
$ curl -s https://poindexter.example.com/api/query/demoblog/records/ \
       -u blog-reader:hunter2 \
       --data '{"filter": {"author": "Jane Doe", "tags": ["hello-world"]}}'
{"records":[{"namespace":"demoblog","record_id":"09c31485-f284-4c1a-9fe3-0d3340470a11","entity_id":"26ec53ce-4060-425a-94a9-a81cc4595650","timestamp":"2024-05-16T03:59:21.937726Z","timestamp_unix_nano":"1715831961937726000","record":{"author":"Jane Doe","id":"09c31485-f284-4c1a-9fe3-0d3340470a11","supersedes_id":"26ec53ce-4060-425a-94a9-a81cc4595650","tags":["frist-post","hello-world","blogging"],"text":"Hello world!","timestamp":1715831961.9377255,"title":"My first blog entry"}}]}
```

Below, a "values" query showing the number of occurrences of all
the different values of a specific field in records matching a query:

```
$ curl -s https://poindexter.example.com/api/query/demoblog/values/title/ \
  -u blog-reader:hunter2 \
  --data '{"filter": {"author": "Jane Doe"}}'
{"fields":[{"field":"title","count":2,"values":[{"value":"My first blog entry","count":1},{"value":"Why would I blog like this?","count":1}]}]}
```
