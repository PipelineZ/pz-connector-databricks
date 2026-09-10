# Pz.Connector.Databricks

Databricks source and sink for [PipelineZ](https://pipelinez.dev) (`pz`), served out of process. A
table or query reads through the **SQL Statement Execution API** as Arrow, one partition per result
chunk; a sink stages rows as Parquet in a Unity Catalog volume and lands them into the target with a
single `insert`, `create or replace table as`, or `merge`.

## Installation

```yaml
# project.yml
connectors:
  - package: Pz.Connector.Databricks
    version: 0.1.0
```

`pz restore` installs the Native AOT binary for your platform (linux-x64, linux-arm64, osx-arm64,
win-x64) and `pz run` spawns it. Needs pz 0.6.0 or newer.

| RID | status |
|---|---|
| `linux-x64` | the platform every test and the packaging proof run on |
| `linux-arm64`, `osx-arm64`, `win-x64` | shipped, never exercised by this connector's own CI |

## Capabilities

| Capability | Meaning |
|---|---|
| `ColumnPruning` | a read projects only the engine's pruned column list |
| `PredicatePushdown` | `ReadHints.PredicateSql` is pushed into the generated `where` as a parenthesized term |
| `BoundedWindow` | a bounded incremental window (`initial`/`max_window`/`until`) is supported |
| `InclusiveWatermarkBound` | the watermark's lower bound can be inclusive (`>=`) as well as exclusive (`>`) |
| `PartitionedRead` | a read fans out across the result's chunks -- however many the service decides to return |
| `Merge` | a sink output can declare `strategy: merge` with `keys:` |
| `ReplaceWrites` | a sink output can declare `strategy: replace` |
| `Transactional` | exactly one statement touches the write target; a crash before it leaves the target untouched, and `AbortAsync` discards everything staged |

There is no native scan or native copy: every read and write goes through the Arrow `RecordBatch`
path.

## Connection

```yaml
# connections.yml
dbx:
  connector: databricks
  host: https://adb-1234567890.12.azuredatabricks.net   # required: workspace origin, no path/query/fragment
  warehouse_id: abcdef1234567890                        # required: SQL warehouse id, lowercase hex
  auth: token                                           # required: token | oauth
  token: ${DATABRICKS_TOKEN}                            # auth: token
  client_id: ${DBX_CLIENT_ID}                           # auth: oauth
  client_secret: ${DBX_CLIENT_SECRET}                   # auth: oauth
  catalog: main                                         # optional: default catalog
  schema: default                                       # optional: default schema
  staging_volume: main.pz.staging                       # required for any write: 3-part UC volume name
```

- `host` must be an `https://` URL with no path, query, or fragment -- it is spliced into every
  request URL.
- `warehouse_id` must match `[a-f0-9]+`.
- `auth: token` requires `token` and forbids `client_id`/`client_secret`; `auth: oauth` requires
  both `client_id` and `client_secret` and forbids `token`. Violations are `PZDB01xx` before any
  network call.
- `catalog`/`schema` become the statement's session defaults (the submit request's own `catalog`/
  `schema` fields), so an unqualified name in `query:` resolves exactly as an entity name does.
- `staging_volume` is validated only when a sink opens: missing or not 3-part is `PZDB0106`. Files
  land under `/Volumes/<catalog>/<schema>/<volume>/pz/<tag>/part-N.parquet`, `tag` a per-session
  random id, so concurrent sinks never collide.
- `pz connector check` (`CheckConnectionAsync`) looks up the warehouse, then, only when it is
  `RUNNING`, runs `select 1` through it. It reports `warehouse <name> RUNNING`,
  `warehouse <name> is STARTING/STOPPED/STOPPING` (not a failure -- a stopped warehouse starts on
  the first statement), or the auth failure.
- `token`, `client_secret`, every minted access token, any `Authorization` header value echoed back
  in an error, and presigned chunk URLs (reduced to `https://<host>/<redacted>`, since they carry
  signatures) are redacted from every message. Statement text is never included in an error, only
  the entity name.

## Naming an entity

An entity name is `table`, `schema.table`, or `catalog.schema.table`. A 2-part name fills `catalog`
from the connection; a 1-part name fills both `catalog` and `schema`. A name that still can't be
resolved (no default on the connection) is `PZDB0104`. Backticks are not accepted (`PZDB0105`) --
the name is the object name, not a SQL fragment; every generated identifier is backtick-quoted
(a literal backtick inside a part is doubled).

```yaml
  entities:
    orders:
      read:
        entity: sales.orders           # optional; defaults to the pz entity name itself
    recent_orders:
      read:
        query: select * from sales.orders where region = 'EU'   # query mode, below
    orders_out:
      write:
        entity: marts.orders_daily     # optional; defaults to the pz entity name
```

## Reading data

### Table mode (default)

`entity:` names a table or view directly. The generated statement is
`select <cols|*> from <entity> where (<predicate>) and (<lower bound>) and (<upper bound>)`, each
term self-parenthesized and present only when the engine supplies it. `hints.Columns` drives the
projection; when a column needs serializing (below) an empty pruned list still projects every
schema column explicitly instead of falling back to `*`. A predicate containing a double quote is
never pushed down (Databricks SQL reads `"..."` as a string literal, not the quoted identifier
DuckDB means it as) and is instead left for the engine to filter locally. Views read fine.

There is no partition-count option: the read fans out across however many result chunks the
service decides to return for that statement, never a count the connector or the config requests.

### Query mode (`query:`)

`query:` runs arbitrary SQL verbatim -- no pushdown of any kind. `entity:` and `query:` cannot both
be set on the same read (`PZDB0201`).

### Incremental reads

Watermark bounds travel as named, typed statement parameters (`:pz_lower`, `:pz_upper`), never
spliced literals. The parameter type comes from the cursor column's Databricks type in the schema
probe; supported cursor types are the integer family, `DECIMAL`, `DATE`, `TIMESTAMP`,
`TIMESTAMP_NTZ`, and `STRING`. A cursor column absent from the schema, or typed something else, is
`PZDB0202` before any statement runs -- and so is a cursor column whose value is JSON/string
serialized in SQL (below), since a projected expression has no storage to compare a parameter
against. The lower-bound comparison is `>` or `>=` per `InclusiveWatermarkBound`; the upper bound
is always `<=`.

### Type mapping (Databricks to Arrow)

| Databricks type | Arrow |
|---|---|
| `TINYINT`/`SMALLINT`/`INT`/`BIGINT` | int8/int16/int32/int64 |
| `BOOLEAN` | boolean |
| `FLOAT`/`DOUBLE` | float/double |
| `DECIMAL(p,s)` | decimal128(p,s) |
| `STRING`, `VARIANT` | utf8 |
| `BINARY` | binary |
| `DATE` | date32 |
| `TIMESTAMP` | timestamp, microsecond, `Etc/UTC` |
| `TIMESTAMP_NTZ` | timestamp, microsecond, no time zone |
| `ARRAY`/`MAP`/`STRUCT` | utf8 (JSON), via `to_json(col)` in the statement |
| `INTERVAL ...` | utf8, via `cast(col as string)` in the statement |

The Statement Execution API's `ARROW_STREAM` disposition returns `ARRAY`/`MAP`/`STRUCT` as native
Arrow list/map/struct types and `INTERVAL` as a native Arrow duration -- not the JSON-encoded utf8
this connector declares for them. To keep the declared schema honest, the connector detects such a
column from the schema probe and rewrites its own projection: `to_json(`c`) as `c`` for
array/map/struct, `cast(`c` as string) as `c`` for interval, every other column a plain backticked
reference. A dataset with no such column reads exactly the SQL it always would (`select *`, or the
query verbatim); a `query:` read with one is instead wrapped as
`select <projection> from (<query>) as pz_query`. `TIMESTAMP` is declared with time zone
`"Etc/UTC"`, matching the wire.

An unrecognized Databricks type is `PZDB0205`; an unresolvable dataset option (e.g. `entity` and
`query` both set, or a key that isn't `entity`/`query`) is `PZDB0206`.

## Writing data

Every write stages rows to a local Parquet spool, then commits with exactly one statement that
touches the target -- a crash before that statement runs leaves the target untouched, and a crash
after it completes is safe to retry (append is at-least-once, replace and merge are idempotent).

1. `BeginWriteAsync` validates the mode (`append`/`replace`/`merge`), the schema policy (only
   `fail_on_change` is supported), the merge keys (present in the schema, and never the reserved
   column `_pz_seq`), `staging_volume` is set and 3-part, and every Arrow column type is writable
   (below) -- entirely offline, nothing is sent.
2. Every batch is appended to the spool as Parquet (Parquet.Net); a file rolls once it exceeds
   128 MiB. For `merge`, a `_pz_seq` int64 column (monotonic per session) is added so "last row
   wins" is well defined.
3. On commit: each spool file is uploaded to
   `/Volumes/<catalog>/<schema>/<volume>/pz/<tag>/part-N.parquet`; the target table is looked up
   (a 404 means it doesn't exist yet) and, for append/merge, every spool column must already be
   present on an existing target (compared case-insensitively, as Databricks compares identifiers
   -- `PZDB0304` names what's missing; target type differences are left to Spark's own insert
   casts) or the target is created (`create table if not exists`, so `merge` always has a target
   to run against); then exactly one statement moves the staged Parquet into the target, reading it
   as `` parquet.`/Volumes/<c>/<s>/<v>/pz/<tag>/` ``:
   - **`append`**: `insert into <target> (<cols>) select <sel> from <staged parquet>`.
   - **`replace`**: `create or replace table <target> as select <sel> from <staged parquet>` --
     atomic in Delta, and it replaces the target's schema with the write's own (a consequence worth
     knowing if the target carries extra columns).
   - **`merge`**: staged rows are deduplicated on the merge keys first (`row_number() over
     (partition by <keys> order by _pz_seq desc)`, keeping only row 1 -- last row within the
     session wins), then `merge into <target> t using (<deduplicated select>) s on t.k1 <=> s.k1
     and ... when matched then update set ... when not matched then insert ...`. `<=>` makes a null
     key match a null key. `_pz_seq` is never written to the target.
4. Best-effort cleanup: every uploaded file is deleted, then the staging directory, then the local
   spool. Cleanup failures are logged, never thrown, and never turn a committed write into a
   reported failure.

`AbortAsync` runs step 4 only. An empty write still runs its target statement: an empty append
inserts nothing, an empty replace yields an empty table, an empty merge is a no-op, and a missing
target is still created either way -- the spool always stages at least one schema-only Parquet
file, so the read-back `parquet.` path is always a real directory even for a zero-row commit.

### Type mapping (Arrow to Parquet spool to Databricks)

| Arrow | Spool | Databricks column |
|---|---|---|
| int8/int16, int32, uint8/uint16 | INT32 | `INT` (`SMALLINT`/`TINYINT` are never created; Spark widens on insert) |
| int64, uint32 | INT64 | `BIGINT` |
| uint64 | UTF8 digit string, cast | `DECIMAL(20,0)` |
| float/double | FLOAT/DOUBLE | `FLOAT`/`DOUBLE` |
| decimal128(p<=38,s) | UTF8 digit string, cast | `DECIMAL(p,s)` |
| utf8/large utf8 | UTF8 | `STRING` |
| binary/large/fixed-size | BYTE_ARRAY | `BINARY` |
| boolean | BOOLEAN | `BOOLEAN` |
| date32/date64 | DATE | `DATE` |
| timestamp with time zone | TIMESTAMP(microsecond, UTC-adjusted) | `TIMESTAMP` |
| timestamp without time zone | UTF8 ISO 8601 microsecond string, cast | `TIMESTAMP_NTZ` |
| decimal256, time32/64, list/struct/map/union/dictionary/interval/duration/null | refused (`PZDB0303`) | -- |

**Decimal and uint64 ruling.** Parquet.Net writes `DECIMAL` through `System.Decimal`, which caps
precision at 28 digits. To stay exact to Arrow's full 38-digit decimal128 range, decimal128 columns
and uint64 columns (too wide for `BIGINT`) are spooled as UTF8 digit strings and cast back by the
target statement's `select` (`cast(src.col as decimal(p,s))`, uint64 as `cast(... as
decimal(20,0))`). Every other type is spooled with its native Parquet logical type.

**Naive timestamp ruling.** A timezone-less Arrow timestamp is spooled as an ISO 8601
microsecond-precision string, not a native Parquet `TIMESTAMP(MICROS, isAdjustedToUTC=false)`
column -- Databricks' `parquet.` reader on a serverless warehouse reads a Parquet
`isAdjustedToUTC=false` column back as a plain `TIMESTAMP`, indistinguishable from a UTC-adjusted
one, so relying on the Parquet flag alone loses the "no time zone" meaning. The target statement
instead casts the spooled string with `cast(col as timestamp_ntz)`, which round-trips exactly.

### Schema policy

Only `fail_on_change` (the default) is supported: when the target already exists, every write
column must already be present on it (case-insensitive name match), or the write is refused naming
what's missing. `schema_policy: evolve` is refused outright (`PZDB0307`) -- align the target by
hand, or drop it and let the sink recreate it. Type differences between the write and an existing
target are not checked here; they are left to Spark's own insert-cast rules. An extra column the
write doesn't mention is left alone.

### Cost

Every read and every write commit runs statements on the SQL warehouse, billed as ordinary
warehouse time (including a stopped warehouse's cold start). An uncleaned staging directory left
behind under `<staging_volume>/pz/<tag>/` by a crash costs only Unity Catalog volume storage and
can be deleted at any time -- it is never read by anything after that write's own commit.

## Permissions

| To | Needs |
|---|---|
| any statement | `CAN USE` on the SQL warehouse |
| read | `USE CATALOG` + `USE SCHEMA` + `SELECT` on the table (or the catalog/schema for `query:`) |
| write | `CREATE TABLE` + `MODIFY` on the target catalog/schema, plus `READ VOLUME` + `WRITE VOLUME` on the staging volume |

## Errors

Every failure is `databricks: PZDB####: <redacted text>`.

| Code | Meaning |
|---|---|
| `PZDB0101` | `host` is missing, not an `https://` URL, or carries a path/query/fragment |
| `PZDB0102` | `warehouse_id` is missing or not lowercase hexadecimal |
| `PZDB0103` | `auth` is missing or not one of `token`/`oauth`, or its required credential(s) are missing/extra |
| `PZDB0104` | an entity name can't be resolved (no default `catalog`/`schema` on the connection) |
| `PZDB0105` | an entity or volume name contains a backtick |
| `PZDB0106` | `staging_volume` is missing or not a 3-part `catalog.schema.volume` name |
| `PZDB0107` | the connection config failed validation (aggregate) |
| `PZDB0201` | a read sets both `entity` and `query` |
| `PZDB0202` | the incremental cursor column is absent from the schema, has no statement-parameter type, or is a serialized (JSON/string-cast) column |
| `PZDB0203` | the statement failed, was canceled, or the schema probe returned no manifest |
| `PZDB0204` | a chunk's external link could not be fetched after one retry (expired signature) |
| `PZDB0205` | a Databricks column type has no Arrow mapping |
| `PZDB0206` | a read dataset option is unknown, or `entity`/`query` are both invalid |
| `PZDB0301` | an unsupported write mode reached the sink (only `append`/`replace`/`merge`) |
| `PZDB0302` | `merge` mode has no keys, a merge key is absent from the write schema, or the write schema uses the reserved column `_pz_seq` |
| `PZDB0303` | an Arrow column type has no Databricks/Parquet mapping |
| `PZDB0304` | the existing target is missing a column the write needs (`fail_on_change`) |
| `PZDB0305` | uploading a spool file to the staging volume failed |
| `PZDB0306` | the target statement (insert/replace/merge) failed |
| `PZDB0307` | a write output option is unknown, or `schema_policy: evolve` was requested |
| `PZDB0401` | `401`/`403` -- check the token or the service principal's permissions on the warehouse and catalog |
| `PZDB0402` | `429`/`502`/`503`/`504`, a warehouse-side transient statement error, or a network failure before any response -- transient |
| `PZDB0403` | the OAuth token endpoint refused the client credentials |
| `PZDB0404` | the warehouse is unavailable while `select 1` is checked, or a statement-side transient condition (starting warehouse, busy cluster) -- transient |

## Development

```bash
dotnet build Pz.Connector.Databricks.slnx -c Release
dotnet test Pz.Connector.Databricks.slnx -c Release --no-build --filter "Category!=LiveDatabricks"
dotnet restore src/Pz.Connector.Databricks -r linux-x64             # once, on a cold cache
dotnet publish src/Pz.Connector.Databricks -c Release -r linux-x64 --no-restore
dotnet pack src/Pz.Connector.Databricks -c Release -o packages      # nupkg with pz.connector.json
```

The `LiveDatabricks`-tagged facts run against a real workspace instead of a fake, and SKIP unless
these environment variables are set:

- `PZ_DATABRICKS_HOST` -- the workspace origin (`https://...`)
- `PZ_DATABRICKS_WAREHOUSE_ID` -- a SQL warehouse id billed for the facts' statements
- `PZ_DATABRICKS_TOKEN` -- a personal access token with the permissions above
- `PZ_DATABRICKS_STAGING_VOLUME` -- a Unity Catalog volume (`catalog.schema.volume`) the token can
  read and write

Optional, for the service-principal OAuth fact:

- `PZ_DATABRICKS_CLIENT_ID`, `PZ_DATABRICKS_CLIENT_SECRET`

Each fact creates and drops its own `pz_live_<tag>` schema. `tests/e2e/` is the pz project CI runs
against a packed nupkg and a real workspace, using the same environment variables above (plus
`PZ_DATABRICKS_CATALOG`).

Releases are tag-triggered (`v*`) and publish to nuget.org through trusted publishing.
