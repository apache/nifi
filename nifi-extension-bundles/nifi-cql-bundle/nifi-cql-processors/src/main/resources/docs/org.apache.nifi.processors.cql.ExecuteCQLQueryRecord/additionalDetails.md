<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# ExecuteCQLQueryRecord

## Description

_ExecuteCQLQueryRecord_ runs a CQL `SELECT` query against Apache Cassandra or ScyllaDB, through the
connection provided by the configured _Cassandra Connection Provider_ controller service (either
`CassandraCQLExecutionService` or `ScyllaDBCQLExecutionService`), and writes the result set using the
configured _Result Set Output Writer_ (any `RecordSetWriterFactory`, so JSON, Avro, CSV, etc. are all valid
targets). Results stream row-by-row rather than buffering the whole result set in memory, so arbitrarily
large result sets are supported.

The processor can run two ways:

* **Triggered by an incoming FlowFile** - the query, and any property using
  [Expression Language](https://nifi.apache.org/docs/nifi-docs/html/expression-language-guide.html), is
  evaluated against that FlowFile's attributes. The incoming FlowFile is routed to `original` on success,
  `retry` on a query failure, or `failure` on any other processing error.
* **Triggered on a schedule (timer or cron)**, with no incoming connection - useful for a polling query that
  isn't driven by upstream flow content. In this mode there is no FlowFile to evaluate Expression Language
  against, so property values must already be resolvable without one, and a processing error yields the
  processor rather than routing anything (there's no FlowFile to route).

The `executecql.row.count` attribute on the routed FlowFile(s) is not written directly by this processor;
row-count reporting comes from the fragmentation attributes described below, which is the accurate signal
when a single query's results are split across more than one FlowFile.

## Query parameters

A query can carry `?` bind markers, each supplied by a dynamic property named `cql.arg.<position>`, where
`<position>` is the marker's 1-based position in the query text:

| Property | Value |
|---|---|
| _CQL select query_ | `SELECT * FROM sensors.readings WHERE sensor_id = ? AND taken_at > ?` |
| `cql.arg.1` | `${sensor.id}` |
| `cql.arg.2` | `${reading.since}` |

Positions must run consecutively from 1 - a gap such as `cql.arg.1` plus `cql.arg.3` is a configuration error
and fails validation, since the parameters are matched to markers by position. The number of parameters must
also match the number of markers in the query; a mismatch fails the query with a message giving both counts.

Each value is evaluated as Expression Language against the incoming FlowFile, then **bound** - sent to the
cluster as a data value, separately from the statement text. Parameter values are therefore never parsed as
CQL: a value of `1' OR '1'='1` is looked up as that literal string, not treated as query syntax.

Because Expression Language always produces text, each parameter is converted to whatever Java type its bind
marker's declared CQL type requires, using the same conversion applied to record values on the write path.
Text works directly for `text`/`ascii` columns, for the boolean, numeric, `date` and `time` types, for `uuid`,
for `timeuuid` (which must still be a genuine version-1 UUID), and for `timestamp`, which accepts either an
ISO-8601 instant such as `2026-08-07T14:30:00Z` or a count of milliseconds since the epoch. A type outside
that set - `blob`, `inet`, `decimal`, `varint` or a collection - is passed to the driver as-is and will fail
the query unless the value is already of a type the driver's codec for that column accepts.

### Interpolating a value into the query text instead

The _CQL select query_ property also supports Expression Language, so a FlowFile attribute can be interpolated
directly into the query text. That is a deliberate capability - it is the only way to vary the parts of a
statement that cannot be parameterized, such as a table name or a column list - but it is **not** a substitute
for parameters, and it carries a real risk:

> Anything interpolated into the query text is parsed as CQL. An attribute whose value is influenced by
> upstream data - the contents of a file, a value from an external system, an HTTP request - can therefore
> alter the structure of the statement, not just a value within it. This is CQL injection, and it is the same
> exposure that string-concatenated SQL has.

Use `?` markers with `cql.arg.<position>` properties for every value that comes from outside the flow's own
configuration. Reserve interpolation into the query text for values you control - and where an identifier
genuinely must be dynamic, constrain the attribute upstream to a known set rather than passing it through
unchecked.

## Data types

Result set values are converted to an Avro schema (and from there, whatever the configured Record Writer
produces) using the same type mapping `PutCQLRecord` uses for writes, in reverse. The value placed into each
record field is whatever the DataStax driver's own codec decoded the column into, unchanged.

* `BOOLEAN` &rarr; boolean; `TINYINT`/`SMALLINT`/`INT` &rarr; int (Avro has no byte/short primitive, so these
  widen); `BIGINT`/`COUNTER` &rarr; long; `FLOAT` &rarr; float; `DOUBLE` &rarr; double; `BLOB` &rarr; bytes;
  `ASCII`/`TEXT` &rarr; string.
* `UUID` and `TIMEUUID` become the record API's native `UUID` field type, holding the `java.util.UUID` the
  driver decoded. A `timeuuid` column's value is always a genuine version-1 (time-based) UUID by the time
  it's read back - Cassandra/ScyllaDB do not accept a non-version-1 value being written to a `timeuuid`
  column in the first place (see `PutCQLRecord`'s documentation for what happens on that write-side
  rejection) - so there is nothing extra to validate on the read side.
* `TIMESTAMP`, `DATE`, `TIME`, `DECIMAL`, `VARINT` and `INET` are declared as a string field, while the value
  itself stays the driver's decoded object - a `java.time.Instant`, `LocalDate`, `LocalTime`,
  `java.math.BigDecimal`, `java.math.BigInteger` or `java.net.InetAddress` respectively. See
  [String-declared fields and their values](#string-declared-fields-and-their-values) below, which explains
  why and what it means for a downstream consumer.
* `LIST`/`SET` become an Avro array, `MAP` becomes an Avro map, and a Cassandra User Defined Type (UDT)
  becomes a nested named Avro record, all recursively, using the same per-element type mapping described
  above - a UDT nested inside another UDT, or inside a `LIST`/`SET`/`MAP`, is resolved the same way with no
  special-casing. Note that Avro maps require string keys: a CQL `map` whose key type isn't already
  string-like is represented with its real (non-string) key objects in the underlying data even though the
  declared schema implies string keys.
* A CQL type with no mapping defined above - `DURATION`, `TUPLE`, `VECTOR`, or any future/custom type - is
  represented as a string field holding the driver's own decoded object. This keeps one unusual column from
  failing the entire query; it does not attempt to give the column's structure (a tuple's individual
  elements, a vector's components) back as anything richer.

### String-declared fields and their values

For the types listed above as string-declared, the field's declared type (`string`) and the value's runtime
class (`Instant`, `BigDecimal`, ...) are deliberately not the same class. This is a supported combination
rather than a defect: NiFi's `STRING` is a **widening** record type over `DATE`, `TIME`, `TIMESTAMP`,
`DECIMAL`, `BIGINT` and others, so `DataTypeUtils` accepts each of these values for a string field and
coerces it **losslessly** — via `toString()` — if and when a Record Writer asks for a string. A nanosecond
`LocalTime` renders in full as `15:14:54.899676065`; a `BigDecimal` renders as its exact decimal text.

Leaving the driver's object in place is what makes both readings available: a writer that wants text gets a
faithful rendering, and a script or custom writer that wants the real `BigDecimal`, `Instant` or
`InetAddress` can take it straight from `getValue(...)` without reparsing a string.

Declaring NiFi's native types instead is not workable for these columns:

* NiFi's native `TIMESTAMP`/`DATE`/`TIME` types are backed by `java.sql.Timestamp`/`Date`/`Time`, not
  `java.time.*`. Pairing them with the driver's values makes conversion **fail outright** for `TIMESTAMP`,
  and converting the values to `java.sql.*` to suit instead **truncates** — CQL `time` is
  nanosecond-resolution and `java.sql.Time` is not, so `15:14:54.899676065` would become `15:14:54`.
* A native `DECIMAL` field needs a fixed precision and scale declared up front, which CQL's
  arbitrary-precision `decimal` cannot supply; a native `BIGINT` is not reachable through the Avro schema
  round trip at all.
* There is no native NiFi record type for an IP address.

One consequence worth knowing: `InetAddress.toString()` renders as `/127.0.0.1`, with a leading slash, so an
`inet` column coerced to text carries that slash. Use the `InetAddress` object directly if that matters.

## Pagination and query overrides

* _Fetch Size_ overrides how many rows the driver requests per page from the server for this query only,
  without changing the connection service's own configured default.
* _Max Wait Time_ overrides the connection service's configured Read Timeout for this query only.

Both are optional; when unset, the connection service's own configured values apply.

## Splitting results across FlowFiles

* _Max Rows Per Flow File_ (default `0`, meaning "one FlowFile for the whole result set") caps how many rows
  go into a single output FlowFile, splitting a large result set into several. When this produces more than
  one FlowFile, all of them share a `fragment.identifier` attribute, each gets a `fragment.index` giving its
  position among the set, and (unless _Output Batch Size_ is also set - see below) a `fragment.count`
  attribute giving the total, so they can be correlated and reassembled downstream.
* _Output Batch Size_ (default `0`, meaning "commit once at the end") commits the session, releasing output
  FlowFiles downstream, every time this many are ready, instead of waiting for the entire result set to
  finish. This bounds memory/queue growth for very large result sets at the cost of releasing FlowFiles
  before the final row count is known - which is exactly why `fragment.count` is intentionally left unset
  whenever _Output Batch Size_ is configured: earlier FlowFiles may already have been committed downstream
  before the true total could be determined. One further consequence: if the query fails after early commits
  have already released the incoming FlowFile to `original`, that FlowFile cannot be recalled, so a new,
  empty FlowFile is created to carry the retry signal to `retry` in its place - and the results committed
  before the failure stay downstream, so a retried query delivers those rows a second time.

If the incoming FlowFile's query returns zero rows, no output FlowFile is created at all, and the incoming
FlowFile (if any) is routed directly to `original`.

## Relationships

* `success` - one or more FlowFiles containing query results.
* `original` - the incoming FlowFile that triggered the query (only present when triggered by an incoming
  FlowFile), routed here once every resulting output FlowFile has been transferred to `success` - or
  immediately, if the query returned no rows at all. Auto-terminated by default, since most flows only need
  the query results themselves, not the triggering FlowFile.
* `failure` - the query could not be executed (for example, invalid CQL, or a schema mismatch between the
  result set and the configured Record Writer).
* `retry` - the query failed in a way that may succeed if attempted again (for example, a transient
  connectivity or consistency-level failure), penalizing the FlowFile before routing it here.
