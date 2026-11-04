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

# PutCQLRecord

## Description

_PutCQLRecord_ is a record-aware processor that reads each FlowFile using the configured _Record Reader_ and
writes the resulting records to an Apache Cassandra or ScyllaDB table, using either a CQL `INSERT` or `UPDATE`
statement, via the connection provided by the configured _Cassandra Connection Provider_ controller service
(either `CassandraCQLExecutionService` or `ScyllaDBCQLExecutionService`). Records are grouped into batches (see
_Batch size_) and written as a single Cassandra `BatchStatement` per batch, one prepared statement per
FlowFile.

## Data types

CQL column types are matched against record field values using the driver's own codec registry wherever
possible, with the following notable behaviors:

* `BOOLEAN`, the integer types (`TINYINT`/`SMALLINT`/`INT`/`BIGINT`/`COUNTER`), `FLOAT`, and `DOUBLE` accept a
  compatible-but-non-exact value (for example a numeric `String`) via custom codecs registered by the
  connection service, not just an exact Java type match.
* `VARINT` is the natural fit for a record's `BIGINT` field (arbitrary-precision integer).
* `CHAR` has no dedicated CQL equivalent and is written to a `TEXT`/`VARCHAR` column.
* `UUID` and `TIMEUUID` both bind from a record's `UUID` field type - see
  [Writing to a timeuuid column](#writing-to-a-timeuuid-column) below for the extra requirement `TIMEUUID`
  imposes.
* `DATE`/`TIME`/`TIMESTAMP` accept `java.sql.Date`/`java.sql.Time`/`Instant`-family values.
* `LIST`/`SET` accept either a record `ARRAY` or an `Object[]`; `MAP` accepts a record `MAP`. Elements are
  converted recursively using these same rules, so a `list<timeuuid>` or `set<uuid>` is subject to the same
  requirements as a scalar column of that type.
* A Cassandra User Defined Type (UDT) column accepts either a nested `RECORD` or a raw `Map`, with fields
  converted recursively the same way, including UDTs nested inside UDTs, or inside a `LIST`/`SET`/`MAP`.

## Statement types

The _Statement Type_ property (or the `cql.statement.type` FlowFile attribute, when _Statement Type_ is set
to use it) selects between two CQL statement shapes:

* **INSERT** - every field in each record is written to the column of the same name. There is no separate
  concept of "which fields are the primary key" for an INSERT: Cassandra requires all primary key columns to
  be supplied as ordinary column values in the statement, the same as any other column.
* **UPDATE** - requires _Update Keys_, a comma-separated list of column names that identify the row to
  update (these must be present as fields in every record, with matching names, unless overridden - see
  [Primary Key Dynamic Properties](#primary-key-dynamic-properties)). Every other field in the record is
  applied via the method selected by _Update Method_:
  * `SET` - overwrites the column's value.
  * `INCREMENT` / `DECREMENT` - adjusts a counter column, and requires _Batch Statement Type_ to be
    `COUNTER` or `UNLOGGED`, since Cassandra/ScyllaDB reject counter mutations in any other batch type.

`INSERT` cannot be combined with a `COUNTER` _Batch Statement Type_, since Cassandra/ScyllaDB do not allow
`INSERT` statements against counter tables - only `UPDATE`.

## Batching

_Batch size_ controls how many records are grouped into a single `BatchStatement` (default 100). _Batch
Statement Type_ selects `UNLOGGED` (the default - higher throughput, no atomicity guarantee), `LOGGED`
(atomic across partitions, at the cost of writing every batch to the distributed batchlog first, so reserve
it for the cases that genuinely need that atomicity), or `COUNTER` (required for counter mutations); it can
also be resolved per FlowFile from the `cql.batch.statement.type` attribute. If a batch fails partway
through a FlowFile with multiple batches, the FlowFile is routed to `retry` (on a query failure) or
`failure` (on any other error) with the
`cql.records.written` attribute set to how many records were already committed in prior batches within that
attempt, since earlier batches may have succeeded even though the FlowFile as a whole did not.

### Run Duration and counters

This processor supports batching in the framework sense as well, so _Run Duration_ can be raised above `0` to
let NiFi batch session commits and trade latency for throughput. That is safe for `INSERT` and `SET`-method
`UPDATE` because a CQL write against a full primary key is an upsert: if a batch is rolled back and the
FlowFiles are reprocessed, the same rows are written again to the same values. Setting _Timestamp Field_ makes
that guarantee exact rather than merely usually-true.

**It is not safe for counters.** `INCREMENT` and `DECREMENT` are the one CQL write that is not idempotent -
reprocessing a FlowFile applies its increments a second time. Leave _Run Duration_ at `0` on any flow using a
counter _Update Method_, and prefer counter flows that can tolerate at-least-once delivery in any case, since
a rollback can also be triggered by a failure that has nothing to do with Run Duration.

## TTL and write timestamp overrides

* _Time To Live_ overrides the connection service's own default TTL for `INSERT` and `SET`-method `UPDATE`
  statements. Ignored for `INCREMENT`/`DECREMENT`, since Cassandra/ScyllaDB do not support a TTL on counter
  columns.
* _Timestamp Field_ names a field in each record whose value supplies the CQL write timestamp for that
  record's statement, instead of the time the statement executes. This makes retries/reprocessing safe:
  resubmitting the same record with the same timestamp is a true no-op rather than a write that could win a
  last-write-wins race against different data written to the same row in the meantime. Also ignored for
  `INCREMENT`/`DECREMENT`, since Cassandra/ScyllaDB do not support a custom write timestamp on counter
  columns.

## Primary Key Dynamic Properties

By default, a column's value comes from the record field with the same name. A dynamic property overrides
that for one specific column of one specific table, sourcing its value from an arbitrary location in the
record instead of requiring a same-named top-level field.

**Property name** must be exactly `<keyspace>.<table>.<field>` - three unquoted CQL-identifier segments
(each starting with a letter, followed by letters/digits/underscores), separated by dots. All three segments
are required; there is no unqualified-table form for dynamic properties (unlike the _Table name_ property
itself, which does accept an unqualified table name and resolves it against the connection service's default
keyspace).

**Property value** is a [RecordPath](https://nifi.apache.org/docs/nifi-docs/html/record-path-guide.html)
expression, evaluated once per record. It must resolve to **exactly one value** - a RecordPath that matches
zero fields, or more than one (for example, one written against an array without narrowing to a single
element), causes that record's write to fail rather than silently picking an arbitrary match. This matters
for a partition key specifically: a value that isn't deterministic per record can route logically identical
records to different partitions.

An override only applies to writes against the keyspace-qualified table named in the property; the same
column name in a different keyspace or table is unaffected, and an unrelated dynamic property never
accidentally applies to the wrong table.

### Example

Given records shaped like:

```json
{
  "customer": {
    "id": "9f2c1e2a-1111-4a11-8a11-abcdefabcdef"
  },
  "event_type": "login"
}
```

being written to `analytics.customer_events`, where the table's partition key column `customer_id` has no
matching top-level record field, add a dynamic property:

* Name: `analytics.customer_events.customer_id`
* Value: `/customer/id`

Every record written to `analytics.customer_events` now resolves `customer_id` from the nested
`customer/id` field instead of requiring a top-level `customer_id` field.

## Writing to a timeuuid column

A CQL `timeuuid` column requires a genuine **version 1 (time-based)** UUID - this is part of the CQL type's
own definition (`now()`, the idiomatic way to populate one in CQL, generates a version-1 value), not
something specific to this processor. Supplying a version-4 (random) UUID - which is what `UUID.randomUUID()`
and most record-generation logic produce by default - is rejected, not silently accepted:

* If the value's version is not 1, `PutCQLRecord` fails that record with a message naming the actual value
  and its version, for example:
  `Value '87ccc9c5-c4fc-4073-98bc-ee6e7e538237' is not a valid timeuuid: version 4, but timeuuid columns
  require a version 1 (time-based) UUID.`
* This check happens before the value is ever sent to the driver. Without it, the same bad value would still
  be rejected, but with a much less helpful driver-level error
  (`CodecNotFoundException: Codec not found for requested operation: [TIMEUUID <-> java.util.UUID]`) that
  reads like a configuration problem rather than a data problem.

**To supply a valid value**, the record field targeting a `timeuuid` column must already carry a real
version-1 UUID by the time it reaches this processor - for example, one generated by an upstream system, or
by the DataStax Java Driver's `Uuids.timeBased()` (always uses the current time, with a randomized node
identifier and an internal clock sequence to keep concurrently-generated values from colliding). A record's
own `TIMESTAMP` field is **not** sufficient on its own: it supplies only the time component of a version-1
UUID's 128 bits, not the clock-sequence/node bits that make the value collision-resistant, so constructing a
"valid-looking" UUID from just a timestamp (for example via the driver's `Uuids.startOf(millis)`/
`Uuids.endOf(millis)`, which are intended for building CQL range-query bounds, not per-record identifiers)
can produce identical values for two different records that happen to share a timestamp.

## Relationships

* `success` - every record in the FlowFile was written.
* `failure` - a configuration or data problem (for example, a mistyped _Timestamp Field_, a missing _Update
  Keys_ for an `UPDATE`, or a `timeuuid` value that isn't version 1) prevented the write; `cql.records.written`
  reflects any batches that were already committed before the failure, if the FlowFile spanned more than one
  batch.
* `retry` - the query itself failed against Cassandra/ScyllaDB (for example, a transient connectivity or
  consistency-level failure). Since `INSERT`/`UPDATE` are idempotent per-row upserts, retrying is normally
  safe; combine with _Timestamp Field_ if last-write-wins ordering against concurrent writers matters.
