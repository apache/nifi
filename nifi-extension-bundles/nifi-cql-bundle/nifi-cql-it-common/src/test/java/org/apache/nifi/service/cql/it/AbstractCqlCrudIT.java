/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.service.cql.it;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.service.cql.api.constants.CqlBatchType;
import org.apache.nifi.service.cql.api.constants.UpdateMethod;
import org.apache.nifi.service.cql.api.exception.QueryFailureException;
import org.apache.nifi.service.cql.api.lookup.CqlRow;
import org.apache.nifi.service.cql.api.lookup.CqlStatementResult;
import org.apache.nifi.service.cql.api.metadata.QualifiedTableName;
import org.apache.nifi.service.cql.api.service.AbstractCQLExecutionService;
import org.apache.nifi.service.cql.api.service.CQLExecutionService;
import org.apache.nifi.service.cql.api.service.QueryOverrides;
import org.apache.nifi.service.cql.api.service.WriteOverrides;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the CRUD-oriented public methods of {@link CQLExecutionService} - {@code insert}, {@code update},
 * {@code query}, and {@code execute} - against whichever backend the concrete subclass wires up. Assumes
 * the subclass has already bootstrapped a {@code testspace} keyspace containing the {@code message},
 * {@code counter_test}, {@code simple_set_test}, and {@code query_test} tables (see {@code init.cql} in the
 * Cassandra module for the canonical schema) and has called {@link #initializeSessionProvider(CqlConnectionInfo)}.
 * <p>
 * Every test here turns on a server- or driver-observable effect a unit test can only assume: a counter's
 * running total, a conditional write handing back the row that beat it, an explicit {@code WRITETIME} or
 * {@code TTL} taking hold, a LOGGED batch refusing a counter mutation, an unsatisfiable consistency level
 * failing on the first {@code execute()}. The driver-metadata-to-{@code PrimaryKey} mapping that backs
 * {@code getMetadata} is covered without a cluster in {@code CassandraTableMetadataMappingTest}.
 * <p>
 * Setup is a plain protected method rather than a JUnit lifecycle callback (e.g. {@code @BeforeAll})
 * deliberately: on a {@code @ParameterizedClass} leaf, {@code @BeforeAll} - even inherited from this
 * superclass - runs BEFORE the leaf's own {@code @BeforeParameterizedClassInvocation} method, so an
 * inherited {@code @BeforeAll} here would run before the container (and connection info) it depends on
 * exists. Calling this method explicitly from the leaf's {@code @BeforeParameterizedClassInvocation} avoids
 * that ordering trap entirely.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractCqlCrudIT {

    protected CQLExecutionService sessionProvider;

    private CqlSession session;

    private CqlConnectionInfo connectionInfo;

    private String kvTableName;

    /**
     * @return a fresh, unconfigured instance of the backend implementation under test
     */
    protected abstract CQLExecutionService newSessionProvider();

    protected void initializeSessionProvider(final CqlConnectionInfo connectionInfo) throws Exception {
        this.connectionInfo = connectionInfo;
        this.session = connectionInfo.session();

        this.kvTableName = connectionInfo.keyspace() + ".execute_kv";
        CqlDdl.executeWithRetry(session, "create table if not exists " + kvTableName + " (k blob primary key, v blob)");

        this.sessionProvider = CqlServiceRunner.forService(newSessionProvider())
                .withConnection(connectionInfo)
                .enable();
    }

    private RecordSchema getMessageSchema() {
        List<RecordField> fields = List.of(
                new RecordField("sender", RecordFieldType.STRING.getDataType()),
                new RecordField("receiver", RecordFieldType.STRING.getDataType()),
                new RecordField("message", RecordFieldType.STRING.getDataType()),
                new RecordField("when_sent", RecordFieldType.TIMESTAMP.getDataType())
        );
        return new SimpleRecordSchema(fields);
    }

    @Test
    @DisplayName("Inserting a record writes it to the table using the CQL execution service")
    void testInsertRecord() {
        RecordSchema schema = getMessageSchema();
        Map<String, Object> rawRecord = new HashMap<>();
        rawRecord.put("sender", "john.smith");
        rawRecord.put("receiver", "jane.smith");
        rawRecord.put("message", "hello");
        rawRecord.put("when_sent", Instant.now());

        MapRecord record = new MapRecord(schema, rawRecord);

        assertDoesNotThrow(() -> sessionProvider.insert(new QualifiedTableName(null, "message"), record, Map.of(), WriteOverrides.NONE));
    }

    @Test
    @DisplayName("Incrementing then decrementing a counter column produces the expected running total")
    void testIncrementAndDecrement() throws Exception {
        RecordField field1 = new RecordField("column_a", RecordFieldType.STRING.getDataType());
        RecordField field2 = new RecordField("increment_field", RecordFieldType.INT.getDataType());
        RecordSchema schema = new SimpleRecordSchema(List.of(field1, field2));

        Map<String, Object> map = new HashMap<>();
        map.put("column_a", "abcdef");
        map.put("increment_field", 1);

        MapRecord record = new MapRecord(schema, map);

        List<String> updateKeys = new ArrayList<>();
        updateKeys.add("column_a");

        //Set the initial value
        sessionProvider.update(new QualifiedTableName(null, "counter_test"), record, Map.of(), updateKeys, UpdateMethod.INCREMENT, WriteOverrides.NONE);

        Thread.sleep(1000);

        sessionProvider.update(new QualifiedTableName(null, "counter_test"), record, Map.of(), updateKeys, UpdateMethod.INCREMENT, WriteOverrides.NONE);

        ResultSet results = session.execute("select increment_field from testspace.counter_test where column_a = 'abcdef'");

        Iterator<Row> rowIterator = results.iterator();

        Row row = rowIterator.next();

        assertEquals(2, row.getLong("increment_field"));

        sessionProvider.update(new QualifiedTableName(null, "counter_test"), record, Map.of(), updateKeys, UpdateMethod.DECREMENT, WriteOverrides.NONE);

        results = session.execute("select increment_field from testspace.counter_test where column_a = 'abcdef'");

        rowIterator = results.iterator();

        row = rowIterator.next();

        assertEquals(1, row.getLong("increment_field"));
    }

    @Test
    @DisplayName("Batch-inserting records with a LOGGED batch type writes them all")
    void testBatchInsert() {
        RecordSchema schema = getMessageSchema();
        List<org.apache.nifi.serialization.record.Record> records = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Map<String, Object> rawRecord = new HashMap<>();
            rawRecord.put("sender", "batch.sender." + i);
            rawRecord.put("receiver", "jane.smith");
            rawRecord.put("message", "hello " + i);
            rawRecord.put("when_sent", Instant.now());
            records.add(new MapRecord(schema, rawRecord));
        }

        assertDoesNotThrow(() -> sessionProvider.insert(new QualifiedTableName(null, "message"), records, Map.of(), CqlBatchType.LOGGED, WriteOverrides.NONE));
    }

    @Test
    @DisplayName("Batch-updating counters requires a COUNTER batch type - Cassandra/ScyllaDB reject counter mutations in a LOGGED batch")
    void testBatchCounterUpdateRequiresCounterBatchType() throws Exception {
        RecordField field1 = new RecordField("column_a", RecordFieldType.STRING.getDataType());
        RecordField field2 = new RecordField("increment_field", RecordFieldType.INT.getDataType());
        RecordSchema schema = new SimpleRecordSchema(List.of(field1, field2));

        List<org.apache.nifi.serialization.record.Record> records = new ArrayList<>();
        for (String key : List.of("batch-counter-a", "batch-counter-b", "batch-counter-c")) {
            Map<String, Object> map = new HashMap<>();
            map.put("column_a", key);
            map.put("increment_field", 1);
            records.add(new MapRecord(schema, map));
        }

        List<String> updateKeys = List.of("column_a");

        // A LOGGED (or UNLOGGED) batch cannot contain counter mutations - the server rejects it outright.
        // This is exactly the bug PutCQLRecord had: BATCH_STATEMENT_TYPE was validated but never forwarded,
        // so every batched counter increment/decrement was silently submitted as LOGGED and would have
        // failed here before the fix.
        assertThrows(Exception.class, () -> sessionProvider.update(new QualifiedTableName(null, "counter_test"), records, Map.of(), updateKeys,
                UpdateMethod.INCREMENT, CqlBatchType.LOGGED, WriteOverrides.NONE));

        sessionProvider.update(new QualifiedTableName(null, "counter_test"), records, Map.of(), updateKeys, UpdateMethod.INCREMENT, CqlBatchType.COUNTER, WriteOverrides.NONE);

        for (String key : List.of("batch-counter-a", "batch-counter-b", "batch-counter-c")) {
            Row row = session.execute("select increment_field from testspace.counter_test where column_a = '" + key + "'").iterator().next();
            assertEquals(1, row.getLong("increment_field"));
        }
    }

    @Test
    @DisplayName("Inserting a record with a TTL override sets an expiration on the written columns")
    void testInsertWithTtlOverrideSetsExpiration() {
        RecordSchema schema = getMessageSchema();
        Map<String, Object> rawRecord = new HashMap<>();
        rawRecord.put("sender", "ttl.sender");
        rawRecord.put("receiver", "ttl.receiver");
        rawRecord.put("message", "expires soon");
        rawRecord.put("when_sent", Instant.now());

        MapRecord record = new MapRecord(schema, rawRecord);

        sessionProvider.insert(new QualifiedTableName(null, "message"), record, Map.of(), new WriteOverrides(Duration.ofSeconds(100), null));

        // "when_sent" (the last clustering column) is deliberately omitted: sender + receiver alone
        // uniquely identify the single row this test wrote, and CQL allows a prefix of clustering
        // columns in an equality filter without needing ALLOW FILTERING or reconstructing the exact
        // timestamp literal that was generated for "when_sent".
        Row row = session.execute(
                "select TTL(message) as remaining_ttl from testspace.message where sender = 'ttl.sender' and receiver = 'ttl.receiver'")
                .iterator().next();

        int remainingTtl = row.getInt("remaining_ttl");
        assertTrue(remainingTtl > 0 && remainingTtl <= 100, "Expected a remaining TTL between 1 and 100 seconds but was " + remainingTtl);
    }

    @Test
    @DisplayName("Inserting with a Timestamp Field override uses that record field's value as the CQL write timestamp")
    void testInsertWithTimestampFieldOverrideUsesRecordValue() {
        RecordSchema schema = getMessageSchema();
        Instant eventTime = Instant.now().minus(Duration.ofDays(30)).truncatedTo(ChronoUnit.MILLIS);

        Map<String, Object> rawRecord = new HashMap<>();
        rawRecord.put("sender", "timestamp.sender");
        rawRecord.put("receiver", "timestamp.receiver");
        rawRecord.put("message", "old event");
        rawRecord.put("when_sent", eventTime);

        MapRecord record = new MapRecord(schema, rawRecord);

        sessionProvider.insert(new QualifiedTableName(null, "message"), record, Map.of(), new WriteOverrides(null, "when_sent"));

        long writeTimeMicros = session.execute(
                "select WRITETIME(message) as write_time from testspace.message where sender = 'timestamp.sender' and receiver = 'timestamp.receiver'")
                .iterator().next().getLong("write_time");

        assertEquals(toEpochMicros(eventTime), writeTimeMicros);
    }

    @Test
    @DisplayName("Each record in a batch insert carries its own write timestamp when a Timestamp Field override is set")
    void testBatchInsertWithTimestampFieldOverrideUsesPerRecordValues() {
        RecordSchema schema = getMessageSchema();
        Instant olderEventTime = Instant.now().minus(Duration.ofDays(30)).truncatedTo(ChronoUnit.MILLIS);
        Instant newerEventTime = Instant.now().minus(Duration.ofDays(1)).truncatedTo(ChronoUnit.MILLIS);

        Map<String, Object> olderRawRecord = new HashMap<>();
        olderRawRecord.put("sender", "batch.timestamp.sender");
        olderRawRecord.put("receiver", "older.receiver");
        olderRawRecord.put("message", "older event");
        olderRawRecord.put("when_sent", olderEventTime);

        Map<String, Object> newerRawRecord = new HashMap<>();
        newerRawRecord.put("sender", "batch.timestamp.sender");
        newerRawRecord.put("receiver", "newer.receiver");
        newerRawRecord.put("message", "newer event");
        newerRawRecord.put("when_sent", newerEventTime);

        List<org.apache.nifi.serialization.record.Record> records = List.of(
                new MapRecord(schema, olderRawRecord),
                new MapRecord(schema, newerRawRecord));

        sessionProvider.insert(new QualifiedTableName(null, "message"), records, Map.of(), CqlBatchType.LOGGED, new WriteOverrides(null, "when_sent"));

        assertEquals(toEpochMicros(olderEventTime), writeTimeForBatchTimestampSender("older.receiver"));
        assertEquals(toEpochMicros(newerEventTime), writeTimeForBatchTimestampSender("newer.receiver"));
    }

    @Test
    @DisplayName("Updating with a Timestamp Field override uses that record field's value as the CQL write timestamp")
    void testUpdateSetWithTimestampFieldOverrideUsesRecordValue() throws Exception {
        // "when_sent" doubles as both an update key (part of message's primary key, so it identifies which row
        // to update) and the Timestamp Field source - a realistic pattern, since it's a real column already
        // being written, not an extra field invented just to carry a timestamp. generateUpdate() only knows
        // how to treat a record field as either a SET target or a WHERE key, so the Timestamp Field has to be
        // one of those, not an arbitrary extra field.
        //
        // The row-establishing insert below deliberately writes with an explicit, much older timestamp than
        // "when_sent" itself (which only serves as the row's identity here). Without that, this insert's own
        // auto-assigned "now" timestamp would land only a fraction of a millisecond away from the override
        // under test below, and Cassandra's last-write-wins would resolve that near-tie essentially at random -
        // making the test flaky/misleading regardless of whether the override actually works.
        Instant whenSent = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        long deliberatelyOldWriteTimestamp = toEpochMicros(whenSent.minus(Duration.ofDays(60)));
        session.execute(String.format(
                "insert into testspace.message (sender, receiver, message, when_sent) values "
                        + "('update.timestamp.sender', 'update.timestamp.receiver', 'original', %d) using timestamp %d",
                whenSent.toEpochMilli(), deliberatelyOldWriteTimestamp));
        Thread.sleep(250);

        RecordSchema schema = getMessageSchema();
        Map<String, Object> map = new HashMap<>();
        map.put("sender", "update.timestamp.sender");
        map.put("receiver", "update.timestamp.receiver");
        map.put("message", "updated");
        map.put("when_sent", whenSent);

        MapRecord record = new MapRecord(schema, map);
        List<String> updateKeys = List.of("sender", "receiver", "when_sent");

        sessionProvider.update(new QualifiedTableName(null, "message"), record, Map.of(), updateKeys, UpdateMethod.SET, new WriteOverrides(null, "when_sent"));

        long writeTimeMicros = session.execute(String.format(
                "select WRITETIME(message) as write_time from testspace.message where sender = 'update.timestamp.sender' "
                        + "and receiver = 'update.timestamp.receiver' and when_sent = %d", whenSent.toEpochMilli()))
                .iterator().next().getLong("write_time");

        assertEquals(toEpochMicros(whenSent), writeTimeMicros);
    }

    private long writeTimeForBatchTimestampSender(String receiver) {
        return session.execute(String.format(
                "select WRITETIME(message) as write_time from testspace.message where sender = 'batch.timestamp.sender' and receiver = '%s'", receiver))
                .iterator().next().getLong("write_time");
    }

    private static long toEpochMicros(Instant instant) {
        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
    }

    @Test
    @DisplayName("A Timestamp Field override is ignored for counter updates, since Cassandra/ScyllaDB do not support a custom write timestamp on counter columns")
    void testCounterUpdateIgnoresTimestampFieldOverride() {
        RecordField field1 = new RecordField("column_a", RecordFieldType.STRING.getDataType());
        RecordField field2 = new RecordField("increment_field", RecordFieldType.INT.getDataType());
        RecordSchema schema = new SimpleRecordSchema(List.of(field1, field2));

        Map<String, Object> map = new HashMap<>();
        map.put("column_a", "timestamp-ignored-for-counters");
        map.put("increment_field", 1);

        MapRecord record = new MapRecord(schema, map);
        List<String> updateKeys = List.of("column_a");

        // "column_a" isn't a timestamp-typed field, but it doesn't matter: if the override were mistakenly
        // applied to a counter UPDATE, Cassandra/ScyllaDB would reject the statement outright regardless of
        // what value was supplied ("Cannot provide custom timestamp for a BATCH containing counters" /
        // similar), so not throwing here proves the override was correctly skipped before ever reading the field.
        assertDoesNotThrow(() -> sessionProvider.update(new QualifiedTableName(null, "counter_test"), record, Map.of(), updateKeys, UpdateMethod.INCREMENT, new WriteOverrides(null, "column_a")));
    }

    @Test
    @DisplayName("A TTL override is ignored for counter updates, since Cassandra/ScyllaDB do not support a TTL on counter columns")
    void testCounterUpdateIgnoresTtlOverride() {
        RecordField field1 = new RecordField("column_a", RecordFieldType.STRING.getDataType());
        RecordField field2 = new RecordField("increment_field", RecordFieldType.INT.getDataType());
        RecordSchema schema = new SimpleRecordSchema(List.of(field1, field2));

        Map<String, Object> map = new HashMap<>();
        map.put("column_a", "ttl-ignored-for-counters");
        map.put("increment_field", 1);

        MapRecord record = new MapRecord(schema, map);
        List<String> updateKeys = List.of("column_a");

        // If the TTL were mistakenly applied to a counter UPDATE, Cassandra/ScyllaDB would reject the
        // statement outright ("Cannot set ttl on a counter column"), so simply not throwing here proves
        // the override was correctly skipped.
        assertDoesNotThrow(() -> sessionProvider.update(new QualifiedTableName(null, "counter_test"), record, Map.of(), updateKeys,
                UpdateMethod.INCREMENT, new WriteOverrides(Duration.ofSeconds(100), null)));
    }

    @Test
    @DisplayName("Updating a record with the SET method overwrites the existing column value")
    void testUpdateSet() throws Exception {
        session.execute("insert into testspace.simple_set_test(username, is_active) values('john.smith', true)");
        Thread.sleep(250);

        RecordField field1 = new RecordField("username", RecordFieldType.STRING.getDataType());
        RecordField field2 = new RecordField("is_active", RecordFieldType.BOOLEAN.getDataType());
        RecordSchema schema = new SimpleRecordSchema(List.of(field1, field2));

        Map<String, Object> map = new HashMap<>();
        map.put("username", "john.smith");
        map.put("is_active", false);

        MapRecord record = new MapRecord(schema, map);

        List<String> updateKeys = new ArrayList<>();
        updateKeys.add("username");

        sessionProvider.update(new QualifiedTableName(null, "simple_set_test"), record, Map.of(), updateKeys, UpdateMethod.SET, WriteOverrides.NONE);

        Iterator<Row> iterator = session.execute("select is_active from testspace.simple_set_test where username = 'john.smith'").iterator();

        Row row = iterator.next();

        assertFalse(row.getBoolean("is_active"));
    }

    @Test
    @DisplayName("Querying rows via the CQL execution service returns them without error")
    void testQueryRecord() {
        String[] statements = """
                insert into testspace.query_test (column_a, column_b, when)
                    values ('abc', 'def', toTimestamp(now()));
                insert into testspace.query_test (column_a, column_b, when)
                    values ('abc', 'ghi', toTimestamp(now()));
                insert into testspace.query_test (column_a, column_b, when)
                    values ('abc', 'jkl', toTimestamp(now()));
            """.trim().split("\\;");
        for (String statement : statements) {
            session.execute(statement);
        }

        CollectingCqlQueryCallback callback = new CollectingCqlQueryCallback();

        sessionProvider.query("select * from testspace.query_test", null, callback, QueryOverrides.NONE);

        assertTrue(callback.getRecords().size() >= statements.length,
                () -> "Expected at least the " + statements.length + " inserted rows back, got " + callback.getRecords().size());
    }

    @Test
    @DisplayName("A query that fails on the driver's initial execute() surfaces as QueryFailureException, not a raw driver exception")
    void testQueryExecutionFailureOnInitialExecuteIsWrappedAsQueryFailureException() throws Exception {
        // A consistency level of THREE can never be satisfied by this single-node cluster, so the driver
        // throws UnavailableException (a QueryExecutionException) synchronously from the very first
        // execute() call, before any rows are fetched - exactly the code path that used to bypass the
        // QueryFailureException translation.
        final CQLExecutionService unsatisfiableConsistencyProvider = CqlServiceRunner.forService(newSessionProvider())
                .withConnection(connectionInfo)
                .withProperty(AbstractCQLExecutionService.CONSISTENCY_LEVEL, "THREE")
                .enable();

        final CollectingCqlQueryCallback callback = new CollectingCqlQueryCallback();

        assertThrows(QueryFailureException.class, () -> unsatisfiableConsistencyProvider.query(
                "select * from testspace.query_test", null, callback, QueryOverrides.NONE));

        assertTrue(callback.getRecords().isEmpty(), "A query that never executed should not have delivered rows");
    }

    // ---- execute(): conditional writes and schema-free reads -------------------------------------------
    //
    // These are the paths query() cannot express: a statement-level outcome, and values read without a
    // schema imposed on them. Both matter for a caller doing compare-and-set over opaque bytes.

    /**
     * The {@code keyspace.table} name the {@code execute()} tests below share. The table itself is created
     * once in {@link #initializeSessionProvider(CqlConnectionInfo)}, before the service session exists, so
     * that session sees it on connect rather than racing a lazy create against its schema metadata.
     */
    private String kvTable() {
        return kvTableName;
    }

    private static ByteBuffer bytes(final String value) {
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("An unconditional statement reports wasApplied, so a caller that never writes conditionally has no special case")
    void testExecuteUnconditionalWriteIsApplied() {
        final String table = kvTable();

        final CqlStatementResult result = sessionProvider.execute("insert into " + table + " (k, v) values (?, ?)",
                List.of(bytes("uncond"), bytes("value")), QueryOverrides.NONE);

        assertTrue(result.wasApplied());
        assertTrue(result.rows().isEmpty(), "an unconditional insert returns no rows");
    }

    @Test
    @DisplayName("A conditional write that applies reports true and returns no row, since there was no prior value")
    void testExecuteConditionalWriteApplied() {
        final String table = kvTable();

        final CqlStatementResult result = sessionProvider.execute(
                "insert into " + table + " (k, v) values (?, ?) if not exists",
                List.of(bytes("lwt-new"), bytes("first")), QueryOverrides.NONE);

        assertTrue(result.wasApplied());
        assertTrue(result.rows().isEmpty(),
                () -> "expected no rows for an applied conditional write, got " + result.rows().size());
    }

    @Test
    @DisplayName("A conditional write that is rejected returns the stored row, which is what makes compare-and-set one round trip")
    void testExecuteConditionalWriteRejectedCarriesStoredRow() {
        final String table = kvTable();
        sessionProvider.execute("insert into " + table + " (k, v) values (?, ?)",
                List.of(bytes("lwt-existing"), bytes("original")), QueryOverrides.NONE);

        final CqlStatementResult result = sessionProvider.execute(
                "insert into " + table + " (k, v) values (?, ?) if not exists",
                List.of(bytes("lwt-existing"), bytes("attempted")), QueryOverrides.NONE);

        assertFalse(result.wasApplied());
        assertEquals(1, result.rows().size());

        final CqlRow stored = result.rows().getFirst();
        assertArrayEquals("original".getBytes(StandardCharsets.UTF_8), stored.getBytes("v"),
                "the losing writer must see the value that is actually stored");

        // The outcome is reported by wasApplied(), not as data: "[applied]" is not a legal identifier, and
        // surfacing it as a column would push that problem onto every caller.
        assertFalse(stored.columnNames().contains("[applied]"),
                () -> "the outcome column leaked into the row: " + stored.columnNames());
    }

    @Test
    @DisplayName("A conditional delete reports whether it applied and returns the row it removed")
    void testExecuteConditionalDelete() {
        final String table = kvTable();
        sessionProvider.execute("insert into " + table + " (k, v) values (?, ?)",
                List.of(bytes("to-delete"), bytes("doomed")), QueryOverrides.NONE);

        final CqlStatementResult deleted = sessionProvider.execute(
                "delete from " + table + " where k = ? if exists", List.of(bytes("to-delete")), QueryOverrides.NONE);
        assertTrue(deleted.wasApplied());

        final CqlStatementResult again = sessionProvider.execute(
                "delete from " + table + " where k = ? if exists", List.of(bytes("to-delete")), QueryOverrides.NONE);
        assertFalse(again.wasApplied(), "deleting an absent row must not report as applied");
    }

    @Test
    @DisplayName("Blob values round-trip as bytes, with no schema imposed on an encoding the caller owns")
    void testExecuteBlobRoundTrip() {
        final String table = kvTable();
        final byte[] payload = "opaque bytes".getBytes(StandardCharsets.UTF_8);

        sessionProvider.execute("insert into " + table + " (k, v) values (?, ?)",
                List.of(bytes("roundtrip"), ByteBuffer.wrap(payload)), QueryOverrides.NONE);

        final CqlStatementResult result = sessionProvider.execute("select k, v from " + table + " where k = ?",
                List.of(bytes("roundtrip")), QueryOverrides.NONE);

        assertEquals(1, result.rows().size());
        assertArrayEquals(payload, result.rows().getFirst().getBytes("v"));
    }

    @Test
    @DisplayName("Cells arrive in selection order, and a column name that is not a legal identifier survives intact")
    void testExecuteExposesCellsInSelectionOrder() {
        final String table = kvTable();
        sessionProvider.execute("insert into " + table + " (k, v) values (?, ?)",
                List.of(bytes("cells"), bytes("value")), QueryOverrides.NONE);

        final CqlStatementResult result = sessionProvider.execute(
                "select v, writetime(v), k from " + table + " where k = ?", List.of(bytes("cells")), QueryOverrides.NONE);

        final CqlRow row = result.rows().getFirst();
        assertEquals(List.of("v", "writetime(v)", "k"), row.columnNames(),
                "selection order is the only ordering information a schema-free caller has");
        assertNotNull(row.getObject("writetime(v)"),
                "a projected function's column name is not a legal identifier, and must survive anyway");
    }

    @Test
    @DisplayName("A query returning no rows yields an empty result rather than a null one")
    void testExecuteEmptyResult() {
        final String table = kvTable();

        final CqlStatementResult result = sessionProvider.execute("select v from " + table + " where k = ?",
                List.of(bytes("never-written")), QueryOverrides.NONE);

        assertTrue(result.rows().isEmpty());
        assertTrue(result.wasApplied(), "a plain SELECT is unconditional");
    }
}
