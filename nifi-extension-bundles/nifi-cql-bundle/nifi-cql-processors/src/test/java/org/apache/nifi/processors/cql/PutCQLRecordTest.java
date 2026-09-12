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

package org.apache.nifi.processors.cql;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.service.cql.api.constants.CqlBatchType;
import org.apache.nifi.service.cql.api.constants.UpdateMethod;
import org.apache.nifi.service.cql.api.exception.QueryFailureException;
import org.apache.nifi.service.cql.api.metadata.PrimaryKeyIdentifier;
import org.apache.nifi.service.cql.api.metadata.QualifiedTableName;
import org.apache.nifi.service.cql.api.service.CQLExecutionService;
import org.apache.nifi.service.cql.api.service.WriteOverrides;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Stubber;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.nifi.processors.cql.constants.StatementType.UPDATE_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PutCQLRecordTest {
    // The batch type the processor resolves when Batch Statement Type is left alone. Most tests below are
    // about something else entirely - batching, TTL, primary key overrides - and only need to name the batch
    // type because it is part of the service call they verify, so they refer to this rather than repeating a
    // literal. testDefaultBatchStatementType is the one test that actually asserts what the default is.
    private static final CqlBatchType DEFAULT_BATCH_TYPE = CqlBatchType.UNLOGGED;

    private TestRunner runner;
    private CQLExecutionService service;
    private MockRecordParser mockReader;

    @BeforeEach
    void setUp() throws Exception {
        runner = TestRunners.newTestRunner(PutCQLRecord.class);
        service = Mockito.mock(CQLExecutionService.class);
        mockReader = new MockRecordParser();
        mockReader.addSchemaField("message", RecordFieldType.STRING);
        mockReader.addSchemaField("sender", RecordFieldType.STRING);

        when(service.getIdentifier()).thenReturn("executionService");
        when(service.getTransitUrl(any(QualifiedTableName.class))).thenReturn("cassandra://message");

        runner.setProperty(PutCQLRecord.CONNECTION_PROVIDER_SERVICE, service.getIdentifier());
        runner.setProperty(PutCQLRecord.TABLE, "message");
        runner.setProperty(PutCQLRecord.RECORD_READER_FACTORY, "reader");

        runner.addControllerService("reader", mockReader);
        runner.enableControllerService(mockReader);

        runner.addControllerService(service.getIdentifier(), service);
        runner.enableControllerService(service);
    }

    @Test
    @DisplayName("Inserting records batches them into the configured batch size and reports success")
    void testInsert() {
        final int recordCount = 1000;
        final int batchCount = 10;

        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");

        for (int i = 0; i < recordCount; i++) {
            mockReader.addRecord("Hello, world", "test_user");
        }

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        verify(service, times(batchCount))
                .insert(eq(new QualifiedTableName(null, "message")), anyList(), anyMap(), eq(DEFAULT_BATCH_TYPE), eq(WriteOverrides.NONE));
    }

    @Test
    @DisplayName("Batch Statement Type defaults to UNLOGGED, so bulk writes do not pay for the distributed batchlog unless asked to")
    void testDefaultBatchStatementType() {
        assertEquals(DEFAULT_BATCH_TYPE.name(), PutCQLRecord.BATCH_STATEMENT_TYPE.getDefaultValue());
    }

    @DisplayName("Updating records with each update method and batch statement type produces the expected batched calls")
    @ParameterizedTest
    @CsvSource({ "DECREMENT,COUNTER", "INCREMENT,COUNTER", "SET,LOGGED" })
    void testUpdate(UpdateMethod updateMethod, String batchStatementType) {
        runner.setProperty(PutCQLRecord.UPDATE_KEYS, "sender");
        runner.setProperty(PutCQLRecord.STATEMENT_TYPE, UPDATE_TYPE.getValue());
        runner.setProperty(PutCQLRecord.UPDATE_METHOD, updateMethod.name());
        runner.setProperty(PutCQLRecord.BATCH_STATEMENT_TYPE, batchStatementType);

        final int recordCount = 1050;
        final int batchCount = 11;

        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");

        for (int i = 0; i < recordCount; i++) {
            mockReader.addRecord("Hello, world", "test_user");
        }

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        verify(service, times(batchCount))
                .update(eq(new QualifiedTableName(null, "message")), anyList(), anyMap(), eq(List.of("sender")), eq(updateMethod),
                        eq(CqlBatchType.valueOf(batchStatementType)), eq(WriteOverrides.NONE));
    }

    @Test
    @DisplayName("An update method resolved from a FlowFile attribute is matched case-insensitively")
    void testUpdateMethodFromAttributeIsCaseInsensitive() {
        runner.setProperty(PutCQLRecord.UPDATE_KEYS, "sender");
        runner.setProperty(PutCQLRecord.STATEMENT_TYPE, UPDATE_TYPE.getValue());
        runner.setProperty(PutCQLRecord.UPDATE_METHOD, "USE_ATTR");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("", Map.of(PutCQLRecord.UPDATE_METHOD_ATTRIBUTE, "set"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        verify(service, times(1))
                .update(eq(new QualifiedTableName(null, "message")), anyList(), anyMap(), eq(List.of("sender")), eq(UpdateMethod.SET), eq(DEFAULT_BATCH_TYPE), eq(WriteOverrides.NONE));
    }

    @Test
    @DisplayName("Setting Time To Live on the processor overrides the connection service's default for inserts")
    void testTtlOverrideAppliesToInsert() {
        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");
        runner.setProperty(PutCQLRecord.TTL, "1 hour");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        verify(service, times(1))
                .insert(eq(new QualifiedTableName(null, "message")), anyList(), anyMap(), eq(DEFAULT_BATCH_TYPE), eq(new WriteOverrides(Duration.ofHours(1), null)));
    }

    @Test
    @DisplayName("Setting Time To Live on the processor overrides the connection service's default for SET updates")
    void testTtlOverrideAppliesToUpdate() {
        runner.setProperty(PutCQLRecord.UPDATE_KEYS, "sender");
        runner.setProperty(PutCQLRecord.STATEMENT_TYPE, UPDATE_TYPE.getValue());
        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");
        runner.setProperty(PutCQLRecord.TTL, "30 min");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        verify(service, times(1))
                .update(eq(new QualifiedTableName(null, "message")), anyList(), anyMap(), eq(List.of("sender")), eq(UpdateMethod.SET),
                        eq(DEFAULT_BATCH_TYPE), eq(new WriteOverrides(Duration.ofMinutes(30), null)));
    }

    @Test
    @DisplayName("Setting Timestamp Field on the processor threads the field name through as a write override")
    void testTimestampFieldOverrideAppliesToInsert() {
        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");
        runner.setProperty(PutCQLRecord.TIMESTAMP_FIELD, "sender");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        verify(service, times(1))
                .insert(eq(new QualifiedTableName(null, "message")), anyList(), anyMap(), eq(DEFAULT_BATCH_TYPE), eq(new WriteOverrides(null, "sender")));
    }

    @Test
    @DisplayName("Setting Timestamp Field to a name absent from the record schema routes to failure without writing anything")
    void testTimestampFieldNotInSchemaRoutesToFailure() {
        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");
        runner.setProperty(PutCQLRecord.TIMESTAMP_FIELD, "does_not_exist");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_FAILURE, 1);

        verify(service, times(0)).insert(any(QualifiedTableName.class), anyList(), anyMap(), any(CqlBatchType.class), any(WriteOverrides.class));
    }

    @Test
    @DisplayName("Configuring INSERT with a COUNTER batch statement type fails validation up front")
    void testInsertWithCounterBatchTypeIsInvalid() {
        // Statement Type defaults to INSERT; Cassandra/ScyllaDB do not allow INSERT against counter tables,
        // so INSERT + COUNTER batch type can never succeed and should fail validation up front.
        runner.setProperty(PutCQLRecord.BATCH_STATEMENT_TYPE, "COUNTER");

        runner.assertNotValid();
    }

    @Test
    @DisplayName("An INSERT with a COUNTER batch statement type resolved from a FlowFile attribute routes to failure at runtime")
    void testInsertWithCounterBatchTypeAttributeRoutesToFailure() {
        // When Batch Statement Type is resolved from a FlowFile attribute, the invalid INSERT + COUNTER
        // combination can't be caught at validation time, so it must be rejected at runtime instead.
        runner.setProperty(PutCQLRecord.BATCH_STATEMENT_TYPE, "USE_ATTR");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("", Map.of(PutCQLRecord.BATCH_STATEMENT_TYPE_ATTRIBUTE, "COUNTER"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_FAILURE, 1);
    }

    /**
     * The three ways an insert can fail partway through a FlowFile's batches, each of which must route to
     * failure and report how many records had already been written.
     *
     * <p>{@code batchesBeforeFailure} of zero is the "fails immediately" case, where nothing was written;
     * one means the first batch landed before the second threw. The exception type is the axis that matters:
     * {@link ProcessException} takes the generic error path and {@link IllegalArgumentException} the
     * configuration-error path (which is how a bad primary key override surfaces), and both have to record
     * progress the same way.
     */
    static Stream<Arguments> insertFailures() {
        return Stream.of(
                arguments("fails on the first batch", new ProcessException("Test"), 0, "0"),
                arguments("fails after one batch", new ProcessException("Test"), 1, "100"),
                arguments("fails after one batch with a configuration error",
                        new IllegalArgumentException("/missing evaluated to no values."), 1, "100"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("insertFailures")
    @DisplayName("An insert failure routes the FlowFile to failure, reporting the records written before it")
    void testInsertFailureRoutesToFailureRecordingProgress(final String description, final RuntimeException failure,
                                                           final int batchesBeforeFailure, final String expectedRecordsWritten) {
        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");

        for (int i = 0; i < 250; i++) {
            mockReader.addRecord("Hello, world", "test_user");
        }

        Stubber stubber = null;
        for (int batch = 0; batch < batchesBeforeFailure; batch++) {
            stubber = stubber == null ? doNothing() : stubber.doNothing();
        }
        (stubber == null ? doThrow(failure) : stubber.doThrow(failure))
                .when(service)
                .insert(any(QualifiedTableName.class), anyList(), anyMap(), any(CqlBatchType.class), any(WriteOverrides.class));

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_FAILURE, 1);

        final MockFlowFile failedFlowFile = runner.getFlowFilesForRelationship(PutCQLRecord.REL_FAILURE).get(0);
        assertEquals(expectedRecordsWritten, failedFlowFile.getAttribute(PutCQLRecord.RECORDS_WRITTEN_ATTRIBUTE));

        // The batches that did land each left a provenance event behind; the one that threw did not, and the
        // batches after it were never attempted.
        assertEquals(batchesBeforeFailure, runner.getProvenanceEvents().size());
    }

    @Test
    @DisplayName("A Cassandra query failure while inserting routes the FlowFile to retry rather than failure")
    void testInsertQueryFailureRoutesToRetry() {
        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");

        mockReader.addRecord("Hello, world", "test_user");

        doThrow(new QueryFailureException())
                .when(service)
                .insert(any(QualifiedTableName.class), anyList(), anyMap(), any(CqlBatchType.class), any(WriteOverrides.class));
        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_RETRY, 1);
    }

    @Test
    @DisplayName("A Cassandra query failure while updating routes the FlowFile to retry rather than failure")
    void testUpdateQueryFailureRoutesToRetry() {
        runner.setProperty(PutCQLRecord.UPDATE_KEYS, "sender");
        runner.setProperty(PutCQLRecord.STATEMENT_TYPE, UPDATE_TYPE.getValue());
        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");

        mockReader.addRecord("Hello, world", "test_user");

        doThrow(new QueryFailureException())
                .when(service)
                .update(any(QualifiedTableName.class), anyList(), anyMap(), eq(List.of("sender")), eq(UpdateMethod.SET), any(CqlBatchType.class), any(WriteOverrides.class));
        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_RETRY, 1);
    }

    @Test
    @DisplayName("Each batch emits its own provenance event, and the final one reports the true total records written")
    void testProvenanceReportsRecordsWrittenPerBatchAndTotal() {
        final int recordCount = 250;

        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");

        for (int i = 0; i < recordCount; i++) {
            mockReader.addRecord("Hello, world", "test_user");
        }

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        assertEquals(3, events.size());
        assertTrue(events.get(0).getDetails().contains("100 total"));
        assertTrue(events.get(1).getDetails().contains("200 total"));
        assertTrue(events.get(2).getDetails().contains("250 total"));
    }

    @Test
    @DisplayName("An invalid table name routes to failure with cql.records.written explicitly zero")
    void testInvalidTableNameReportsZeroRecordsWritten() {
        runner.setProperty(PutCQLRecord.TABLE, "not.a.valid.table.name");

        mockReader.addRecord("Hello, world", "test_user");
        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_FAILURE, 1);

        MockFlowFile failedFlowFile = runner.getFlowFilesForRelationship(PutCQLRecord.REL_FAILURE).get(0);
        assertEquals("0", failedFlowFile.getAttribute(PutCQLRecord.RECORDS_WRITTEN_ATTRIBUTE));
    }

    @Test
    @DisplayName("A dynamic property named keyspace.table.field is accepted and described in terms of that field/table/keyspace")
    void testDynamicPropertyDescriptorAcceptsQualifiedFieldName() {
        PutCQLRecord processor = new PutCQLRecord();

        PropertyDescriptor descriptor = processor.getSupportedDynamicPropertyDescriptor("my_keyspace.my_table.id_field");

        assertTrue(descriptor.isDynamic());
        assertEquals("my_keyspace.my_table.id_field", descriptor.getName());
        assertEquals("This property sets a record path to be evaluated for field id_field on table my_keyspace.my_table",
                descriptor.getDescription());
    }

    @ParameterizedTest
    @DisplayName("A dynamic property name that isn't keyspace.table.field is rejected")
    @ValueSource(strings = {
            "id_field",                          // missing keyspace and table
            "my_table.id_field",                 // missing keyspace
            "my_keyspace.my_table.extra.id_field", // too many segments
            "",                                   // empty
            "my_keyspace.my_table.",              // trailing dot, empty field
            ".my_table.id_field",                 // leading dot, empty keyspace
            "1keyspace.my_table.id_field",        // segment starting with a digit
            "my-keyspace.my_table.id_field",      // hyphen is not a valid CQL identifier character
            "my_keyspace.my_table.id field"       // whitespace inside a segment
    })
    void testDynamicPropertyDescriptorRejectsNonQualifiedName(String propertyName) {
        PutCQLRecord processor = new PutCQLRecord();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> processor.getSupportedDynamicPropertyDescriptor(propertyName));

        // Asserted on every case rather than once in a separate test: a user who mistypes one of several
        // dynamic properties needs the message to say which one, and that is only useful if it holds for
        // every rejection rather than for the one input a standalone test happened to pick.
        assertTrue(exception.getMessage().contains(propertyName),
                "Expected the exception message to name the invalid property, but was: " + exception.getMessage());
    }

    @Test
    @SuppressWarnings("unchecked")
    @DisplayName("A dynamic property RecordPath override for the target table is passed through as a primary key override on insert")
    void testDynamicPropertyOverrideAppliesToInsert() {
        runner.setProperty(PutCQLRecord.TABLE, "my_keyspace.message");
        runner.setProperty("my_keyspace.message.id", "/sender");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        final ArgumentCaptor<Map<PrimaryKeyIdentifier, RecordPath>> captor = ArgumentCaptor.forClass(Map.class);
        verify(service).insert(eq(new QualifiedTableName("my_keyspace", "message")), anyList(), captor.capture(),
                eq(DEFAULT_BATCH_TYPE), eq(WriteOverrides.NONE));

        final Map<PrimaryKeyIdentifier, RecordPath> overrides = captor.getValue();
        final PrimaryKeyIdentifier identifier = new PrimaryKeyIdentifier("my_keyspace", "message", "id");
        assertEquals(1, overrides.size());
        assertTrue(overrides.containsKey(identifier));
        assertEquals("/sender", overrides.get(identifier).getPath());
    }

    @Test
    @SuppressWarnings("unchecked")
    @DisplayName("Multiple dynamic property RecordPath overrides for the same table are all passed through together")
    void testMultipleDynamicPropertyOverridesApplyToInsert() {
        runner.setProperty(PutCQLRecord.TABLE, "my_keyspace.message");
        runner.setProperty("my_keyspace.message.id", "/sender");
        runner.setProperty("my_keyspace.message.ts", "/message");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        final ArgumentCaptor<Map<PrimaryKeyIdentifier, RecordPath>> captor = ArgumentCaptor.forClass(Map.class);
        verify(service).insert(eq(new QualifiedTableName("my_keyspace", "message")), anyList(), captor.capture(),
                eq(DEFAULT_BATCH_TYPE), eq(WriteOverrides.NONE));

        final Map<PrimaryKeyIdentifier, RecordPath> overrides = captor.getValue();
        assertEquals(2, overrides.size());
        assertEquals("/sender", overrides.get(new PrimaryKeyIdentifier("my_keyspace", "message", "id")).getPath());
        assertEquals("/message", overrides.get(new PrimaryKeyIdentifier("my_keyspace", "message", "ts")).getPath());
    }

    @Test
    @SuppressWarnings("unchecked")
    @DisplayName("A dynamic property RecordPath override for a different table is excluded from the overrides passed for the current write")
    void testDynamicPropertyOverrideForOtherTableIsExcluded() {
        runner.setProperty(PutCQLRecord.TABLE, "my_keyspace.message");
        runner.setProperty("my_keyspace.other_table.id", "/sender");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        final ArgumentCaptor<Map<PrimaryKeyIdentifier, RecordPath>> captor = ArgumentCaptor.forClass(Map.class);
        verify(service).insert(eq(new QualifiedTableName("my_keyspace", "message")), anyList(), captor.capture(),
                eq(DEFAULT_BATCH_TYPE), eq(WriteOverrides.NONE));

        assertTrue(captor.getValue().isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    @DisplayName("An unqualified Table name never matches any configured dynamic property override, since overrides always require a keyspace segment")
    void testDynamicPropertyOverrideIgnoredWhenTableIsUnqualified() {
        // TABLE is left at its unqualified default of "message" (set in setUp()), so the QualifiedTableName
        // built at runtime has a null keyspace - which can never equal a dynamic property's required,
        // non-null keyspace segment, so the override is silently never applied.
        runner.setProperty("my_keyspace.message.id", "/sender");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        final ArgumentCaptor<Map<PrimaryKeyIdentifier, RecordPath>> captor = ArgumentCaptor.forClass(Map.class);
        verify(service).insert(eq(new QualifiedTableName(null, "message")), anyList(), captor.capture(),
                eq(DEFAULT_BATCH_TYPE), eq(WriteOverrides.NONE));

        assertTrue(captor.getValue().isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    @DisplayName("A dynamic property RecordPath override for the target table is passed through as a primary key override on update")
    void testDynamicPropertyOverrideAppliesToUpdate() {
        runner.setProperty(PutCQLRecord.TABLE, "my_keyspace.message");
        runner.setProperty(PutCQLRecord.UPDATE_KEYS, "sender");
        runner.setProperty(PutCQLRecord.STATEMENT_TYPE, UPDATE_TYPE.getValue());
        runner.setProperty("my_keyspace.message.id", "/message");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        final ArgumentCaptor<Map<PrimaryKeyIdentifier, RecordPath>> captor = ArgumentCaptor.forClass(Map.class);
        verify(service).update(eq(new QualifiedTableName("my_keyspace", "message")), anyList(), captor.capture(),
                eq(List.of("sender")), eq(UpdateMethod.SET), eq(DEFAULT_BATCH_TYPE), eq(WriteOverrides.NONE));

        final Map<PrimaryKeyIdentifier, RecordPath> overrides = captor.getValue();
        final PrimaryKeyIdentifier identifier = new PrimaryKeyIdentifier("my_keyspace", "message", "id");
        assertEquals(1, overrides.size());
        assertEquals("/message", overrides.get(identifier).getPath());
    }
}
