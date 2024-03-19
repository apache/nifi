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

import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processors.cql.mock.FailingRecordSetWriterFactory;
import org.apache.nifi.processors.cql.mock.MockCQLQueryExecutionService;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.service.cql.api.service.QueryOverrides;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExecuteCQLQueryRecordTest {
    private TestRunner testRunner;

    @BeforeEach
    public void setup() {
        testRunner = TestRunners.newTestRunner(ExecuteCQLQueryRecord.class);
        testRunner.setProperty(ExecuteCQLQueryRecord.CQL_SELECT_QUERY, "SELECT * FROM cql_table");
    }

    @DisplayName("Verify the normal write behavior with different record counts and expected batch sizes")
    @ParameterizedTest(name = "rows per file={0},generated records={1},expected ff count={2},output batch size={3}")
    @CsvSource({
        "5,4,1,0",
        "5,100,20,0",
        "5,101,21,0",
        "5,100,20,1",
        "50,3000,60,5",
        "0,100,1,0"
    })
    public void testSimpleWriteScenario(int rowsPerFlowFile, int recordCount,
                                        int expectedFlowFileCount,
                                        int outputBatchSize) throws Exception {
        testRunner.setProperty(ExecuteCQLQueryRecord.MAX_ROWS_PER_FLOW_FILE, String.valueOf(rowsPerFlowFile));
        testRunner.setProperty(ExecuteCQLQueryRecord.OUTPUT_BATCH_SIZE, String.valueOf(outputBatchSize));

        MockRecordWriter parser = new MockRecordWriter();
        RecordField field1 = new RecordField("a", RecordFieldType.STRING.getDataType());
        RecordField field2 = new RecordField("b", RecordFieldType.STRING.getDataType());
        SimpleRecordSchema schema = new SimpleRecordSchema(List.of(field1, field2));

        List<Record> data = new ArrayList<>();

        for (int i = 0; i < recordCount; i++) {
            Map<String, Object> rec = Map.of("a", UUID.randomUUID().toString(), "b", UUID.randomUUID().toString());
            MapRecord testData = new MapRecord(schema, rec);
            data.add(testData);
        }

        MockCQLQueryExecutionService service = new MockCQLQueryExecutionService(data.iterator());

        testRunner.setProperty(ExecuteCQLQueryRecord.CONNECTION_PROVIDER_SERVICE, "connection");
        testRunner.addControllerService("connection", service);
        testRunner.enableControllerService(service);
        testRunner.setProperty(ExecuteCQLQueryRecord.OUTPUT_WRITER, "writer");
        testRunner.addControllerService("writer", parser);
        testRunner.enableControllerService(parser);
        testRunner.assertValid();

        testRunner.enqueue("parent_file");
        testRunner.run();

        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_SUCCESS, expectedFlowFileCount);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ExecuteCQLQueryRecord.REL_SUCCESS);
        assertEquals(expectedFlowFileCount, flowFiles.size());
        flowFiles.sort(Comparator.comparingInt(ff -> Integer.parseInt(ff.getAttribute(FragmentAttributes.FRAGMENT_INDEX.key()))));

        final boolean batching = outputBatchSize > 0;
        final Set<String> fragmentIds = new HashSet<>();
        int recordsRemaining = recordCount;

        for (MockFlowFile ff : flowFiles) {
            String fragmentId = ff.getAttribute(FragmentAttributes.FRAGMENT_ID.key());
            assertNotNull(fragmentId);
            assertDoesNotThrow(() -> UUID.fromString(fragmentId));
            fragmentIds.add(fragmentId);

            assertNotNull(ff.getAttribute(FragmentAttributes.FRAGMENT_INDEX.key()));
            int fragmentIndex = Integer.parseInt(ff.getAttribute(FragmentAttributes.FRAGMENT_INDEX.key()));
            assertTrue(fragmentIndex < expectedFlowFileCount);

            // fragment.count is only knowable once the whole result set has been processed, which isn't the
            // case once Output Batch Size starts releasing FlowFiles early - see OUTPUT_BATCH_SIZE's description.
            if (batching) {
                assertNull(ff.getAttribute(FragmentAttributes.FRAGMENT_COUNT.key()));
            } else {
                assertEquals(String.valueOf(expectedFlowFileCount), ff.getAttribute(FragmentAttributes.FRAGMENT_COUNT.key()));
            }

            int rowsInFile = ff.getContent().isEmpty() ? 0 : ff.getContent().split("\n").length;
            int expectedRowsInFile = rowsPerFlowFile == 0 ? recordCount : Math.min(rowsPerFlowFile, recordsRemaining);
            assertEquals(expectedRowsInFile, rowsInFile);
            recordsRemaining -= rowsInFile;
        }

        assertEquals(1, fragmentIds.size());
        assertEquals(0, recordsRemaining);
    }

    private MockCQLQueryExecutionService configureServices() throws Exception {
        MockRecordWriter parser = new MockRecordWriter();
        RecordField field1 = new RecordField("a", RecordFieldType.STRING.getDataType());
        SimpleRecordSchema schema = new SimpleRecordSchema(List.of(field1));

        List<Record> data = new ArrayList<>();
        data.add(new MapRecord(schema, Map.of("a", "value")));

        MockCQLQueryExecutionService service = new MockCQLQueryExecutionService(data.iterator());

        testRunner.setProperty(ExecuteCQLQueryRecord.CONNECTION_PROVIDER_SERVICE, "connection");
        testRunner.addControllerService("connection", service);
        testRunner.enableControllerService(service);
        testRunner.setProperty(ExecuteCQLQueryRecord.OUTPUT_WRITER, "writer");
        testRunner.addControllerService("writer", parser);
        testRunner.enableControllerService(parser);

        return service;
    }

    private MockCQLQueryExecutionService runWithSingleRecord() throws Exception {
        return runWithSingleRecord(Map.of());
    }

    private MockCQLQueryExecutionService runWithSingleRecord(final Map<String, String> attributes) throws Exception {
        testRunner.setProperty(ExecuteCQLQueryRecord.MAX_ROWS_PER_FLOW_FILE, "10");

        MockCQLQueryExecutionService service = configureServices();
        testRunner.assertValid();

        testRunner.enqueue("parent_file", attributes);
        testRunner.run();

        return service;
    }

    /**
     * Runs a query that delivers {@code rowsBeforeFailure} rows and then fails partway through the result set,
     * against a writer that may itself be set to fail. Returns the runner for the caller to assert routing on.
     */
    private void runWithMidStreamFailure(final int rowsBeforeFailure, final int maxRowsPerFlowFile,
                                         final int outputBatchSize, final MockRecordWriter writer) throws Exception {
        testRunner.setProperty(ExecuteCQLQueryRecord.MAX_ROWS_PER_FLOW_FILE, String.valueOf(maxRowsPerFlowFile));
        testRunner.setProperty(ExecuteCQLQueryRecord.OUTPUT_BATCH_SIZE, String.valueOf(outputBatchSize));

        final RecordField field = new RecordField("a", RecordFieldType.STRING.getDataType());
        final SimpleRecordSchema schema = new SimpleRecordSchema(List.of(field));

        final List<Record> data = new ArrayList<>();
        for (int i = 0; i < rowsBeforeFailure; i++) {
            data.add(new MapRecord(schema, Map.of("a", "value-" + i)));
        }

        final MockCQLQueryExecutionService service =
                new MockCQLQueryExecutionService(data.iterator()).failAfter(rowsBeforeFailure);

        testRunner.setProperty(ExecuteCQLQueryRecord.CONNECTION_PROVIDER_SERVICE, "connection");
        testRunner.addControllerService("connection", service);
        testRunner.enableControllerService(service);
        testRunner.setProperty(ExecuteCQLQueryRecord.OUTPUT_WRITER, "writer");
        testRunner.addControllerService("writer", writer);
        testRunner.enableControllerService(writer);
        testRunner.assertValid();

        testRunner.enqueue("parent_file");
        testRunner.run();
    }

    @DisplayName("Verify that a failure partway through a result set leaves no FlowFile stranded, so the session still commits and the retry takes effect")
    @ParameterizedTest(name = "rows before failure={0}, rows per flow file={1}")
    @CsvSource({
        "5,2",
        "3,0",
        "0,2"
    })
    public void testMidStreamFailureRoutesParentToRetry(final int rowsBeforeFailure, final int maxRowsPerFlowFile) throws Exception {
        runWithMidStreamFailure(rowsBeforeFailure, maxRowsPerFlowFile, 0, new MockRecordWriter());

        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_RETRY, 1);
        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_SUCCESS, 0);
        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_ORIGINAL, 0);
        assertTrue(testRunner.getFlowFilesForRelationship(ExecuteCQLQueryRecord.REL_RETRY).getFirst().isPenalized());
    }

    @Test
    @DisplayName("A query failure with no incoming connection yields the processor instead of routing an empty FlowFile to retry")
    public void testSourceModeQueryFailureYieldsInsteadOfRouting() throws Exception {
        final MockCQLQueryExecutionService service =
                new MockCQLQueryExecutionService(Collections.<Record>emptyList().iterator()).failAfter(0);

        testRunner.setProperty(ExecuteCQLQueryRecord.CONNECTION_PROVIDER_SERVICE, "connection");
        testRunner.addControllerService("connection", service);
        testRunner.enableControllerService(service);
        testRunner.setProperty(ExecuteCQLQueryRecord.OUTPUT_WRITER, "writer");
        final MockRecordWriter writer = new MockRecordWriter();
        testRunner.addControllerService("writer", writer);
        testRunner.enableControllerService(writer);
        testRunner.assertValid();

        testRunner.setIncomingConnection(false);
        testRunner.run();

        // Nothing routed anywhere - a created-from-nothing FlowFile has no attributes to re-drive the query,
        // so the retry is the next scheduled run, signalled by yielding.
        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_RETRY, 0);
        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_FAILURE, 0);
        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_SUCCESS, 0);
        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_ORIGINAL, 0);
        assertTrue(((org.apache.nifi.util.MockProcessContext) testRunner.getProcessContext()).isYieldCalled());
    }

    @Test
    @DisplayName("Verify that a failure after Output Batch Size has already committed FlowFiles retries on a new FlowFile rather than one the session released")
    public void testMidStreamFailureAfterOutputBatchCommitStillRetries() throws Exception {
        runWithMidStreamFailure(9, 2, 2, new MockRecordWriter());

        // The four FlowFiles committed before the failure stay downstream - they cannot be recalled - and the
        // parent went to 'original' with them, so the retry has to be carried by a FlowFile created afterwards.
        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_SUCCESS, 4);
        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_RETRY, 1);
    }

    @Test
    @DisplayName("Verify that a record writer failing mid-result-set routes to failure with no FlowFile left stranded")
    public void testWriterFailureMidResultSetRoutesToFailure() throws Exception {
        // failAfterN counts per writer instance and the callback builds a fresh writer per FlowFile, so this
        // only reaches the failure with a single writer covering the whole result set - hence 0 (unlimited)
        // rows per FlowFile. The writer throws at row 5, before the query's own failure at row 9 would have
        // fired, so this is the writer-failure path rather than the query-failure one.
        runWithMidStreamFailure(9, 0, 0, new MockRecordWriter(null, false, 5));

        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_FAILURE, 1);
        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_SUCCESS, 0);
        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_ORIGINAL, 0);
        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_RETRY, 0);
    }

    @Test
    @DisplayName("Verify that cql.arg.N properties are forwarded as positional parameters in order, leaving the query text untouched")
    public void testPositionalParametersForwardedInOrder() throws Exception {
        testRunner.setProperty(ExecuteCQLQueryRecord.CQL_SELECT_QUERY, "SELECT * FROM cql_table WHERE id = ? AND region = ?");
        testRunner.setProperty("cql.arg.1", "${row.id}");
        testRunner.setProperty("cql.arg.2", "${row.region}");

        MockCQLQueryExecutionService service = runWithSingleRecord(Map.of("row.id", "42", "row.region", "us-east"));

        assertEquals(List.of("42", "us-east"), service.getLastParameters());
        assertEquals("SELECT * FROM cql_table WHERE id = ? AND region = ?", service.getLastCql());
    }

    @Test
    @DisplayName("Verify that CQL inside a parameter value stays data - it is bound, never concatenated into the query text")
    public void testParameterValueIsNotInterpolatedIntoQueryText() throws Exception {
        final String injected = "1' OR '1'='1";

        testRunner.setProperty(ExecuteCQLQueryRecord.CQL_SELECT_QUERY, "SELECT * FROM cql_table WHERE id = ?");
        testRunner.setProperty("cql.arg.1", "${row.id}");

        MockCQLQueryExecutionService service = runWithSingleRecord(Map.of("row.id", injected));

        assertEquals(List.of(injected), service.getLastParameters());
        assertEquals("SELECT * FROM cql_table WHERE id = ?", service.getLastCql());
    }

    @Test
    @DisplayName("Verify that a query with no parameters forwards an empty parameter list")
    public void testNoParametersForwardsEmptyList() throws Exception {
        MockCQLQueryExecutionService service = runWithSingleRecord();

        assertEquals(List.of(), service.getLastParameters());
    }

    @Test
    @DisplayName("Verify that a gap in the positional parameter numbering fails validation")
    public void testNonConsecutiveParameterNumberingIsInvalid() throws Exception {
        configureServices();
        testRunner.setProperty("cql.arg.1", "first");
        testRunner.setProperty("cql.arg.3", "third");

        testRunner.assertNotValid();
    }

    @Test
    @DisplayName("Verify that positional parameter numbering must start at 1")
    public void testParameterNumberingNotStartingAtOneIsInvalid() throws Exception {
        configureServices();
        testRunner.setProperty("cql.arg.2", "second");

        testRunner.assertNotValid();
    }

    @Test
    @DisplayName("Verify that a dynamic property which is not a positional parameter is rejected outright")
    public void testUnrecognizedDynamicPropertyNameIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> testRunner.setProperty("cql.parameter.1", "value"));
    }

    @Test
    @DisplayName("Verify that fetch size and query timeout overrides are unset by default, deferring to the connection service")
    public void testQueryOverridesDefaultToUnset() throws Exception {
        MockCQLQueryExecutionService service = runWithSingleRecord();

        QueryOverrides overrides = service.getLastQueryOverrides();
        assertNotNull(overrides);
        assertNull(overrides.fetchSize());
        assertNull(overrides.timeout());
    }

    @Test
    @DisplayName("Verify that fetch size and query timeout overrides are forwarded to the connection service when set")
    public void testQueryOverridesForwardedWhenSet() throws Exception {
        testRunner.setProperty(ExecuteCQLQueryRecord.FETCH_SIZE, "250");
        testRunner.setProperty(ExecuteCQLQueryRecord.QUERY_TIMEOUT, "5 sec");

        MockCQLQueryExecutionService service = runWithSingleRecord();

        QueryOverrides overrides = service.getLastQueryOverrides();
        assertNotNull(overrides);
        assertEquals(250, overrides.fetchSize());
        assertEquals(Duration.ofSeconds(5), overrides.timeout());
    }

    @Test
    @DisplayName("Verify that an incoming FlowFile is routed to 'original' when the query returns no rows")
    public void testEmptyResultSetRoutesParentToOriginal() throws Exception {
        testRunner.setProperty(ExecuteCQLQueryRecord.MAX_ROWS_PER_FLOW_FILE, "5");

        MockRecordWriter parser = new MockRecordWriter();
        MockCQLQueryExecutionService service = new MockCQLQueryExecutionService(Collections.<Record>emptyList().iterator());

        testRunner.setProperty(ExecuteCQLQueryRecord.CONNECTION_PROVIDER_SERVICE, "connection");
        testRunner.addControllerService("connection", service);
        testRunner.enableControllerService(service);
        testRunner.setProperty(ExecuteCQLQueryRecord.OUTPUT_WRITER, "writer");
        testRunner.addControllerService("writer", parser);
        testRunner.enableControllerService(parser);
        testRunner.assertValid();

        testRunner.enqueue("parent_file");
        testRunner.run();

        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_SUCCESS, 0);
        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_FAILURE, 0);
        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_RETRY, 0);
    }

    @Test
    @DisplayName("Verify that a record writer creation failure routes to failure without leaking the newly-created FlowFile")
    public void testWriterCreationFailureRoutesToFailureWithoutLeakingFlowFiles() throws Exception {
        testRunner.setProperty(ExecuteCQLQueryRecord.MAX_ROWS_PER_FLOW_FILE, "5");

        RecordField field1 = new RecordField("a", RecordFieldType.STRING.getDataType());
        SimpleRecordSchema schema = new SimpleRecordSchema(List.of(field1));

        List<Record> data = new ArrayList<>();
        data.add(new MapRecord(schema, Map.of("a", "value")));

        MockCQLQueryExecutionService service = new MockCQLQueryExecutionService(data.iterator());
        FailingRecordSetWriterFactory writer = new FailingRecordSetWriterFactory();

        testRunner.setProperty(ExecuteCQLQueryRecord.CONNECTION_PROVIDER_SERVICE, "connection");
        testRunner.addControllerService("connection", service);
        testRunner.enableControllerService(service);
        testRunner.setProperty(ExecuteCQLQueryRecord.OUTPUT_WRITER, "writer");
        testRunner.addControllerService("writer", writer);
        testRunner.enableControllerService(writer);
        testRunner.assertValid();

        testRunner.enqueue("parent_file");
        testRunner.run();

        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_FAILURE, 1);
        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_SUCCESS, 0);
        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_ORIGINAL, 0);
        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_RETRY, 0);
    }
}
