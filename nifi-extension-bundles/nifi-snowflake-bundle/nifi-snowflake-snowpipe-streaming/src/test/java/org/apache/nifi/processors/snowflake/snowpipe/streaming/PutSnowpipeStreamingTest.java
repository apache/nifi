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
package org.apache.nifi.processors.snowflake.snowpipe.streaming;

import net.snowflake.ingest.streaming.DropChannelRequest;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.InsertValidationResponse.InsertError;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.internal.ColumnProperties;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.Utils;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.key.service.api.PrivateKeyService;
import org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.PutSnowpipeStreaming.PropertyEvaluator;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.PutSnowpipeStreaming.RecordPathRecordOffsetFunction;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.PutSnowpipeStreaming.PrefixFlowFileFilter;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.channel.ConcurrencyClaim;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.channel.ConcurrencyManager;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.converter.LogicalDataType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@ExtendWith(MockitoExtension.class)
class PutSnowpipeStreamingTest {
    private static final String START_OFFSET_ATTRIBUTE = "start.offset";
    private static final String DEFAULT_TABLE_NAME = "table";

    private MockSnowflakeStreamingIngestClient client;
    private PutSnowpipeStreaming processor;
    private TestRunner runner;
    private Map<String, TableSchema> tableSchemas;
    private Map<String, ChannelConfiguration> channelConfigurations;
    private MockRecordParser recordReader;
    private int flowFileId = 0;

    private final List<Object[]> recordData = List.of(
        new Object[] {"John Doe", "USA", 48.23},
        new Object[] {"Jane Doe", "USA", 32.84},
        new Object[] {"Jake Doe", "Poland", 15.44},
        new Object[] {"June Doe", "Poland", 551.332}
    );


    @BeforeEach
    void setup() throws InitializationException {
        flowFileId = 0;
        tableSchemas = new HashMap<>();
        channelConfigurations = new HashMap<>();

        client = new MockSnowflakeStreamingIngestClient();
        processor = new MockPutSnowpipeStreaming();
        runner = TestRunners.newTestRunner(processor);

        runner.setProperty(PutSnowpipeStreaming.ACCOUNT, "account");
        runner.setProperty(PutSnowpipeStreaming.DATABASE, "database");
        runner.setProperty(PutSnowpipeStreaming.TABLE, DEFAULT_TABLE_NAME);
        runner.setProperty(PutSnowpipeStreaming.SCHEMA, "schema");
        runner.setProperty(PutSnowpipeStreaming.ROLE, "role");
        runner.setProperty(PutSnowpipeStreaming.USER, "user");
        runner.setProperty(PutSnowpipeStreaming.SNOWPIPE_CHANNEL_PREFIX, "prefix");
        runner.setProperty(PutSnowpipeStreaming.SNOWPIPE_CHANNEL_INDEX, "1");

        final PrivateKeyService privateKeyService = new MockPrivateKeyService();
        runner.addControllerService("privateKeyService", privateKeyService);
        runner.enableControllerService(privateKeyService);
        runner.setProperty(PutSnowpipeStreaming.PRIVATE_KEY_SERVICE, "privateKeyService");

        recordReader = new MockRecordParser();
        runner.addControllerService("recordReader", recordReader);
        runner.enableControllerService(recordReader);
        runner.setProperty(PutSnowpipeStreaming.RECORD_READER, "recordReader");

        // Setup schemas
        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("country", RecordFieldType.STRING);
        recordReader.addSchemaField("favoriteNumber", RecordFieldType.DECIMAL);

        addTable(DEFAULT_TABLE_NAME);

        // We can disable validation because internally we check if Expression Language is present and if not
        // we evaluate the value up-front instead of evaluating for every FlowFile.
        runner.setValidateExpressionUsage(false);
    }

    private void addTable(final String tableName) {
        tableSchemas.put(tableName, new TableSchema(Map.of(
            "name", columnProperties(LogicalDataType.TEXT),
            "country", columnProperties(LogicalDataType.TEXT),
            "favoriteNumber", columnProperties(LogicalDataType.REAL)
        )));
    }

    @AfterEach
    public void assertAllChannelsClosed() {
        if (client != null) {
            for (final MockSnowflakeStreamingIngestChannel channel : client.getCreatedChannels()) {
                assertTrue(channel.isClosed());
            }
        }

        // There won't be a concurrency manager if using exactly once. But if there is a concurrency manager,
        // ensure that it's empty, meaning we've fully released any concurrency counts.
        final ConcurrencyManager concurrencyManager = processor.getConcurrencyManager();
        if (concurrencyManager != null) {
            assertTrue(concurrencyManager.isEmpty());
        }
    }

    private ColumnProperties columnProperties(final LogicalDataType type) {
        final ColumnProperties properties = Mockito.mock(ColumnProperties.class, withSettings().strictness(Strictness.LENIENT));
        when(properties.getLogicalType()).thenReturn(type.name());
        return properties;
    }

    @Test
    void testPropertyEvaluator() throws UnknownHostException {
        final PropertyEvaluator explicitEvaluator = createEvaluator("explicit");
        assertEquals("explicit", explicitEvaluator.evaluate(new MockFlowFile(1)));

        final PropertyEvaluator attributeReferenceEvaluator = createEvaluator("${greeting}");
        assertEquals("hello", attributeReferenceEvaluator.evaluate(createFlowFile(Map.of("greeting", "hello"))));

        final PropertyEvaluator missingAttributeEvaluator = createEvaluator("${missing}");
        assertEquals("", missingAttributeEvaluator.evaluate(new MockFlowFile(1)));

        final PropertyEvaluator multipleAttributesEvaluator = createEvaluator("${greeting} ${name}");
        assertEquals("hello world", multipleAttributesEvaluator.evaluate(createFlowFile(Map.of("greeting", "hello", "name", "world"))));
        assertEquals("hola mundo", multipleAttributesEvaluator.evaluate(createFlowFile(Map.of("greeting", "hola", "name", "mundo"))));
    }

    private PropertyEvaluator createEvaluator(final String propertyText) throws UnknownHostException {
        final PropertyValue propertyValue = new StandardPropertyValue(propertyText, null, null);
        return processor.createEvaluator(propertyValue);
    }

    @Test
    void testVerify() {
        final ProcessContext context = runner.getProcessContext();
        final MockComponentLog componentLog = new MockComponentLog(processor.getIdentifier(), processor);

        final List<ConfigVerificationResult> results = processor.verify(context, componentLog, Map.of());

        assertNotNull(results);

        final ConfigVerificationResult firstResult = results.getFirst();
        assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, firstResult.getOutcome());
    }

    @Test
    public void testExactlyOnceWithOffsetGreaterThanChannel() {
        runner.setProperty(PutSnowpipeStreaming.DELIVERY_GUARANTEE, PutSnowpipeStreaming.DELIVERY_EXACTLY_ONCE);
        runner.setProperty(PutSnowpipeStreaming.RECORD_OFFSET_STRATEGY, PutSnowpipeStreaming.OFFSET_FROM_EXPRESSION_LANGUAGE);
        runner.setProperty(PutSnowpipeStreaming.RECORD_OFFSET_EL, "${" + START_OFFSET_ATTRIBUTE + "}");

        recordData.forEach(recordReader::addRecord);
        runner.enqueue(createFlowFile("3"));

        runner.run();

        runner.assertAllFlowFilesTransferred(PutSnowpipeStreaming.REL_SUCCESS, 1);
        final List<MockSnowflakeStreamingIngestChannel> channels = client.getCreatedChannels();
        assertEquals(1, channels.size());

        final MockSnowflakeStreamingIngestChannel channel = channels.getFirst();
        assertRowsEqual(recordData, channel.getRowsInserted());
    }

    @Test
    public void testExactlyOnceWithAllRecordsDuplicate() {
        runner.setProperty(PutSnowpipeStreaming.DELIVERY_GUARANTEE, PutSnowpipeStreaming.DELIVERY_EXACTLY_ONCE);
        runner.setProperty(PutSnowpipeStreaming.RECORD_OFFSET_STRATEGY, PutSnowpipeStreaming.OFFSET_FROM_EXPRESSION_LANGUAGE);
        runner.setProperty(PutSnowpipeStreaming.RECORD_OFFSET_EL, "${" + START_OFFSET_ATTRIBUTE + "}");

        recordData.forEach(recordReader::addRecord);
        runner.enqueue(createFlowFile("3"));
        getChannelConfiguration().setLatestOffsetToken("1000");

        runner.run();
        runner.assertAllFlowFilesTransferred(PutSnowpipeStreaming.REL_SUCCESS, 1);

        final List<MockSnowflakeStreamingIngestChannel> channels = client.getCreatedChannels();
        assertEquals(1, channels.size());
        final List<Map<String, Object>> rowsInserted = channels.getFirst().getRowsInserted();
        assertEquals(0, rowsInserted.size());
    }

    @Test
    public void testExceptionOnClose() {
        recordData.forEach(recordReader::addRecord);
        runner.enqueue(createFlowFile("3"));
        getChannelConfiguration().setFailOnClose(true);

        runner.run();
        runner.assertAllFlowFilesTransferred(PutSnowpipeStreaming.REL_FAILURE, 1);
        assertEquals(1, client.getCreatedChannels().size());

        // We should still have written the records to the channel, even though we failed to close.
        final MockSnowflakeStreamingIngestChannel channel = client.getCreatedChannels().getFirst();
        assertRowsEqual(recordData, channel.getRowsInserted());
    }

    @Test
    public void testTwoDestinationTablesCanBeWrittenSimultaneously() throws InterruptedException {
        runner.setProperty(PutSnowpipeStreaming.TABLE, "${tableName}");
        runner.setProperty(PutSnowpipeStreaming.SNOWPIPE_CHANNEL_PREFIX, "${tableName}");

        recordData.forEach(recordReader::addRecord);

        final MockFlowFile table1FlowFile = createFlowFile(Map.of("tableName", DEFAULT_TABLE_NAME));
        runner.enqueue(table1FlowFile);

        getChannelConfiguration().blockRowInsertions();

        final Thread backgroundThread = new Thread(runner::run);
        backgroundThread.start();

        // Wait until records are written, so that we know that the first thread has finished pulling FlowFiles from the queue.
        waitForRecordsWritten();

        // Add Table 2 to those known by our client, queue up a FlowFile for table2, and run.
        addTable("table2");
        final MockFlowFile table2FlowFile = createFlowFile(Map.of("tableName", "table2"));
        runner.enqueue(table2FlowFile);
        runner.run();

        // We should get one FlowFile out for table2 and nothing for table1.
        runner.assertAllFlowFilesTransferred(PutSnowpipeStreaming.REL_SUCCESS, 1);
        final MockFlowFile outputFlowFile = runner.getFlowFilesForRelationship(PutSnowpipeStreaming.REL_SUCCESS).getFirst();
        outputFlowFile.assertAttributeEquals("tableName", "table2");
        runner.clearTransferState();

        // Release the writer for table1 and wait for the thread to finish.
        getChannelConfiguration().releaseRowInsertions();
        backgroundThread.join();

        // The first FlowFile should now go to success.
        runner.assertAllFlowFilesTransferred(PutSnowpipeStreaming.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(PutSnowpipeStreaming.REL_SUCCESS).getFirst().assertAttributeEquals("tableName", DEFAULT_TABLE_NAME);
    }


    @Test
    public void testConcurrencyGroupLimitsWriting() throws InterruptedException {
        runner.setProperty(PutSnowpipeStreaming.DELIVERY_GUARANTEE, PutSnowpipeStreaming.DELIVERY_AT_LEAST_ONCE);
        runner.setProperty(PutSnowpipeStreaming.CONCURRENCY_GROUP, "${concurrency.group}");
        runner.setProperty(PutSnowpipeStreaming.MAX_TASKS_PER_GROUP, "1");
        runner.setProperty(PutSnowpipeStreaming.TABLE, "${tableName}");
        runner.setProperty(PutSnowpipeStreaming.SNOWPIPE_CHANNEL_PREFIX, "${tableName}");

        recordData.forEach(recordReader::addRecord);

        final MockFlowFile table1FlowFile = createFlowFile(Map.of("tableName", DEFAULT_TABLE_NAME,
            "concurrency.group", "group"));
        runner.enqueue(table1FlowFile);

        // Run the first thread, and block before finishing.
        getChannelConfiguration().blockRowInsertions();
        final Thread backgroundThread = new Thread(runner::run);
        backgroundThread.start();

        // Wait until data has been written so that we know the first thread is done pulling FlowFiles.
        waitForRecordsWritten();

        // Add a FlowFile with all records, pointing to a different table, but the same concurrency group.
        addTable("table2");
        final MockFlowFile table2FlowFile = createFlowFile(Map.of("tableName", "table2",
            "concurrency.group", "group"));
        runner.enqueue(table2FlowFile);

        // Run several iterations to make sure that we don't attempt to pull the data from the queue
        runner.run(10, false, false);
        runner.assertAllFlowFilesTransferred(PutSnowpipeStreaming.REL_SUCCESS, 0);

        getChannelConfiguration().releaseRowInsertions();
        backgroundThread.join();

        runner.assertAllFlowFilesTransferred(PutSnowpipeStreaming.REL_SUCCESS, 2);
        assertRowsEqual(recordData, DEFAULT_TABLE_NAME);
        assertRowsEqual(recordData, "table2");
    }


    @Test
    public void testExactlyOnceWithSomeDuplicates() {
        runner.setProperty(PutSnowpipeStreaming.DELIVERY_GUARANTEE, PutSnowpipeStreaming.DELIVERY_EXACTLY_ONCE);
        runner.setProperty(PutSnowpipeStreaming.RECORD_OFFSET_STRATEGY, PutSnowpipeStreaming.OFFSET_FROM_EXPRESSION_LANGUAGE);
        runner.setProperty(PutSnowpipeStreaming.RECORD_OFFSET_EL, "${" + START_OFFSET_ATTRIBUTE + "}");

        recordData.forEach(recordReader::addRecord);
        runner.enqueue(createFlowFile("0"));
        getChannelConfiguration().setLatestOffsetToken("1"); // Records 0 and 1 have already been pushed.

        runner.run();

        runner.assertAllFlowFilesTransferred(PutSnowpipeStreaming.REL_SUCCESS, 1);
        final List<MockSnowflakeStreamingIngestChannel> channels = client.getCreatedChannels();
        assertEquals(1, channels.size());

        final MockSnowflakeStreamingIngestChannel channel = channels.getFirst();
        assertTrue(channel.isClosed());
        final List<Map<String, Object>> rowsInserted = channel.getRowsInserted();
        final List<Object[]> expectedRecordData = recordData.subList(2, recordData.size());
        assertRowsEqual(expectedRecordData, rowsInserted);
    }

    @Test
    public void testAtLeastOnceWritesAllRecords() {
        runner.setProperty(PutSnowpipeStreaming.DELIVERY_GUARANTEE, PutSnowpipeStreaming.DELIVERY_AT_LEAST_ONCE);

        recordData.forEach(recordReader::addRecord);
        runner.enqueue(createFlowFile("0"));
        getChannelConfiguration().setLatestOffsetToken("1");

        runner.run();
        runner.assertAllFlowFilesTransferred(PutSnowpipeStreaming.REL_SUCCESS, 1);

        final List<MockSnowflakeStreamingIngestChannel> channels = client.getCreatedChannels();
        assertEquals(1, channels.size());
        final List<Map<String, Object>> rowsInserted = channels.getFirst().getRowsInserted();
        assertEquals(recordData.size(), rowsInserted.size());
    }

    @Test
    public void testOneTableFailsOnPublishWhileBatching() {
        runner.setProperty(PutSnowpipeStreaming.DELIVERY_GUARANTEE, PutSnowpipeStreaming.DELIVERY_AT_LEAST_ONCE);
        runner.setProperty(PutSnowpipeStreaming.TABLE, "${tableName}");

        addTable("table1");
        addTable("table2");
        addTable("table3");

        recordData.forEach(recordReader::addRecord);
        runner.enqueue(createFlowFile(Map.of("tableName", "table1")));
        runner.enqueue(createFlowFile(Map.of("tableName", "table2")));
        runner.enqueue(createFlowFile(Map.of("tableName", "table3")));

        final InsertError insertError = new InsertError(new Object(), 1);
        insertError.setExtraColNames(List.of("name"));
        insertError.setException(new SFException(ErrorCode.INVALID_DATA_IN_CHUNK));
        getChannelConfiguration("table2").setInsertErrors(List.of(insertError));

        runner.run();
        runner.assertTransferCount(PutSnowpipeStreaming.REL_SUCCESS, 1);
        runner.assertTransferCount(PutSnowpipeStreaming.REL_FAILURE, 1);

        assertRowsEqual(recordData, "table1");
        assertRowsEqual(Collections.emptyList(), "table3");
        assertEquals(1, runner.getQueueSize().getObjectCount()); // Third FlowFile should not have been consumed because second FlowFile caused error.

        runner.getFlowFilesForRelationship(PutSnowpipeStreaming.REL_FAILURE).getFirst().assertAttributeEquals("tableName", "table2");
    }

    @Test
    public void testOneTableFailsOnCloseWhileBatching() {
        runner.setProperty(PutSnowpipeStreaming.DELIVERY_GUARANTEE, PutSnowpipeStreaming.DELIVERY_AT_LEAST_ONCE);
        runner.setProperty(PutSnowpipeStreaming.TABLE, "${tableName}");

        addTable("table1");
        addTable("table2");
        addTable("table3");

        recordData.forEach(recordReader::addRecord);
        runner.enqueue(createFlowFile(Map.of("tableName", "table1")));
        runner.enqueue(createFlowFile(Map.of("tableName", "table2")));
        runner.enqueue(createFlowFile(Map.of("tableName", "table3")));

        getChannelConfiguration("table2").setFailOnClose(true);

        runner.run();
        runner.assertTransferCount(PutSnowpipeStreaming.REL_SUCCESS, 2);
        runner.assertTransferCount(PutSnowpipeStreaming.REL_FAILURE, 1);

        assertRowsEqual(recordData, "table1");
        assertRowsEqual(recordData, "table3");

        runner.getFlowFilesForRelationship(PutSnowpipeStreaming.REL_FAILURE).getFirst().assertAttributeEquals("tableName", "table2");
    }

    @Test
    public void testSingleTaskCanRetrieveFlowFilesFromMultipleClaims() {
        runner.setProperty(PutSnowpipeStreaming.DELIVERY_GUARANTEE, PutSnowpipeStreaming.DELIVERY_AT_LEAST_ONCE);
        runner.setProperty(PutSnowpipeStreaming.TABLE, "${tableName}");
        runner.setProperty(PutSnowpipeStreaming.CONCURRENCY_GROUP, "${concurrencyGroup}");
        runner.setProperty(PutSnowpipeStreaming.MAX_TASKS_PER_GROUP, "1");

        addTable("table2");

        recordData.forEach(recordReader::addRecord);
        runner.enqueue(createFlowFile(Map.of("tableName", DEFAULT_TABLE_NAME, "concurrencyGroup", "group1")));
        runner.enqueue(createFlowFile(Map.of("tableName", "table2", "concurrencyGroup", "group2")));
        runner.enqueue(createFlowFile(Map.of("tableName", "table2")));

        runner.run();
        runner.assertAllFlowFilesTransferred(PutSnowpipeStreaming.REL_SUCCESS, 3);
    }

    @Test
    public void testSingleTaskCanRetrieveMultipleBatchesWithSameConcurrencyGroup() {
        runner.setProperty(PutSnowpipeStreaming.DELIVERY_GUARANTEE, PutSnowpipeStreaming.DELIVERY_AT_LEAST_ONCE);
        runner.setProperty(PutSnowpipeStreaming.TABLE, "${tableName}");
        runner.setProperty(PutSnowpipeStreaming.CONCURRENCY_GROUP, "${concurrencyGroup}");
        runner.setProperty(PutSnowpipeStreaming.MAX_TASKS_PER_GROUP, "1");

        addTable("table2");

        recordData.forEach(recordReader::addRecord);
        runner.enqueue(createFlowFile(Map.of("tableName", DEFAULT_TABLE_NAME, "concurrencyGroup", "group1")));
        runner.enqueue(createFlowFile(Map.of("tableName", "table2", "concurrencyGroup", "group1")));
        runner.enqueue(createFlowFile(Map.of("tableName", "table2", "concurrencyGroup", "group1")));

        runner.run();
        runner.assertAllFlowFilesTransferred(PutSnowpipeStreaming.REL_SUCCESS, 3);
    }

    @Test
    void testRecordPathOffsetFunction() {
        final RecordPathRecordOffsetFunction longOffsetFunction = new RecordPathRecordOffsetFunction("/longOffset");
        final RecordPathRecordOffsetFunction stringOffsetFunction = new RecordPathRecordOffsetFunction("/stringOffset");
        final RecordPathRecordOffsetFunction wrongTypeOffsetFunction = new RecordPathRecordOffsetFunction("/wrongType");
        final RecordPathRecordOffsetFunction nonExistingFieldOffsetFunction = new RecordPathRecordOffsetFunction("/nonExistingField");

        final MockFlowFile flowFile = new MockFlowFile(1);

        final RecordField child1Field = new RecordField("longOffset", RecordFieldType.LONG.getDataType());
        final RecordField child2Field = new RecordField("stringOffset", RecordFieldType.STRING.getDataType());
        final RecordField child3Field = new RecordField("wrongType", RecordFieldType.BOOLEAN.getDataType());
        final RecordSchema rootSchema = new SimpleRecordSchema(List.of(child1Field, child2Field, child3Field));

        final MapRecord record = new MapRecord(rootSchema, Map.of("longOffset", 123, "stringOffset", "456", "wrongType", true));

        assertEquals(BigInteger.valueOf(123L), longOffsetFunction.getOffset(record, flowFile, 0));
        assertEquals(BigInteger.valueOf(456L), stringOffsetFunction.getOffset(record, flowFile, 0));
        assertThrows(IllegalArgumentException.class, () -> wrongTypeOffsetFunction.getOffset(record, flowFile, 0));
        assertEquals(BigInteger.ZERO, nonExistingFieldOffsetFunction.getOffset(record, flowFile, 0));
    }

    private void assertRowsEqual(final List<Object[]> recordValues, final String tableName) {
        final List<Map<String, Object>> insertedRows = client.getCreatedChannels().stream()
            .filter(channel -> channel.getTableName().equals(tableName))
            .flatMap(channel -> channel.getRowsInserted().stream())
            .toList();

        assertRowsEqual(recordValues, insertedRows);
    }

    private void assertRowsEqual(final List<Object[]> recordValues, final List<Map<String, Object>> insertedRows) {
        assertEquals(recordValues.size(), insertedRows.size(), "Expected " + recordValues.size() + " rows to be inserted but got " + insertedRows.size());
        for (int i = 0; i < recordValues.size(); i++) {
            final Object[] rowValues = recordValues.get(i);
            final Map<String, Object> insertedRow = insertedRows.get(i);
            assertRowsEqual(rowValues, insertedRow);
        }
    }

    private void assertRowsEqual(final Object[] recordValues, final Map<String, Object> insertedRow) {
        final String name = (String) recordValues[0];
        final String country = (String) recordValues[1];
        final double favoriteNumber = (double) recordValues[2];

        assertEquals(name, insertedRow.get("name"));
        assertEquals(country, insertedRow.get("country"));
        assertEquals(favoriteNumber, insertedRow.get("favoriteNumber"));
    }

    private void waitForRecordsWritten() throws InterruptedException {
        while (true) {
            final List<MockSnowflakeStreamingIngestChannel> channels = client.getCreatedChannels().stream()
                .filter(channel -> channel.getTableName().equals(DEFAULT_TABLE_NAME))
                .toList();

            if (channels.isEmpty()) {
                TimeUnit.MILLISECONDS.sleep(10L);
                continue;
            }

            for (final MockSnowflakeStreamingIngestChannel channel : channels) {
                if (!channel.getRowsInserted().isEmpty()) {
                    return;
                }
            }

            TimeUnit.MILLISECONDS.sleep(10L);
        }
    }

    private ChannelConfiguration getChannelConfiguration() {
        return getChannelConfiguration(DEFAULT_TABLE_NAME);
    }

    private ChannelConfiguration getChannelConfiguration(final String tableName) {
        return channelConfigurations.computeIfAbsent(tableName, k -> new ChannelConfiguration());
    }

    private MockFlowFile createFlowFile(final String startOffset) {
        return createFlowFile(Map.of(START_OFFSET_ATTRIBUTE, startOffset));
    }

    private MockFlowFile createFlowFile(final Map<String, String> attributes) {
        final MockFlowFile flowFile = new MockFlowFile(flowFileId++);
        flowFile.putAttributes(attributes);
        return flowFile;
    }


    //
    // Mocks for testing purposes
    //

    public static class MockPrivateKeyService extends AbstractControllerService implements PrivateKeyService {
        @Override
        public PrivateKey getPrivateKey() {
            return Mockito.mock(PrivateKey.class);
        }
    }

    public class MockPutSnowpipeStreaming extends PutSnowpipeStreaming {
        @Override
        protected SnowflakeStreamingIngestClient getClient(final ProcessContext context) {
            return client;
        }
    }

    private class MockSnowflakeStreamingIngestClient implements SnowflakeStreamingIngestClient {
        private volatile boolean closed = false;
        private final List<MockSnowflakeStreamingIngestChannel> createdChannels = new ArrayList<>();

        @Override
        public SnowflakeStreamingIngestChannel openChannel(final OpenChannelRequest request) {
            final ChannelConfiguration channelConfiguration = channelConfigurations.computeIfAbsent(request.getTableName(), k -> new ChannelConfiguration());
            final MockSnowflakeStreamingIngestChannel channel = new MockSnowflakeStreamingIngestChannel(request, channelConfiguration);
            createdChannels.add(channel);
            return channel;
        }

        public List<MockSnowflakeStreamingIngestChannel> getCreatedChannels() {
            return createdChannels;
        }

        @Override
        public void dropChannel(final DropChannelRequest request) {
        }

        @Override
        public String getName() {
            return getClass().getSimpleName();
        }

        @Override
        public void setRefreshToken(final String refreshToken) {
        }

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public Map<String, String> getLatestCommittedOffsetTokens(final List<SnowflakeStreamingIngestChannel> channels) {
            return Map.of();
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    @Nested
    class TestPrefixFlowFileFilter {
        private static final String PREFIX_ATTRIBUTE = "channel.prefix";

        @BeforeEach
        public void setup() {
            runner.setProperty(PutSnowpipeStreaming.TABLE, "${table.name:replaceNull('table')}");
            runner.setProperty(PutSnowpipeStreaming.SNOWPIPE_CHANNEL_PREFIX, "${" + PREFIX_ATTRIBUTE + "}");
        }

        private PrefixFlowFileFilter createFilter() {
            return createFilter(new ConcurrencyClaim());
        }

        private PrefixFlowFileFilter createFilter(final ConcurrencyClaim concurrencyClaim) {
            runner.run(0, false, true); // Initialize processor

            final PutSnowpipeStreaming processor = (PutSnowpipeStreaming) runner.getProcessor();
            return processor.createFlowFileFilter(concurrencyClaim);
        }

        @Test
        public void testOnlyRetrieveFlowFilesForSamePrefixInBatch() {
            // First FlowFile should be accepted. Any other FlowFile should be rejected as long as it doesn't have the same prefix
            final PrefixFlowFileFilter filter = createFilter();
            assertSame(FlowFileFilterResult.ACCEPT_AND_CONTINUE, filter.filter(createFlowFile(Map.of(PREFIX_ATTRIBUTE, "prefix_abc"))));
            for (int i = 0; i < 100; i++) {
                assertSame(FlowFileFilterResult.REJECT_AND_CONTINUE, filter.filter(createFlowFile(Map.of(PREFIX_ATTRIBUTE, "prefix_" + i))));
            }

            assertSame(FlowFileFilterResult.ACCEPT_AND_CONTINUE, filter.filter(createFlowFile(Map.of(PREFIX_ATTRIBUTE, "prefix_abc"))));
        }

        @Test
        public void testStopAfterFlowFileLimit() {
            final PrefixFlowFileFilter filter = createFilter();
            final MockFlowFile flowFile = createFlowFile(Map.of(PREFIX_ATTRIBUTE, "prefix"));

            for (int i = 0; i < 999; i++) {
                assertSame(FlowFileFilterResult.ACCEPT_AND_CONTINUE, filter.filter(flowFile));
            }

            assertSame(FlowFileFilterResult.ACCEPT_AND_TERMINATE, filter.filter(flowFile));
        }

        @Test
        public void testRejectsSamePrefixDifferentTable() {
            final PrefixFlowFileFilter filter = createFilter();
            assertSame(FlowFileFilterResult.ACCEPT_AND_CONTINUE, filter.filter(createFlowFile(Map.of("table.name", "table1", PREFIX_ATTRIBUTE, "prefix1"))));
            assertSame(FlowFileFilterResult.REJECT_AND_CONTINUE, filter.filter(createFlowFile(Map.of("table.name", "table2", PREFIX_ATTRIBUTE, "prefix1"))));
        }

        @Test
        public void testExactlyOnceEnsuresChannelPrefixInBatch() {
            // Stop processor so it can be reconfigured
            runner.setProperty(PutSnowpipeStreaming.DELIVERY_GUARANTEE, PutSnowpipeStreaming.DELIVERY_EXACTLY_ONCE);
            runner.setProperty(PutSnowpipeStreaming.SNOWPIPE_CHANNEL_INDEX, "${channel.index}");
            runner.setProperty(PutSnowpipeStreaming.RECORD_OFFSET_STRATEGY, PutSnowpipeStreaming.OFFSET_FROM_EXPRESSION_LANGUAGE);
            runner.setProperty(PutSnowpipeStreaming.RECORD_OFFSET_EL, "${offset}");

            final PrefixFlowFileFilter filter = createFilter();
            assertSame(FlowFileFilterResult.ACCEPT_AND_CONTINUE, filter.filter(createFlowFile(
                Map.of(PREFIX_ATTRIBUTE, "prefix_abc", "channel.index", "1", "offset", "1"))));

            assertSame(FlowFileFilterResult.REJECT_AND_CONTINUE, filter.filter(createFlowFile(
                Map.of(PREFIX_ATTRIBUTE, "prefix_abc", "channel.index", "2", "offset", "1"))));

            assertSame(FlowFileFilterResult.ACCEPT_AND_CONTINUE, filter.filter(createFlowFile(
                Map.of(PREFIX_ATTRIBUTE, "prefix_abc", "channel.index", "1", "offset", "3"))));
        }

        @Test
        public void testAbidesByConcurrencyManagerWithAtLeastDeliveryGuarantee() {
            runner.setProperty(PutSnowpipeStreaming.CONCURRENCY_GROUP, "${concurrency.group}");
            runner.setProperty(PutSnowpipeStreaming.MAX_TASKS_PER_GROUP, "3");

            for (int i = 0; i < 4; i++) {
                final PrefixFlowFileFilter filter = createFilter(new ConcurrencyClaim());
                final FlowFileFilterResult expectedResult = (i < 3) ? FlowFileFilterResult.ACCEPT_AND_CONTINUE : FlowFileFilterResult.REJECT_AND_CONTINUE;
                assertSame(expectedResult, filter.filter(createFlowFile(Map.of(PREFIX_ATTRIBUTE, "prefix_abc", "concurrency.group", "group1"))));
            }

            // A concurrency claim that has already acquired the group should be passed along properly so that it is accepted.
            final ConcurrencyClaim newClaim = new ConcurrencyClaim() {
                @Override
                public boolean isGroupAcquired(final String group) {
                    return group.equals("group1");
                }
            };
            assertSame(FlowFileFilterResult.ACCEPT_AND_CONTINUE, createFilter(newClaim).filter(
                createFlowFile(Map.of(PREFIX_ATTRIBUTE, "prefix_abc"))
            ));
        }
    }


    private class MockSnowflakeStreamingIngestChannel implements SnowflakeStreamingIngestChannel {
        private final OpenChannelRequest openRequest;
        private volatile boolean closed = false;
        private volatile CompletableFuture<Void> closeFuture = null;
        private final List<Map<String, Object>> rowsInserted = new ArrayList<>();
        private final ChannelConfiguration channelConfiguration;

        public MockSnowflakeStreamingIngestChannel(final OpenChannelRequest request, final ChannelConfiguration channelConfiguration) {
            this.openRequest = request;
            this.channelConfiguration = channelConfiguration;
        }

        @Override
        public String getFullyQualifiedName() {
            return Utils.getFullyQualifiedChannelName(openRequest.getDBName(),
                openRequest.getSchemaName(),
                openRequest.getTableName(),
                openRequest.getChannelName());
        }

        @Override
        public String getName() {
            return openRequest.getChannelName();
        }

        @Override
        public String getDBName() {
            return openRequest.getDBName();
        }

        @Override
        public String getSchemaName() {
            return openRequest.getSchemaName();
        }

        @Override
        public String getTableName() {
            return openRequest.getTableName();
        }

        @Override
        public String getFullyQualifiedTableName() {
            return openRequest.getFullyQualifiedTableName();
        }

        @Override
        public boolean isValid() {
            return true;
        }

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public CompletableFuture<Void> close() {
            closed = true;
            if (closeFuture != null) {
                return closeFuture;
            }

            final CompletableFuture<Void> future = new CompletableFuture<>();
            Thread.ofVirtual().name("Close " + getFullyQualifiedName())
                .start(() -> performClose(future));

            closeFuture = future;
            return closeFuture;
        }

        private void performClose(final CompletableFuture<Void> future) {
            if (channelConfiguration.isFailOnClose()) {
                future.completeExceptionally(new IOException("Intentional IOException for unit testing purposes"));
                return;
            }

            future.complete(null);
        }

        @Override
        public CompletableFuture<Void> close(final boolean drop) {
            return close();
        }

        @Override
        public InsertValidationResponse insertRow(final Map<String, Object> row, final String offsetToken) {
            return insertRows(List.of(row), offsetToken);
        }

        @Override
        public InsertValidationResponse insertRows(final Iterable<Map<String, Object>> rows, final String offsetToken) {
            return insertRows(rows, null, offsetToken);
        }

        @Override
        public InsertValidationResponse insertRows(final Iterable<Map<String, Object>> rows, final String startOffsetToken, final String endOffsetToken) {
            rows.forEach(rowsInserted::add);
            channelConfiguration.setLatestOffsetToken(startOffsetToken);

            final InsertValidationResponse response = new InsertValidationResponse();
            final List<InsertError> insertErrors = channelConfiguration.getInsertErrors();
            if (insertErrors != null) {
                insertErrors.forEach(response::addError);
            }

            // If channel configured to wait on write, wait until the block is released.
            channelConfiguration.waitIfNecessary();
            return response;
        }

        public List<Map<String, Object>> getRowsInserted() {
            return rowsInserted;
        }

        @Override
        public String getLatestCommittedOffsetToken() {
            return channelConfiguration.getLatestOffsetToken();
        }

        @Override
        public Map<String, ColumnProperties> getTableSchema() {
            final TableSchema tableSchema = tableSchemas.get(getTableName());
            if (tableSchema == null) {
                Assertions.fail("Table " + getTableName() + " not found");
            }

            return tableSchema.columns();
        }
    }

    // This record exists solely to avoid the complexity of generic types in the map
    private record TableSchema(Map<String, ColumnProperties> columns) {
    }

    private static class ChannelConfiguration {
        private List<InsertError> insertErrors = new ArrayList<>();
        private String latestOffsetToken;
        private boolean failOnClose = false;
        private Object objectMonitor;

        public List<InsertError> getInsertErrors() {
            return insertErrors;
        }

        public void setInsertErrors(final List<InsertError> insertErrors) {
            this.insertErrors = insertErrors;
        }

        public String getLatestOffsetToken() {
            return latestOffsetToken;
        }

        public void setLatestOffsetToken(final String latestOffsetToken) {
            this.latestOffsetToken = latestOffsetToken;
        }

        public boolean isFailOnClose() {
            return failOnClose;
        }

        public void setFailOnClose(final boolean failOnClose) {
            this.failOnClose = failOnClose;
        }

        public void blockRowInsertions() {
            this.objectMonitor = new Object();
        }

        public void releaseRowInsertions() {
            synchronized (Objects.requireNonNull(objectMonitor)) {
                objectMonitor.notifyAll();
            }
        }

        public void waitIfNecessary() {
            if (objectMonitor != null) {
                synchronized (Objects.requireNonNull(objectMonitor)) {
                    try {
                        objectMonitor.wait();
                    } catch (final InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }
}
