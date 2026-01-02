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
package org.apache.nifi.processors.aws.kinesis;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.aws.kinesis.ReaderRecordProcessor.ProcessingResult;
import org.apache.nifi.processors.aws.kinesis.converter.ValueRecordConverter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.APPROXIMATE_ARRIVAL_TIMESTAMP;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.FIRST_SEQUENCE_NUMBER;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.FIRST_SUB_SEQUENCE_NUMBER;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.LAST_SEQUENCE_NUMBER;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.LAST_SUB_SEQUENCE_NUMBER;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.MIME_TYPE;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.PARTITION_KEY;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.RECORD_COUNT;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.RECORD_ERROR_MESSAGE;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.SHARD_ID;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.STREAM_NAME;
import static org.apache.nifi.processors.aws.kinesis.JsonRecordAssert.assertFlowFileRecords;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReaderRecordProcessorTest {

    private static final String TEST_STREAM_NAME = "stream-test";
    private static final String TEST_SHARD_ID = "shardId-test";

    private static final String USER_JSON_1 = "{\"name\":\"John\",\"age\":30}";
    private static final String USER_JSON_2 = "{\"name\":\"Jane\",\"age\":25}";
    private static final String USER_JSON_3 = "{\"name\":\"Bob\",\"age\":35}";

    private static final String CITY_JSON_1 = "{\"name\":\"Seattle\",\"country\":\"US\"}";
    private static final String CITY_JSON_2 = "{\"name\":\"Warsaw\",\"country\":\"PL\"}";

    private static final String INVALID_JSON = "{invalid json}";

    private MockProcessSession session;
    private ComponentLog logger;

    private JsonRecordSetWriter jsonWriter;
    private ReaderRecordProcessor processor;

    @BeforeEach
    void setUp() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(ConsumeKinesis.class);
        final SharedSessionState sharedState = new SharedSessionState(runner.getProcessor(), new AtomicLong(0));
        session = new MockProcessSession(sharedState, runner.getProcessor());
        logger = runner.getLogger();

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("json-reader", jsonReader);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA.getValue());
        runner.enableControllerService(jsonReader);

        jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("json-writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA.getValue());
        runner.enableControllerService(jsonWriter);

        processor = new ReaderRecordProcessor(jsonReader, new ValueRecordConverter(), jsonWriter, logger);
    }

    @Test
    void testProcessSingleRecord() {
        final KinesisClientRecord record = KinesisClientRecord.builder()
                .data(ByteBuffer.wrap(USER_JSON_1.getBytes(UTF_8)))
                .sequenceNumber("1")
                .subSequenceNumber(2)
                .approximateArrivalTimestamp(Instant.now())
                .partitionKey("key-123")
                .build();
        final List<KinesisClientRecord> records = List.of(record);

        final ProcessingResult result = processor.processRecords(session, TEST_STREAM_NAME, TEST_SHARD_ID, records);

        assertEquals(1, result.successFlowFiles().size());
        assertEquals(0, result.parseFailureFlowFiles().size());

        final FlowFile successFlowFile = result.successFlowFiles().getFirst();

        assertEquals(TEST_STREAM_NAME, successFlowFile.getAttribute(STREAM_NAME));
        assertEquals(TEST_SHARD_ID, successFlowFile.getAttribute(SHARD_ID));

        assertEquals(record.sequenceNumber(), successFlowFile.getAttribute(FIRST_SEQUENCE_NUMBER));
        assertEquals(String.valueOf(record.subSequenceNumber()), successFlowFile.getAttribute(FIRST_SUB_SEQUENCE_NUMBER));
        assertEquals(record.sequenceNumber(), successFlowFile.getAttribute(LAST_SEQUENCE_NUMBER));
        assertEquals(String.valueOf(record.subSequenceNumber()), successFlowFile.getAttribute(LAST_SUB_SEQUENCE_NUMBER));

        assertEquals(record.partitionKey(), successFlowFile.getAttribute(PARTITION_KEY));
        assertEquals(String.valueOf(record.approximateArrivalTimestamp().toEpochMilli()), successFlowFile.getAttribute(APPROXIMATE_ARRIVAL_TIMESTAMP));

        assertEquals("application/json", successFlowFile.getAttribute(MIME_TYPE));
        assertEquals("1", successFlowFile.getAttribute(RECORD_COUNT));

        assertFlowFileRecords(successFlowFile, records);
    }

    @Test
    void testProcessMultipleRecordsWithSameSchema() {
        final List<KinesisClientRecord> records = List.of(
                createKinesisRecord(USER_JSON_1, "1"),
                createKinesisRecord(USER_JSON_2, "2"),
                createKinesisRecord(USER_JSON_3, "3")
        );

        final ProcessingResult result = processor.processRecords(session, TEST_STREAM_NAME, TEST_SHARD_ID, records);

        assertEquals(1, result.successFlowFiles().size());
        assertEquals(0, result.parseFailureFlowFiles().size());

        final FlowFile successFlowFile = result.successFlowFiles().getFirst();
        assertEquals(TEST_STREAM_NAME, successFlowFile.getAttribute(STREAM_NAME));
        assertEquals(TEST_SHARD_ID, successFlowFile.getAttribute(SHARD_ID));
        assertEquals("3", successFlowFile.getAttribute(RECORD_COUNT));

        assertEquals(records.getFirst().sequenceNumber(), successFlowFile.getAttribute(FIRST_SEQUENCE_NUMBER));
        assertEquals(String.valueOf(records.getFirst().subSequenceNumber()), successFlowFile.getAttribute(FIRST_SUB_SEQUENCE_NUMBER));
        assertEquals(records.getLast().sequenceNumber(), successFlowFile.getAttribute(LAST_SEQUENCE_NUMBER));
        assertEquals(String.valueOf(records.getLast().subSequenceNumber()), successFlowFile.getAttribute(LAST_SUB_SEQUENCE_NUMBER));

        assertFlowFileRecords(successFlowFile, records);
    }

    @Test
    void testEmptyRecordsList() {
        final ProcessingResult result = processor.processRecords(session, TEST_STREAM_NAME, TEST_SHARD_ID, Collections.emptyList());

        assertEquals(0, result.successFlowFiles().size());
        assertEquals(0, result.parseFailureFlowFiles().size());
    }

    @Test
    void testSchemaChangeCreatesNewFlowFile() {
        final List<KinesisClientRecord> records = List.of(
                createKinesisRecord(USER_JSON_1, "1"),
                createKinesisRecord(CITY_JSON_1, "2")
        );

        final ProcessingResult result = processor.processRecords(session, TEST_STREAM_NAME, TEST_SHARD_ID, records);

        assertEquals(2, result.successFlowFiles().size()); // Two different schemas = two FlowFiles
        assertEquals(0, result.parseFailureFlowFiles().size());

        final FlowFile firstFlowFile = result.successFlowFiles().getFirst();
        assertEquals("1", firstFlowFile.getAttribute(RECORD_COUNT));
        assertFlowFileRecords(firstFlowFile, records.getFirst());

        final FlowFile secondFlowFile = result.successFlowFiles().get(1);
        assertEquals("1", secondFlowFile.getAttribute(RECORD_COUNT));
        assertFlowFileRecords(secondFlowFile, records.get(1));
    }

    @Test
    void testSchemaChangeWithMultipleRecordsInBetween() {
        final List<KinesisClientRecord> records = List.of(
                createKinesisRecord(USER_JSON_1, "1"),
                createKinesisRecord(USER_JSON_2, "2"),
                createKinesisRecord(CITY_JSON_1, "3"),
                createKinesisRecord(CITY_JSON_2, "4")
        );

        final ProcessingResult result = processor.processRecords(session, TEST_STREAM_NAME, TEST_SHARD_ID, records);

        assertEquals(2, result.successFlowFiles().size());
        assertEquals(0, result.parseFailureFlowFiles().size());

        final FlowFile firstFlowFile = result.successFlowFiles().getFirst();
        assertEquals("2", firstFlowFile.getAttribute(RECORD_COUNT));
        assertFlowFileRecords(firstFlowFile, records.subList(0, 2));

        final FlowFile secondFlowFile = result.successFlowFiles().get(1);
        assertEquals("2", secondFlowFile.getAttribute(RECORD_COUNT));
        assertFlowFileRecords(secondFlowFile, records.subList(2, 4));
    }

    @Test
    void testSingleMalformedRecord() {
        final List<KinesisClientRecord> records = List.of(
                createKinesisRecord(INVALID_JSON, "1")
        );

        final ProcessingResult result = processor.processRecords(session, TEST_STREAM_NAME, TEST_SHARD_ID, records);

        assertEquals(0, result.successFlowFiles().size());
        assertEquals(1, result.parseFailureFlowFiles().size());

        final MockFlowFile failureFlowFile = (MockFlowFile) result.parseFailureFlowFiles().getFirst();
        assertEquals(TEST_SHARD_ID, failureFlowFile.getAttribute(SHARD_ID));
        assertEquals("1", failureFlowFile.getAttribute(FIRST_SEQUENCE_NUMBER));
        assertEquals("1", failureFlowFile.getAttribute(LAST_SEQUENCE_NUMBER));
        assertNotNull(failureFlowFile.getAttribute(RECORD_ERROR_MESSAGE));

        failureFlowFile.assertContentEquals(INVALID_JSON, UTF_8);
    }

    @Test
    void testMalformedRecordBetweenValid() {
        final List<KinesisClientRecord> records = List.of(
                createKinesisRecord(USER_JSON_1, "1"),
                createKinesisRecord(INVALID_JSON, "2"),
                createKinesisRecord(USER_JSON_2, "3"),
                createKinesisRecord(INVALID_JSON, "4"),
                createKinesisRecord(USER_JSON_3, "5")
        );

        final ProcessingResult result = processor.processRecords(session, TEST_STREAM_NAME, TEST_SHARD_ID, records);

        assertEquals(1, result.successFlowFiles().size());
        assertEquals(2, result.parseFailureFlowFiles().size());

        final FlowFile successFlowFile = result.successFlowFiles().getFirst();
        assertEquals(TEST_SHARD_ID, successFlowFile.getAttribute(SHARD_ID));
        assertEquals("3", successFlowFile.getAttribute(RECORD_COUNT));
        assertEquals("1", successFlowFile.getAttribute(FIRST_SEQUENCE_NUMBER));
        assertEquals("5", successFlowFile.getAttribute(LAST_SEQUENCE_NUMBER));
        assertFlowFileRecords(successFlowFile, records.get(0), records.get(2), records.get(4));

        assertAll(result.parseFailureFlowFiles().stream().map(
                failureFlowFile -> () -> {
                    assertNotNull(failureFlowFile.getAttribute(RECORD_ERROR_MESSAGE));
                    assertEquals(TEST_SHARD_ID, failureFlowFile.getAttribute(SHARD_ID));
                }
        ));
    }

    @Test
    void testIOExceptionDuringReaderCreation() {
        final RecordReaderFactory failingReaderFactory = new MockRecordParser() {
            @Override
            public RecordReader createRecordReader(Map<String, String> variables, InputStream in, long inputLength, ComponentLog logger) throws IOException {
                throw new IOException("Failed to create reader");
            }
        };

        final ReaderRecordProcessor processor = new ReaderRecordProcessor(failingReaderFactory, new ValueRecordConverter(), jsonWriter, logger);

        final KinesisClientRecord record = createKinesisRecord(USER_JSON_1, "1");
        final List<KinesisClientRecord> records = List.of(record);

        final ProcessingResult result = processor.processRecords(session, TEST_STREAM_NAME, TEST_SHARD_ID, records);

        assertEquals(0, result.successFlowFiles().size());
        assertEquals(1, result.parseFailureFlowFiles().size());

        final MockFlowFile failureFlowFile = (MockFlowFile) result.parseFailureFlowFiles().getFirst();
        assertTrue(failureFlowFile.getAttribute(RECORD_ERROR_MESSAGE).contains("Failed to create reader"));
        failureFlowFile.assertContentEquals(KinesisRecordPayload.extract(record), UTF_8);
    }

    @Test
    void testMalformedRecordExceptionDuringReading() {
        final ReaderRecordProcessor processor = new ReaderRecordProcessor(getMalformedRecordExceptionReader(), new ValueRecordConverter(), jsonWriter, logger);

        final KinesisClientRecord record = createKinesisRecord(USER_JSON_1, "1");
        final List<KinesisClientRecord> records = Collections.singletonList(record);

        final ProcessingResult result = processor.processRecords(session, TEST_STREAM_NAME, TEST_SHARD_ID, records);

        assertEquals(0, result.successFlowFiles().size());
        assertEquals(1, result.parseFailureFlowFiles().size());

        final MockFlowFile failureFlowFile = (MockFlowFile) result.parseFailureFlowFiles().getFirst();
        assertTrue(failureFlowFile.getAttribute(RECORD_ERROR_MESSAGE).contains("Test exception"));
        failureFlowFile.assertContentEquals(KinesisRecordPayload.extract(record), UTF_8);
    }

    @Test
    void testInvalidRecordsWithSchemaEvolution() {
        final List<KinesisClientRecord> records = List.of(
                createKinesisRecord(USER_JSON_1, "1"), // Schema A
                createKinesisRecord(USER_JSON_2, "2"), // Schema A
                createKinesisRecord(INVALID_JSON, "3"),
                createKinesisRecord(CITY_JSON_1, "4"), // Schema B
                createKinesisRecord(INVALID_JSON, "5"),
                createKinesisRecord(CITY_JSON_2, "6"), // Schema B
                createKinesisRecord("{\"category\":\"electronics\",\"price\":99.99}", "7") // Schema C
        );

        final ProcessingResult result = processor.processRecords(session, TEST_STREAM_NAME, TEST_SHARD_ID, records);

        assertEquals(3, result.successFlowFiles().size());
        assertEquals(2, result.parseFailureFlowFiles().size());

        final FlowFile firstFlowFile = result.successFlowFiles().getFirst();
        assertEquals("2", firstFlowFile.getAttribute(RECORD_COUNT));
        assertEquals("1", firstFlowFile.getAttribute(FIRST_SEQUENCE_NUMBER));
        assertEquals("2", firstFlowFile.getAttribute(LAST_SEQUENCE_NUMBER));
        assertFlowFileRecords(firstFlowFile, records.subList(0, 2));

        final FlowFile secondFlowFile = result.successFlowFiles().get(1);
        assertEquals("2", secondFlowFile.getAttribute(RECORD_COUNT));
        assertEquals("4", secondFlowFile.getAttribute(FIRST_SEQUENCE_NUMBER));
        assertEquals("6", secondFlowFile.getAttribute(LAST_SEQUENCE_NUMBER));
        assertFlowFileRecords(secondFlowFile, records.get(3), records.get(5));

        final FlowFile thirdFlowFile = result.successFlowFiles().get(2);
        assertEquals("1", thirdFlowFile.getAttribute(RECORD_COUNT));
        assertEquals("7", thirdFlowFile.getAttribute(FIRST_SEQUENCE_NUMBER));
        assertEquals("7", thirdFlowFile.getAttribute(LAST_SEQUENCE_NUMBER));
        assertFlowFileRecords(thirdFlowFile, records.get(6));

        final List<FlowFile> failureFlowFiles = result.parseFailureFlowFiles();

        final MockFlowFile firstFailureFlowFile = (MockFlowFile) failureFlowFiles.getFirst();
        assertEquals("3", firstFailureFlowFile.getAttribute(FIRST_SEQUENCE_NUMBER));
        assertEquals("3", firstFailureFlowFile.getAttribute(LAST_SEQUENCE_NUMBER));
        assertNotNull(firstFailureFlowFile.getAttribute(RECORD_ERROR_MESSAGE));
        assertEquals(TEST_SHARD_ID, firstFailureFlowFile.getAttribute(SHARD_ID));
        firstFailureFlowFile.assertContentEquals(KinesisRecordPayload.extract(records.get(2)), UTF_8);

        final MockFlowFile secondFailureFlowFile = (MockFlowFile) failureFlowFiles.get(1);
        assertEquals("5", secondFailureFlowFile.getAttribute(FIRST_SEQUENCE_NUMBER));
        assertEquals("5", secondFailureFlowFile.getAttribute(LAST_SEQUENCE_NUMBER));
        assertNotNull(secondFailureFlowFile.getAttribute(RECORD_ERROR_MESSAGE));
        assertEquals(TEST_SHARD_ID, secondFailureFlowFile.getAttribute(SHARD_ID));
        secondFailureFlowFile.assertContentEquals(KinesisRecordPayload.extract(records.get(4)), UTF_8);
    }

    private static KinesisClientRecord createKinesisRecord(final String data, final String sequenceNumber) {
        return KinesisClientRecord.builder()
                .data(ByteBuffer.wrap(data.getBytes(UTF_8)))
                .sequenceNumber(sequenceNumber)
                .partitionKey("key")
                .approximateArrivalTimestamp(Instant.now())
                .build();
    }

    private static RecordReaderFactory getMalformedRecordExceptionReader() {
        return new MockRecordParser() {
            @Override
            public RecordReader createRecordReader(Map<String, String> variables, InputStream in, long inputLength, ComponentLog logger) {
                return new RecordReader() {
                    @Override
                    public void close() {
                    }

                    @Override
                    public Record nextRecord(boolean coerceTypes, boolean dropUnknownFields) throws MalformedRecordException {
                        throw new MalformedRecordException("Test exception");
                    }

                    @Override
                    public RecordSchema getSchema() {
                        return null;
                    }
                };
            }
        };
    }
}
