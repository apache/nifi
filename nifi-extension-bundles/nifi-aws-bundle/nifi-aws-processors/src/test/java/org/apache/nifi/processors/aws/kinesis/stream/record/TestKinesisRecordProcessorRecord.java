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
package org.apache.nifi.processors.aws.kinesis.stream.record;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processors.aws.kinesis.stream.ConsumeKinesisStream;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TestKinesisRecordProcessorRecord {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private final TestRunner runner = TestRunners.newTestRunner(ConsumeKinesisStream.class);

    private final ProcessSessionFactory processSessionFactory = mock(ProcessSessionFactory.class);

    private final SharedSessionState sharedState = new SharedSessionState(runner.getProcessor(), new AtomicLong(0));
    private final MockProcessSession session = new MockProcessSession(sharedState, runner.getProcessor());

    private AbstractKinesisRecordProcessor fixture;
    private final RecordReaderFactory reader = new JsonTreeReader();
    private final RecordSetWriterFactory writer = new JsonRecordSetWriter();

    @Mock
    private final RecordProcessorCheckpointer checkpointer = mock(RecordProcessorCheckpointer.class);

    @Mock
    private final KinesisClientRecord kinesisRecord = mock(KinesisClientRecord.class);

    @BeforeEach
    public void setUp() throws InitializationException {
        runner.addControllerService("record-reader", reader);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA.getValue());
        runner.enableControllerService(reader);
        runner.setProperty(ConsumeKinesisStream.RECORD_READER, "record-reader");

        runner.addControllerService("record-writer", writer);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA.getValue());
        runner.setProperty(writer, "output-grouping", "output-oneline");
        runner.enableControllerService(writer);
        runner.setProperty(ConsumeKinesisStream.RECORD_WRITER, "record-writer");

        // default test fixture will try operations twice with very little wait in between
        fixture = new KinesisRecordProcessorRecord(processSessionFactory, runner.getLogger(), "kinesis-test",
                "endpoint-prefix", null, 10_000L, 1L, 2, DATE_TIME_FORMATTER,
                reader, writer);
    }

    @AfterEach
    public void tearDown() {
        verifyNoMoreInteractions(checkpointer, kinesisRecord, processSessionFactory);
        reset(checkpointer, kinesisRecord, processSessionFactory);
    }

    @Test
    public void testProcessRecordsEmpty() {
        final ProcessRecordsInput processRecordsInput = ProcessRecordsInput.builder()
                .records(Collections.emptyList())
                .checkpointer(checkpointer)
                .cacheEntryTime(Instant.now().minus(1, ChronoUnit.MINUTES))
                .cacheExitTime(Instant.now().minus(1, ChronoUnit.SECONDS))
                .millisBehindLatest(100L)
                .build();

        // would checkpoint (but should skip because there are no records processed)
        fixture.setNextCheckpointTimeInMillis(System.currentTimeMillis() - 10_000L);

        fixture.processRecords(processRecordsInput);

        session.assertTransferCount(ConsumeKinesisStream.REL_SUCCESS, 0);
        assertTrue(sharedState.getProvenanceEvents().isEmpty());
        session.assertNotCommitted();
        session.assertNotRolledBack();
    }

    @Test
    public void testProcessRecordsNoCheckpoint() {
        processMultipleRecordsAssertProvenance(false);
    }

    @Test
    public void testProcessRecordsWithEndpointOverride() {
        processMultipleRecordsAssertProvenance(true);
    }

    private void processMultipleRecordsAssertProvenance(final boolean endpointOverridden) {
        final Date firstDate = Date.from(Instant.now().minus(1, ChronoUnit.MINUTES));
        final Date secondDate = new Date();

        final ProcessRecordsInput processRecordsInput = ProcessRecordsInput.builder()
                .records(Arrays.asList(
                        KinesisClientRecord.builder().approximateArrivalTimestamp(firstDate.toInstant())
                                .partitionKey("partition-1")
                                .sequenceNumber("1")
                                .data(ByteBuffer.wrap("{\"record\":\"1\"}\n{\"record\":\"1b\"}".getBytes(StandardCharsets.UTF_8)))
                                .build(),
                        KinesisClientRecord.builder().approximateArrivalTimestamp(null)
                                .partitionKey("partition-no-date")
                                .sequenceNumber("no-date")
                                .data(ByteBuffer.wrap("{\"record\":\"no-date\"}".getBytes(StandardCharsets.UTF_8)))
                                .build(),
                        KinesisClientRecord.builder().approximateArrivalTimestamp(secondDate.toInstant())
                                .partitionKey("partition-2")
                                .sequenceNumber("2")
                                .data(ByteBuffer.wrap("{\"record\":\"2\"}".getBytes(StandardCharsets.UTF_8)))
                                .build()
                ))
                .checkpointer(checkpointer)
                .cacheEntryTime(null)
                .cacheExitTime(null)
                .millisBehindLatest(null)
                .build();

        final String transitUriPrefix = endpointOverridden ? "https://another-endpoint.com:8443" : "http://endpoint-prefix.amazonaws.com";
        if (endpointOverridden) {
            fixture = new KinesisRecordProcessorRecord(processSessionFactory, runner.getLogger(), "kinesis-test",
                    "endpoint-prefix", "https://another-endpoint.com:8443", 10_000L, 1L, 2, DATE_TIME_FORMATTER,
                    reader, writer);
        }

        // skip checkpoint
        fixture.setNextCheckpointTimeInMillis(System.currentTimeMillis() + 10_000L);
        fixture.setKinesisShardId("another-shard");

        when(processSessionFactory.createSession()).thenReturn(session);
        fixture.processRecords(processRecordsInput);
        verify(processSessionFactory, times(1)).createSession();

        session.assertTransferCount(ConsumeKinesisStream.REL_SUCCESS, 1);
        assertEquals(1, sharedState.getProvenanceEvents().size());
        assertEquals(String.format("%s/another-shard", transitUriPrefix), sharedState.getProvenanceEvents().getFirst().getTransitUri());

        final List<MockFlowFile> flowFiles = session.getFlowFilesForRelationship(ConsumeKinesisStream.REL_SUCCESS);
        // 4 records in single output file, attributes equating to that of the last record
        assertFlowFile(flowFiles.get(0), secondDate, "partition-2", "2", "another-shard", "{\"record\":\"1\"}\n" +
                "{\"record\":\"1b\"}\n" +
                "{\"record\":\"no-date\"}\n" +
                "{\"record\":\"2\"}",4);
        session.assertTransferCount(ConsumeKinesisStream.REL_PARSE_FAILURE, 0);

        session.assertCommitted();
        session.assertNotRolledBack();
    }

    @Test
    public void testProcessPoisonPillRecordButNoRawOutputWithCheckpoint() throws ShutdownException, InvalidStateException {
        final ProcessRecordsInput processRecordsInput = ProcessRecordsInput.builder()
                .records(Arrays.asList(
                        KinesisClientRecord.builder().approximateArrivalTimestamp(null)
                                .partitionKey("partition-1")
                                .sequenceNumber("1")
                                .data(ByteBuffer.wrap("{\"record\":\"1\"}".getBytes(StandardCharsets.UTF_8)))
                                .build(),
                        kinesisRecord,
                        KinesisClientRecord.builder().approximateArrivalTimestamp(null)
                                .partitionKey("partition-3")
                                .sequenceNumber("3")
                                .data(ByteBuffer.wrap("{\"record\":\"3\"}".getBytes(StandardCharsets.UTF_8)))
                                .build()
                ))
                .checkpointer(checkpointer)
                .cacheEntryTime(Instant.now().minus(1, ChronoUnit.MINUTES))
                .cacheExitTime(Instant.now().minus(1, ChronoUnit.SECONDS))
                .millisBehindLatest(100L)
                .build();

        when(kinesisRecord.data()).thenThrow(new IllegalStateException("illegal state"));
        when(kinesisRecord.toString()).thenReturn("poison-pill");

        fixture.setKinesisShardId("test-shard");

        when(processSessionFactory.createSession()).thenReturn(session);
        fixture.processRecords(processRecordsInput);
        verify(processSessionFactory, times(1)).createSession();

        // check non-poison pill records are output successfully
        session.assertTransferCount(ConsumeKinesisStream.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = session.getFlowFilesForRelationship(ConsumeKinesisStream.REL_SUCCESS);
        // 2 successful records in single output file, attributes equating to that of the last successful record
        assertFlowFile(flowFiles.get(0), null, "partition-3", "3", "test-shard", "{\"record\":\"1\"}\n" +
                "{\"record\":\"3\"}", 2);

        // check no poison-pill output (as the raw data could not be retrieved)
        session.assertTransferCount(ConsumeKinesisStream.REL_PARSE_FAILURE, 0);

        // check the "poison pill" record was retried a 2nd time
        assertNull(verify(kinesisRecord, times(2)).data());
        verify(checkpointer, times(1)).checkpoint();

        assertFalse(runner.getLogger().getErrorMessages().isEmpty());

        session.assertCommitted();
        session.assertNotRolledBack();
    }

    @Test
    public void testProcessUnparsableRecordWithRawOutputWithCheckpoint() throws ShutdownException, InvalidStateException {
        final ProcessRecordsInput processRecordsInput = ProcessRecordsInput.builder()
                .records(Arrays.asList(
                        KinesisClientRecord.builder().approximateArrivalTimestamp(null)
                                .partitionKey("partition-1")
                                .sequenceNumber("1")
                                .data(ByteBuffer.wrap("{\"record\":\"1\"}".getBytes(StandardCharsets.UTF_8)))
                                .build(),
                        kinesisRecord,
                        KinesisClientRecord.builder().approximateArrivalTimestamp(null)
                                .partitionKey("partition-3")
                                .sequenceNumber("3")
                                .data(ByteBuffer.wrap("{\"record\":\"3\"}".getBytes(StandardCharsets.UTF_8)))
                                .build()
                ))
                .checkpointer(checkpointer)
                .cacheEntryTime(Instant.now().minus(1, ChronoUnit.MINUTES))
                .cacheExitTime(Instant.now().minus(1, ChronoUnit.SECONDS))
                .millisBehindLatest(100L)
                .build();

        when(kinesisRecord.data()).thenReturn(ByteBuffer.wrap("invalid-json".getBytes(StandardCharsets.UTF_8)));
        when(kinesisRecord.partitionKey()).thenReturn("unparsable-partition");
        when(kinesisRecord.sequenceNumber()).thenReturn("unparsable-sequence");
        when(kinesisRecord.approximateArrivalTimestamp()).thenReturn(null);

        fixture.setKinesisShardId("test-shard");

        when(processSessionFactory.createSession()).thenReturn(session);
        fixture.processRecords(processRecordsInput);
        verify(processSessionFactory, times(1)).createSession();

        // check non-poison pill records are output successfully
        session.assertTransferCount(ConsumeKinesisStream.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = session.getFlowFilesForRelationship(ConsumeKinesisStream.REL_SUCCESS);
        // 2 successful records in single output file, attributes equating to that of the last successful record
        assertFlowFile(flowFiles.get(0), null, "partition-3", "3", "test-shard", "{\"record\":\"1\"}\n" +
                "{\"record\":\"3\"}", 2);

        // check poison-pill output (as the raw data could not be retrieved)
        session.assertTransferCount(ConsumeKinesisStream.REL_PARSE_FAILURE, 1);
        final List<MockFlowFile> failureFlowFiles = session.getFlowFilesForRelationship(ConsumeKinesisStream.REL_PARSE_FAILURE);
        assertFlowFile(failureFlowFiles.get(0), null, "unparsable-partition", "unparsable-sequence", "test-shard", "invalid-json", 0);
        failureFlowFiles.get(0).assertAttributeExists("record.error.message");

        // check the invalid json record was *not* retried a 2nd time
        assertNull(verify(kinesisRecord, times(1)).partitionKey());
        assertNull(verify(kinesisRecord, times(1)).sequenceNumber());
        assertNull(verify(kinesisRecord, times(1)).approximateArrivalTimestamp());
        assertNull(verify(kinesisRecord, times(1)).data());
        verify(checkpointer, times(1)).checkpoint();

        assertEquals(1, runner.getLogger().getErrorMessages().size());

        session.assertCommitted();
        session.assertNotRolledBack();
    }

    private void assertFlowFile(final MockFlowFile flowFile, final Date approxTimestamp, final String partitionKey,
                                final String sequenceNumber, final String shard, final String content, final int recordCount) {
        if (approxTimestamp != null) {
            flowFile.assertAttributeEquals(AbstractKinesisRecordProcessor.AWS_KINESIS_APPROXIMATE_ARRIVAL_TIMESTAMP,
                    DATE_TIME_FORMATTER.format(ZonedDateTime.ofInstant(approxTimestamp.toInstant(), ZoneId.systemDefault())));
        } else {
            flowFile.assertAttributeNotExists(AbstractKinesisRecordProcessor.AWS_KINESIS_APPROXIMATE_ARRIVAL_TIMESTAMP);
        }
        flowFile.assertAttributeEquals(AbstractKinesisRecordProcessor.AWS_KINESIS_PARTITION_KEY, partitionKey);
        flowFile.assertAttributeEquals(AbstractKinesisRecordProcessor.AWS_KINESIS_SEQUENCE_NUMBER, sequenceNumber);
        flowFile.assertAttributeEquals(AbstractKinesisRecordProcessor.AWS_KINESIS_SHARD_ID, shard);

        if (recordCount > 0) {
            flowFile.assertAttributeEquals("record.count", String.valueOf(recordCount));
            flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        } else {
            flowFile.assertAttributeNotExists("record.count");
            flowFile.assertAttributeNotExists(CoreAttributes.MIME_TYPE.key());
        }

        flowFile.assertContentEquals(content);
    }
}
