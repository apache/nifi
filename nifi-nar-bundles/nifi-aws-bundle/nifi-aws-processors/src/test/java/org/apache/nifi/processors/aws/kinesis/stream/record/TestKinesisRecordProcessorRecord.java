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

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.model.Record;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TestKinesisRecordProcessorRecord {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private final TestRunner runner = TestRunners.newTestRunner(ConsumeKinesisStream.class);

    @Mock
    private ProcessSessionFactory processSessionFactory;

    private final SharedSessionState sharedState = new SharedSessionState(runner.getProcessor(), new AtomicLong(0));
    private final MockProcessSession session = new MockProcessSession(sharedState, runner.getProcessor());

    private AbstractKinesisRecordProcessor fixture;
    private final RecordReaderFactory reader = new JsonTreeReader();
    private final RecordSetWriterFactory writer = new JsonRecordSetWriter();

    @Mock
    private IRecordProcessorCheckpointer checkpointer;

    @Mock
    private Record kinesisRecord;

    @Before
    public void setUp() throws InitializationException {
        MockitoAnnotations.initMocks(this);

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

    @After
    public void tearDown() {
        verifyNoMoreInteractions(checkpointer, kinesisRecord, processSessionFactory);
        reset(checkpointer, kinesisRecord, processSessionFactory);
    }

    @Test
    public void testProcessRecordsEmpty() {
        final ProcessRecordsInput processRecordsInput = new ProcessRecordsInput()
                .withRecords(Collections.emptyList())
                .withCheckpointer(checkpointer)
                .withCacheEntryTime(Instant.now().minus(1, ChronoUnit.MINUTES))
                .withCacheExitTime(Instant.now().minus(1, ChronoUnit.SECONDS))
                .withMillisBehindLatest(100L);

        // would checkpoint (but should skip because there are no records processed)
        fixture.setNextCheckpointTimeInMillis(System.currentTimeMillis() - 10_000L);

        fixture.processRecords(processRecordsInput);

        session.assertTransferCount(ConsumeKinesisStream.REL_SUCCESS, 0);
        assertThat(sharedState.getProvenanceEvents().size(), is(0));
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

        final ProcessRecordsInput processRecordsInput = new ProcessRecordsInput()
                .withRecords(Arrays.asList(
                        new Record().withApproximateArrivalTimestamp(firstDate)
                                .withPartitionKey("partition-1")
                                .withSequenceNumber("1")
                                .withData(ByteBuffer.wrap("{\"record\":\"1\"}\n{\"record\":\"1b\"}".getBytes(StandardCharsets.UTF_8))),
                        new Record().withApproximateArrivalTimestamp(null)
                                .withPartitionKey("partition-no-date")
                                .withSequenceNumber("no-date")
                                .withData(ByteBuffer.wrap("{\"record\":\"no-date\"}".getBytes(StandardCharsets.UTF_8))),
                        new Record().withApproximateArrivalTimestamp(secondDate)
                                .withPartitionKey("partition-2")
                                .withSequenceNumber("2")
                                .withData(ByteBuffer.wrap("{\"record\":\"2\"}".getBytes(StandardCharsets.UTF_8)))
                ))
                .withCheckpointer(checkpointer)
                .withCacheEntryTime(null)
                .withCacheExitTime(null)
                .withMillisBehindLatest(null);

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
        assertThat(sharedState.getProvenanceEvents().size(), is(1));
        assertThat(sharedState.getProvenanceEvents().get(0).getTransitUri(), is(String.format("%s/another-shard", transitUriPrefix)));

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
        final ProcessRecordsInput processRecordsInput = new ProcessRecordsInput()
                .withRecords(Arrays.asList(
                        new Record().withApproximateArrivalTimestamp(null)
                                .withPartitionKey("partition-1")
                                .withSequenceNumber("1")
                                .withData(ByteBuffer.wrap("{\"record\":\"1\"}".getBytes(StandardCharsets.UTF_8))),
                        kinesisRecord,
                        new Record().withApproximateArrivalTimestamp(null)
                                .withPartitionKey("partition-3")
                                .withSequenceNumber("3")
                                .withData(ByteBuffer.wrap("{\"record\":\"3\"}".getBytes(StandardCharsets.UTF_8)))
                ))
                .withCheckpointer(checkpointer)
                .withCacheEntryTime(Instant.now().minus(1, ChronoUnit.MINUTES))
                .withCacheExitTime(Instant.now().minus(1, ChronoUnit.SECONDS))
                .withMillisBehindLatest(100L);

        when(kinesisRecord.getData()).thenThrow(new IllegalStateException("illegal state"));
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
        assertNull(verify(kinesisRecord, times(2)).getData());
        verify(checkpointer, times(1)).checkpoint();

        // ERROR messages don't have their fields replaced in the MockComponentLog
        assertThat(runner.getLogger().getErrorMessages().stream().filter(logMessage -> logMessage.getMsg()
                .endsWith("Caught Exception while processing Kinesis record {}: {}"))
                .count(), is(2L));
        assertThat(runner.getLogger().getErrorMessages().stream().anyMatch(logMessage -> logMessage.getMsg()
                .endsWith("Couldn't process Kinesis record {}, skipping.")), is(true));

        session.assertCommitted();
        session.assertNotRolledBack();
    }

    @Test
    public void testProcessUnparsableRecordWithRawOutputWithCheckpoint() throws ShutdownException, InvalidStateException {
        final ProcessRecordsInput processRecordsInput = new ProcessRecordsInput()
                .withRecords(Arrays.asList(
                        new Record().withApproximateArrivalTimestamp(null)
                                .withPartitionKey("partition-1")
                                .withSequenceNumber("1")
                                .withData(ByteBuffer.wrap("{\"record\":\"1\"}".getBytes(StandardCharsets.UTF_8))),
                        kinesisRecord,
                        new Record().withApproximateArrivalTimestamp(null)
                                .withPartitionKey("partition-3")
                                .withSequenceNumber("3")
                                .withData(ByteBuffer.wrap("{\"record\":\"3\"}".getBytes(StandardCharsets.UTF_8)))
                ))
                .withCheckpointer(checkpointer)
                .withCacheEntryTime(Instant.now().minus(1, ChronoUnit.MINUTES))
                .withCacheExitTime(Instant.now().minus(1, ChronoUnit.SECONDS))
                .withMillisBehindLatest(100L);

        when(kinesisRecord.getData()).thenReturn(ByteBuffer.wrap("invalid-json".getBytes(StandardCharsets.UTF_8)));
        when(kinesisRecord.getPartitionKey()).thenReturn("unparsable-partition");
        when(kinesisRecord.getSequenceNumber()).thenReturn("unparsable-sequence");
        when(kinesisRecord.getApproximateArrivalTimestamp()).thenReturn(null);

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
        assertNull(verify(kinesisRecord, times(1)).getPartitionKey());
        assertNull(verify(kinesisRecord, times(1)).getSequenceNumber());
        assertNull(verify(kinesisRecord, times(1)).getApproximateArrivalTimestamp());
        assertNull(verify(kinesisRecord, times(2)).getData());
        verify(checkpointer, times(1)).checkpoint();

        // ERROR messages don't have their fields replaced in the MockComponentLog
        assertThat(runner.getLogger().getErrorMessages().stream().filter(logMessage -> logMessage.getMsg()
                .endsWith("Failed to parse message from Kinesis Stream using configured Record Reader and Writer due to {}: {}"))
                .count(), is(1L));

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
