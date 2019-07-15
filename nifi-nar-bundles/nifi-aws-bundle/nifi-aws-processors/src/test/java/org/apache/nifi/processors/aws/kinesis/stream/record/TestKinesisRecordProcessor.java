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
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import org.apache.nifi.processors.aws.kinesis.stream.GetKinesisStream;
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TestKinesisRecordProcessor {
    private final TestRunner runner = TestRunners.newTestRunner(GetKinesisStream.class);
    private final MockProcessSession session = new MockProcessSession(new SharedSessionState(runner.getProcessor(), new AtomicLong(0)), runner.getProcessor());

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private IRecordProcessor fixture;

    @Mock
    private IRecordProcessorCheckpointer checkpointer;

    @Mock
    private Record record;

    @Before
    public void setUp() {
        // default test fixture will try operations twice with very little wait in between
        fixture = new KinesisRecordProcessor(session, runner.getLogger(), "kinesis-test", 10_000L, 1L, 2, DATE_TIME_FORMATTER);

        MockitoAnnotations.initMocks(this);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(checkpointer, record);
        reset(checkpointer, record);
    }

    @Test
    public void testInitialisation() {
        final ExtendedSequenceNumber esn = new ExtendedSequenceNumber(InitialPositionInStream.AT_TIMESTAMP.toString(), 123L);
        final InitializationInput initializationInput = new InitializationInput()
                .withExtendedSequenceNumber(esn)
                .withShardId("shard-id");

        fixture.initialize(initializationInput);

        assertThat(((KinesisRecordProcessor) fixture).nextCheckpointTimeInMillis > System.currentTimeMillis(), is(true));
        assertThat(((KinesisRecordProcessor) fixture).kinesisShardId, equalTo("shard-id"));

        // DEBUG messages don't have their fields replaced in the MockComponentLog
        assertThat(runner.getLogger().getDebugMessages().stream().anyMatch(logMessage -> logMessage.getMsg()
                .endsWith("Initializing record processor for shard: {}; from sequence number: {}")), is(true));
    }

    @Test
    public void testInitialisationWithPendingCheckpoint() {
        final ExtendedSequenceNumber esn = new ExtendedSequenceNumber(InitialPositionInStream.AT_TIMESTAMP.toString(), 123L);
        final ExtendedSequenceNumber prev = new ExtendedSequenceNumber(InitialPositionInStream.LATEST.toString(), 456L);
        final InitializationInput initializationInput = new InitializationInput()
                .withExtendedSequenceNumber(esn)
                .withPendingCheckpointSequenceNumber(prev)
                .withShardId("shard-id");

        fixture.initialize(initializationInput);

        assertThat(((KinesisRecordProcessor) fixture).nextCheckpointTimeInMillis > System.currentTimeMillis(), is(true));
        assertThat(((KinesisRecordProcessor) fixture).kinesisShardId, equalTo("shard-id"));

        assertThat(runner.getLogger().getWarnMessages().stream().anyMatch(logMessage -> logMessage.getMsg()
                .contains(String.format(
                        "Initializing record processor for shard %s; from sequence number: %s; indicates previously uncheckpointed sequence number: %s",
                        "shard-id", esn, prev
                ))), is(true));
    }

    @Test
    public void testShutdown() throws InvalidStateException, ShutdownException {
        final ShutdownInput shutdownInput = new ShutdownInput()
                .withShutdownReason(ShutdownReason.REQUESTED)
                .withCheckpointer(checkpointer);

        ((KinesisRecordProcessor) fixture).kinesisShardId = "test-shard";
        fixture.shutdown(shutdownInput);

        verify(checkpointer, times(1)).checkpoint();

        assertThat(runner.getLogger().getDebugMessages().stream().anyMatch(logMessage -> logMessage.getMsg()
                .endsWith("Checkpointing shard test-shard")), is(true));
    }

    @Test
    public void testShutdownWithThrottlingFailures() throws InvalidStateException, ShutdownException {
        final ShutdownInput shutdownInput = new ShutdownInput()
                .withShutdownReason(ShutdownReason.REQUESTED)
                .withCheckpointer(checkpointer);

        doThrow(new ThrottlingException("throttled")).when(checkpointer).checkpoint();

        fixture.shutdown(shutdownInput);

        verify(checkpointer, times(2)).checkpoint();

        assertThat(runner.getLogger().getWarnMessages().stream().anyMatch(logMessage -> logMessage.getMsg()
                .endsWith(String.format(
                        "Transient issue when checkpointing - attempt %d of %d: %s: %s",
                        1, 2, ThrottlingException.class.getName(), "throttled"
                ))), is(true));

        // ERROR messages don't have their fields replaced in the MockComponentLog
        assertThat(runner.getLogger().getErrorMessages().stream().anyMatch(logMessage -> logMessage.getMsg()
                .endsWith("Checkpoint failed after {} attempts.: {}")), is(true));
    }

    @Test
    public void testShutdownWithShutdownFailure() throws InvalidStateException, ShutdownException {
        final ShutdownInput shutdownInput = new ShutdownInput()
                .withShutdownReason(ShutdownReason.REQUESTED)
                .withCheckpointer(checkpointer);

        doThrow(new ShutdownException("shutdown")).when(checkpointer).checkpoint();

        fixture.shutdown(shutdownInput);

        verify(checkpointer, times(1)).checkpoint();

        assertThat(runner.getLogger().getInfoMessages().stream().anyMatch(logMessage -> logMessage.getMsg()
                .endsWith("Caught shutdown exception, skipping checkpoint.")), is(true));
    }

    @Test
    public void testShutdownWithInvalidStateFailure() throws InvalidStateException, ShutdownException {
        final ShutdownInput shutdownInput = new ShutdownInput()
                .withShutdownReason(ShutdownReason.ZOMBIE)
                .withCheckpointer(checkpointer);

        doThrow(new InvalidStateException("invalid state")).when(checkpointer).checkpoint();

        fixture.shutdown(shutdownInput);

        verify(checkpointer, times(1)).checkpoint();

        assertThat(runner.getLogger().getErrorMessages().stream().anyMatch(logMessage -> logMessage.getMsg()
                .endsWith("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.")), is(true));
    }

    @Test
    public void testShutdownTerminateRecordsNotProcessing() throws InvalidStateException, ShutdownException {
        final ShutdownInput shutdownInput = new ShutdownInput()
                .withShutdownReason(ShutdownReason.TERMINATE)
                .withCheckpointer(checkpointer);

        ((KinesisRecordProcessor) fixture).kinesisShardId = "test-shard";
        ((KinesisRecordProcessor) fixture).processingRecords = false;
        fixture.shutdown(shutdownInput);

        verify(checkpointer, times(1)).checkpoint();

        assertThat(runner.getLogger().getDebugMessages().stream().anyMatch(logMessage -> logMessage.getMsg()
                .endsWith("Checkpointing shard test-shard")), is(true));

        // DEBUG messages don't have their fields replaced in the MockComponentLog
        assertThat(runner.getLogger().getDebugMessages().stream().anyMatch(logMessage -> logMessage.getMsg()
                .endsWith("Shutting down record processor for shard: {} with reason: {}")), is(true));

        // no waiting loop when records aren't processing
        assertThat(runner.getLogger().getDebugMessages().stream().anyMatch(logMessage -> logMessage.getMsg()
                .endsWith("Record Processor for shard {} still processing records, waiting before shutdown")), is(false));
    }

    @Test
    public void testShutdownTerminateRecordsProcessing() throws InvalidStateException, ShutdownException {
        final ShutdownInput shutdownInput = new ShutdownInput()
                .withShutdownReason(ShutdownReason.TERMINATE)
                .withCheckpointer(checkpointer);

        ((KinesisRecordProcessor) fixture).kinesisShardId = "test-shard";
        ((KinesisRecordProcessor) fixture).processingRecords = true;
        fixture.shutdown(shutdownInput);

        verify(checkpointer, times(1)).checkpoint();

        assertThat(runner.getLogger().getDebugMessages().stream().anyMatch(logMessage -> logMessage.getMsg()
                .endsWith("Checkpointing shard test-shard")), is(true));

        // DEBUG messages don't have their fields replaced in the MockComponentLog
        assertThat(runner.getLogger().getDebugMessages().stream().anyMatch(logMessage -> logMessage.getMsg()
                .endsWith("Shutting down record processor for shard: {} with reason: {}")), is(true));

        // wait loop when records are processing
        assertThat(runner.getLogger().getDebugMessages().stream().filter(logMessage -> logMessage.getMsg()
                .endsWith("Record Processor for shard {} still processing records, waiting before shutdown"))
                .count(), is(2L));
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
        ((KinesisRecordProcessor) fixture).nextCheckpointTimeInMillis = System.currentTimeMillis() - 10_000L;

        fixture.processRecords(processRecordsInput);

        session.assertTransferCount(GetKinesisStream.REL_SUCCESS, 0);
        session.assertNotCommitted();
        session.assertNotRolledBack();

        // DEBUG messages don't have their fields replaced in the MockComponentLog
        assertThat(runner.getLogger().getDebugMessages().stream().anyMatch(logMessage -> logMessage.getMsg()
                .endsWith("Processing {} records from {}; cache entry: {}; cache exit: {}; millis behind latest: {}")), is(true));
    }

    @Test
    public void testProcessRecordsNoCheckpoint() {
        final Date firstDate = Date.from(Instant.now().minus(1, ChronoUnit.MINUTES));
        final Date secondDate = new Date();

        final ProcessRecordsInput processRecordsInput = new ProcessRecordsInput()
                .withRecords(Arrays.asList(
                        new Record().withApproximateArrivalTimestamp(firstDate)
                                .withPartitionKey("partition-1")
                                .withSequenceNumber("1")
                                .withData(ByteBuffer.wrap("record-1".getBytes(StandardCharsets.UTF_8))),
                        new Record().withApproximateArrivalTimestamp(secondDate)
                                .withPartitionKey("partition-2")
                                .withSequenceNumber("2")
                                .withData(ByteBuffer.wrap("record-2".getBytes(StandardCharsets.UTF_8))),
                        new Record().withApproximateArrivalTimestamp(null)
                                .withPartitionKey("partition-no-date")
                                .withSequenceNumber("no-date")
                                .withData(ByteBuffer.wrap("record-no-date".getBytes(StandardCharsets.UTF_8)))
                ))
                .withCheckpointer(checkpointer)
                .withCacheEntryTime(null)
                .withCacheExitTime(null)
                .withMillisBehindLatest(null);

        // skip checkpoint
        ((KinesisRecordProcessor) fixture).nextCheckpointTimeInMillis = System.currentTimeMillis() + 10_000L;
        ((KinesisRecordProcessor) fixture).kinesisShardId = "test-shard";
        fixture.processRecords(processRecordsInput);

        session.assertTransferCount(GetKinesisStream.REL_SUCCESS, processRecordsInput.getRecords().size());
        final List<MockFlowFile> flowFiles = session.getFlowFilesForRelationship(GetKinesisStream.REL_SUCCESS);
        assertFlowFile(flowFiles.get(0), firstDate, "partition-1", "1", "record-1");
        assertFlowFile(flowFiles.get(1), secondDate, "partition-2", "2", "record-2");
        assertFlowFile(flowFiles.get(2), null, "partition-no-date", "no-date", "record-no-date");

        session.assertCommitted();
        session.assertNotRolledBack();
    }

    @Test
    public void testProcessPoisonPillRecordWithCheckpoint() {
        final ProcessRecordsInput processRecordsInput = new ProcessRecordsInput()
                .withRecords(Arrays.asList(
                        new Record().withApproximateArrivalTimestamp(null)
                                .withPartitionKey("partition-1")
                                .withSequenceNumber("1")
                                .withData(ByteBuffer.wrap("record-1".getBytes(StandardCharsets.UTF_8))),
                        record,
                        new Record().withApproximateArrivalTimestamp(null)
                                .withPartitionKey("partition-3")
                                .withSequenceNumber("3")
                                .withData(ByteBuffer.wrap("record-3".getBytes(StandardCharsets.UTF_8)))
                ))
                .withCheckpointer(checkpointer)
                .withCacheEntryTime(Instant.now().minus(1, ChronoUnit.MINUTES))
                .withCacheExitTime(Instant.now().minus(1, ChronoUnit.SECONDS))
                .withMillisBehindLatest(100L);

        when(record.getData()).thenThrow(new IllegalStateException("illegal state"));
        when(record.toString()).thenReturn("poison-pill");

        // skip checkpoint
        ((KinesisRecordProcessor) fixture).nextCheckpointTimeInMillis = System.currentTimeMillis() + 10_000L;
        ((KinesisRecordProcessor) fixture).kinesisShardId = "test-shard";
        fixture.processRecords(processRecordsInput);

        // check non-poison pill records are output successfully
        session.assertTransferCount(GetKinesisStream.REL_SUCCESS, 2);
        final List<MockFlowFile> flowFiles = session.getFlowFilesForRelationship(GetKinesisStream.REL_SUCCESS);
        assertFlowFile(flowFiles.get(0), null, "partition-1", "1", "record-1");
        assertFlowFile(flowFiles.get(1), null, "partition-3", "3", "record-3");

        // check the "poison pill" record was retried a 2nd time
        assertNull(verify(record, times(2)).getPartitionKey());
        assertNull(verify(record, times(2)).getSequenceNumber());
        assertNull(verify(record, times(2)).getApproximateArrivalTimestamp());
        assertNull(verify(record, times(2)).getData());

        // ERROR messages don't have their fields replaced in the MockComponentLog
        assertThat(runner.getLogger().getErrorMessages().stream().filter(logMessage -> logMessage.getMsg()
                .endsWith("Caught Exception while processing record {}: {}"))
                .count(), is(2L));
        assertThat(runner.getLogger().getErrorMessages().stream().anyMatch(logMessage -> logMessage.getMsg()
                .endsWith("Couldn't process record {}. Skipping the record.")), is(true));

        session.assertCommitted();
        session.assertNotRolledBack();
    }

    private void assertFlowFile(final MockFlowFile flowFile, final Date approxTimestamp, final String partitionKey,
                                final String sequenceNumber, final String content) {
        if (approxTimestamp != null) {
            flowFile.assertAttributeEquals(KinesisRecordProcessor.AWS_KINESIS_APPROXIMATE_ARRIVAL_TIMESTAMP,
                    DATE_TIME_FORMATTER.format(ZonedDateTime.ofInstant(approxTimestamp.toInstant(), ZoneId.systemDefault())));
        } else {
            flowFile.assertAttributeNotExists(KinesisRecordProcessor.AWS_KINESIS_APPROXIMATE_ARRIVAL_TIMESTAMP);
        }
        flowFile.assertAttributeEquals(KinesisRecordProcessor.AWS_KINESIS_PARTITION_KEY, partitionKey);
        flowFile.assertAttributeEquals(KinesisRecordProcessor.AWS_KINESIS_SEQUENCE_NUMBER, sequenceNumber);
        flowFile.assertAttributeEquals(KinesisRecordProcessor.AWS_KINESIS_SHARD_ID, "test-shard");
        flowFile.assertContentEquals(content);
    }
}
