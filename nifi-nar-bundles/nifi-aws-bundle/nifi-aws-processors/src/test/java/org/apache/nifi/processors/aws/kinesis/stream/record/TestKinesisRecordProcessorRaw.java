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
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processors.aws.kinesis.stream.ConsumeKinesisStream;
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

public class TestKinesisRecordProcessorRaw {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private final TestRunner runner = TestRunners.newTestRunner(ConsumeKinesisStream.class);

    @Mock
    private ProcessSessionFactory processSessionFactory;

    private final SharedSessionState sharedState = new SharedSessionState(runner.getProcessor(), new AtomicLong(0));
    private final MockProcessSession session = new MockProcessSession(sharedState, runner.getProcessor());

    private AbstractKinesisRecordProcessor fixture;

    @Mock
    private IRecordProcessorCheckpointer checkpointer;

    @Mock
    private Record kinesisRecord;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        // default test fixture will try operations twice with very little wait in between
        fixture = new KinesisRecordProcessorRaw(processSessionFactory, runner.getLogger(), "kinesis-test",
                "endpoint-prefix", null, 10_000L, 1L, 2, DATE_TIME_FORMATTER);
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

        final String transitUriPrefix = endpointOverridden ? "https://another-endpoint.com:8443" : "http://endpoint-prefix.amazonaws.com";
        if (endpointOverridden) {
            fixture = new KinesisRecordProcessorRaw(processSessionFactory, runner.getLogger(), "kinesis-test",
                    "endpoint-prefix", "https://another-endpoint.com:8443", 10_000L, 1L, 2, DATE_TIME_FORMATTER);
        }

        // skip checkpoint
        fixture.setNextCheckpointTimeInMillis(System.currentTimeMillis() + 10_000L);
        fixture.setKinesisShardId("test-shard");

        when(processSessionFactory.createSession()).thenReturn(session);
        fixture.processRecords(processRecordsInput);
        verify(processSessionFactory, times(1)).createSession();

        session.assertTransferCount(ConsumeKinesisStream.REL_SUCCESS, processRecordsInput.getRecords().size());
        assertThat(sharedState.getProvenanceEvents().size(), is(processRecordsInput.getRecords().size()));
        assertThat(sharedState.getProvenanceEvents().get(0).getTransitUri(), is(String.format("%s/test-shard/partition-1#1", transitUriPrefix)));
        assertThat(sharedState.getProvenanceEvents().get(1).getTransitUri(), is(String.format("%s/test-shard/partition-2#2", transitUriPrefix)));
        assertThat(sharedState.getProvenanceEvents().get(2).getTransitUri(), is(String.format("%s/test-shard/partition-no-date#no-date", transitUriPrefix)));

        final List<MockFlowFile> flowFiles = session.getFlowFilesForRelationship(ConsumeKinesisStream.REL_SUCCESS);
        assertFlowFile(flowFiles.get(0), firstDate, "partition-1", "1", "record-1");
        assertFlowFile(flowFiles.get(1), secondDate, "partition-2", "2", "record-2");
        assertFlowFile(flowFiles.get(2), null, "partition-no-date", "no-date", "record-no-date");

        session.assertCommitted();
        session.assertNotRolledBack();
    }

    @Test
    public void testProcessPoisonPillRecordWithCheckpoint() throws ShutdownException, InvalidStateException {
        final ProcessRecordsInput processRecordsInput = new ProcessRecordsInput()
                .withRecords(Arrays.asList(
                        new Record().withApproximateArrivalTimestamp(null)
                                .withPartitionKey("partition-1")
                                .withSequenceNumber("1")
                                .withData(ByteBuffer.wrap("record-1".getBytes(StandardCharsets.UTF_8))),
                        kinesisRecord,
                        new Record().withApproximateArrivalTimestamp(null)
                                .withPartitionKey("partition-3")
                                .withSequenceNumber("3")
                                .withData(ByteBuffer.wrap("record-3".getBytes(StandardCharsets.UTF_8)))
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
        session.assertTransferCount(ConsumeKinesisStream.REL_SUCCESS, 2);
        final List<MockFlowFile> flowFiles = session.getFlowFilesForRelationship(ConsumeKinesisStream.REL_SUCCESS);
        assertFlowFile(flowFiles.get(0), null, "partition-1", "1", "record-1");
        assertFlowFile(flowFiles.get(1), null, "partition-3", "3", "record-3");

        // check the "poison pill" record was retried a 2nd time
        assertNull(verify(kinesisRecord, times(2)).getPartitionKey());
        assertNull(verify(kinesisRecord, times(2)).getSequenceNumber());
        assertNull(verify(kinesisRecord, times(2)).getApproximateArrivalTimestamp());
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

    private void assertFlowFile(final MockFlowFile flowFile, final Date approxTimestamp, final String partitionKey,
                                final String sequenceNumber, final String content) {
        if (approxTimestamp != null) {
            flowFile.assertAttributeEquals(AbstractKinesisRecordProcessor.AWS_KINESIS_APPROXIMATE_ARRIVAL_TIMESTAMP,
                    DATE_TIME_FORMATTER.format(ZonedDateTime.ofInstant(approxTimestamp.toInstant(), ZoneId.systemDefault())));
        } else {
            flowFile.assertAttributeNotExists(AbstractKinesisRecordProcessor.AWS_KINESIS_APPROXIMATE_ARRIVAL_TIMESTAMP);
        }
        flowFile.assertAttributeEquals(AbstractKinesisRecordProcessor.AWS_KINESIS_PARTITION_KEY, partitionKey);
        flowFile.assertAttributeEquals(AbstractKinesisRecordProcessor.AWS_KINESIS_SEQUENCE_NUMBER, sequenceNumber);
        flowFile.assertAttributeEquals(AbstractKinesisRecordProcessor.AWS_KINESIS_SHARD_ID, "test-shard");
        flowFile.assertContentEquals(content);
    }
}
