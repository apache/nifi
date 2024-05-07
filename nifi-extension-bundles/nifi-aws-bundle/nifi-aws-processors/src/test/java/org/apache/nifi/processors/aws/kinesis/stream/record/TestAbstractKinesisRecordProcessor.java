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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processors.aws.kinesis.stream.ConsumeKinesisStream;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TestAbstractKinesisRecordProcessor {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private final TestRunner runner = TestRunners.newTestRunner(ConsumeKinesisStream.class);

    private final ProcessSessionFactory processSessionFactory = mock(ProcessSessionFactory.class);

    private final MockProcessSession session = new MockProcessSession(new SharedSessionState(runner.getProcessor(), new AtomicLong(0)), runner.getProcessor());
    private AbstractKinesisRecordProcessor fixture;
    private final RecordProcessorCheckpointer checkpointer = mock(RecordProcessorCheckpointer.class);

    private final KinesisClientRecord kinesisRecord = mock(KinesisClientRecord.class);

    @BeforeEach
    public void setUp() {
        when(processSessionFactory.createSession()).thenReturn(session);

        // default test fixture will try operations twice with very little wait in between
        fixture = new AbstractKinesisRecordProcessor(processSessionFactory, runner.getLogger(), "kinesis-test",
                "endpoint-prefix", null, 10_000L, 1L, 2, DATE_TIME_FORMATTER) {
            @Override
            void processRecord(List<FlowFile> flowFiles, KinesisClientRecord kinesisRecord, boolean lastRecord, ProcessSession session, StopWatch stopWatch) {
                // intentionally blank
            }
        };
    }

    @AfterEach
    public void tearDown() {
        verifyNoMoreInteractions(checkpointer, kinesisRecord, processSessionFactory);
        reset(checkpointer, kinesisRecord, processSessionFactory);
    }

    @Test
    public void testInitialisation() {
        final ExtendedSequenceNumber esn = new ExtendedSequenceNumber(InitialPositionInStream.AT_TIMESTAMP.toString(), 123L);
        final InitializationInput initializationInput = InitializationInput.builder()
                .extendedSequenceNumber(esn)
                .shardId("shard-id")
                .build();

        fixture.initialize(initializationInput);

        assertTrue(fixture.getNextCheckpointTimeInMillis() > System.currentTimeMillis());
        assertEquals("shard-id", fixture.getKinesisShardId());
    }

    @Test
    public void testInitialisationWithPendingCheckpoint() {
        final ExtendedSequenceNumber esn = new ExtendedSequenceNumber(InitialPositionInStream.AT_TIMESTAMP.toString(), 123L);
        final ExtendedSequenceNumber prev = new ExtendedSequenceNumber(InitialPositionInStream.LATEST.toString(), 456L);
        final InitializationInput initializationInput = InitializationInput.builder()
                .extendedSequenceNumber(esn)
                .pendingCheckpointSequenceNumber(prev)
                .shardId("shard-id")
                .build();

        fixture.initialize(initializationInput);

        assertTrue(fixture.getNextCheckpointTimeInMillis() > System.currentTimeMillis());
        assertEquals("shard-id", fixture.getKinesisShardId());
    }

    @Test
    public void testShutdown() throws InvalidStateException, ShutdownException {
        final ShutdownRequestedInput shutdownInput = ShutdownRequestedInput.builder()
                .checkpointer(checkpointer)
                .build();

        fixture.setKinesisShardId("test-shard");
        fixture.shutdownRequested(shutdownInput);

        verify(checkpointer, times(1)).checkpoint();
    }

    @Test
    public void testShutdownWithThrottlingFailures() throws InvalidStateException, ShutdownException {
        final ShutdownRequestedInput shutdownInput = ShutdownRequestedInput.builder()
                .checkpointer(checkpointer)
                .build();

        doThrow(new ThrottlingException("throttled")).when(checkpointer).checkpoint();

        fixture.shutdownRequested(shutdownInput);

        verify(checkpointer, times(2)).checkpoint();
    }

    @Test
    public void testShutdownWithShutdownFailure() throws InvalidStateException, ShutdownException {
        final ShutdownRequestedInput shutdownInput = ShutdownRequestedInput.builder()
                .checkpointer(checkpointer)
                .build();

        doThrow(new ShutdownException("shutdown")).when(checkpointer).checkpoint();

        fixture.shutdownRequested(shutdownInput);

        verify(checkpointer, times(1)).checkpoint();
    }

    @Test
    public void testShutdownWithInvalidStateFailure() throws InvalidStateException, ShutdownException {
        final ShutdownRequestedInput shutdownInput = ShutdownRequestedInput.builder()
                .checkpointer(checkpointer)
                .build();

        doThrow(new InvalidStateException("invalid state")).when(checkpointer).checkpoint();

        fixture.shutdownRequested(shutdownInput);

        verify(checkpointer, times(1)).checkpoint();

        assertFalse(runner.getLogger().getErrorMessages().isEmpty());
    }

    @Test
    public void testShutdownTerminateRecordsNotProcessing() throws InvalidStateException, ShutdownException {
        final ShutdownRequestedInput shutdownInput = ShutdownRequestedInput.builder()
                .checkpointer(checkpointer)
                .build();

        fixture.setKinesisShardId("test-shard");
        fixture.setProcessingRecords(false);
        fixture.shutdownRequested(shutdownInput);

        verify(checkpointer, times(1)).checkpoint();

        assertTrue(runner.getLogger().getWarnMessages().isEmpty());
    }

    @Test
    public void testShutdownTerminateRecordsProcessing() throws InvalidStateException, ShutdownException {
        final ShutdownRequestedInput shutdownInput = ShutdownRequestedInput.builder()
                .checkpointer(checkpointer)
                .build();

        fixture.setKinesisShardId("test-shard");
        fixture.setProcessingRecords(true);
        fixture.shutdownRequested(shutdownInput);

        verify(checkpointer, times(1)).checkpoint();

        assertFalse(runner.getLogger().getWarnMessages().isEmpty());
    }
}
