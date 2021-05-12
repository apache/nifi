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
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processors.aws.kinesis.stream.ConsumeKinesisStream;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TestAbstractKinesisRecordProcessor {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private final TestRunner runner = TestRunners.newTestRunner(ConsumeKinesisStream.class);

    @Mock
    private ProcessSessionFactory processSessionFactory;

    private final MockProcessSession session = new MockProcessSession(new SharedSessionState(runner.getProcessor(), new AtomicLong(0)), runner.getProcessor());

    private AbstractKinesisRecordProcessor fixture;

    @Mock
    private IRecordProcessorCheckpointer checkpointer;

    @Mock
    private Record kinesisRecord;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        when(processSessionFactory.createSession()).thenReturn(session);

        // default test fixture will try operations twice with very little wait in between
        fixture = new AbstractKinesisRecordProcessor(processSessionFactory, runner.getLogger(), "kinesis-test",
                "endpoint-prefix", null, 10_000L, 1L, 2, DATE_TIME_FORMATTER) {
            @Override
            void processRecord(List<FlowFile> flowFiles, Record kinesisRecord, boolean lastRecord, ProcessSession session, StopWatch stopWatch) {
                // intentionally blank
            }
        };
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(checkpointer, kinesisRecord, processSessionFactory);
        reset(checkpointer, kinesisRecord, processSessionFactory);
    }

    @Test
    public void testInitialisation() {
        final ExtendedSequenceNumber esn = new ExtendedSequenceNumber(InitialPositionInStream.AT_TIMESTAMP.toString(), 123L);
        final InitializationInput initializationInput = new InitializationInput()
                .withExtendedSequenceNumber(esn)
                .withShardId("shard-id");

        fixture.initialize(initializationInput);

        assertThat(fixture.getNextCheckpointTimeInMillis() > System.currentTimeMillis(), is(true));
        assertThat(fixture.getKinesisShardId(), equalTo("shard-id"));

        // DEBUG messages don't have their fields replaced in the MockComponentLog
        assertThat(runner.getLogger().getDebugMessages().stream().anyMatch(logMessage -> logMessage.getMsg()
                .endsWith("Initializing record processor for stream: {} / shard: {}; from sequence number: {}")), is(true));
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

        assertThat(fixture.getNextCheckpointTimeInMillis() > System.currentTimeMillis(), is(true));
        assertThat(fixture.getKinesisShardId(), equalTo("shard-id"));

        assertThat(runner.getLogger().getWarnMessages().stream().anyMatch(logMessage -> logMessage.getMsg()
                .contains(String.format(
                        "Initializing record processor for stream: %s / shard %s; from sequence number: %s; indicates previously uncheckpointed sequence number: %s",
                        "kinesis-test", "shard-id", esn, prev
                ))), is(true));
    }

    @Test
    public void testShutdown() throws InvalidStateException, ShutdownException {
        final ShutdownInput shutdownInput = new ShutdownInput()
                .withShutdownReason(ShutdownReason.REQUESTED)
                .withCheckpointer(checkpointer);

        fixture.setKinesisShardId("test-shard");
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

        fixture.setKinesisShardId("test-shard");
        fixture.setProcessingRecords(false);
        fixture.shutdown(shutdownInput);

        verify(checkpointer, times(1)).checkpoint();

        assertThat(runner.getLogger().getDebugMessages().stream().anyMatch(logMessage -> logMessage.getMsg()
                .endsWith("Checkpointing shard test-shard")), is(true));

        // DEBUG messages don't have their fields replaced in the MockComponentLog
        assertThat(runner.getLogger().getDebugMessages().stream().anyMatch(logMessage -> logMessage.getMsg()
                .endsWith("Shutting down Record Processor for shard: {} with reason: {}")), is(true));

        // no waiting loop when records aren't processing
        assertThat(runner.getLogger().getDebugMessages().stream().anyMatch(logMessage -> logMessage.getMsg()
                .endsWith("Record Processor for shard {} still processing records, waiting before shutdown")), is(false));

        assertThat(runner.getLogger().getWarnMessages().size(), is(0));
    }

    @Test
    public void testShutdownTerminateRecordsProcessing() throws InvalidStateException, ShutdownException {
        final ShutdownInput shutdownInput = new ShutdownInput()
                .withShutdownReason(ShutdownReason.TERMINATE)
                .withCheckpointer(checkpointer);

        fixture.setKinesisShardId("test-shard");
        fixture.setProcessingRecords(true);
        fixture.shutdown(shutdownInput);

        verify(checkpointer, times(1)).checkpoint();

        assertThat(runner.getLogger().getDebugMessages().stream().anyMatch(logMessage -> logMessage.getMsg()
                .endsWith("Checkpointing shard test-shard")), is(true));

        // DEBUG messages don't have their fields replaced in the MockComponentLog
        assertThat(runner.getLogger().getDebugMessages().stream().anyMatch(logMessage -> logMessage.getMsg()
                .endsWith("Shutting down Record Processor for shard: {} with reason: {}")), is(true));

        // wait loop when records are processing
        assertThat(runner.getLogger().getDebugMessages().stream().filter(logMessage -> logMessage.getMsg()
                .endsWith("Record Processor for shard {} still processing records, waiting before shutdown"))
                .count(), is(2L));

        assertThat(runner.getLogger().getWarnMessages().stream().filter(logMessage -> logMessage.getMsg()
                .endsWith("Record Processor for shard test-shard still running, but maximum wait time elapsed, checkpoint will be attempted"))
                .count(), is(1L));
    }
}
