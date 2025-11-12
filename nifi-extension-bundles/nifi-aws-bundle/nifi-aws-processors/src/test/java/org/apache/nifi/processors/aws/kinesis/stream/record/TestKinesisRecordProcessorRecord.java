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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processors.aws.kinesis.property.SchemaDifferenceHandlingStrategy;
import org.apache.nifi.processors.aws.kinesis.stream.ConsumeKinesisStream;
import org.apache.nifi.processors.aws.kinesis.stream.pause.StandardRecordProcessorBlocker;
import org.apache.nifi.processors.aws.kinesis.stream.record.converter.RecordConverterIdentity;
import org.apache.nifi.processors.aws.kinesis.stream.record.converter.RecordConverterWrapper;
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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
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
import java.util.stream.Stream;

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
    private static final StandardRecordProcessorBlocker NOOP_RECORD_PROCESSOR_BLOCKER = StandardRecordProcessorBlocker.create();

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
        NOOP_RECORD_PROCESSOR_BLOCKER.unblockIndefinitely();

        runner.addControllerService("record-reader", reader);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA.getValue());
        runner.enableControllerService(reader);
        runner.setProperty(ConsumeKinesisStream.RECORD_READER, "record-reader");

        runner.addControllerService("record-writer", writer);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA.getValue());
        runner.setProperty(writer, JsonRecordSetWriter.OUTPUT_GROUPING, "output-oneline");
        runner.enableControllerService(writer);
        runner.setProperty(ConsumeKinesisStream.RECORD_WRITER, "record-writer");
    }

    private KinesisRecordProcessorRecord defaultFixtureWithStrategy(final SchemaDifferenceHandlingStrategy strategy) {
        // default test fixture will try operations twice with very little wait in between
        return new KinesisRecordProcessorRecord(processSessionFactory, runner.getLogger(), "kinesis-test",
                "endpoint-prefix", null, 10_000L, 1L, 2, DATE_TIME_FORMATTER,
                reader, writer, new RecordConverterIdentity(), NOOP_RECORD_PROCESSOR_BLOCKER, strategy);
    }

    @AfterEach
    public void tearDown() {
        verifyNoMoreInteractions(checkpointer, kinesisRecord, processSessionFactory);
        reset(checkpointer, kinesisRecord, processSessionFactory);
    }

    @ParameterizedTest
    @EnumSource(SchemaDifferenceHandlingStrategy.class)
    public void testProcessRecordsEmpty(final SchemaDifferenceHandlingStrategy strategy) {
        fixture = defaultFixtureWithStrategy(strategy);
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

    private static Stream<Arguments.ArgumentSet> testProcessRecordsArgs() {
        final List<Pair<String, Boolean>> endpointOverriden = List.of(
                Pair.of("Overriden Endpoint", true),
                Pair.of("Default Endpoint", false)
        );
        final List<Pair<String, Boolean>> usingWrapper = List.of(
                Pair.of("Using Schema Wrapper", true),
                Pair.of("Default Schema", false)
        );
        final List<Pair<String, SchemaDifferenceHandlingStrategy>> schemaChangeStrategy = Arrays.stream(SchemaDifferenceHandlingStrategy.values())
                .map(strategy -> Pair.of(strategy.name(), strategy))
                .toList();
        return endpointOverriden.stream()
                .flatMap(endpoint -> schemaChangeStrategy.stream()
                        .flatMap(strategy -> usingWrapper.stream()
                                .map(wrapper -> Arguments.argumentSet(
                                        String.format("%s, %s, %s", endpoint.getLeft(), wrapper.getLeft(), strategy.getLeft()),
                                        endpoint.getRight(),
                                        wrapper.getRight(),
                                        strategy.getRight()
                                ))));
    }

    @ParameterizedTest
    @MethodSource("testProcessRecordsArgs")
    void processMultipleRecordsAssertProvenance(final boolean endpointOverridden, final boolean useWrapper, final SchemaDifferenceHandlingStrategy strategy) {
        fixture = defaultFixtureWithStrategy(strategy);
        final Instant referenceInstant = Instant.parse("2021-01-01T00:00:00.000Z");
        final Date firstDate = Date.from(referenceInstant.minus(1, ChronoUnit.MINUTES));
        final Date secondDate = Date.from(referenceInstant);

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
        fixture = new KinesisRecordProcessorRecord(processSessionFactory, runner.getLogger(), "kinesis-test",
                "endpoint-prefix", transitUriPrefix, 10_000L, 1L, 2, DATE_TIME_FORMATTER,
                reader, writer, useWrapper ? new RecordConverterWrapper() : new RecordConverterIdentity(), NOOP_RECORD_PROCESSOR_BLOCKER, strategy);

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
        final String expectedContent = useWrapper ? expectRecordContentWrapper() : expectRecordContentIdentity();
        assertFlowFile(flowFiles.getFirst(), secondDate, "partition-2", "2", "another-shard", expectedContent, 4);
        session.assertTransferCount(ConsumeKinesisStream.REL_PARSE_FAILURE, 0);

        session.assertCommitted();
        session.assertNotRolledBack();
    }

    private String expectRecordContentIdentity() {
        return """
                {"record":"1"}
                {"record":"1b"}
                {"record":"no-date"}
                {"record":"2"}""";
    }

    private String expectRecordContentWrapper() {
        return """
                {"metadata":{"stream":"kinesis-test","shardId":"another-shard","sequenceNumber":"1","subSequenceNumber":0,\
                "shardedSequenceNumber":"100000000000000000000","partitionKey":"partition-1","approximateArrival":1609459140000},"value":{"record":"1"}}
                {"metadata":{"stream":"kinesis-test","shardId":"another-shard","sequenceNumber":"1","subSequenceNumber":0,\
                "shardedSequenceNumber":"100000000000000000000","partitionKey":"partition-1","approximateArrival":1609459140000},"value":{"record":"1b"}}
                {"metadata":{"stream":"kinesis-test","shardId":"another-shard","sequenceNumber":"no-date","subSequenceNumber":0,\
                "shardedSequenceNumber":"no-date00000000000000000000","partitionKey":"partition-no-date","approximateArrival":null},"value":{"record":"no-date"}}
                {"metadata":{"stream":"kinesis-test","shardId":"another-shard","sequenceNumber":"2","subSequenceNumber":0,\
                "shardedSequenceNumber":"200000000000000000000","partitionKey":"partition-2","approximateArrival":1609459200000},"value":{"record":"2"}}""";
    }

    @ParameterizedTest
    @EnumSource(SchemaDifferenceHandlingStrategy.class)
    public void testProcessPoisonPillRecordButNoRawOutputWithCheckpoint(final SchemaDifferenceHandlingStrategy strategy) throws ShutdownException, InvalidStateException {
        fixture = defaultFixtureWithStrategy(strategy);
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
        assertFlowFile(flowFiles.getFirst(), null, "partition-3", "3", "test-shard", "{\"record\":\"1\"}\n" +
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

    private static Stream<Arguments.ArgumentSet> unparsableRecordsLists() {
        final KinesisClientRecord unparsableRecordMock = mock(KinesisClientRecord.class);
        final KinesisClientRecord parsableRecord1 = KinesisClientRecord.builder().approximateArrivalTimestamp(null)
                .partitionKey("partition-1")
                .sequenceNumber("1")
                .data(ByteBuffer.wrap("{\"record\":\"1\"}".getBytes(StandardCharsets.UTF_8)))
                .build();
        final KinesisClientRecord parsableRecord3 = KinesisClientRecord.builder().approximateArrivalTimestamp(null)
                .partitionKey("partition-3")
                .sequenceNumber("3")
                .data(ByteBuffer.wrap("{\"record\":\"3\"}".getBytes(StandardCharsets.UTF_8)))
                .build();
        final List<List<KinesisClientRecord>> recordLists = List.of(
                Arrays.asList(
                        unparsableRecordMock,
                        parsableRecord1,
                        parsableRecord3
                ),
                Arrays.asList(
                        parsableRecord1,
                        unparsableRecordMock,
                        parsableRecord3
                ),
                Arrays.asList(
                        parsableRecord1,
                        parsableRecord3,
                        unparsableRecordMock
                )
        );
        return Arrays.stream(SchemaDifferenceHandlingStrategy.values())
                .flatMap(strategy -> recordLists.stream().map(recordList -> Pair.of(strategy, recordList)))
                .map(pair -> {
                    final SchemaDifferenceHandlingStrategy strategy = pair.getLeft();
                    final List<KinesisClientRecord> inputRecords = pair.getRight();
                    String position = "Middle";
                    if (inputRecords.getFirst() == unparsableRecordMock) {
                        position = "Beginning";
                    }
                    if (inputRecords.getLast() == unparsableRecordMock) {
                        position = "End";
                    }
                    return Arguments.argumentSet(
                            String.format("Strategy=%s, Unparsable Record Position=%s", strategy, position),
                            inputRecords,
                            unparsableRecordMock,
                            strategy
                    );
                })
                .peek(it -> {
                    parsableRecord1.data().rewind();
                    parsableRecord3.data().rewind();
                    Mockito.reset(unparsableRecordMock);
                });
    }

    @ParameterizedTest
    @MethodSource("unparsableRecordsLists")
    void testProcessUnparsableRecordWithRawOutputWithCheckpoint(
            final List<KinesisClientRecord> inputRecords,
            final KinesisClientRecord kinesisRecord,
            final SchemaDifferenceHandlingStrategy strategy) throws ShutdownException, InvalidStateException {
        fixture = defaultFixtureWithStrategy(strategy);
        final ProcessRecordsInput processRecordsInput = ProcessRecordsInput.builder()
                .records(inputRecords)
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
        assertFlowFile(flowFiles.getFirst(), null, "partition-3", "3", "test-shard", "{\"record\":\"1\"}\n" +
                "{\"record\":\"3\"}", 2);

        // check poison-pill output (as the raw data could not be retrieved)
        session.assertTransferCount(ConsumeKinesisStream.REL_PARSE_FAILURE, 1);
        final List<MockFlowFile> failureFlowFiles = session.getFlowFilesForRelationship(ConsumeKinesisStream.REL_PARSE_FAILURE);
        assertFlowFile(failureFlowFiles.getFirst(), null, "unparsable-partition", "unparsable-sequence", "test-shard", "invalid-json", 0);
        failureFlowFiles.getFirst().assertAttributeExists("record.error.message");

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

    @ParameterizedTest
    @EnumSource(SchemaDifferenceHandlingStrategy.class)
    void testProcessMultipleRecordInSingleKinesisRecord(final SchemaDifferenceHandlingStrategy strategy) throws ShutdownException, InvalidStateException {
        fixture = defaultFixtureWithStrategy(strategy);
        testFlowFileContents(List.of("[{\"record\":1},{\"record\":2},{\"record\":3}]"), List.of(EmittedFlowFile.of("{\"record\":1}\n{\"record\":2}\n{\"record\":3}", 3)));
    }

    @Nested
    class StateHandlerStrategyTest {

        private static List<String> inputJsonRecords() {
            return List.of(
                    "{\"colA\":1}", // inferred to ["colA":int]
                    "{\"colA\":%d}".formatted(Long.MAX_VALUE), // inferred to ["colA":long]
                    "{\"colA\":3}"
            );
        }

        @Test
        void testProcessIncompatibleSchemaKinesisRecordsStrategyGrouping() throws ShutdownException, InvalidStateException {
            fixture = defaultFixtureWithStrategy(SchemaDifferenceHandlingStrategy.GROUP_RECORDS);
            final List<String> inputJsonRecords = inputJsonRecords();
            testFlowFileContents(
                    inputJsonRecords,
                    List.of(
                            EmittedFlowFile.of(String.join("\n", inputJsonRecords.get(0), inputJsonRecords.get(2)), 2),
                            EmittedFlowFile.of(inputJsonRecords.get(1), 1)
                    )
            );
        }

        @Test
        void testProcessIncompatibleSchemaKinesisRecordsStrategyRolling() throws ShutdownException, InvalidStateException {
            fixture = defaultFixtureWithStrategy(SchemaDifferenceHandlingStrategy.CREATE_FLOW_FILE);
            final List<String> inputJsonRecords = inputJsonRecords();
            testFlowFileContents(
                    inputJsonRecords,
                    List.of(
                            EmittedFlowFile.of(inputJsonRecords.get(0), 1),
                            EmittedFlowFile.of(inputJsonRecords.get(1), 1),
                            EmittedFlowFile.of(inputJsonRecords.get(2), 1)
                    )
            );
        }
    }

    void testFlowFileContents(final List<String> kinesisRecordJsonContents, final List<EmittedFlowFile> emittedFlowFiles) throws ShutdownException, InvalidStateException {
        final List<KinesisClientRecord> kinesisRecords = kinesisRecordJsonContents.stream()
                .map(json -> KinesisClientRecord.builder()
                        .approximateArrivalTimestamp(null)
                        .partitionKey("partition-1")
                        .sequenceNumber("1")
                        .data(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)))
                        .build())
                .toList();
        final ProcessRecordsInput processRecordsInput = ProcessRecordsInput.builder()
                .records(kinesisRecords)
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
        session.assertTransferCount(ConsumeKinesisStream.REL_SUCCESS, emittedFlowFiles.size());
        final List<MockFlowFile> flowFiles = session.getFlowFilesForRelationship(ConsumeKinesisStream.REL_SUCCESS);
        for (int i = 0; i < emittedFlowFiles.size(); i++) {
            final EmittedFlowFile emittedFlowFile = emittedFlowFiles.get(i);
            assertFlowFile(flowFiles.get(i), null, "partition-1", "1", "test-shard", emittedFlowFile.content, emittedFlowFile.recordCount);
        }

        verify(checkpointer, times(1)).checkpoint();

        session.assertCommitted();
        session.assertNotRolledBack();
    }

    private record EmittedFlowFile(String content, int recordCount) {
        public static EmittedFlowFile of(final String content, final int recordCount) {
            return new EmittedFlowFile(content, recordCount);
        }
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
