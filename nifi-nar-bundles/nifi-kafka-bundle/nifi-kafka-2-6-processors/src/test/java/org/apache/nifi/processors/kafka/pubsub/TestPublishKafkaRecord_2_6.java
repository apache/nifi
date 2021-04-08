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

package org.apache.nifi.processors.kafka.pubsub;

import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.kafka.pubsub.util.MockRecordParser;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.AdditionalMatchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestPublishKafkaRecord_2_6 {

    private static final String TOPIC_NAME = "unit-test";

    private PublisherPool mockPool;
    private PublisherLease mockLease;
    private TestRunner runner;

    @Before
    public void setup() throws InitializationException, IOException {
        mockPool = mock(PublisherPool.class);
        mockLease = mock(PublisherLease.class);
        Mockito.doCallRealMethod().when(mockLease).publish(any(FlowFile.class), any(RecordSet.class), any(RecordSetWriterFactory.class),
            any(RecordSchema.class), any(String.class), any(String.class), nullable(Function.class));

        when(mockPool.obtainPublisher()).thenReturn(mockLease);

        runner = TestRunners.newTestRunner(new PublishKafkaRecord_2_6() {
            @Override
            protected PublisherPool createPublisherPool(final ProcessContext context) {
                return mockPool;
            }
        });

        runner.setProperty(PublishKafkaRecord_2_6.TOPIC, TOPIC_NAME);

        final String readerId = "record-reader";
        final MockRecordParser readerService = new MockRecordParser();
        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);
        runner.addControllerService(readerId, readerService);
        runner.enableControllerService(readerService);

        final String writerId = "record-writer";
        final RecordSetWriterFactory writerService = new MockRecordWriter("name, age");
        runner.addControllerService(writerId, writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(PublishKafkaRecord_2_6.RECORD_READER, readerId);
        runner.setProperty(PublishKafkaRecord_2_6.RECORD_WRITER, writerId);
        runner.setProperty(PublishKafka_2_6.DELIVERY_GUARANTEE, PublishKafka_2_6.DELIVERY_REPLICATED);
    }

    @Test
    public void testSingleSuccess() throws IOException {
        final MockFlowFile flowFile = runner.enqueue("John Doe, 48");

        when(mockLease.complete()).thenReturn(createAllSuccessPublishResult(flowFile, 1));

        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafkaRecord_2_6.REL_SUCCESS, 1);

        verify(mockLease, times(1)).publish(any(FlowFile.class), any(RecordSet.class), any(RecordSetWriterFactory.class),
                AdditionalMatchers.or(any(RecordSchema.class), isNull()), eq(null), eq(TOPIC_NAME), nullable(Function.class));
        verify(mockLease, times(1)).complete();
        verify(mockLease, times(0)).poison();
        verify(mockLease, times(1)).close();
    }

    @Test
    public void testMultipleSuccess() throws IOException {
        final Set<FlowFile> flowFiles = new HashSet<>();
        flowFiles.add(runner.enqueue("John Doe, 48"));
        flowFiles.add(runner.enqueue("John Doe, 48"));
        flowFiles.add(runner.enqueue("John Doe, 48"));

        when(mockLease.complete()).thenReturn(createAllSuccessPublishResult(flowFiles, 1));

        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafkaRecord_2_6.REL_SUCCESS, 3);

        verify(mockLease, times(3)).publish(any(FlowFile.class), any(RecordSet.class), any(RecordSetWriterFactory.class),
                AdditionalMatchers.or(any(RecordSchema.class), isNull()), eq(null), eq(TOPIC_NAME), nullable(Function.class));
        verify(mockLease, times(1)).complete();
        verify(mockLease, times(0)).poison();
        verify(mockLease, times(1)).close();
    }

    @Test
    public void testSingleFailure() throws IOException {
        final MockFlowFile flowFile = runner.enqueue("John Doe, 48");

        when(mockLease.complete()).thenReturn(createFailurePublishResult(flowFile));

        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafkaRecord_2_6.REL_FAILURE, 1);

        verify(mockLease, times(1)).publish(any(FlowFile.class), any(RecordSet.class), any(RecordSetWriterFactory.class),
                AdditionalMatchers.or(any(RecordSchema.class), isNull()), eq(null), eq(TOPIC_NAME), nullable(Function.class));
        verify(mockLease, times(1)).complete();
        verify(mockLease, times(1)).close();
    }

    @Test
    public void testSingleFailureWithRollback() throws IOException {
        runner.setProperty(KafkaProcessorUtils.FAILURE_STRATEGY, KafkaProcessorUtils.FAILURE_STRATEGY_ROLLBACK);

        final MockFlowFile flowFile = runner.enqueue("John Doe, 48");

        when(mockLease.complete()).thenReturn(createFailurePublishResult(flowFile));

        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafkaRecord_2_6.REL_FAILURE, 0);

        verify(mockLease, times(1)).publish(any(FlowFile.class), any(RecordSet.class), any(RecordSetWriterFactory.class),
            AdditionalMatchers.or(any(RecordSchema.class), isNull()), eq(null), eq(TOPIC_NAME), nullable(Function.class));
        verify(mockLease, times(1)).close();

        assertEquals(1, runner.getQueueSize().getObjectCount());
    }


    @Test
    public void testFailureWhenCreationgTransaction() {
        runner.enqueue("John Doe, 48");

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(final InvocationOnMock invocationOnMock) {
                throw new ProducerFencedException("Intentional ProducedFencedException for unit test");
            }
        }).when(mockLease).beginTransaction();

        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafkaRecord_2_6.REL_FAILURE, 1);

        verify(mockLease, times(1)).poison();
        verify(mockLease, times(1)).close();
    }

    @Test
    public void testFailureWhenCreatingTransactionWithRollback() {
        runner.setProperty(KafkaProcessorUtils.FAILURE_STRATEGY, KafkaProcessorUtils.FAILURE_STRATEGY_ROLLBACK);
        runner.enqueue("John Doe, 48");

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(final InvocationOnMock invocationOnMock) {
                throw new ProducerFencedException("Intentional ProducedFencedException for unit test");
            }
        }).when(mockLease).beginTransaction();

        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafkaRecord_2_6.REL_FAILURE, 0);

        verify(mockLease, times(1)).poison();
        verify(mockLease, times(1)).close();
        assertEquals(1, runner.getQueueSize().getObjectCount());
    }

    @Test
    public void testMultipleFailures() throws IOException {
        final Set<FlowFile> flowFiles = new HashSet<>();
        flowFiles.add(runner.enqueue("John Doe, 48"));
        flowFiles.add(runner.enqueue("John Doe, 48"));
        flowFiles.add(runner.enqueue("John Doe, 48"));

        when(mockLease.complete()).thenReturn(createFailurePublishResult(flowFiles));

        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafkaRecord_2_6.REL_FAILURE, 3);

        verify(mockLease, times(3)).publish(any(FlowFile.class), any(RecordSet.class), any(RecordSetWriterFactory.class),
                AdditionalMatchers.or(any(RecordSchema.class), isNull()), eq(null), eq(TOPIC_NAME), nullable(Function.class));
        verify(mockLease, times(1)).complete();
        verify(mockLease, times(1)).close();
    }

    @Test
    public void testMultipleFailuresWithRollback() throws IOException {
        runner.setProperty(KafkaProcessorUtils.FAILURE_STRATEGY, KafkaProcessorUtils.FAILURE_STRATEGY_ROLLBACK);
        final Set<FlowFile> flowFiles = new HashSet<>();
        flowFiles.add(runner.enqueue("John Doe, 48"));
        flowFiles.add(runner.enqueue("John Doe, 48"));
        flowFiles.add(runner.enqueue("John Doe, 48"));

        when(mockLease.complete()).thenReturn(createFailurePublishResult(flowFiles));

        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafkaRecord_2_6.REL_FAILURE, 0);

        verify(mockLease, times(3)).publish(any(FlowFile.class), any(RecordSet.class), any(RecordSetWriterFactory.class),
            AdditionalMatchers.or(any(RecordSchema.class), isNull()), eq(null), eq(TOPIC_NAME), nullable(Function.class));
        verify(mockLease, times(1)).complete();
        verify(mockLease, times(1)).close();

        assertEquals(3, runner.getQueueSize().getObjectCount());
    }

    @Test
    public void testMultipleMessagesPerFlowFile() throws IOException {
        final List<FlowFile> flowFiles = new ArrayList<>();
        flowFiles.add(runner.enqueue("John Doe, 48\nJane Doe, 47"));
        flowFiles.add(runner.enqueue("John Doe, 48\nJane Doe, 29"));

        final Map<FlowFile, Integer> msgCounts = new HashMap<>();
        msgCounts.put(flowFiles.get(0), 10);
        msgCounts.put(flowFiles.get(1), 20);

        final PublishResult result = createPublishResult(msgCounts, new HashSet<>(flowFiles), Collections.emptyMap());

        when(mockLease.complete()).thenReturn(result);

        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafkaRecord_2_6.REL_SUCCESS, 2);

        verify(mockLease, times(2)).publish(any(FlowFile.class), any(RecordSet.class), any(RecordSetWriterFactory.class),
                AdditionalMatchers.or(any(RecordSchema.class), isNull()), eq(null), eq(TOPIC_NAME), nullable(Function.class));
        verify(mockLease, times(0)).publish(
            any(FlowFile.class), any(Map.class), eq(null), any(byte[].class), eq(TOPIC_NAME), any(InFlightMessageTracker.class), any(Integer.class));
        verify(mockLease, times(1)).complete();
        verify(mockLease, times(0)).poison();
        verify(mockLease, times(1)).close();

        runner.assertAllFlowFilesContainAttribute("msg.count");
        assertEquals(1, runner.getFlowFilesForRelationship(PublishKafkaRecord_2_6.REL_SUCCESS).stream()
            .filter(ff -> ff.getAttribute("msg.count").equals("10"))
            .count());
        assertEquals(1, runner.getFlowFilesForRelationship(PublishKafkaRecord_2_6.REL_SUCCESS).stream()
            .filter(ff -> ff.getAttribute("msg.count").equals("20"))
            .count());
    }

    @Test
    public void testNoRecordsInFlowFile() throws IOException {
        final List<FlowFile> flowFiles = new ArrayList<>();
        flowFiles.add(runner.enqueue(new byte[0]));

        final Map<FlowFile, Integer> msgCounts = new HashMap<>();
        msgCounts.put(flowFiles.get(0), 0);

        final PublishResult result = createPublishResult(msgCounts, new HashSet<>(flowFiles), Collections.emptyMap());

        when(mockLease.complete()).thenReturn(result);

        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafkaRecord_2_6.REL_SUCCESS, 1);

        verify(mockLease, times(1)).publish(any(FlowFile.class), any(RecordSet.class), any(RecordSetWriterFactory.class),
                AdditionalMatchers.or(any(RecordSchema.class), isNull()), eq(null), eq(TOPIC_NAME), nullable(Function.class));
        verify(mockLease, times(1)).complete();
        verify(mockLease, times(0)).poison();
        verify(mockLease, times(1)).close();

        final MockFlowFile mff = runner.getFlowFilesForRelationship(PublishKafkaRecord_2_6.REL_SUCCESS).get(0);
        mff.assertAttributeEquals("msg.count", "0");
    }

    @Test
    public void testRecordPathPartition() throws IOException {
        runner.setProperty(PublishKafkaRecord_2_6.PARTITION_CLASS, PublishKafkaRecord_2_6.RECORD_PATH_PARTITIONING);
        runner.setProperty(PublishKafkaRecord_2_6.PARTITION, "/age");

        final List<FlowFile> flowFiles = new ArrayList<>();
        flowFiles.add(runner.enqueue("John Doe, 48\nJane Doe, 48\nJim Doe, 13"));

        final Map<FlowFile, Integer> msgCounts = new HashMap<>();
        msgCounts.put(flowFiles.get(0), 0);

        final PublishResult result = createPublishResult(msgCounts, new HashSet<>(flowFiles), Collections.emptyMap());


        mockLease = mock(PublisherLease.class);

        when(mockLease.complete()).thenReturn(result);
        when(mockPool.obtainPublisher()).thenReturn(mockLease);

        final Map<Integer, List<Integer>> partitionsByAge = new HashMap<>();
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(final InvocationOnMock invocationOnMock) throws Throwable {
                final Function<Record, Integer> partitioner = invocationOnMock.getArgument(6, Function.class);
                final RecordSet recordSet = invocationOnMock.getArgument(1, RecordSet.class);

                Record record;
                while ((record = recordSet.next()) != null) {
                    final int partition = partitioner.apply(record);
                    final Integer age = record.getAsInt("age");

                    partitionsByAge.computeIfAbsent(age, k -> new ArrayList<>()).add(partition);
                }

                return null;
            }
        }).when(mockLease).publish(any(FlowFile.class), any(RecordSet.class), any(RecordSetWriterFactory.class),
            nullable(RecordSchema.class), nullable(String.class), any(String.class), nullable(Function.class));

        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafkaRecord_2_6.REL_SUCCESS, 1);

        verify(mockLease, times(1)).publish(any(FlowFile.class), any(RecordSet.class), any(RecordSetWriterFactory.class),
            nullable(RecordSchema.class), nullable(String.class), any(String.class), nullable(Function.class));

        assertEquals(2, partitionsByAge.size()); // 2 ages

        final List<Integer> partitionsForAge13 = partitionsByAge.get(13);
        assertEquals(1, partitionsForAge13.size());

        final List<Integer> partitionsForAge48 = partitionsByAge.get(48);
        assertEquals(2, partitionsForAge48.size());
        assertEquals(partitionsForAge48.get(0), partitionsForAge48.get(1));
    }


    @Test
    public void testSomeSuccessSomeFailure() throws IOException {
        final List<FlowFile> flowFiles = new ArrayList<>();
        flowFiles.add(runner.enqueue("John Doe, 48"));
        flowFiles.add(runner.enqueue("John Doe, 48"));
        flowFiles.add(runner.enqueue("John Doe, 48"));
        flowFiles.add(runner.enqueue("John Doe, 48"));

        final Map<FlowFile, Integer> msgCounts = new HashMap<>();
        msgCounts.put(flowFiles.get(0), 10);
        msgCounts.put(flowFiles.get(1), 20);

        final Map<FlowFile, Exception> failureMap = new HashMap<>();
        failureMap.put(flowFiles.get(2), new RuntimeException("Intentional Unit Test Exception"));
        failureMap.put(flowFiles.get(3), new RuntimeException("Intentional Unit Test Exception"));

        final PublishResult result = createPublishResult(msgCounts, new HashSet<>(flowFiles.subList(0, 2)), failureMap);

        when(mockLease.complete()).thenReturn(result);

        runner.run();
        runner.assertTransferCount(PublishKafkaRecord_2_6.REL_SUCCESS, 0);
        runner.assertTransferCount(PublishKafkaRecord_2_6.REL_FAILURE, 4);

        verify(mockLease, times(4)).publish(any(FlowFile.class), any(RecordSet.class), any(RecordSetWriterFactory.class),
                AdditionalMatchers.or(any(RecordSchema.class), isNull()), eq(null), eq(TOPIC_NAME), nullable(Function.class));
        verify(mockLease, times(1)).complete();
        verify(mockLease, times(1)).close();

        assertTrue(runner.getFlowFilesForRelationship(PublishKafkaRecord_2_6.REL_FAILURE).stream()
            .noneMatch(ff -> ff.getAttribute("msg.count") != null));
    }


    private PublishResult createAllSuccessPublishResult(final FlowFile successfulFlowFile, final int msgCount) {
        return createAllSuccessPublishResult(Collections.singleton(successfulFlowFile), msgCount);
    }

    private PublishResult createAllSuccessPublishResult(final Set<FlowFile> successfulFlowFiles, final int msgCountPerFlowFile) {
        final Map<FlowFile, Integer> msgCounts = new HashMap<>();
        for (final FlowFile ff : successfulFlowFiles) {
            msgCounts.put(ff, msgCountPerFlowFile);
        }
        return createPublishResult(msgCounts, successfulFlowFiles, Collections.emptyMap());
    }

    private PublishResult createFailurePublishResult(final FlowFile failure) {
        return createFailurePublishResult(Collections.singleton(failure));
    }

    private PublishResult createFailurePublishResult(final Set<FlowFile> failures) {
        final Map<FlowFile, Exception> failureMap = failures.stream().collect(Collectors.toMap(ff -> ff, ff -> new RuntimeException("Intentional Unit Test Exception")));
        return createPublishResult(Collections.emptyMap(), Collections.emptySet(), failureMap);
    }

    private PublishResult createPublishResult(final Map<FlowFile, Integer> msgCounts, final Set<FlowFile> successFlowFiles, final Map<FlowFile, Exception> failures) {
        // sanity check.
        for (final FlowFile success : successFlowFiles) {
            if (failures.containsKey(success)) {
                throw new IllegalArgumentException("Found same FlowFile in both 'success' and 'failures' collections: " + success);
            }
        }

        return new PublishResult() {

            @Override
            public int getSuccessfulMessageCount(FlowFile flowFile) {
                Integer count = msgCounts.get(flowFile);
                return count == null ? 0 : count.intValue();
            }

            @Override
            public Exception getReasonForFailure(FlowFile flowFile) {
                return failures.get(flowFile);
            }

            @Override
            public boolean isFailure() {
                return !failures.isEmpty();
            }
        };
    }
}
