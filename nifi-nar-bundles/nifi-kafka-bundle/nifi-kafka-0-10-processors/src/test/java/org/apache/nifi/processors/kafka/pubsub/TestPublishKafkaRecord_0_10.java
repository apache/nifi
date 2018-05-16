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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.kafka.pubsub.util.MockRecordParser;
import org.apache.nifi.processors.kafka.pubsub.util.MockRecordWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestPublishKafkaRecord_0_10 {

    private static final String TOPIC_NAME = "unit-test";

    private PublisherPool mockPool;
    private PublisherLease mockLease;
    private TestRunner runner;

    @Before
    public void setup() throws InitializationException, IOException {
        mockPool = mock(PublisherPool.class);
        mockLease = mock(PublisherLease.class);
        Mockito.doCallRealMethod().when(mockLease).publish(any(FlowFile.class), any(RecordSet.class), any(RecordSetWriterFactory.class),
            any(RecordSchema.class), any(String.class), any(String.class));

        when(mockPool.obtainPublisher()).thenReturn(mockLease);

        runner = TestRunners.newTestRunner(new PublishKafkaRecord_0_10() {
            @Override
            protected PublisherPool createPublisherPool(final ProcessContext context) {
                return mockPool;
            }
        });

        runner.setProperty(PublishKafkaRecord_0_10.TOPIC, TOPIC_NAME);

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

        runner.setProperty(PublishKafkaRecord_0_10.RECORD_READER, readerId);
        runner.setProperty(PublishKafkaRecord_0_10.RECORD_WRITER, writerId);
    }

    @Test
    public void testSingleSuccess() throws IOException {
        final MockFlowFile flowFile = runner.enqueue("John Doe, 48");

        when(mockLease.complete()).thenReturn(createAllSuccessPublishResult(flowFile, 1));

        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafkaRecord_0_10.REL_SUCCESS, 1);

        verify(mockLease, times(1)).publish(any(FlowFile.class), any(RecordSet.class), any(RecordSetWriterFactory.class), any(RecordSchema.class), eq(null), eq(TOPIC_NAME));
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
        runner.assertAllFlowFilesTransferred(PublishKafkaRecord_0_10.REL_SUCCESS, 3);

        verify(mockLease, times(3)).publish(any(FlowFile.class), any(RecordSet.class), any(RecordSetWriterFactory.class), any(RecordSchema.class), eq(null), eq(TOPIC_NAME));
        verify(mockLease, times(1)).complete();
        verify(mockLease, times(0)).poison();
        verify(mockLease, times(1)).close();
    }

    @Test
    public void testSingleFailure() throws IOException {
        final MockFlowFile flowFile = runner.enqueue("John Doe, 48");

        when(mockLease.complete()).thenReturn(createFailurePublishResult(flowFile));

        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafkaRecord_0_10.REL_FAILURE, 1);

        verify(mockLease, times(1)).publish(any(FlowFile.class), any(RecordSet.class), any(RecordSetWriterFactory.class), any(RecordSchema.class), eq(null), eq(TOPIC_NAME));
        verify(mockLease, times(1)).complete();
        verify(mockLease, times(1)).close();
    }

    @Test
    public void testMultipleFailures() throws IOException {
        final Set<FlowFile> flowFiles = new HashSet<>();
        flowFiles.add(runner.enqueue("John Doe, 48"));
        flowFiles.add(runner.enqueue("John Doe, 48"));
        flowFiles.add(runner.enqueue("John Doe, 48"));

        when(mockLease.complete()).thenReturn(createFailurePublishResult(flowFiles));

        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafkaRecord_0_10.REL_FAILURE, 3);

        verify(mockLease, times(3)).publish(any(FlowFile.class), any(RecordSet.class), any(RecordSetWriterFactory.class), any(RecordSchema.class), eq(null), eq(TOPIC_NAME));
        verify(mockLease, times(1)).complete();
        verify(mockLease, times(1)).close();
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
        runner.assertAllFlowFilesTransferred(PublishKafkaRecord_0_10.REL_SUCCESS, 2);

        verify(mockLease, times(2)).publish(any(FlowFile.class), any(RecordSet.class), any(RecordSetWriterFactory.class), any(RecordSchema.class), eq(null), eq(TOPIC_NAME));
        verify(mockLease, times(4)).publish(any(FlowFile.class), eq(null), any(byte[].class), eq(TOPIC_NAME), any(InFlightMessageTracker.class));
        verify(mockLease, times(1)).complete();
        verify(mockLease, times(0)).poison();
        verify(mockLease, times(1)).close();

        runner.assertAllFlowFilesContainAttribute("msg.count");
        assertEquals(1, runner.getFlowFilesForRelationship(PublishKafkaRecord_0_10.REL_SUCCESS).stream()
            .filter(ff -> ff.getAttribute("msg.count").equals("10"))
            .count());
        assertEquals(1, runner.getFlowFilesForRelationship(PublishKafkaRecord_0_10.REL_SUCCESS).stream()
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
        runner.assertAllFlowFilesTransferred(PublishKafkaRecord_0_10.REL_SUCCESS, 1);

        verify(mockLease, times(1)).publish(any(FlowFile.class), any(RecordSet.class), any(RecordSetWriterFactory.class), any(RecordSchema.class), eq(null), eq(TOPIC_NAME));
        verify(mockLease, times(1)).complete();
        verify(mockLease, times(0)).poison();
        verify(mockLease, times(1)).close();

        final MockFlowFile mff = runner.getFlowFilesForRelationship(PublishKafkaRecord_0_10.REL_SUCCESS).get(0);
        mff.assertAttributeEquals("msg.count", "0");
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
        runner.assertTransferCount(PublishKafkaRecord_0_10.REL_SUCCESS, 2);
        runner.assertTransferCount(PublishKafkaRecord_0_10.REL_FAILURE, 2);

        verify(mockLease, times(4)).publish(any(FlowFile.class), any(RecordSet.class), any(RecordSetWriterFactory.class), any(RecordSchema.class), eq(null), eq(TOPIC_NAME));
        verify(mockLease, times(1)).complete();
        verify(mockLease, times(1)).close();

        assertEquals(1, runner.getFlowFilesForRelationship(PublishKafkaRecord_0_10.REL_SUCCESS).stream()
            .filter(ff -> "10".equals(ff.getAttribute("msg.count")))
            .count());
        assertEquals(1, runner.getFlowFilesForRelationship(PublishKafkaRecord_0_10.REL_SUCCESS).stream()
            .filter(ff -> "20".equals(ff.getAttribute("msg.count")))
            .count());

        assertTrue(runner.getFlowFilesForRelationship(PublishKafkaRecord_0_10.REL_FAILURE).stream()
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
            public Collection<FlowFile> getSuccessfulFlowFiles() {
                return successFlowFiles;
            }

            @Override
            public Collection<FlowFile> getFailedFlowFiles() {
                return failures.keySet();
            }

            @Override
            public int getSuccessfulMessageCount(FlowFile flowFile) {
                Integer count = msgCounts.get(flowFile);
                return count == null ? 0 : count.intValue();
            }

            @Override
            public Exception getReasonForFailure(FlowFile flowFile) {
                return failures.get(flowFile);
            }
        };
    }
}
