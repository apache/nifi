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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestPublishKafka_2_0 {
    private static final String TOPIC_NAME = "unit-test";

    private PublisherPool mockPool;
    private PublisherLease mockLease;
    private TestRunner runner;

    @Before
    public void setup() {
        mockPool = mock(PublisherPool.class);
        mockLease = mock(PublisherLease.class);

        when(mockPool.obtainPublisher()).thenReturn(mockLease);

        runner = TestRunners.newTestRunner(new PublishKafka_2_0() {
            @Override
            protected PublisherPool createPublisherPool(final ProcessContext context) {
                return mockPool;
            }
        });

        runner.setProperty(PublishKafka_2_0.TOPIC, TOPIC_NAME);
        runner.setProperty(PublishKafka_2_0.DELIVERY_GUARANTEE, PublishKafka_2_0.DELIVERY_REPLICATED);
    }

    @Test
    public void testSingleSuccess() throws IOException {
        final MockFlowFile flowFile = runner.enqueue("hello world");

        when(mockLease.complete()).thenReturn(createAllSuccessPublishResult(flowFile, 1));

        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafka_2_0.REL_SUCCESS, 1);

        verify(mockLease, times(1)).publish(any(FlowFile.class), any(InputStream.class), eq(null), eq(null), eq(TOPIC_NAME));
        verify(mockLease, times(1)).complete();
        verify(mockLease, times(0)).poison();
        verify(mockLease, times(1)).close();
    }

    @Test
    public void testMultipleSuccess() throws IOException {
        final Set<FlowFile> flowFiles = new HashSet<>();
        flowFiles.add(runner.enqueue("hello world"));
        flowFiles.add(runner.enqueue("hello world"));
        flowFiles.add(runner.enqueue("hello world"));


        when(mockLease.complete()).thenReturn(createAllSuccessPublishResult(flowFiles, 1));

        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafka_2_0.REL_SUCCESS, 3);

        verify(mockLease, times(3)).publish(any(FlowFile.class), any(InputStream.class), eq(null), eq(null), eq(TOPIC_NAME));
        verify(mockLease, times(1)).complete();
        verify(mockLease, times(0)).poison();
        verify(mockLease, times(1)).close();
    }

    @Test
    public void testSingleFailure() throws IOException {
        final MockFlowFile flowFile = runner.enqueue("hello world");

        when(mockLease.complete()).thenReturn(createFailurePublishResult(flowFile));

        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafka_2_0.REL_FAILURE, 1);

        verify(mockLease, times(1)).publish(any(FlowFile.class), any(InputStream.class), eq(null), eq(null), eq(TOPIC_NAME));
        verify(mockLease, times(1)).complete();
        verify(mockLease, times(1)).close();
    }

    @Test
    public void testMultipleFailures() throws IOException {
        final Set<FlowFile> flowFiles = new HashSet<>();
        flowFiles.add(runner.enqueue("hello world"));
        flowFiles.add(runner.enqueue("hello world"));
        flowFiles.add(runner.enqueue("hello world"));

        when(mockLease.complete()).thenReturn(createFailurePublishResult(flowFiles));

        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafka_2_0.REL_FAILURE, 3);

        verify(mockLease, times(3)).publish(any(FlowFile.class), any(InputStream.class), eq(null), eq(null), eq(TOPIC_NAME));
        verify(mockLease, times(1)).complete();
        verify(mockLease, times(1)).close();
    }

    @Test
    public void testMultipleMessagesPerFlowFile() throws IOException {
        final List<FlowFile> flowFiles = new ArrayList<>();
        flowFiles.add(runner.enqueue("hello world"));
        flowFiles.add(runner.enqueue("hello world"));

        final Map<FlowFile, Integer> msgCounts = new HashMap<>();
        msgCounts.put(flowFiles.get(0), 10);
        msgCounts.put(flowFiles.get(1), 20);

        final PublishResult result = createPublishResult(msgCounts, new HashSet<>(flowFiles), Collections.emptyMap());

        when(mockLease.complete()).thenReturn(result);

        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafka_2_0.REL_SUCCESS, 2);

        verify(mockLease, times(2)).publish(any(FlowFile.class), any(InputStream.class), eq(null), eq(null), eq(TOPIC_NAME));
        verify(mockLease, times(1)).complete();
        verify(mockLease, times(0)).poison();
        verify(mockLease, times(1)).close();

        runner.assertAllFlowFilesContainAttribute("msg.count");
        assertEquals(1, runner.getFlowFilesForRelationship(PublishKafka_2_0.REL_SUCCESS).stream()
            .filter(ff -> ff.getAttribute("msg.count").equals("10"))
            .count());
        assertEquals(1, runner.getFlowFilesForRelationship(PublishKafka_2_0.REL_SUCCESS).stream()
            .filter(ff -> ff.getAttribute("msg.count").equals("20"))
            .count());
    }


    @Test
    public void testSomeSuccessSomeFailure() throws IOException {
        final List<FlowFile> flowFiles = new ArrayList<>();
        flowFiles.add(runner.enqueue("hello world"));
        flowFiles.add(runner.enqueue("hello world"));
        flowFiles.add(runner.enqueue("hello world"));
        flowFiles.add(runner.enqueue("hello world"));

        final Map<FlowFile, Integer> msgCounts = new HashMap<>();
        msgCounts.put(flowFiles.get(0), 10);
        msgCounts.put(flowFiles.get(1), 20);

        final Map<FlowFile, Exception> failureMap = new HashMap<>();
        failureMap.put(flowFiles.get(2), new RuntimeException("Intentional Unit Test Exception"));
        failureMap.put(flowFiles.get(3), new RuntimeException("Intentional Unit Test Exception"));

        final PublishResult result = createPublishResult(msgCounts, new HashSet<>(flowFiles.subList(0, 2)), failureMap);

        when(mockLease.complete()).thenReturn(result);

        runner.run();
        runner.assertTransferCount(PublishKafka_2_0.REL_SUCCESS, 0);
        runner.assertTransferCount(PublishKafka_2_0.REL_FAILURE, 4);

        verify(mockLease, times(4)).publish(any(FlowFile.class), any(InputStream.class), eq(null), eq(null), eq(TOPIC_NAME));
        verify(mockLease, times(1)).complete();
        verify(mockLease, times(1)).close();

        assertTrue(runner.getFlowFilesForRelationship(PublishKafka_2_0.REL_FAILURE).stream()
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
            public boolean isFailure() {
                return !failures.isEmpty();
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
