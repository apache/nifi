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
package org.apache.nifi.controller.repository.metrics;

import org.apache.nifi.controller.metrics.ComponentMetricContext;
import org.apache.nifi.controller.metrics.ProcessSessionEvent;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProcessSessionEventBuilderTest {

    private static final ComponentMetricContext COMPONENT_CONTEXT = new ComponentMetricContext(
            "component-1",
            "Component 1",
            "Processor",
            Map.of("key", "value")
    );

    private static final int INVOCATIONS = 1;
    private static final int FLOW_FILES_IN = 2;
    private static final int FLOW_FILES_OUT = 3;
    private static final int FLOW_FILES_REMOVED = 4;
    private static final int FLOW_FILES_SENT = 5;
    private static final int FLOW_FILES_RECEIVED = 6;
    private static final long CONTENT_SIZE_IN = 7L;
    private static final long CONTENT_SIZE_OUT = 8L;
    private static final long CONTENT_SIZE_REMOVED = 9L;
    private static final long BYTES_READ = 10L;
    private static final long BYTES_WRITTEN = 11L;
    private static final long BYTES_SENT = 12L;
    private static final long BYTES_RECEIVED = 13L;
    private static final long PROCESSING_NANOSECONDS = 14L;
    private static final long CPU_NANOSECONDS = 15L;
    private static final long CONTENT_READ_NANOSECONDS = 16L;
    private static final long CONTENT_WRITE_NANOSECONDS = 17L;
    private static final long SESSION_COMMIT_NANOSECONDS = 18L;
    private static final long GARBAGE_COLLECTION_MILLIS = 19L;
    private static final long AGGREGATE_LINEAGE_MILLIS = 20L;

    private static final String COUNTER_NAME = "counter";
    private static final long COUNTER_VALUE = 21L;

    @Test
    void testBuildPopulatesEveryField() {
        final ProcessSessionEvent event = populatedBuilder(COMPONENT_CONTEXT).build();

        assertSame(COMPONENT_CONTEXT, event.getComponentMetricContext());
        assertEquals(INVOCATIONS, event.getInvocations());
        assertEquals(FLOW_FILES_IN, event.getFlowFilesIn());
        assertEquals(FLOW_FILES_OUT, event.getFlowFilesOut());
        assertEquals(FLOW_FILES_REMOVED, event.getFlowFilesRemoved());
        assertEquals(FLOW_FILES_SENT, event.getFlowFilesSent());
        assertEquals(FLOW_FILES_RECEIVED, event.getFlowFilesReceived());
        assertEquals(CONTENT_SIZE_IN, event.getContentSizeIn());
        assertEquals(CONTENT_SIZE_OUT, event.getContentSizeOut());
        assertEquals(CONTENT_SIZE_REMOVED, event.getContentSizeRemoved());
        assertEquals(BYTES_READ, event.getBytesRead());
        assertEquals(BYTES_WRITTEN, event.getBytesWritten());
        assertEquals(BYTES_SENT, event.getBytesSent());
        assertEquals(BYTES_RECEIVED, event.getBytesReceived());
        assertEquals(PROCESSING_NANOSECONDS, event.getProcessingNanoseconds());
        assertEquals(CPU_NANOSECONDS, event.getCpuNanoseconds());
        assertEquals(CONTENT_READ_NANOSECONDS, event.getContentReadNanoseconds());
        assertEquals(CONTENT_WRITE_NANOSECONDS, event.getContentWriteNanoseconds());
        assertEquals(SESSION_COMMIT_NANOSECONDS, event.getSessionCommitNanoseconds());
        assertEquals(GARBAGE_COLLECTION_MILLIS, event.getGarbageCollectionMillis());
        assertEquals(AGGREGATE_LINEAGE_MILLIS, event.getAggregateLineageMillis());
        assertEquals(Map.of(COUNTER_NAME, COUNTER_VALUE), event.getCounters());
    }

    @Test
    void testAddAccumulatesFlowFilesAndContentSize() {
        final ProcessSessionEvent event = ProcessSessionEventBuilder.forComponent(COMPONENT_CONTEXT)
                .addFlowFilesIn(1)
                .addFlowFilesIn(2)
                .addFlowFilesOut(3)
                .addContentSizeIn(4L)
                .addContentSizeIn(5L)
                .addContentSizeOut(6L)
                .build();

        assertEquals(3, event.getFlowFilesIn());
        assertEquals(3, event.getFlowFilesOut());
        assertEquals(9L, event.getContentSizeIn());
        assertEquals(6L, event.getContentSizeOut());
    }

    @Test
    void testMergeSumsMetricsAndRetainsContext() {
        final ProcessSessionEvent source = populatedBuilder(COMPONENT_CONTEXT).build();

        final ComponentMetricContext otherContext = new ComponentMetricContext(
                "component-2", "Component 2", "Processor", Map.of());
        final ProcessSessionEvent merged = populatedBuilder(otherContext)
                .merge(source)
                .build();

        assertSame(otherContext, merged.getComponentMetricContext());
        assertEquals(INVOCATIONS * 2, merged.getInvocations());
        assertEquals(FLOW_FILES_IN * 2, merged.getFlowFilesIn());
        assertEquals(FLOW_FILES_RECEIVED * 2, merged.getFlowFilesReceived());
        assertEquals(CONTENT_SIZE_IN * 2, merged.getContentSizeIn());
        assertEquals(BYTES_READ * 2, merged.getBytesRead());
        assertEquals(PROCESSING_NANOSECONDS * 2, merged.getProcessingNanoseconds());
        assertEquals(GARBAGE_COLLECTION_MILLIS * 2, merged.getGarbageCollectionMillis());
        assertEquals(AGGREGATE_LINEAGE_MILLIS * 2, merged.getAggregateLineageMillis());
        assertEquals(Map.of(COUNTER_NAME, COUNTER_VALUE * 2), merged.getCounters());
    }

    @Test
    void testEmptyReturnsSharedZeroedEvent() {
        final ProcessSessionEvent empty = ProcessSessionEventBuilder.empty();

        assertSame(empty, ProcessSessionEventBuilder.empty());
        assertEquals(0, empty.getInvocations());
        assertEquals(0, empty.getFlowFilesIn());
        assertEquals(0L, empty.getBytesRead());
        assertTrue(empty.getCounters().isEmpty());
    }

    private ProcessSessionEventBuilder populatedBuilder(final ComponentMetricContext componentMetricContext) {
        return ProcessSessionEventBuilder.forComponent(componentMetricContext)
                .invocations(INVOCATIONS)
                .flowFilesIn(FLOW_FILES_IN)
                .flowFilesOut(FLOW_FILES_OUT)
                .flowFilesRemoved(FLOW_FILES_REMOVED)
                .flowFilesSent(FLOW_FILES_SENT)
                .flowFilesReceived(FLOW_FILES_RECEIVED)
                .contentSizeIn(CONTENT_SIZE_IN)
                .contentSizeOut(CONTENT_SIZE_OUT)
                .contentSizeRemoved(CONTENT_SIZE_REMOVED)
                .bytesRead(BYTES_READ)
                .bytesWritten(BYTES_WRITTEN)
                .bytesSent(BYTES_SENT)
                .bytesReceived(BYTES_RECEIVED)
                .processingNanoseconds(PROCESSING_NANOSECONDS)
                .cpuNanoseconds(CPU_NANOSECONDS)
                .contentReadNanoseconds(CONTENT_READ_NANOSECONDS)
                .contentWriteNanoseconds(CONTENT_WRITE_NANOSECONDS)
                .sessionCommitNanoseconds(SESSION_COMMIT_NANOSECONDS)
                .garbageCollectionMillis(GARBAGE_COLLECTION_MILLIS)
                .aggregateLineageMillis(AGGREGATE_LINEAGE_MILLIS)
                .counters(Map.of(COUNTER_NAME, COUNTER_VALUE));
    }
}
