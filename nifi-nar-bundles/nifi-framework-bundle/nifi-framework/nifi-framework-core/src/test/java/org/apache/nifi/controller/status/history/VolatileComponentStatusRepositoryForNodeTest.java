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
package org.apache.nifi.controller.status.history;

import org.apache.nifi.controller.status.NodeStatus;
import org.apache.nifi.controller.status.StorageStatus;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class VolatileComponentStatusRepositoryForNodeTest extends AbstractStatusHistoryRepositoryTest {

    @Test
    public void testNodeStatusHistory() {
        // given
        final NiFiProperties niFiProperties = Mockito.mock(NiFiProperties.class);
        Mockito.when(niFiProperties.getIntegerProperty(VolatileComponentStatusRepository.NUM_DATA_POINTS_PROPERTY, VolatileComponentStatusRepository.DEFAULT_NUM_DATA_POINTS)).thenReturn(10);
        final VolatileComponentStatusRepository testSubject = new VolatileComponentStatusRepository(niFiProperties);
        final List<NodeStatus> nodeStatuses = Arrays.asList(
                givenNodeStatus(0),
                givenNodeStatus(1)
        );

        final Date capturedAt = new Date();
        testSubject.capture(nodeStatuses.get(0), givenSimpleRootProcessGroupStatus(), givenGarbageCollectionStatuses(capturedAt, 1, 100, 2, 300), capturedAt);
        testSubject.capture(nodeStatuses.get(1), givenSimpleRootProcessGroupStatus(), givenGarbageCollectionStatuses(capturedAt, 1, 100, 5, 700), capturedAt);

        // when
        final StatusHistory result = testSubject.getNodeStatusHistory(new Date(0), new Date());

        // then
        // checking on snapshots
        assertEquals(nodeStatuses.size(), result.getStatusSnapshots().size());

        // metrics based on NodeStatus
        for (int i = 0; i < result.getStatusSnapshots().size(); i++) {
            final StatusSnapshot snapshot = result.getStatusSnapshots().get(i);
            final NodeStatus nodeStatus = nodeStatuses.get(i);

            assertEquals(nodeStatus.getFreeHeap(), snapshot.getStatusMetric(NodeStatusDescriptor.FREE_HEAP.getDescriptor()).longValue());
            assertEquals(nodeStatus.getUsedHeap(), snapshot.getStatusMetric(NodeStatusDescriptor.USED_HEAP.getDescriptor()).longValue());
            assertEquals(nodeStatus.getHeapUtilization(), snapshot.getStatusMetric(NodeStatusDescriptor.HEAP_UTILIZATION.getDescriptor()).longValue());
            assertEquals(nodeStatus.getFreeNonHeap(), snapshot.getStatusMetric(NodeStatusDescriptor.FREE_NON_HEAP.getDescriptor()).longValue());
            assertEquals(nodeStatus.getUsedNonHeap(), snapshot.getStatusMetric(NodeStatusDescriptor.USED_NON_HEAP.getDescriptor()).longValue());
            assertEquals(nodeStatus.getOpenFileHandlers(), snapshot.getStatusMetric(NodeStatusDescriptor.OPEN_FILE_HANDLES.getDescriptor()).longValue());
            assertEquals(
                    Double.valueOf(nodeStatus.getProcessorLoadAverage() * MetricDescriptor.FRACTION_MULTIPLIER).longValue(),
                    snapshot.getStatusMetric(NodeStatusDescriptor.PROCESSOR_LOAD_AVERAGE.getDescriptor()).longValue());
            assertEquals(nodeStatus.getTotalThreads(), snapshot.getStatusMetric(NodeStatusDescriptor.TOTAL_THREADS.getDescriptor()).longValue());
            assertEquals(nodeStatus.getTimerDrivenThreads(), snapshot.getStatusMetric(NodeStatusDescriptor.TIME_DRIVEN_THREADS.getDescriptor()).longValue());
            assertEquals(nodeStatus.getFlowFileRepositoryFreeSpace(), snapshot.getStatusMetric(NodeStatusDescriptor.FLOW_FILE_REPOSITORY_FREE_SPACE.getDescriptor()).longValue());
            assertEquals(nodeStatus.getFlowFileRepositoryUsedSpace(), snapshot.getStatusMetric(NodeStatusDescriptor.FLOW_FILE_REPOSITORY_USED_SPACE.getDescriptor()).longValue());
            assertEquals(
                    nodeStatus.getContentRepositories().stream().map(StorageStatus::getFreeSpace).reduce(0L, Long::sum).longValue(),
                    snapshot.getStatusMetric(NodeStatusDescriptor.CONTENT_REPOSITORY_FREE_SPACE.getDescriptor()).longValue());
            assertEquals(
                    nodeStatus.getContentRepositories().stream().map(StorageStatus::getUsedSpace).reduce(0L, Long::sum).longValue(),
                    snapshot.getStatusMetric(NodeStatusDescriptor.CONTENT_REPOSITORY_USED_SPACE.getDescriptor()).longValue());
            assertEquals(
                    nodeStatus.getProvenanceRepositories().stream().map(StorageStatus::getFreeSpace).reduce(0L, Long::sum).longValue(),
                    snapshot.getStatusMetric(NodeStatusDescriptor.PROVENANCE_REPOSITORY_FREE_SPACE.getDescriptor()).longValue());
            assertEquals(
                    nodeStatus.getProvenanceRepositories().stream().map(StorageStatus::getUsedSpace).reduce(0L, Long::sum).longValue(),
                    snapshot.getStatusMetric(NodeStatusDescriptor.PROVENANCE_REPOSITORY_USED_SPACE.getDescriptor()).longValue());

            // metrics based on repositories
            assertEquals(12 + i, getMetricAtOrdinal(snapshot, 16)); // c1 used
            assertEquals(13 + i, getMetricAtOrdinal(snapshot, 15)); // c1 free
            assertEquals(14 + i, getMetricAtOrdinal(snapshot, 18)); // c2 used
            assertEquals(15 + i, getMetricAtOrdinal(snapshot, 17)); // c2 free

            assertEquals(16 + i, getMetricAtOrdinal(snapshot, 20)); // p1 used
            assertEquals(17 + i, getMetricAtOrdinal(snapshot, 19)); // p1 free
            assertEquals(18 + i, getMetricAtOrdinal(snapshot, 22)); // p2 used
            assertEquals(19 + i, getMetricAtOrdinal(snapshot, 21)); // p2 free
        }

        // metrics based on GarbageCollectionStatus (The ordinal numbers are true for setup, in production it might differ)
        final int g0TimeOrdinal = 23;
        final int g0CountOrdinal = 24;
        final int g0TimeDiffOrdinal = 25;
        final int g0CountDiffOrdinal = 26;
        final int g1TimeOrdinal = 27;
        final int g1CountOrdinal = 28;
        final int g1TimeDiffOrdinal = 29;
        final int g1CountDiffOrdinal = 30;

        final StatusSnapshot snapshot1 = result.getStatusSnapshots().get(0);
        final StatusSnapshot snapshot2 = result.getStatusSnapshots().get(1);

        assertEquals(100L, getMetricAtOrdinal(snapshot1, g0TimeOrdinal));
        assertEquals(0L, getMetricAtOrdinal(snapshot1, g0TimeDiffOrdinal));
        assertEquals(1L, getMetricAtOrdinal(snapshot1, g0CountOrdinal));
        assertEquals(0L, getMetricAtOrdinal(snapshot1, g0CountDiffOrdinal));
        assertEquals(300L, getMetricAtOrdinal(snapshot1, g1TimeOrdinal));
        assertEquals(0L, getMetricAtOrdinal(snapshot1, g1TimeDiffOrdinal));
        assertEquals(2L, getMetricAtOrdinal(snapshot1, g1CountOrdinal));
        assertEquals(0L, getMetricAtOrdinal(snapshot1, g1CountDiffOrdinal));

        assertEquals(100L, getMetricAtOrdinal(snapshot2, g0TimeOrdinal));
        assertEquals(0L, getMetricAtOrdinal(snapshot2, g0TimeDiffOrdinal));
        assertEquals(1L, getMetricAtOrdinal(snapshot2, g0CountOrdinal));
        assertEquals(0L, getMetricAtOrdinal(snapshot2, g0CountDiffOrdinal));
        assertEquals(700L, getMetricAtOrdinal(snapshot2, g1TimeOrdinal));
        assertEquals(400L, getMetricAtOrdinal(snapshot2, g1TimeDiffOrdinal));
        assertEquals(5L, getMetricAtOrdinal(snapshot2, g1CountOrdinal));
        assertEquals(3L, getMetricAtOrdinal(snapshot2, g1CountDiffOrdinal));
    }

    private static long getMetricAtOrdinal(final StatusSnapshot snapshot, final long ordinal) {
        final Set<MetricDescriptor<?>> metricDescriptors = snapshot.getMetricDescriptors();

        for (final MetricDescriptor<?> metricDescriptor : metricDescriptors) {
            if (metricDescriptor.getMetricIdentifier() == ordinal) {
                return snapshot.getStatusMetric(metricDescriptor);
            }
        }

        fail();
        return Long.MIN_VALUE;
    }
}
