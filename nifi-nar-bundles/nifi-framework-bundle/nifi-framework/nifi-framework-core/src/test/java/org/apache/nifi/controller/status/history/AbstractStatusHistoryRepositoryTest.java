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

import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.NodeStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.controller.status.StorageStatus;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractStatusHistoryRepositoryTest {
    protected static final String ROOT_GROUP_ID = "697199b5-8318-4004-8e18-75c6e882f29e";
    protected static final String CHILD_GROUP_ID = "fc4aef2e-84d6-4007-9e67-c53050d22398";
    protected static final String PROCESSOR_ID = "3ab6181b-925c-4803-8f72-ea1fdc065db8";
    protected static final String PROCESSOR_WITH_COUNTER_ID = "f09c5bb8-7b67-4d3b-81c5-5373e1c03ed1";
    protected static final String CONNECTION_ID = "d5452f70-a0d9-44c5-aeda-c2026482e4ee";
    protected static final String REMOTE_PROCESS_GROUP_ID = "f5f4fe2a-0209-4ba7-8f15-f33df942cde5";

    protected ProcessGroupStatus givenSimpleRootProcessGroupStatus() {
        final ProcessGroupStatus status = new ProcessGroupStatus();
        status.setId(ROOT_GROUP_ID);
        status.setName("Root");
        status.setInputCount(1);
        status.setInputContentSize(2L);
        status.setOutputCount(3);
        status.setOutputContentSize(4L);
        status.setActiveThreadCount(5);
        status.setTerminatedThreadCount(6);
        status.setQueuedCount(7);
        status.setQueuedContentSize(8L);
        status.setBytesRead(9L);
        status.setBytesWritten(10L);
        status.setFlowFilesReceived(11);
        status.setBytesReceived(12L);
        status.setFlowFilesSent(13);
        status.setBytesSent(14);
        status.setFlowFilesTransferred(15);
        status.setBytesTransferred(16L);
        return status;
    }

    protected ProcessGroupStatus givenRootProcessGroupStatus() {
        final ProcessGroupStatus status = givenSimpleRootProcessGroupStatus();
        status.setConnectionStatus(Collections.singleton(givenConnectionStatus()));
        status.setProcessGroupStatus(Collections.singleton(givenChildProcessGroupStatus()));
        status.setProcessorStatus(Collections.singleton(givenProcessorStatus()));
        return status;
    }

    protected ProcessGroupStatus givenChildProcessGroupStatus() {
        final ProcessGroupStatus status = new ProcessGroupStatus();
        status.setId(CHILD_GROUP_ID);
        status.setName("Child");
        status.setInputCount(21);
        status.setInputContentSize(22L);
        status.setOutputCount(23);
        status.setOutputContentSize(24L);
        status.setActiveThreadCount(25);
        status.setTerminatedThreadCount(26);
        status.setQueuedCount(27);
        status.setQueuedContentSize(28L);
        status.setBytesRead(29L);
        status.setBytesWritten(30L);
        status.setFlowFilesReceived(31);
        status.setBytesReceived(32L);
        status.setFlowFilesSent(33);
        status.setBytesSent(34);
        status.setFlowFilesTransferred(35);
        status.setBytesTransferred(36L);

        status.setRemoteProcessGroupStatus(Collections.singleton(givenRemoteProcessGroupStatus()));
        status.setProcessorStatus(Collections.singleton(givenProcessorWithCounterStatus()));

        return status;
    }

    protected ProcessorStatus givenProcessorStatus() {
        final ProcessorStatus status = new ProcessorStatus();
        status.setId(PROCESSOR_ID);
        status.setName("Processor");
        status.setInputCount(61);
        status.setInputBytes(62);
        status.setOutputCount(63);
        status.setOutputBytes(64);
        status.setBytesRead(65);
        status.setBytesWritten(66);
        status.setInvocations(67);
        status.setProcessingNanos(68000000);
        status.setFlowFilesRemoved(69);
        status.setAverageLineageDuration(70);
        status.setActiveThreadCount(71);
        status.setTerminatedThreadCount(72);
        status.setFlowFilesReceived(73);
        status.setBytesReceived(74);
        status.setFlowFilesSent(75);
        status.setBytesSent(76);
        return status;
    }

    protected ProcessorStatus givenProcessorWithCounterStatus() {
        final Map<String, Long> counters = new HashMap<>();
        counters.put("counter1", 97L);
        counters.put("counter2", 98L);

        final ProcessorStatus status = new ProcessorStatus();
        status.setId(PROCESSOR_WITH_COUNTER_ID);
        status.setName("ProcessorWithCounter");
        status.setInputCount(81);
        status.setInputBytes(82);
        status.setOutputCount(83);
        status.setOutputBytes(84);
        status.setBytesRead(85);
        status.setBytesWritten(86);
        status.setInvocations(87);
        status.setProcessingNanos(88000000);
        status.setFlowFilesRemoved(89);
        status.setAverageLineageDuration(90);
        status.setActiveThreadCount(91);
        status.setTerminatedThreadCount(92);
        status.setFlowFilesReceived(93);
        status.setBytesReceived(94);
        status.setFlowFilesSent(95);
        status.setBytesSent(96);
        status.setCounters(counters);
        return status;
    }

    protected ConnectionStatus givenConnectionStatus() {
        final ConnectionStatus status = new ConnectionStatus();
        status.setId(CONNECTION_ID);
        status.setName("Connection");
        status.setInputCount(101);
        status.setInputBytes(102);
        status.setQueuedCount(103);
        status.setQueuedBytes(104);
        status.setOutputCount(105);
        status.setOutputBytes(106);
        status.setMaxQueuedCount(107);
        status.setMaxQueuedBytes(108);

        status.setTotalQueuedDuration(103L * 110L);
        status.setMaxQueuedDuration(111);

        return status;
    }

    protected RemoteProcessGroupStatus givenRemoteProcessGroupStatus() {
        final RemoteProcessGroupStatus status = new RemoteProcessGroupStatus();
        status.setId(REMOTE_PROCESS_GROUP_ID);
        status.setName("RemoteProcessGroup");
        status.setActiveThreadCount(121);
        status.setSentCount(122);
        status.setSentContentSize(123000L);
        status.setReceivedCount(124);
        status.setReceivedContentSize(125000L);
        status.setActiveRemotePortCount(126);
        status.setInactiveRemotePortCount(127);
        status.setAverageLineageDuration(128000);
        return status;
    }

    protected void assertStatusHistoryIsEmpty(final StatusHistory statusHistory) {
        Assert.assertTrue(statusHistory.getStatusSnapshots().isEmpty());
    }

    protected void assertRootProcessGroupStatusSnapshot(final StatusSnapshot snapshot) {
        Assert.assertEquals(9L, snapshot.getStatusMetric(ProcessGroupStatusDescriptor.BYTES_READ.getDescriptor()).longValue());
        Assert.assertEquals(10L, snapshot.getStatusMetric(ProcessGroupStatusDescriptor.BYTES_WRITTEN.getDescriptor()).longValue());
        Assert.assertEquals(9L+10L, snapshot.getStatusMetric(ProcessGroupStatusDescriptor.BYTES_TRANSFERRED.getDescriptor()).longValue());
        Assert.assertEquals(2L, snapshot.getStatusMetric(ProcessGroupStatusDescriptor.INPUT_BYTES.getDescriptor()).longValue());
        Assert.assertEquals(1L, snapshot.getStatusMetric(ProcessGroupStatusDescriptor.INPUT_COUNT.getDescriptor()).longValue());
        Assert.assertEquals(4L, snapshot.getStatusMetric(ProcessGroupStatusDescriptor.OUTPUT_BYTES.getDescriptor()).longValue());
        Assert.assertEquals(3L, snapshot.getStatusMetric(ProcessGroupStatusDescriptor.OUTPUT_COUNT.getDescriptor()).longValue());
        Assert.assertEquals(8L, snapshot.getStatusMetric(ProcessGroupStatusDescriptor.QUEUED_BYTES.getDescriptor()).longValue());
        Assert.assertEquals(7L, snapshot.getStatusMetric(ProcessGroupStatusDescriptor.QUEUED_COUNT.getDescriptor()).longValue());

        // Information is lost due to nano->micro conversion!
        Assert.assertEquals(Double.valueOf((68000000L + 88000000L)/1000/1000).longValue(), snapshot.getStatusMetric(ProcessGroupStatusDescriptor.TASK_MILLIS.getDescriptor()).longValue());
    }

    protected void assertChildProcessGroupStatusSnapshot(final StatusSnapshot snapshot) {
        Assert.assertEquals(29L, snapshot.getStatusMetric(ProcessGroupStatusDescriptor.BYTES_READ.getDescriptor()).longValue());
        Assert.assertEquals(30L, snapshot.getStatusMetric(ProcessGroupStatusDescriptor.BYTES_WRITTEN.getDescriptor()).longValue());
        Assert.assertEquals(29L+30L, snapshot.getStatusMetric(ProcessGroupStatusDescriptor.BYTES_TRANSFERRED.getDescriptor()).longValue());
        Assert.assertEquals(22L, snapshot.getStatusMetric(ProcessGroupStatusDescriptor.INPUT_BYTES.getDescriptor()).longValue());
        Assert.assertEquals(21L, snapshot.getStatusMetric(ProcessGroupStatusDescriptor.INPUT_COUNT.getDescriptor()).longValue());
        Assert.assertEquals(24L, snapshot.getStatusMetric(ProcessGroupStatusDescriptor.OUTPUT_BYTES.getDescriptor()).longValue());
        Assert.assertEquals(23L, snapshot.getStatusMetric(ProcessGroupStatusDescriptor.OUTPUT_COUNT.getDescriptor()).longValue());
        Assert.assertEquals(28L, snapshot.getStatusMetric(ProcessGroupStatusDescriptor.QUEUED_BYTES.getDescriptor()).longValue());
        Assert.assertEquals(27L, snapshot.getStatusMetric(ProcessGroupStatusDescriptor.QUEUED_COUNT.getDescriptor()).longValue());
        Assert.assertEquals(Double.valueOf((88000000L)/1000/1000).longValue(), snapshot.getStatusMetric(ProcessGroupStatusDescriptor.TASK_MILLIS.getDescriptor()).longValue());
    }

    protected void assertProcessorStatusSnapshot(final StatusSnapshot snapshot) {
        Assert.assertEquals(65L, snapshot.getStatusMetric(ProcessorStatusDescriptor.BYTES_READ.getDescriptor()).longValue());
        Assert.assertEquals(66L, snapshot.getStatusMetric(ProcessorStatusDescriptor.BYTES_WRITTEN.getDescriptor()).longValue());
        Assert.assertEquals(65L+66L, snapshot.getStatusMetric(ProcessorStatusDescriptor.BYTES_TRANSFERRED.getDescriptor()).longValue());
        Assert.assertEquals(62L, snapshot.getStatusMetric(ProcessorStatusDescriptor.INPUT_BYTES.getDescriptor()).longValue());
        Assert.assertEquals(61L, snapshot.getStatusMetric(ProcessorStatusDescriptor.INPUT_COUNT.getDescriptor()).longValue());
        Assert.assertEquals(64L, snapshot.getStatusMetric(ProcessorStatusDescriptor.OUTPUT_BYTES.getDescriptor()).longValue());
        Assert.assertEquals(63L, snapshot.getStatusMetric(ProcessorStatusDescriptor.OUTPUT_COUNT.getDescriptor()).longValue());
        Assert.assertEquals(67L, snapshot.getStatusMetric(ProcessorStatusDescriptor.TASK_COUNT.getDescriptor()).longValue());
        Assert.assertEquals(68L, snapshot.getStatusMetric(ProcessorStatusDescriptor.TASK_MILLIS.getDescriptor()).longValue());
        Assert.assertEquals(68000000L, snapshot.getStatusMetric(ProcessorStatusDescriptor.TASK_NANOS.getDescriptor()).longValue());
        Assert.assertEquals(69L, snapshot.getStatusMetric(ProcessorStatusDescriptor.FLOWFILES_REMOVED.getDescriptor()).longValue());
        Assert.assertEquals(70L, snapshot.getStatusMetric(ProcessorStatusDescriptor.AVERAGE_LINEAGE_DURATION.getDescriptor()).longValue());
        Assert.assertEquals(Double.valueOf(68000000/67).longValue(), snapshot.getStatusMetric(ProcessorStatusDescriptor.AVERAGE_TASK_NANOS.getDescriptor()).longValue());
    }

    protected void assertProcessorWithCounterStatusSnapshot(final StatusSnapshot snapshot, final boolean withCounter) {
        Assert.assertEquals(85L, snapshot.getStatusMetric(ProcessorStatusDescriptor.BYTES_READ.getDescriptor()).longValue());
        Assert.assertEquals(86L, snapshot.getStatusMetric(ProcessorStatusDescriptor.BYTES_WRITTEN.getDescriptor()).longValue());
        Assert.assertEquals(85L+86L, snapshot.getStatusMetric(ProcessorStatusDescriptor.BYTES_TRANSFERRED.getDescriptor()).longValue());
        Assert.assertEquals(82L, snapshot.getStatusMetric(ProcessorStatusDescriptor.INPUT_BYTES.getDescriptor()).longValue());
        Assert.assertEquals(81L, snapshot.getStatusMetric(ProcessorStatusDescriptor.INPUT_COUNT.getDescriptor()).longValue());
        Assert.assertEquals(84L, snapshot.getStatusMetric(ProcessorStatusDescriptor.OUTPUT_BYTES.getDescriptor()).longValue());
        Assert.assertEquals(83L, snapshot.getStatusMetric(ProcessorStatusDescriptor.OUTPUT_COUNT.getDescriptor()).longValue());
        Assert.assertEquals(87L, snapshot.getStatusMetric(ProcessorStatusDescriptor.TASK_COUNT.getDescriptor()).longValue());
        Assert.assertEquals(88L, snapshot.getStatusMetric(ProcessorStatusDescriptor.TASK_MILLIS.getDescriptor()).longValue());
        Assert.assertEquals(88000000L, snapshot.getStatusMetric(ProcessorStatusDescriptor.TASK_NANOS.getDescriptor()).longValue());
        Assert.assertEquals(89L, snapshot.getStatusMetric(ProcessorStatusDescriptor.FLOWFILES_REMOVED.getDescriptor()).longValue());
        Assert.assertEquals(90L, snapshot.getStatusMetric(ProcessorStatusDescriptor.AVERAGE_LINEAGE_DURATION.getDescriptor()).longValue());
        Assert.assertEquals(Double.valueOf(88000000/87).longValue(), snapshot.getStatusMetric(ProcessorStatusDescriptor.AVERAGE_TASK_NANOS.getDescriptor()).longValue());

        final Map<String, ? extends MetricDescriptor<?>> counterMetrics = snapshot.getMetricDescriptors()
                .stream().filter(d -> d instanceof CounterMetricDescriptor).collect(Collectors.toMap(d -> d.getLabel(), d -> d));

        if (withCounter) {
            Assert.assertEquals(2, counterMetrics.size());
            Assert.assertEquals(97L, snapshot.getStatusMetric(counterMetrics.get("counter1 (5 mins)")).longValue());
            Assert.assertEquals(98L, snapshot.getStatusMetric(counterMetrics.get("counter2 (5 mins)")).longValue());
        } else {
            Assert.assertTrue(counterMetrics.isEmpty());
        }
    }

    protected void assertConnectionStatusSnapshot(final StatusSnapshot snapshot) {
        Assert.assertEquals(102L, snapshot.getStatusMetric(ConnectionStatusDescriptor.INPUT_BYTES.getDescriptor()).longValue());
        Assert.assertEquals(101L, snapshot.getStatusMetric(ConnectionStatusDescriptor.INPUT_COUNT.getDescriptor()).longValue());
        Assert.assertEquals(106L, snapshot.getStatusMetric(ConnectionStatusDescriptor.OUTPUT_BYTES.getDescriptor()).longValue());
        Assert.assertEquals(105L, snapshot.getStatusMetric(ConnectionStatusDescriptor.OUTPUT_COUNT.getDescriptor()).longValue());
        Assert.assertEquals(104L, snapshot.getStatusMetric(ConnectionStatusDescriptor.QUEUED_BYTES.getDescriptor()).longValue());
        Assert.assertEquals(103L, snapshot.getStatusMetric(ConnectionStatusDescriptor.QUEUED_COUNT.getDescriptor()).longValue());

        Assert.assertEquals(103L * 110L, snapshot.getStatusMetric(ConnectionStatusDescriptor.TOTAL_QUEUED_DURATION.getDescriptor()).longValue());
        Assert.assertEquals(111L, snapshot.getStatusMetric(ConnectionStatusDescriptor.MAX_QUEUED_DURATION.getDescriptor()).longValue());
        Assert.assertEquals(110L, snapshot.getStatusMetric(ConnectionStatusDescriptor.AVERAGE_QUEUED_DURATION.getDescriptor()).longValue());
    }

    protected void assertRemoteProcessGroupSnapshot(final StatusSnapshot snapshot) {
        Assert.assertEquals(123000L, snapshot.getStatusMetric(RemoteProcessGroupStatusDescriptor.SENT_BYTES.getDescriptor()).longValue());
        Assert.assertEquals(122L, snapshot.getStatusMetric(RemoteProcessGroupStatusDescriptor.SENT_COUNT.getDescriptor()).longValue());
        Assert.assertEquals(125000L, snapshot.getStatusMetric(RemoteProcessGroupStatusDescriptor.RECEIVED_BYTES.getDescriptor()).longValue());
        Assert.assertEquals(124L, snapshot.getStatusMetric(RemoteProcessGroupStatusDescriptor.RECEIVED_COUNT.getDescriptor()).longValue());
        Assert.assertEquals(Double.valueOf(125000L/300).longValue(), snapshot.getStatusMetric(RemoteProcessGroupStatusDescriptor.RECEIVED_BYTES_PER_SECOND.getDescriptor()).longValue());
        Assert.assertEquals(Double.valueOf(123000L/300).longValue(), snapshot.getStatusMetric(RemoteProcessGroupStatusDescriptor.SENT_BYTES_PER_SECOND.getDescriptor()).longValue());
        Assert.assertEquals(Double.valueOf((123000L+125000L)/300).longValue(), snapshot.getStatusMetric(RemoteProcessGroupStatusDescriptor.TOTAL_BYTES_PER_SECOND.getDescriptor()).longValue());
        Assert.assertEquals(Double.valueOf(128000L).longValue(), snapshot.getStatusMetric(RemoteProcessGroupStatusDescriptor.AVERAGE_LINEAGE_DURATION.getDescriptor()).longValue());
    }

    protected NodeStatus givenNodeStatus(final int number) {
        final NodeStatus result = new NodeStatus();
        result.setCreatedAtInMs(System.currentTimeMillis());
        result.setFreeHeap(1 + number);
        result.setUsedHeap(2 + number);
        result.setHeapUtilization(3 + number);
        result.setFreeNonHeap(4 + number);
        result.setUsedNonHeap(5 + number);
        result.setOpenFileHandlers(6 + number);
        result.setProcessorLoadAverage(7.1d + number);
        result.setTotalThreads(9 + number);
        result.setEventDrivenThreads(20 + number);
        result.setTimerDrivenThreads(21 + number);
        result.setFlowFileRepositoryFreeSpace(10 + number);
        result.setFlowFileRepositoryUsedSpace(11 + number);
        result.setContentRepositories(Arrays.asList(
                givenStorageStatus("c1", 12 + number, 13 + number),
                givenStorageStatus("c2", 14 + number, 15 + number)
        ));
        result.setProvenanceRepositories(Arrays.asList(
                givenStorageStatus("p1", 16 + number, 17 + number),
                givenStorageStatus("p2", 18 + number, 19 + number)
        ));

        return result;
    }

    protected NodeStatus givenNodeStatus() {
        final NodeStatus status = new NodeStatus();
        status.setFreeHeap(11);
        status.setUsedHeap(12);
        status.setHeapUtilization(13);
        status.setFreeNonHeap(14);
        status.setUsedNonHeap(15);
        status.setOpenFileHandlers(16);
        status.setProcessorLoadAverage(17);
        status.setTotalThreads(18);
        status.setEventDrivenThreads(19);
        status.setTimerDrivenThreads(20);
        status.setContentRepositories(Arrays.asList(
                givenStorageStatus("c1", 21, 22),
                givenStorageStatus("c2", 23, 24)
        ));
        status.setProvenanceRepositories(Arrays.asList(
                givenStorageStatus("p1", 25, 26),
                givenStorageStatus("p2", 27, 28)
        ));

        return status;
    }

    protected StorageStatus givenStorageStatus(final String name, final long usedSpace, final long freeSpace) {
        final StorageStatus storageStatus = new StorageStatus();
        storageStatus.setName(name);
        storageStatus.setUsedSpace(usedSpace);
        storageStatus.setFreeSpace(freeSpace);
        return storageStatus;
    }

    protected List<GarbageCollectionStatus> givenGarbageCollectionStatuses(final Date measuredAt, long gc1Count, long gc1Millis, long gc2Count, long gc2Millis) {
        final GarbageCollectionStatus status1 = new StandardGarbageCollectionStatus("gc1", measuredAt, gc1Count, gc1Millis);
        final GarbageCollectionStatus status2 = new StandardGarbageCollectionStatus("gc2", measuredAt, gc2Count, gc2Millis);
        return Arrays.asList(status1, status2);
    }

    protected List<GarbageCollectionStatus> givenGarbageCollectionStatuses(final Date measuredAt) {
        return givenGarbageCollectionStatuses(measuredAt, 31, 32, 41, 42);
    }

    protected void assertNodeStatusHistory(final StatusSnapshot snapshot) {
        // Default metrics
        Assert.assertEquals(11, snapshot.getStatusMetric(NodeStatusDescriptor.FREE_HEAP.getDescriptor()).longValue());
        Assert.assertEquals(12, snapshot.getStatusMetric(NodeStatusDescriptor.USED_HEAP.getDescriptor()).longValue());
        Assert.assertEquals(13, snapshot.getStatusMetric(NodeStatusDescriptor.HEAP_UTILIZATION.getDescriptor()).longValue());
        Assert.assertEquals(14, snapshot.getStatusMetric(NodeStatusDescriptor.FREE_NON_HEAP.getDescriptor()).longValue());
        Assert.assertEquals(15, snapshot.getStatusMetric(NodeStatusDescriptor.USED_NON_HEAP.getDescriptor()).longValue());
        Assert.assertEquals(16, snapshot.getStatusMetric(NodeStatusDescriptor.OPEN_FILE_HANDLES.getDescriptor()).longValue());
        Assert.assertEquals(17000000, snapshot.getStatusMetric(NodeStatusDescriptor.PROCESSOR_LOAD_AVERAGE.getDescriptor()).longValue());
        Assert.assertEquals(18, snapshot.getStatusMetric(NodeStatusDescriptor.TOTAL_THREADS.getDescriptor()).longValue());
        Assert.assertEquals(19, snapshot.getStatusMetric(NodeStatusDescriptor.EVENT_DRIVEN_THREADS.getDescriptor()).longValue());
        Assert.assertEquals(20, snapshot.getStatusMetric(NodeStatusDescriptor.TIME_DRIVEN_THREADS.getDescriptor()).longValue());

        // Storage metrics
        Assert.assertEquals(21, snapshot.getStatusMetric(getDescriptor(snapshot, "contentStorage1Used")).longValue());
        Assert.assertEquals(22, snapshot.getStatusMetric(getDescriptor(snapshot, "contentStorage1Free")).longValue());
        Assert.assertEquals(23, snapshot.getStatusMetric(getDescriptor(snapshot, "contentStorage2Used")).longValue());
        Assert.assertEquals(24, snapshot.getStatusMetric(getDescriptor(snapshot, "contentStorage2Free")).longValue());
        Assert.assertEquals(25, snapshot.getStatusMetric(getDescriptor(snapshot, "provenanceStorage3Used")).longValue());
        Assert.assertEquals(26, snapshot.getStatusMetric(getDescriptor(snapshot, "provenanceStorage3Free")).longValue());
        Assert.assertEquals(27, snapshot.getStatusMetric(getDescriptor(snapshot, "provenanceStorage4Used")).longValue());
        Assert.assertEquals(28, snapshot.getStatusMetric(getDescriptor(snapshot, "provenanceStorage4Free")).longValue());
    }

    private MetricDescriptor<?> getDescriptor(final StatusSnapshot snapshot, final String field) {
        return snapshot.getMetricDescriptors().stream().filter(md -> md.getField().equals(field)).collect(Collectors.toList()).get(0);
    }

    protected void assertGc1Status(final List<GarbageCollectionStatus> gc1) {
        Assert.assertEquals(1, gc1.size());
        final GarbageCollectionStatus status = gc1.get(0);
        Assert.assertEquals(31, status.getCollectionCount());
        Assert.assertEquals(32, status.getCollectionMillis());
    }

    protected void assertGc2Status(final List<GarbageCollectionStatus> gc2) {
        Assert.assertEquals(1, gc2.size());
        final GarbageCollectionStatus status = gc2.get(0);
        Assert.assertEquals(41, status.getCollectionCount());
        Assert.assertEquals(42, status.getCollectionMillis());
    }
}
