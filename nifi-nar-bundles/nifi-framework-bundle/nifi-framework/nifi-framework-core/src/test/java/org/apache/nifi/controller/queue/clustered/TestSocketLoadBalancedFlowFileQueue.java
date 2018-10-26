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

package org.apache.nifi.controller.queue.clustered;

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.ClusterTopologyEventListener;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.MockFlowFileRecord;
import org.apache.nifi.controller.MockSwapManager;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.queue.NopConnectionEventListener;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.queue.clustered.client.async.AsyncLoadBalanceClientRegistry;
import org.apache.nifi.controller.queue.clustered.partition.FlowFilePartitioner;
import org.apache.nifi.controller.queue.clustered.partition.QueuePartition;
import org.apache.nifi.controller.queue.clustered.partition.RoundRobinPartitioner;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestSocketLoadBalancedFlowFileQueue {

    private Connection connection;
    private FlowFileRepository flowFileRepo;
    private ContentRepository contentRepo;
    private ProvenanceEventRepository provRepo;
    private ResourceClaimManager claimManager;
    private ClusterCoordinator clusterCoordinator;
    private MockSwapManager swapManager;
    private EventReporter eventReporter;
    private SocketLoadBalancedFlowFileQueue queue;
    private volatile ClusterTopologyEventListener clusterTopologyEventListener;

    private List<NodeIdentifier> nodeIds;
    private int nodePort = 4096;

    @Before
    public void setup() {
        MockFlowFileRecord.resetIdGenerator();
        connection = mock(Connection.class);
        when(connection.getIdentifier()).thenReturn("unit-test");

        flowFileRepo = mock(FlowFileRepository.class);
        contentRepo = mock(ContentRepository.class);
        provRepo = mock(ProvenanceEventRepository.class);
        claimManager = new StandardResourceClaimManager();
        clusterCoordinator = mock(ClusterCoordinator.class);
        swapManager = new MockSwapManager();
        eventReporter = EventReporter.NO_OP;

        final NodeIdentifier localNodeIdentifier = createNodeIdentifier("00000000-0000-0000-0000-000000000000");

        nodeIds = new ArrayList<>();
        nodeIds.add(localNodeIdentifier);
        nodeIds.add(createNodeIdentifier("11111111-1111-1111-1111-111111111111"));
        nodeIds.add(createNodeIdentifier("22222222-2222-2222-2222-222222222222"));

        Mockito.doAnswer(new Answer<Set<NodeIdentifier>>() {
            @Override
            public Set<NodeIdentifier> answer(InvocationOnMock invocation) throws Throwable {
                return new HashSet<>(nodeIds);
            }
        }).when(clusterCoordinator).getNodeIdentifiers();

        when(clusterCoordinator.getLocalNodeIdentifier()).thenReturn(localNodeIdentifier);

        doAnswer(new Answer() {
            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable {
                clusterTopologyEventListener = invocation.getArgumentAt(0, ClusterTopologyEventListener.class);
                return null;
            }
        }).when(clusterCoordinator).registerEventListener(Mockito.any(ClusterTopologyEventListener.class));

        final ProcessScheduler scheduler = mock(ProcessScheduler.class);

        final AsyncLoadBalanceClientRegistry registry = mock(AsyncLoadBalanceClientRegistry.class);
        queue = new SocketLoadBalancedFlowFileQueue("unit-test", new NopConnectionEventListener(), scheduler, flowFileRepo, provRepo,
            contentRepo, claimManager, clusterCoordinator, registry, swapManager, 10000, eventReporter);
    }

    private NodeIdentifier createNodeIdentifier() {
        return createNodeIdentifier(UUID.randomUUID().toString());
    }

    private NodeIdentifier createNodeIdentifier(final String uuid) {
        return new NodeIdentifier(uuid, "localhost", nodePort++, "localhost", nodePort++,
            "localhost", nodePort++, "localhost", nodePort++, nodePort++, true, Collections.emptySet());
    }

    @Test
    public void testBinsAccordingToPartitioner() {
        final FlowFilePartitioner partitioner = new StaticFlowFilePartitioner(1);
        queue.setFlowFilePartitioner(partitioner);

        final QueuePartition desiredPartition = queue.getPartition(1);
        for (int i = 0; i < 100; i++) {
            final MockFlowFileRecord flowFile = new MockFlowFileRecord(0L);
            final QueuePartition partition = queue.putAndGetPartition(flowFile);
            assertSame(desiredPartition, partition);
        }
    }

    @Test
    public void testPutAllBinsFlowFilesSeparately() {
        // Partition data based on size. FlowFiles with 0 bytes will go to partition 0 (local partition),
        // FlowFiles with 1 byte will go to partition 1, and FlowFiles with 2 bytes will go to partition 2.
        final FlowFilePartitioner partitioner = new FlowFileSizePartitioner();
        queue.setFlowFilePartitioner(partitioner);

        // Add 3 FlowFiles for each size
        final List<FlowFileRecord> flowFiles = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            flowFiles.add(new MockFlowFileRecord(0));
            flowFiles.add(new MockFlowFileRecord(1));
            flowFiles.add(new MockFlowFileRecord(2));
        }

        final Map<QueuePartition, List<FlowFileRecord>> partitionMap = queue.putAllAndGetPartitions(flowFiles);
        assertEquals(3, partitionMap.size());

        // For each partition, get the List of FlowFiles added to it, then verify that there are 3 FlowFiles with that size.
        for (int i = 0; i < 3; i++) {
            final QueuePartition partition = queue.getPartition(i);
            final List<FlowFileRecord> flowFilesForPartition = partitionMap.get(partition);
            assertNotNull(flowFilesForPartition);
            assertEquals(3, flowFilesForPartition.size());

            for (final FlowFileRecord flowFile : flowFilesForPartition) {
                assertEquals(i, flowFile.getSize());
            }
        }
    }

    private int determineRemotePartitionIndex() {
        final QueuePartition localPartition = queue.getLocalPartition();
        if (queue.getPartition(0) == localPartition) {
            return 1;
        } else {
            return 0;
        }
    }

    private int determineLocalPartitionIndex() {
        final QueuePartition localPartition = queue.getLocalPartition();
        for (int i=0; i < clusterCoordinator.getNodeIdentifiers().size(); i++) {
            if (queue.getPartition(i) == localPartition) {
                return i;
            }
        }

        throw new IllegalStateException("Could not determine local partition index");
    }

    @Test
    public void testIsEmptyWhenFlowFileInRemotePartition() {
        queue.setFlowFilePartitioner(new StaticFlowFilePartitioner(determineRemotePartitionIndex()));

        assertTrue(queue.isEmpty());
        assertTrue(queue.isActiveQueueEmpty());
        assertEquals(new QueueSize(0, 0L), queue.size());

        queue.put(new MockFlowFileRecord(0L));
        assertFalse(queue.isEmpty());
        assertTrue(queue.isActiveQueueEmpty());
        assertEquals(new QueueSize(1, 0L), queue.size());

        assertNull(queue.poll(new HashSet<>()));
        assertFalse(queue.isEmpty());
        assertTrue(queue.isActiveQueueEmpty());
        assertEquals(new QueueSize(1, 0L), queue.size());
    }

    @Test
    public void testIsEmptyWhenFlowFileInLocalPartition() {
        queue.setFlowFilePartitioner(new StaticFlowFilePartitioner(determineLocalPartitionIndex()));

        // Ensure queue is empty
        assertTrue(queue.isEmpty());
        assertTrue(queue.isActiveQueueEmpty());
        assertEquals(new QueueSize(0, 0L), queue.size());

        // add a flowfile
        final FlowFileRecord flowFile = new MockFlowFileRecord(0L);
        queue.put(flowFile);
        assertFalse(queue.isEmpty());
        assertFalse(queue.isActiveQueueEmpty());
        assertEquals(new QueueSize(1, 0L), queue.size());

        // Ensure that we get the same FlowFile back. This will not decrement
        // the queue size, only acknowledging the FlowFile will do that.
        assertSame(flowFile, queue.poll(new HashSet<>()));
        assertFalse(queue.isEmpty());
        assertTrue(queue.isActiveQueueEmpty());
        assertEquals(new QueueSize(1, 0L), queue.size());

        // Acknowledging FlowFile should reduce queue size
        queue.acknowledge(flowFile);
        assertTrue(queue.isEmpty());
        assertTrue(queue.isActiveQueueEmpty());
        assertEquals(new QueueSize(0, 0L), queue.size());

        // Add FlowFile back in, poll it to ensure that we get it back, and
        // then acknowledge as a Collection and ensure the correct sizes.
        queue.put(flowFile);
        assertFalse(queue.isEmpty());
        assertFalse(queue.isActiveQueueEmpty());
        assertEquals(new QueueSize(1, 0L), queue.size());

        assertSame(flowFile, queue.poll(new HashSet<>()));
        assertFalse(queue.isEmpty());
        assertTrue(queue.isActiveQueueEmpty());
        assertEquals(new QueueSize(1, 0L), queue.size());

        queue.acknowledge(Collections.singleton(flowFile));
        assertTrue(queue.isEmpty());
        assertTrue(queue.isActiveQueueEmpty());
        assertEquals(new QueueSize(0, 0L), queue.size());
    }

    @Test
    public void testGetFlowFile() throws IOException {
        queue.setFlowFilePartitioner(new FlowFileSizePartitioner());

        final Map<String, String> localAttributes = Collections.singletonMap("uuid", "local");
        final MockFlowFileRecord localFlowFile = new MockFlowFileRecord(localAttributes, determineLocalPartitionIndex());

        final Map<String, String> remoteAttributes = Collections.singletonMap("uuid", "remote");
        final MockFlowFileRecord remoteFlowFile = new MockFlowFileRecord(remoteAttributes, determineRemotePartitionIndex());

        queue.put(localFlowFile);
        queue.put(remoteFlowFile);

        assertSame(localFlowFile, queue.getFlowFile("local"));
        assertNull(queue.getFlowFile("remote"));
        assertNull(queue.getFlowFile("other"));
    }


    @Test
    public void testRecoverSwapFiles() throws IOException {
        for (int partitionIndex = 0; partitionIndex < 3; partitionIndex++) {
            final String partitionName = queue.getPartition(partitionIndex).getSwapPartitionName();

            final List<FlowFileRecord> flowFiles = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                flowFiles.add(new MockFlowFileRecord(100L));
            }

            swapManager.swapOut(flowFiles, queue, partitionName);
        }

        final List<FlowFileRecord> flowFiles = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            flowFiles.add(new MockFlowFileRecord(100L));
        }

        swapManager.swapOut(flowFiles, queue, "other-partition");

        final SwapSummary swapSummary = queue.recoverSwappedFlowFiles();
        assertEquals(399L, swapSummary.getMaxFlowFileId().longValue());
        assertEquals(400, swapSummary.getQueueSize().getObjectCount());
        assertEquals(400 * 100L, swapSummary.getQueueSize().getByteCount());
    }


    @Test(timeout = 10000)
    public void testChangeInClusterTopologyTriggersRebalance() throws InterruptedException {
        // Create partitioner that sends first 2 FlowFiles to Partition 0, next 2 to Partition 1, and then next 4 to Partition 3.
        queue.setFlowFilePartitioner(new StaticSequencePartitioner(new int[] {0, 0, 1, 1, 3, 3, 3, 3}, true));

        for (int i = 0; i < 4; i++) {
            queue.put(new MockFlowFileRecord());
        }

        assertEquals(2, queue.getPartition(0).size().getObjectCount());
        assertEquals(2, queue.getPartition(1).size().getObjectCount());
        assertEquals(0, queue.getPartition(2).size().getObjectCount());

        final Set<NodeIdentifier> updatedNodeIdentifiers = new HashSet<>(nodeIds);
        // Add a Node Identifier with an of ID consisting of a bunch of Z's so that the new partition will be Partition Number 3.
        updatedNodeIdentifiers.add(new NodeIdentifier("ZZZZZZZZZZZZZZ", "localhost", nodePort++, "localhost", nodePort++,
            "localhost", nodePort++, "localhost", nodePort++, nodePort++, true, Collections.emptySet()));

        queue.setNodeIdentifiers(updatedNodeIdentifiers, false);

        final int[] expectedPartitionSizes = new int[] {0, 0, 0, 4};
        final int[] partitionSizes = new int[4];
        while (!Arrays.equals(expectedPartitionSizes, partitionSizes)) {
            Thread.sleep(10L);

            for (int i = 0; i < 4; i++) {
                partitionSizes[i] = queue.getPartition(i).size().getObjectCount();
            }
        }
    }


    @Test(timeout = 30000)
    public void testChangeInClusterTopologyTriggersRebalanceOnlyOnRemovedNodeIfNecessary() throws InterruptedException {
        // Create partitioner that sends first 1 FlowFile to Partition 0, next to Partition 2, and then next 2 to Partition 2.
        // Then, cycle back to partitions 0 and 1. This will result in partitions 0 & 1 getting 1 FlowFile each and Partition 2
        // getting 2 FlowFiles. Then, when Partition 2 is removed, those 2 FlowFiles will be rebalanced to Partitions 0 and 1.
        queue.setFlowFilePartitioner(new StaticSequencePartitioner(new int[] {0, 1, 2, 2, 0, 1}, false));

        for (int i = 0; i < 4; i++) {
            queue.put(new MockFlowFileRecord());
        }

        assertEquals(1, queue.getPartition(0).size().getObjectCount());
        assertEquals(1, queue.getPartition(1).size().getObjectCount());
        assertEquals(2, queue.getPartition(2).size().getObjectCount());

        final Set<NodeIdentifier> updatedNodeIdentifiers = new HashSet<>();
        updatedNodeIdentifiers.add(nodeIds.get(0));
        updatedNodeIdentifiers.add(nodeIds.get(1));
        queue.setNodeIdentifiers(updatedNodeIdentifiers, false);

        final int[] expectedPartitionSizes = new int[] {2, 2};
        final int[] partitionSizes = new int[2];

        while (!Arrays.equals(expectedPartitionSizes, partitionSizes)) {
            Thread.sleep(10L);

            for (int i = 0; i < 2; i++) {
                partitionSizes[i] = queue.getPartition(i).size().getObjectCount();
            }
        }
    }

    @Test(timeout = 10000)
    public void testChangeInPartitionerTriggersRebalance() throws InterruptedException {
        // Create partitioner that sends first 2 FlowFiles to Partition 0, next 2 to Partition 1, and then next 4 to Partition 3.
        queue.setFlowFilePartitioner(new StaticSequencePartitioner(new int[] {0, 1, 0, 1}, false));

        for (int i = 0; i < 4; i++) {
            queue.put(new MockFlowFileRecord());
        }

        assertEquals(2, queue.getPartition(0).size().getObjectCount());
        assertEquals(2, queue.getPartition(1).size().getObjectCount());
        assertEquals(0, queue.getPartition(2).size().getObjectCount());

        queue.setFlowFilePartitioner(new StaticSequencePartitioner(new int[] {0, 1, 2, 2}, true));

        final int[] expectedPartitionSizes = new int[] {1, 1, 2};
        assertPartitionSizes(expectedPartitionSizes);
    }

    @Test(timeout = 100000)
    public void testLocalNodeIdentifierSet() throws InterruptedException {
        nodeIds.clear();

        final NodeIdentifier id1 = createNodeIdentifier();
        final NodeIdentifier id2 = createNodeIdentifier();
        final NodeIdentifier id3 = createNodeIdentifier();
        nodeIds.add(id1);
        nodeIds.add(id2);
        nodeIds.add(id3);

        when(clusterCoordinator.getLocalNodeIdentifier()).thenReturn(null);

        final AsyncLoadBalanceClientRegistry registry = mock(AsyncLoadBalanceClientRegistry.class);
        queue = new SocketLoadBalancedFlowFileQueue("unit-test", new NopConnectionEventListener(), mock(ProcessScheduler.class), flowFileRepo, provRepo,
                contentRepo, claimManager, clusterCoordinator, registry, swapManager, 10000, eventReporter);

        queue.setFlowFilePartitioner(new RoundRobinPartitioner());

        // Queue up data without knowing the local node id.
        final Map<String, String> attributes = new HashMap<>();
        for (int i=0; i < 6; i++) {
            attributes.put("i", String.valueOf(i));
            queue.put(new MockFlowFileRecord(attributes, 0));
        }

        for (int i=0; i < 3; i++) {
            assertEquals(2, queue.getPartition(i).size().getObjectCount());
        }

        assertEquals(0, queue.getLocalPartition().size().getObjectCount());

        when(clusterCoordinator.getLocalNodeIdentifier()).thenReturn(id1);
        clusterTopologyEventListener.onLocalNodeIdentifierSet(id1);

        assertPartitionSizes(new int[] {2, 2, 2});

        while (queue.getLocalPartition().size().getObjectCount() != 2) {
            Thread.sleep(10L);
        }
    }

    private void assertPartitionSizes(final int[] expectedSizes) {
        final int[] partitionSizes = new int[queue.getPartitionCount()];
        while (!Arrays.equals(expectedSizes, partitionSizes)) {
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                Assert.fail("Interrupted");
            }

            for (int i = 0; i < partitionSizes.length; i++) {
                partitionSizes[i] = queue.getPartition(i).size().getObjectCount();
            }
        }
    }


    private static class StaticFlowFilePartitioner implements FlowFilePartitioner {
        private final int partitionIndex;

        public StaticFlowFilePartitioner(final int partition) {
            this.partitionIndex = partition;
        }

        @Override
        public QueuePartition getPartition(final FlowFileRecord flowFile, final QueuePartition[] partitions, final QueuePartition localPartition) {
            return partitions[partitionIndex];
        }

        @Override
        public boolean isRebalanceOnClusterResize() {
            return false;
        }

        @Override
        public boolean isRebalanceOnFailure() {
            return false;
        }
    }

    private static class FlowFileSizePartitioner implements FlowFilePartitioner {
        @Override
        public QueuePartition getPartition(final FlowFileRecord flowFile, final QueuePartition[] partitions, final QueuePartition localPartition) {
            return partitions[(int) flowFile.getSize()];
        }

        @Override
        public boolean isRebalanceOnClusterResize() {
            return false;
        }

        @Override
        public boolean isRebalanceOnFailure() {
            return false;
        }
    }

    private static class StaticSequencePartitioner implements FlowFilePartitioner {
        private final int[] partitionIndices;
        private final boolean requireRebalance;
        private int index = 0;

        public StaticSequencePartitioner(final int[] partitions, final boolean requireRebalance) {
            this.partitionIndices = partitions;
            this.requireRebalance = requireRebalance;
        }

        @Override
        public QueuePartition getPartition(final FlowFileRecord flowFile, final QueuePartition[] partitions, final QueuePartition localPartition) {
            final int partitionIndex = partitionIndices[index++];
            return partitions[partitionIndex];
        }

        @Override
        public boolean isRebalanceOnClusterResize() {
            return requireRebalance;
        }

        @Override
        public boolean isRebalanceOnFailure() {
            return false;
        }
    }
}
