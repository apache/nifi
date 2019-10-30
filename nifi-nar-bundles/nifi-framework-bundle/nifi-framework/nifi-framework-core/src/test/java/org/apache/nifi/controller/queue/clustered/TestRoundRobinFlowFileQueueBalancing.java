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
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.MockFlowFileRecord;
import org.apache.nifi.controller.MockSwapManager;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.controller.queue.NopConnectionEventListener;
import org.apache.nifi.controller.queue.clustered.client.async.AsyncLoadBalanceClientRegistry;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRoundRobinFlowFileQueueBalancing {

    private Connection connection;
    private FlowFileRepository flowFileRepo;
    private ContentRepository contentRepo;
    private ProvenanceEventRepository provRepo;
    private ResourceClaimManager claimManager;
    private ClusterCoordinator clusterCoordinator;
    private MockSwapManager swapManager;
    private EventReporter eventReporter;
    private SocketLoadBalancedFlowFileQueue queue;

    private List<NodeIdentifier> nodeIds;
    private int nodePort = 4096;

    private NodeIdentifier localNodeIdentifier;
    private NodeIdentifier remoteNodeIdentifier1;
    private NodeIdentifier remoteNodeIdentifier2;

    private int backPressureObjectThreshold = 10;

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

        localNodeIdentifier = createNodeIdentifier("00000000-0000-0000-0000-000000000000");
        remoteNodeIdentifier1 = createNodeIdentifier("11111111-1111-1111-1111-111111111111");
        remoteNodeIdentifier2 = createNodeIdentifier("22222222-2222-2222-2222-222222222222");

        nodeIds = new ArrayList<>();
        nodeIds.add(localNodeIdentifier);
        nodeIds.add(remoteNodeIdentifier1);
        nodeIds.add(remoteNodeIdentifier2);

        doAnswer((Answer<Set<NodeIdentifier>>) invocation -> new HashSet<>(nodeIds)).when(clusterCoordinator).getNodeIdentifiers();
        when(clusterCoordinator.getLocalNodeIdentifier()).thenReturn(localNodeIdentifier);

        final ProcessScheduler scheduler = mock(ProcessScheduler.class);

        final AsyncLoadBalanceClientRegistry registry = mock(AsyncLoadBalanceClientRegistry.class);
        queue = new SocketLoadBalancedFlowFileQueue("unit-test", new NopConnectionEventListener(), scheduler, flowFileRepo, provRepo,
                contentRepo, claimManager, clusterCoordinator, registry, swapManager, 10000, eventReporter);


        queue.setLoadBalanceStrategy(LoadBalanceStrategy.ROUND_ROBIN, null);
        queue.setBackPressureObjectThreshold(backPressureObjectThreshold);
    }

    private NodeIdentifier createNodeIdentifier(final String uuid) {
        return new NodeIdentifier(uuid, "localhost", nodePort++, "localhost", nodePort++,
                "localhost", nodePort++, "localhost", nodePort++, nodePort++, true, Collections.emptySet());
    }

    @Test
    public void testIsFullShouldReturnFalseWhenLocalIsFullRemotesAreNot() {
        // GIVEN
        boolean expected = false;
        int[] expectedPartitionSizes = {10, 0, 0};

        // WHEN
        IntStream.rangeClosed(1, backPressureObjectThreshold).forEach(__ -> queue.getPartition(0).put(new MockFlowFileRecord(0L)));

        // THEN
        testIsFull(expected, expectedPartitionSizes);
    }

    @Test
    public void testIsFullShouldReturnFalseWhenLocalAndOneRemoteIsFullOtherRemoteIsNot() {
        // GIVEN
        boolean expected = false;
        int[] expectedPartitionSizes = {10, 10, 0};

        // WHEN
        IntStream.rangeClosed(1, backPressureObjectThreshold).forEach(__ -> queue.getPartition(0).put(new MockFlowFileRecord(0L)));
        IntStream.rangeClosed(1, backPressureObjectThreshold).forEach(__ -> queue.getPartition(1).put(new MockFlowFileRecord(0L)));

        // THEN
        testIsFull(expected, expectedPartitionSizes);
    }

    @Test
    public void testIsFullShouldReturnTrueWhenAllPartitionsAreFull() {
        // GIVEN
        boolean expected = true;
        int[] expectedPartitionSizes = {10, 10, 10};

        // WHEN
        IntStream.rangeClosed(1, backPressureObjectThreshold).forEach(__ -> queue.getPartition(0).put(new MockFlowFileRecord(0L)));
        IntStream.rangeClosed(1, backPressureObjectThreshold).forEach(__ -> queue.getPartition(1).put(new MockFlowFileRecord(0L)));
        IntStream.rangeClosed(1, backPressureObjectThreshold).forEach(__ -> queue.getPartition(2).put(new MockFlowFileRecord(0L)));

        // THEN
        testIsFull(expected, expectedPartitionSizes);
    }

    @Test
    public void testBalancingWhenAllPartitionsAreEmpty() {
        // GIVEN
        int[] expectedPartitionSizes = {3, 3, 3};

        // WHEN
        IntStream.rangeClosed(1, 9).forEach(__ -> queue.put(new MockFlowFileRecord(0L)));

        // THEN
        assertPartitionSizes(expectedPartitionSizes);
    }

    @Test
    public void testBalancingWhenLocalPartitionIsFull() {
        // GIVEN
        int[] expectedPartitionSizes = {10, 2, 2};

        IntStream.rangeClosed(1, backPressureObjectThreshold).forEach(__ -> queue.getPartition(0).put(new MockFlowFileRecord(0L)));

        // WHEN
        IntStream.rangeClosed(1, 4).forEach(__ -> queue.put(new MockFlowFileRecord(0L)));

        // THEN
        assertPartitionSizes(expectedPartitionSizes);
    }

    private void testIsFull(boolean expected, int[] expectedPartitionSizes) {
        // GIVEN

        // WHEN
        boolean actual = queue.isFull();

        // THEN
        assertEquals(expected, actual);
        assertPartitionSizes(expectedPartitionSizes);
    }

    private void assertPartitionSizes(final int[] expectedSizes) {
        final int[] partitionSizes = new int[queue.getPartitionCount()];

        for (int i = 0; i < partitionSizes.length; i++) {
            partitionSizes[i] = queue.getPartition(i).size().getObjectCount();
        }

        assertArrayEquals(expectedSizes, partitionSizes);
    }
}
