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

package org.apache.nifi.controller.queue.clustered.partition;

import org.apache.nifi.controller.MockFlowFileRecord;
import org.apache.nifi.controller.MockSwapManager;
import org.apache.nifi.controller.queue.DropFlowFileAction;
import org.apache.nifi.controller.queue.LoadBalancedFlowFileQueue;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.queue.SwappablePriorityQueue;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.events.EventReporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StandardRebalancingPartitionTest {
    private static final String QUEUE_IDENTIFIER = "unit-test";
    private static final String REBALANCE_PARTITION_NAME = "rebalance";
    private static final int SWAP_THRESHOLD = 10_000;
    private static final long DISTRIBUTION_TIMEOUT_SECONDS = 30L;

    private final BlockingQueue<List<FlowFileRecord>> distributedBatches = new LinkedBlockingQueue<>();

    private LoadBalancedFlowFileQueue flowFileQueue;
    private MockSwapManager swapManager;
    private SwappablePriorityQueue queue;

    @BeforeEach
    void setup() {
        MockFlowFileRecord.resetIdGenerator();
        distributedBatches.clear();

        flowFileQueue = mock(LoadBalancedFlowFileQueue.class);
        when(flowFileQueue.getIdentifier()).thenReturn(QUEUE_IDENTIFIER);
        doAnswer(invocation -> {
            final Collection<FlowFileRecord> flowFiles = invocation.getArgument(0);
            distributedBatches.add(new ArrayList<>(flowFiles));
            return null;
        }).when(flowFileQueue).distributeToPartitions(anyCollection());

        swapManager = new MockSwapManager();

        final DropFlowFileAction dropAction = (flowFiles, requestor) -> new QueueSize(flowFiles.size(), 0L);
        queue = new SwappablePriorityQueue(swapManager, SWAP_THRESHOLD, EventReporter.NO_OP, flowFileQueue, dropAction, REBALANCE_PARTITION_NAME);
    }

    @Test
    void testFlowFilesOfferedWhileStoppedRetainedUntilStarted() throws InterruptedException {
        final ControlledExecutor executor = new ControlledExecutor();
        final StandardRebalancingPartition partition = new StandardRebalancingPartition(queue, flowFileQueue, executor);

        partition.start(new LocalPartitionPartitioner());
        assertEquals(1, executor.getPendingTaskCount());

        executor.runPendingTasks();
        assertTrue(distributedBatches.isEmpty());

        partition.stop();

        final List<FlowFileRecord> offeredWhileStopped = List.of(new MockFlowFileRecord(), new MockFlowFileRecord());
        partition.rebalance(offeredWhileStopped);

        assertEquals(0, executor.getPendingTaskCount());
        assertTrue(distributedBatches.isEmpty());
        assertEquals(offeredWhileStopped.size(), partition.size().getObjectCount());

        partition.start(new LocalPartitionPartitioner());
        assertEquals(1, executor.getPendingTaskCount());

        executor.runPendingTasks();
        assertNextDistribution(offeredWhileStopped);
        assertTrue(distributedBatches.isEmpty());
        assertEquals(0, partition.size().getObjectCount());
    }

    @Test
    @Timeout(60)
    void testStopDoesNotWaitForActiveDistributionAndRetiresIt() throws Exception {
        final CountDownLatch distributionStarted = new CountDownLatch(1);
        final CountDownLatch distributionRelease = new CountDownLatch(1);

        doAnswer(invocation -> {
            final Collection<FlowFileRecord> flowFiles = invocation.getArgument(0);
            distributedBatches.add(new ArrayList<>(flowFiles));

            if (distributionStarted.getCount() > 0) {
                distributionStarted.countDown();
                distributionRelease.await();
            }

            return null;
        }).when(flowFileQueue).distributeToPartitions(anyCollection());

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            final StandardRebalancingPartition partition = new StandardRebalancingPartition(queue, flowFileQueue, executorService);
            partition.start(new LocalPartitionPartitioner());

            final List<FlowFileRecord> firstBatch = List.of(new MockFlowFileRecord());
            partition.rebalance(firstBatch);
            assertTrue(distributionStarted.await(DISTRIBUTION_TIMEOUT_SECONDS, TimeUnit.SECONDS));

            partition.stop();

            final List<FlowFileRecord> offeredWhileStopped = List.of(new MockFlowFileRecord(), new MockFlowFileRecord());
            partition.rebalance(offeredWhileStopped);

            distributionRelease.countDown();
            assertNextDistribution(firstBatch);

            awaitWorkerIdle(executorService);
            assertTrue(distributedBatches.isEmpty());
            assertEquals(offeredWhileStopped.size(), partition.size().getObjectCount());

            partition.start(new LocalPartitionPartitioner());
            assertNextDistribution(offeredWhileStopped);

            awaitWorkerIdle(executorService);
            assertTrue(distributedBatches.isEmpty());
            assertEquals(0, partition.size().getObjectCount());
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void testRepeatedOffersSubmitSingleTask() throws InterruptedException {
        final ControlledExecutor executor = new ControlledExecutor();
        final StandardRebalancingPartition partition = new StandardRebalancingPartition(queue, flowFileQueue, executor);

        partition.start(new LocalPartitionPartitioner());
        executor.runPendingTasks();

        final List<FlowFileRecord> flowFiles = List.of(new MockFlowFileRecord(), new MockFlowFileRecord(), new MockFlowFileRecord(), new MockFlowFileRecord());
        partition.rebalance(List.of(flowFiles.get(0)));
        partition.rebalance(List.of(flowFiles.get(1)));
        partition.put(flowFiles.get(2));
        partition.putAll(List.of(flowFiles.get(3)));

        assertEquals(1, executor.getPendingTaskCount());

        executor.runPendingTasks();
        assertNextDistribution(flowFiles);
        assertTrue(distributedBatches.isEmpty());
        assertEquals(0, partition.size().getObjectCount());
    }

    @Test
    void testPutAndPutAllDistributeFlowFiles() throws InterruptedException {
        final ControlledExecutor executor = new ControlledExecutor();
        final StandardRebalancingPartition partition = new StandardRebalancingPartition(queue, flowFileQueue, executor);

        partition.start(new LocalPartitionPartitioner());
        executor.runPendingTasks();

        final FlowFileRecord single = new MockFlowFileRecord();
        partition.put(single);
        assertEquals(1, executor.getPendingTaskCount());

        executor.runPendingTasks();
        assertNextDistribution(List.of(single));

        final List<FlowFileRecord> multiple = List.of(new MockFlowFileRecord(), new MockFlowFileRecord());
        partition.putAll(multiple);
        assertEquals(1, executor.getPendingTaskCount());

        executor.runPendingTasks();
        assertNextDistribution(multiple);
        assertEquals(0, partition.size().getObjectCount());
    }

    @Test
    void testRecoveredSwapFilesDistributed() throws Exception {
        final List<FlowFileRecord> swappedFlowFiles = List.of(new MockFlowFileRecord(), new MockFlowFileRecord(), new MockFlowFileRecord());
        swapManager.swapOut(swappedFlowFiles, flowFileQueue, REBALANCE_PARTITION_NAME);

        final ControlledExecutor executor = new ControlledExecutor();
        final StandardRebalancingPartition partition = new StandardRebalancingPartition(queue, flowFileQueue, executor);

        partition.start(new LocalPartitionPartitioner());
        executor.runPendingTasks();

        final SwapSummary swapSummary = partition.recoverSwappedFlowFiles();
        assertEquals(swappedFlowFiles.size(), swapSummary.getQueueSize().getObjectCount());
        assertEquals(1, executor.getPendingTaskCount());

        executor.runPendingTasks();
        assertNextDistribution(swappedFlowFiles);
        assertEquals(0, partition.size().getObjectCount());
    }

    private void assertNextDistribution(final Collection<FlowFileRecord> expectedFlowFiles) throws InterruptedException {
        final List<FlowFileRecord> distributed = distributedBatches.poll(DISTRIBUTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(distributed);
        assertEquals(expectedFlowFiles.size(), distributed.size());
        assertTrue(distributed.containsAll(expectedFlowFiles));
    }

    private void awaitWorkerIdle(final ExecutorService executorService) throws Exception {
        // The single worker thread reaches this no-op only after any previously submitted Rebalance Task has returned
        executorService.submit(() -> {
        }).get(DISTRIBUTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private static class ControlledExecutor implements Executor {
        private final List<Runnable> pendingTasks = new ArrayList<>();

        @Override
        public void execute(final Runnable task) {
            pendingTasks.add(task);
        }

        private int getPendingTaskCount() {
            return pendingTasks.size();
        }

        private void runPendingTasks() {
            final List<Runnable> tasksToRun = new ArrayList<>(pendingTasks);
            pendingTasks.clear();

            for (final Runnable task : tasksToRun) {
                task.run();
            }
        }
    }
}
