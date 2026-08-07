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
import org.apache.nifi.controller.queue.BlockingSwappablePriorityQueue;
import org.apache.nifi.controller.queue.LoadBalancedFlowFileQueue;
import org.apache.nifi.controller.queue.PollStrategy;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.events.EventReporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestStandardRebalancingPartition {
    private LoadBalancedFlowFileQueue flowFileQueue;
    private ControllableBlockingQueue queue;
    private StandardRebalancingPartition partition;

    @BeforeEach
    void setUp() {
        flowFileQueue = mock(LoadBalancedFlowFileQueue.class);
        when(flowFileQueue.getIdentifier()).thenReturn("unit-test");
        queue = new ControllableBlockingQueue(flowFileQueue);
        partition = new StandardRebalancingPartition(queue, flowFileQueue);
    }

    @Test
    @Timeout(10)
    void testStopInterruptsBlockedPollAndWaitsForWorker() throws Exception {
        final BlockingPollQueue blockingQueue = new BlockingPollQueue(flowFileQueue);
        final StandardRebalancingPartition blockingPartition = new StandardRebalancingPartition(blockingQueue, flowFileQueue);
        blockingPartition.start(mock(FlowFilePartitioner.class));
        assertTrue(blockingQueue.awaitPoll());

        try (final ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<?> stopFuture = executor.submit(blockingPartition::stop);
            assertTrue(blockingQueue.awaitStopInterrupt());
            assertFalse(stopFuture.isDone());

            blockingQueue.releasePoll();
            stopFuture.get(5, TimeUnit.SECONDS);
        }

        verify(flowFileQueue, never()).distributeToPartitions(org.mockito.ArgumentMatchers.anyCollection());
    }

    @Test
    @Timeout(10)
    void testStopRestoresPolledFlowFilesBeforeRestart() throws Exception {
        final FlowFileRecord flowFile = new MockFlowFileRecord(10L);
        partition.rebalance(List.of(flowFile));
        partition.start(mock(FlowFilePartitioner.class));

        assertTrue(queue.awaitBatchPoll());

        try (final ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<?> stopFuture = executor.submit(partition::stop);
            assertTrue(queue.awaitStopInterrupt());
            queue.releaseBatchPoll();
            stopFuture.get(5, TimeUnit.SECONDS);
        }

        verify(flowFileQueue, never()).distributeToPartitions(org.mockito.ArgumentMatchers.anyCollection());
        assertEquals(new QueueSize(1, 10L), partition.size());

        partition.start(mock(FlowFilePartitioner.class));
        verify(flowFileQueue, timeout(5000).times(1)).distributeToPartitions(List.of(flowFile));
        partition.stop();
        assertEquals(new QueueSize(0, 0L), partition.size());
    }

    @Test
    @Timeout(10)
    void testStartWaitsForStopToComplete() throws Exception {
        final FlowFileRecord flowFile = new MockFlowFileRecord(10L);
        partition.rebalance(List.of(flowFile));
        partition.start(mock(FlowFilePartitioner.class));
        assertTrue(queue.awaitBatchPoll());

        try (final ExecutorService executor = Executors.newFixedThreadPool(2)) {
            final Future<?> stopFuture = executor.submit(partition::stop);
            assertTrue(queue.awaitStopInterrupt());

            final CountDownLatch startInvoked = new CountDownLatch(1);
            final Future<?> startFuture = executor.submit(() -> {
                startInvoked.countDown();
                partition.start(mock(FlowFilePartitioner.class));
            });
            assertTrue(startInvoked.await(5, TimeUnit.SECONDS));
            verify(flowFileQueue, never()).distributeToPartitions(org.mockito.ArgumentMatchers.anyCollection());
            assertFalse(startFuture.isDone());

            queue.releaseBatchPoll();
            stopFuture.get(5, TimeUnit.SECONDS);
            startFuture.get(5, TimeUnit.SECONDS);
        }

        verify(flowFileQueue, timeout(5000).times(1)).distributeToPartitions(List.of(flowFile));
        partition.stop();
        assertEquals(new QueueSize(0, 0L), partition.size());
    }

    private static class ControllableBlockingQueue extends BlockingSwappablePriorityQueue {
        private final CountDownLatch batchPollEntered = new CountDownLatch(1);
        private final CountDownLatch stopInterruptObserved = new CountDownLatch(1);
        private final CountDownLatch releaseBatchPoll = new CountDownLatch(1);
        private final AtomicBoolean blockBatchPoll = new AtomicBoolean(true);

        ControllableBlockingQueue(final LoadBalancedFlowFileQueue flowFileQueue) {
            super(new MockSwapManager(), 10000, EventReporter.NO_OP, flowFileQueue, (flowFiles, requestor) -> new QueueSize(0, 0L), "rebalance");
        }

        @Override
        public List<FlowFileRecord> poll(final int maxResults, final Set<FlowFileRecord> expiredRecords,
                                        final long expirationMillis, final PollStrategy pollStrategy) {
            if (blockBatchPoll.compareAndSet(true, false)) {
                batchPollEntered.countDown();
                boolean interrupted = false;
                while (true) {
                    try {
                        releaseBatchPoll.await();
                        break;
                    } catch (final InterruptedException e) {
                        interrupted = true;
                        stopInterruptObserved.countDown();
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }

            return Collections.emptyList();
        }

        boolean awaitBatchPoll() throws InterruptedException {
            return batchPollEntered.await(5, TimeUnit.SECONDS);
        }

        boolean awaitStopInterrupt() throws InterruptedException {
            return stopInterruptObserved.await(5, TimeUnit.SECONDS);
        }

        void releaseBatchPoll() {
            releaseBatchPoll.countDown();
        }
    }

    private static class BlockingPollQueue extends BlockingSwappablePriorityQueue {
        private final CountDownLatch pollEntered = new CountDownLatch(1);
        private final CountDownLatch stopInterruptObserved = new CountDownLatch(1);
        private final CountDownLatch releasePoll = new CountDownLatch(1);

        BlockingPollQueue(final LoadBalancedFlowFileQueue flowFileQueue) {
            super(new MockSwapManager(), 10000, EventReporter.NO_OP, flowFileQueue, (flowFiles, requestor) -> new QueueSize(0, 0L), "rebalance");
        }

        @Override
        public FlowFileRecord poll(final Set<FlowFileRecord> expiredRecords, final long expirationMillis,
                                   final long waitMillis, final PollStrategy pollStrategy) throws InterruptedException {
            pollEntered.countDown();
            boolean interrupted = false;
            while (true) {
                try {
                    releasePoll.await();
                    break;
                } catch (final InterruptedException e) {
                    interrupted = true;
                    stopInterruptObserved.countDown();
                }
            }
            if (interrupted) {
                throw new InterruptedException();
            }
            return null;
        }

        boolean awaitPoll() throws InterruptedException {
            return pollEntered.await(5, TimeUnit.SECONDS);
        }

        boolean awaitStopInterrupt() throws InterruptedException {
            return stopInterruptObserved.await(5, TimeUnit.SECONDS);
        }

        void releasePoll() {
            releasePoll.countDown();
        }
    }
}
