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

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.queue.DropFlowFileAction;
import org.apache.nifi.controller.queue.DropFlowFileRequest;
import org.apache.nifi.controller.queue.FlowFileQueueContents;
import org.apache.nifi.controller.queue.LoadBalancedFlowFileQueue;
import org.apache.nifi.controller.queue.PollStrategy;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.queue.SelectiveDropResult;
import org.apache.nifi.controller.queue.SwappablePriorityQueue;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

public class StandardRebalancingPartition implements RebalancingPartition {
    private static final String DESCRIPTION_FORMAT = "RebalancingPartition[queueId=%s]";
    private static final String SWAP_PARTITION_NAME = "rebalance";
    private static final int DISTRIBUTION_BATCH_SIZE = 1000;
    private static final long NO_EXPIRATION = -1L;
    private static final long IDLE_THREAD_TIMEOUT_SECONDS = 15L;
    private static final long UNPOLLABLE_RETRY_MILLIS = 1000L;

    private static final Logger logger = LoggerFactory.getLogger(StandardRebalancingPartition.class);

    private final SwappablePriorityQueue queue;
    private final LoadBalancedFlowFileQueue flowFileQueue;
    private final Executor rebalanceExecutor;
    private final String description;

    private final ReentrantLock lifecycleLock = new ReentrantLock();
    private boolean running;
    private RebalanceTask rebalanceTask;

    public StandardRebalancingPartition(
            final FlowFileSwapManager swapManager,
            final int swapThreshold,
            final EventReporter eventReporter,
            final LoadBalancedFlowFileQueue flowFileQueue,
            final DropFlowFileAction dropAction
    ) {
        this(
                new SwappablePriorityQueue(swapManager, swapThreshold, eventReporter, flowFileQueue, dropAction, SWAP_PARTITION_NAME),
                flowFileQueue,
                createExecutor(DESCRIPTION_FORMAT.formatted(flowFileQueue.getIdentifier()))
        );
    }

    StandardRebalancingPartition(
            final SwappablePriorityQueue queue,
            final LoadBalancedFlowFileQueue flowFileQueue,
            final Executor rebalanceExecutor
    ) {
        this.queue = queue;
        this.flowFileQueue = flowFileQueue;
        this.rebalanceExecutor = rebalanceExecutor;
        this.description = DESCRIPTION_FORMAT.formatted(flowFileQueue.getIdentifier());
    }

    private static Executor createExecutor(final String threadName) {
        final ThreadFactory threadFactory = runnable -> {
            final Thread thread = new Thread(runnable);
            thread.setName(threadName);
            thread.setDaemon(true);
            return thread;
        };

        // One Thread enforces one Rebalance Task per Partition
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, IDLE_THREAD_TIMEOUT_SECONDS, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), threadFactory);
        // Allowing Core Thread Time Out means that a Connection with nothing to rebalance holds no threads
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    @Override
    public Optional<NodeIdentifier> getNodeIdentifier() {
        return Optional.empty();
    }

    @Override
    public QueueSize size() {
        return queue.size();
    }

    @Override
    public void lockForSnapshot() {
        queue.lockForSnapshot();
    }

    @Override
    public void unlockForSnapshot() {
        queue.unlockForSnapshot();
    }

    @Override
    public long getTotalActiveQueuedDuration(long fromTimestamp) {
        return queue.getTotalQueuedDuration(fromTimestamp);
    }

    @Override
    public long getMinLastQueueDate() {
        return queue.getMinLastQueueDate();
    }

    @Override
    public SwapSummary recoverSwappedFlowFiles() {
        final SwapSummary swapSummary = queue.recoverSwappedFlowFiles();
        startRebalanceTask();
        return swapSummary;
    }

    @Override
    public String getSwapPartitionName() {
        return SWAP_PARTITION_NAME;
    }

    @Override
    public void put(final FlowFileRecord flowFile) {
        queue.put(flowFile);
        startRebalanceTask();
    }

    @Override
    public void putAll(final Collection<FlowFileRecord> flowFiles) {
        queue.putAll(flowFiles);
        startRebalanceTask();
    }

    @Override
    public void dropFlowFiles(DropFlowFileRequest dropRequest, String requestor) {
        queue.dropFlowFiles(dropRequest, requestor);
    }

    @Override
    public SelectiveDropResult dropFlowFiles(final Predicate<FlowFile> predicate) throws IOException {
        return queue.dropFlowFiles(predicate);
    }

    @Override
    public void setPriorities(final List<FlowFilePrioritizer> newPriorities) {
        queue.setPriorities(newPriorities);
    }

    /**
     * Start Rebalancing after acquiring lock and submitting Rebalance Task for polling
     *
     * @param partitionerUsed Partitioner used to determine which FlowFiles should belong to this Partition
     */
    @Override
    public void start(final FlowFilePartitioner partitionerUsed) {
        lifecycleLock.lock();
        try {
            running = true;
            submitRebalanceTask();
        } finally {
            lifecycleLock.unlock();
        }
    }

    /**
     * Stop Rebalancing after acquiring lock and setting flag to prevent additional polling
     */
    @Override
    public void stop() {
        lifecycleLock.lock();
        try {
            running = false;
            rebalanceTask = null;
        } finally {
            lifecycleLock.unlock();
        }
    }

    private void startRebalanceTask() {
        lifecycleLock.lock();
        try {
            submitRebalanceTask();
        } finally {
            lifecycleLock.unlock();
        }
    }

    /**
     * Submit Rebalance Task must be invoked after acquiring Lifecycle Lock
     */
    private void submitRebalanceTask() {
        if (running) {
            if (rebalanceTask == null) {
                rebalanceTask = new RebalanceTask();
                rebalanceExecutor.execute(rebalanceTask);
                logger.debug("{} running: Rebalance Task started", description);
            } else {
                logger.debug("{} running: Rebalance Task already started", description);
            }
        } else {
            logger.debug("{} not running: Rebalance Task not started", description);
        }
    }

    @Override
    public void rebalance(final FlowFileQueueContents queueContents) {
        if (queueContents.getActiveFlowFiles().isEmpty() && queueContents.getSwapLocations().isEmpty()) {
            return;
        }

        logger.debug("Adding {} to Rebalance queue for {}", queueContents, this);

        queue.inheritQueueContents(queueContents);
        startRebalanceTask();
    }

    @Override
    public void rebalance(final Collection<FlowFileRecord> flowFiles) {
        logger.debug("Adding {} to Rebalance queue for {}", flowFiles, this);

        queue.putAll(flowFiles);
        startRebalanceTask();
    }

    @Override
    public FlowFileQueueContents packageForRebalance(String newPartitionName) {
        return queue.packageForRebalance(newPartitionName);
    }

    /**
     * Rebalance Task runs until FlowFile Queue is emptied or the Thread is interrupted
     */
    private class RebalanceTask implements Runnable {
        private static final Set<FlowFileRecord> EXPIRED_RECORDS = Set.of();

        @Override
        public void run() {
            boolean taskInterrupted = false;
            List<FlowFileRecord> nextBatch = pollNextBatch(taskInterrupted);

            while (nextBatch != null) {
                if (nextBatch.isEmpty()) {
                    logger.warn("{} Queue Size [{}] but no FlowFiles found: sleeping for {} ms", StandardRebalancingPartition.this, queue.size(), UNPOLLABLE_RETRY_MILLIS);

                    try {
                        Thread.sleep(UNPOLLABLE_RETRY_MILLIS);
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        taskInterrupted = true;
                    }
                } else {
                    logger.debug("{} Rebalancing {}", StandardRebalancingPartition.this, nextBatch);

                    // Distribute for partitioning and acknowledge FlowFiles distributed
                    flowFileQueue.distributeToPartitions(nextBatch);
                    queue.acknowledge(nextBatch);
                }

                nextBatch = pollNextBatch(taskInterrupted);
            }
        }

        private List<FlowFileRecord> pollNextBatch(final boolean taskInterrupted) {
            lifecycleLock.lock();
            try {
                final List<FlowFileRecord> nextBatch;

                // Check whether this Task is still the active Task before polling to determine Partition stopped status
                if (rebalanceTask == this) {
                    final List<FlowFileRecord> polled = queue.poll(DISTRIBUTION_BATCH_SIZE, EXPIRED_RECORDS, NO_EXPIRATION, PollStrategy.ALL_FLOWFILES);

                    if (polled.isEmpty() && (queue.isEmpty() || taskInterrupted)) {
                        rebalanceTask = null;
                        logger.debug("Rebalance Task completed for {}", StandardRebalancingPartition.this);
                        nextBatch = null;
                    } else {
                        nextBatch = polled;
                    }
                } else {
                    logger.debug("Rebalance Task retired for {}", StandardRebalancingPartition.this);
                    nextBatch = null;
                }

                return nextBatch;
            } finally {
                lifecycleLock.unlock();
            }
        }
    }

    @Override
    public String toString() {
        return description;
    }
}
