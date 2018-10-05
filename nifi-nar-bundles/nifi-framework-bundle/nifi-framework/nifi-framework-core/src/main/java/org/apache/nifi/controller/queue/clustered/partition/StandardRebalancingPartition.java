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
import org.apache.nifi.controller.queue.BlockingSwappablePriorityQueue;
import org.apache.nifi.controller.queue.DropFlowFileAction;
import org.apache.nifi.controller.queue.DropFlowFileRequest;
import org.apache.nifi.controller.queue.FlowFileQueueContents;
import org.apache.nifi.controller.queue.LoadBalancedFlowFileQueue;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.flowfile.FlowFilePrioritizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class StandardRebalancingPartition implements RebalancingPartition {
    private final String SWAP_PARTITION_NAME = "rebalance";
    private final String queueIdentifier;
    private final BlockingSwappablePriorityQueue queue;
    private final LoadBalancedFlowFileQueue flowFileQueue;
    private final String description;

    private volatile boolean stopped = true;
    private RebalanceTask rebalanceTask;


    public StandardRebalancingPartition(final FlowFileSwapManager swapManager, final int swapThreshold, final EventReporter eventReporter,
                                        final LoadBalancedFlowFileQueue flowFileQueue, final DropFlowFileAction dropAction) {

        this.queue = new BlockingSwappablePriorityQueue(swapManager, swapThreshold, eventReporter, flowFileQueue, dropAction, SWAP_PARTITION_NAME);
        this.queueIdentifier = flowFileQueue.getIdentifier();
        this.flowFileQueue = flowFileQueue;
        this.description = "RebalancingPartition[queueId=" + queueIdentifier + "]";
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
    public SwapSummary recoverSwappedFlowFiles() {
        return this.queue.recoverSwappedFlowFiles();
    }

    @Override
    public String getSwapPartitionName() {
        return SWAP_PARTITION_NAME;
    }

    @Override
    public void put(final FlowFileRecord flowFile) {
        queue.put(flowFile);
    }

    @Override
    public void putAll(final Collection<FlowFileRecord> flowFiles) {
        queue.putAll(flowFiles);
    }

    @Override
    public void dropFlowFiles(DropFlowFileRequest dropRequest, String requestor) {
        queue.dropFlowFiles(dropRequest, requestor);
    }

    @Override
    public void setPriorities(final List<FlowFilePrioritizer> newPriorities) {
        queue.setPriorities(newPriorities);
    }

    @Override
    public synchronized void start(final FlowFilePartitioner partitionerUsed) {
        stopped = false;
        rebalanceFromQueue();
    }

    @Override
    public synchronized void stop() {
        stopped = true;

        if (this.rebalanceTask != null) {
            this.rebalanceTask.stop();
        }

        this.rebalanceTask = null;
    }

    private synchronized void rebalanceFromQueue() {
        if (stopped) {
            return;
        }

        // If a task is already defined, do nothing. There's already a thread running.
        if (rebalanceTask != null) {
            return;
        }

        this.rebalanceTask = new RebalanceTask();

        final Thread rebalanceThread = new Thread(this.rebalanceTask);
        rebalanceThread.setName("Rebalance queued data for Connection " + queueIdentifier);
        rebalanceThread.start();
    }

    @Override
    public void rebalance(final FlowFileQueueContents queueContents) {
        if (queueContents.getActiveFlowFiles().isEmpty() && queueContents.getSwapLocations().isEmpty()) {
            return;
        }

        queue.inheritQueueContents(queueContents);
        rebalanceFromQueue();
    }

    @Override
    public void rebalance(final Collection<FlowFileRecord> flowFiles) {
        queue.putAll(flowFiles);
        rebalanceFromQueue();
    }

    @Override
    public FlowFileQueueContents packageForRebalance(String newPartitionName) {
        return queue.packageForRebalance(newPartitionName);
    }

    private synchronized boolean complete() {
        if (!queue.isEmpty()) {
            return false;
        }

        this.rebalanceTask = null;
        return true;
    }


    private class RebalanceTask implements Runnable {
        private volatile boolean stopped = false;
        private final Set<FlowFileRecord> expiredRecords = new HashSet<>();
        private final long pollWaitMillis = 100L;

        public void stop() {
            stopped = true;
        }

        @Override
        public void run() {
            while (!stopped) {
                final FlowFileRecord polled;

                expiredRecords.clear();

                // Wait up to #pollWaitMillis milliseconds to get a FlowFile. If none, then check if stopped
                // and if not, poll again.
                try {
                    polled = queue.poll(expiredRecords, -1, pollWaitMillis);
                } catch (final InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    continue;
                }

                if (polled == null) {
                    flowFileQueue.handleExpiredRecords(expiredRecords);

                    if (complete()) {
                        return;
                    } else {
                        continue;
                    }
                }

                // We got 1 FlowFile. Try a second poll to obtain up to 999 more (for a total of 1,000).
                final List<FlowFileRecord> toDistribute = new ArrayList<>();
                toDistribute.add(polled);

                final List<FlowFileRecord> additionalRecords = queue.poll(999, expiredRecords, -1);
                toDistribute.addAll(additionalRecords);

                flowFileQueue.handleExpiredRecords(expiredRecords);

                // Transfer all of the FlowFiles that we got back to the FlowFileQueue itself. This will cause the data to be
                // re-partitioned and binned appropriately. We also then need to ensure that we acknowledge the data from our
                // own SwappablePriorityQueue to ensure that the sizes are kept in check.
                flowFileQueue.distributeToPartitions(toDistribute);
                queue.acknowledge(toDistribute);
            }
        }
    }

    @Override
    public String toString() {
        return description;
    }
}
