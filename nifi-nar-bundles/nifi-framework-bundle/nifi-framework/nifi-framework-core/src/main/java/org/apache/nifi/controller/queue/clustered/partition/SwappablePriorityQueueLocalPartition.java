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
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.FlowFileQueueContents;
import org.apache.nifi.controller.queue.LocalQueuePartitionDiagnostics;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.queue.SwappablePriorityQueue;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.processor.FlowFileFilter;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A Local Queue Partition that whose implementation is based on the use of a {@link SwappablePriorityQueue}.
 */
public class SwappablePriorityQueueLocalPartition implements LocalQueuePartition {
    private static final String SWAP_PARTITION_NAME = "local";

    private final SwappablePriorityQueue priorityQueue;
    private final FlowFileQueue flowFileQueue;
    private final String description;

    public SwappablePriorityQueueLocalPartition(final FlowFileSwapManager swapManager, final int swapThreshold, final EventReporter eventReporter,
            final FlowFileQueue flowFileQueue, final DropFlowFileAction dropAction) {
        this.priorityQueue = new SwappablePriorityQueue(swapManager, swapThreshold, eventReporter, flowFileQueue, dropAction, SWAP_PARTITION_NAME);
        this.flowFileQueue = flowFileQueue;
        this.description = "SwappablePriorityQueueLocalPartition[queueId=" + flowFileQueue.getIdentifier() + "]";
    }

    @Override
    public String getSwapPartitionName() {
        return SWAP_PARTITION_NAME;
    }

    @Override
    public QueueSize size() {
        return priorityQueue.size();
    }

    @Override
    public boolean isUnacknowledgedFlowFile() {
        return priorityQueue.isUnacknowledgedFlowFile();
    }

    @Override
    public Optional<NodeIdentifier> getNodeIdentifier() {
        return Optional.empty();
    }

    @Override
    public void put(final FlowFileRecord flowFile) {
        priorityQueue.put(flowFile);
    }

    @Override
    public void putAll(final Collection<FlowFileRecord> flowFiles) {
        priorityQueue.putAll(flowFiles);
    }

    @Override
    public boolean isActiveQueueEmpty() {
        return priorityQueue.isActiveQueueEmpty();
    }

    @Override
    public FlowFileRecord poll(final Set<FlowFileRecord> expiredRecords) {
        return priorityQueue.poll(expiredRecords, getExpiration());
    }

    @Override
    public List<FlowFileRecord> poll(final int maxResults, final Set<FlowFileRecord> expiredRecords) {
        return priorityQueue.poll(maxResults, expiredRecords, getExpiration());
    }

    @Override
    public List<FlowFileRecord> poll(final FlowFileFilter filter, final Set<FlowFileRecord> expiredRecords) {
        return priorityQueue.poll(filter, expiredRecords, getExpiration());
    }

    private int getExpiration() {
        return flowFileQueue.getFlowFileExpiration(TimeUnit.MILLISECONDS);
    }

    @Override
    public FlowFileRecord getFlowFile(final String flowFileUuid) throws IOException {
        return priorityQueue.getFlowFile(flowFileUuid);
    }

    @Override
    public List<FlowFileRecord> getListableFlowFiles() {
        return priorityQueue.getActiveFlowFiles();
    }

    @Override
    public void dropFlowFiles(final DropFlowFileRequest dropRequest, final String requestor) {
        priorityQueue.dropFlowFiles(dropRequest, requestor);
    }

    @Override
    public SwapSummary recoverSwappedFlowFiles() {
        return priorityQueue.recoverSwappedFlowFiles();
    }

    @Override
    public void setPriorities(final List<FlowFilePrioritizer> newPriorities) {
        priorityQueue.setPriorities(newPriorities);
    }

    @Override
    public void acknowledge(final FlowFileRecord flowFile) {
        priorityQueue.acknowledge(flowFile);
    }

    @Override
    public void acknowledge(final Collection<FlowFileRecord> flowFiles) {
        priorityQueue.acknowledge(flowFiles);
    }

    @Override
    public LocalQueuePartitionDiagnostics getQueueDiagnostics() {
        return priorityQueue.getQueueDiagnostics();
    }

    @Override
    public FlowFileQueueContents packageForRebalance(String newPartitionName) {
        return priorityQueue.packageForRebalance(newPartitionName);
    }

    @Override
    public void start(final FlowFilePartitioner partitionerUsed) {
    }

    @Override
    public void stop() {
    }

    @Override
    public void inheritQueueContents(final FlowFileQueueContents queueContents) {
        priorityQueue.inheritQueueContents(queueContents);
    }

    @Override
    public String toString() {
        return description;
    }
}
