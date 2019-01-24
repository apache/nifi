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
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.queue.AbstractFlowFileQueue;
import org.apache.nifi.controller.queue.ConnectionEventListener;
import org.apache.nifi.controller.queue.DropFlowFileRequest;
import org.apache.nifi.controller.queue.DropFlowFileState;
import org.apache.nifi.controller.queue.FlowFileQueueContents;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.controller.queue.LoadBalancedFlowFileQueue;
import org.apache.nifi.controller.queue.LocalQueuePartitionDiagnostics;
import org.apache.nifi.controller.queue.QueueDiagnostics;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.queue.RemoteQueuePartitionDiagnostics;
import org.apache.nifi.controller.queue.StandardQueueDiagnostics;
import org.apache.nifi.controller.queue.SwappablePriorityQueue;
import org.apache.nifi.controller.queue.clustered.client.async.AsyncLoadBalanceClientRegistry;
import org.apache.nifi.controller.queue.clustered.partition.CorrelationAttributePartitioner;
import org.apache.nifi.controller.queue.clustered.partition.FirstNodePartitioner;
import org.apache.nifi.controller.queue.clustered.partition.FlowFilePartitioner;
import org.apache.nifi.controller.queue.clustered.partition.LocalPartitionPartitioner;
import org.apache.nifi.controller.queue.clustered.partition.LocalQueuePartition;
import org.apache.nifi.controller.queue.clustered.partition.NonLocalPartitionPartitioner;
import org.apache.nifi.controller.queue.clustered.partition.QueuePartition;
import org.apache.nifi.controller.queue.clustered.partition.RebalancingPartition;
import org.apache.nifi.controller.queue.clustered.partition.RemoteQueuePartition;
import org.apache.nifi.controller.queue.clustered.partition.RoundRobinPartitioner;
import org.apache.nifi.controller.queue.clustered.partition.StandardRebalancingPartition;
import org.apache.nifi.controller.queue.clustered.partition.SwappablePriorityQueueLocalPartition;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.repository.RepositoryRecord;
import org.apache.nifi.controller.repository.StandardRepositoryRecord;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.swap.StandardSwapSummary;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.provenance.ProvenanceEventBuilder;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.reporting.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SocketLoadBalancedFlowFileQueue extends AbstractFlowFileQueue implements LoadBalancedFlowFileQueue {
    private static final Logger logger = LoggerFactory.getLogger(SocketLoadBalancedFlowFileQueue.class);
    private static final int NODE_SWAP_THRESHOLD = 1000;

    private final List<FlowFilePrioritizer> prioritizers = new ArrayList<>();
    private final ConnectionEventListener eventListener;
    private final AtomicReference<QueueSize> totalSize = new AtomicReference<>(new QueueSize(0, 0L));
    private final LocalQueuePartition localPartition;
    private final RebalancingPartition rebalancingPartition;
    private final FlowFileSwapManager swapManager;
    private final EventReporter eventReporter;
    private final ClusterCoordinator clusterCoordinator;
    private final AsyncLoadBalanceClientRegistry clientRegistry;

    private final FlowFileRepository flowFileRepo;
    private final ProvenanceEventRepository provRepo;
    private final ContentRepository contentRepo;
    private final Set<NodeIdentifier> nodeIdentifiers;

    private final ReadWriteLock partitionLock = new ReentrantReadWriteLock();
    private final Lock partitionReadLock = partitionLock.readLock();
    private final Lock partitionWriteLock = partitionLock.writeLock();
    private QueuePartition[] queuePartitions;
    private FlowFilePartitioner partitioner;
    private boolean stopped = true;
    private volatile boolean offloaded = false;


    public SocketLoadBalancedFlowFileQueue(final String identifier, final ConnectionEventListener eventListener, final ProcessScheduler scheduler, final FlowFileRepository flowFileRepo,
                                           final ProvenanceEventRepository provRepo, final ContentRepository contentRepo, final ResourceClaimManager resourceClaimManager,
                                           final ClusterCoordinator clusterCoordinator, final AsyncLoadBalanceClientRegistry clientRegistry, final FlowFileSwapManager swapManager,
                                           final int swapThreshold, final EventReporter eventReporter) {

        super(identifier, scheduler, flowFileRepo, provRepo, resourceClaimManager);
        this.eventListener = eventListener;
        this.eventReporter = eventReporter;
        this.swapManager = swapManager;
        this.flowFileRepo = flowFileRepo;
        this.provRepo = provRepo;
        this.contentRepo = contentRepo;
        this.clusterCoordinator = clusterCoordinator;
        this.clientRegistry = clientRegistry;

        localPartition = new SwappablePriorityQueueLocalPartition(swapManager, swapThreshold, eventReporter, this, this::drop);
        rebalancingPartition = new StandardRebalancingPartition(swapManager, swapThreshold, eventReporter, this, this::drop);

        // Create a RemoteQueuePartition for each node
        nodeIdentifiers = clusterCoordinator == null ? Collections.emptySet() : clusterCoordinator.getNodeIdentifiers();

        final List<NodeIdentifier> sortedNodeIdentifiers = new ArrayList<>(nodeIdentifiers);
        sortedNodeIdentifiers.sort(Comparator.comparing(NodeIdentifier::getApiAddress));

        if (sortedNodeIdentifiers.isEmpty()) {
            queuePartitions = new QueuePartition[] { localPartition };
        } else {
            queuePartitions = new QueuePartition[sortedNodeIdentifiers.size()];

            for (int i = 0; i < sortedNodeIdentifiers.size(); i++) {
                final NodeIdentifier nodeId = sortedNodeIdentifiers.get(i);
                if (nodeId.equals(clusterCoordinator.getLocalNodeIdentifier())) {
                    queuePartitions[i] = localPartition;
                } else {
                    queuePartitions[i] = createRemotePartition(nodeId);
                }
            }

        }

        partitioner = new LocalPartitionPartitioner();

        if (clusterCoordinator != null) {
            clusterCoordinator.registerEventListener(new ClusterEventListener());
        }

        rebalancingPartition.start(partitioner);
    }


    @Override
    public synchronized void setLoadBalanceStrategy(final LoadBalanceStrategy strategy, final String partitioningAttribute) {
        final LoadBalanceStrategy currentStrategy = getLoadBalanceStrategy();
        final String currentPartitioningAttribute = getPartitioningAttribute();

        super.setLoadBalanceStrategy(strategy, partitioningAttribute);

        if (strategy == currentStrategy && Objects.equals(partitioningAttribute, currentPartitioningAttribute)) {
            // Nothing changed.
            return;
        }

        if (clusterCoordinator == null) {
            // Not clustered so nothing to worry about.
            return;
        }

        if (!offloaded) {
            // We are already load balancing but are changing how we are load balancing.
            final FlowFilePartitioner partitioner;
            partitioner = getPartitionerForLoadBalancingStrategy(strategy, partitioningAttribute);

            setFlowFilePartitioner(partitioner);
        }
    }

    private FlowFilePartitioner getPartitionerForLoadBalancingStrategy(LoadBalanceStrategy strategy, String partitioningAttribute) {
        FlowFilePartitioner partitioner;
        switch (strategy) {
            case DO_NOT_LOAD_BALANCE:
                partitioner = new LocalPartitionPartitioner();
                break;
            case PARTITION_BY_ATTRIBUTE:
                partitioner = new CorrelationAttributePartitioner(partitioningAttribute);
                break;
            case ROUND_ROBIN:
                partitioner = new RoundRobinPartitioner();
                break;
            case SINGLE_NODE:
                partitioner = new FirstNodePartitioner();
                break;
            default:
                throw new IllegalArgumentException();
        }
        return partitioner;
    }

    @Override
    public void offloadQueue() {
        if (clusterCoordinator == null) {
            // Not clustered, cannot offload the queue to other nodes
            return;
        }

        logger.debug("Setting queue {} on node {} as offloaded", this, clusterCoordinator.getLocalNodeIdentifier());
        offloaded = true;

        partitionWriteLock.lock();
        try {
            final Set<NodeIdentifier> nodesToKeep = new HashSet<>();

            // If we have any nodes that are connected, we only want to send data to the connected nodes.
            for (final QueuePartition partition : queuePartitions) {
                final Optional<NodeIdentifier> nodeIdOption = partition.getNodeIdentifier();
                if (!nodeIdOption.isPresent()) {
                    continue;
                }

                final NodeIdentifier nodeId = nodeIdOption.get();
                final NodeConnectionStatus status = clusterCoordinator.getConnectionStatus(nodeId);
                if (status != null && status.getState() == NodeConnectionState.CONNECTED) {
                    nodesToKeep.add(nodeId);
                }
            }

            if (!nodesToKeep.isEmpty()) {
                setNodeIdentifiers(nodesToKeep, false);
            }

            // Update our partitioner so that we don't keep any data on the local partition
            setFlowFilePartitioner(new NonLocalPartitionPartitioner());
        } finally {
            partitionWriteLock.unlock();
        }
    }

    @Override
    public void resetOffloadedQueue() {
        if (clusterCoordinator == null) {
            // Not clustered, was not offloading the queue to other nodes
            return;
        }

        if (offloaded) {
            // queue was offloaded previously, allow files to be added to the local partition
            offloaded = false;
            logger.debug("Queue {} on node {} was previously offloaded, resetting offloaded status to {}",
                    this, clusterCoordinator.getLocalNodeIdentifier(), offloaded);
            // reset the partitioner based on the load balancing strategy, since offloading previously changed the partitioner
            FlowFilePartitioner partitioner = getPartitionerForLoadBalancingStrategy(getLoadBalanceStrategy(), getPartitioningAttribute());
            setFlowFilePartitioner(partitioner);
            logger.debug("Queue {} is no longer offloaded, restored load balance strategy to {} and partitioning attribute to \"{}\"",
                    this, getLoadBalanceStrategy(), getPartitioningAttribute());
        }
    }

    public synchronized void startLoadBalancing() {
        logger.debug("{} started. Will begin distributing FlowFiles across the cluster", this);

        if (!stopped) {
            return;
        }

        stopped = false;

        partitionReadLock.lock();
        try {
            rebalancingPartition.start(partitioner);

            for (final QueuePartition queuePartition : queuePartitions) {
                queuePartition.start(partitioner);
            }
        } finally {
            partitionReadLock.unlock();
        }
    }

    public synchronized void stopLoadBalancing() {
        logger.debug("{} stopped. Will no longer distribute FlowFiles across the cluster", this);

        if (stopped) {
            return;
        }

        stopped = true;

        partitionReadLock.lock();
        try {
            rebalancingPartition.stop();
            for (final QueuePartition queuePartition : queuePartitions) {
                queuePartition.stop();
            }
        } finally {
            partitionReadLock.unlock();
        }
    }

    @Override
    public boolean isActivelyLoadBalancing() {
        final QueueSize size = size();
        if (size.getObjectCount() == 0) {
            return false;
        }

        final int localObjectCount = localPartition.size().getObjectCount();
        return (size.getObjectCount() > localObjectCount);
    }

    private QueuePartition createRemotePartition(final NodeIdentifier nodeId) {
        final SwappablePriorityQueue partitionQueue = new SwappablePriorityQueue(swapManager, NODE_SWAP_THRESHOLD, eventReporter, this, this::drop, nodeId.getId());

        final TransferFailureDestination failureDestination = new TransferFailureDestination() {
            @Override
            public void putAll(final Collection<FlowFileRecord> flowFiles, final FlowFilePartitioner partitionerUsed) {
                if (flowFiles.isEmpty()) {
                    return;
                }

                partitionReadLock.lock();
                try {
                    if (isRebalanceOnFailure(partitionerUsed)) {
                        logger.debug("Transferring {} FlowFiles to Rebalancing Partition from node {}", flowFiles.size(), nodeId);
                        rebalancingPartition.rebalance(flowFiles);
                    } else {
                        logger.debug("Returning {} FlowFiles to their queue for node {} because Partitioner {} indicates that the FlowFiles should stay where they are", flowFiles.size(), nodeId,
                            partitioner);
                        partitionQueue.putAll(flowFiles);
                    }
                } finally {
                    partitionReadLock.unlock();
                }
            }

            @Override
            public void putAll(final Function<String, FlowFileQueueContents> queueContentsFunction, final FlowFilePartitioner partitionerUsed) {
                partitionReadLock.lock();
                try {
                    if (isRebalanceOnFailure(partitionerUsed)) {
                        final FlowFileQueueContents contents = queueContentsFunction.apply(rebalancingPartition.getSwapPartitionName());
                        rebalancingPartition.rebalance(contents);
                        logger.debug("Transferring all {} FlowFiles and {} Swap Files queued for node {} to Rebalancing Partition",
                            contents.getActiveFlowFiles().size(), contents.getSwapLocations().size(), nodeId);
                    } else {
                        logger.debug("Will not transfer FlowFiles queued for node {} to Rebalancing Partition because Partitioner {} indicates that the FlowFiles should stay where they are", nodeId,
                            partitioner);
                    }
                } finally {
                    partitionReadLock.unlock();
                }
            }

            @Override
            public boolean isRebalanceOnFailure(final FlowFilePartitioner partitionerUsed) {
                partitionReadLock.lock();
                try {
                    if (!partitionerUsed.equals(partitioner)) {
                        return true;
                    }

                    return partitioner.isRebalanceOnFailure();
                } finally {
                    partitionReadLock.unlock();
                }
            }
        };

        final QueuePartition partition = new RemoteQueuePartition(nodeId, partitionQueue, failureDestination, flowFileRepo, provRepo, contentRepo, clientRegistry, this);

        if (!stopped) {
            partition.start(partitioner);
        }

        return partition;
    }

    @Override
    public synchronized List<FlowFilePrioritizer> getPriorities() {
        return new ArrayList<>(prioritizers);
    }

    @Override
    public synchronized void setPriorities(final List<FlowFilePrioritizer> newPriorities) {
        prioritizers.clear();
        prioritizers.addAll(newPriorities);

        partitionReadLock.lock();
        try {
            for (final QueuePartition partition : queuePartitions) {
                partition.setPriorities(newPriorities);
            }

            rebalancingPartition.setPriorities(newPriorities);
        } finally {
            partitionReadLock.unlock();
        }
    }


    @Override
    public SwapSummary recoverSwappedFlowFiles() {
        partitionReadLock.lock();
        try {
            final List<SwapSummary> summaries = new ArrayList<>(queuePartitions.length);

            // Discover the names of all partitions that have data swapped out.
            Set<String> partitionNamesToRecover;
            try {
                partitionNamesToRecover = swapManager.getSwappedPartitionNames(this);
                logger.debug("For {}, partition names to recover are {}", this, partitionNamesToRecover);
            } catch (final IOException ioe) {
                logger.error("Failed to determine the names of the Partitions that have swapped FlowFiles for queue with ID {}.", getIdentifier(), ioe);
                if (eventReporter != null) {
                    eventReporter.reportEvent(Severity.ERROR, "FlowFile Swapping", "Failed to determine the names of Partitions that have swapped FlowFiles for queue with ID " +
                            getIdentifier() + "; see logs for more detials");
                }

                partitionNamesToRecover = Collections.emptySet();
            }

            // For each Queue Partition, recover swapped FlowFiles.
            for (final QueuePartition partition : queuePartitions) {
                partitionNamesToRecover.remove(partition.getSwapPartitionName());

                final SwapSummary summary = partition.recoverSwappedFlowFiles();
                summaries.add(summary);
            }

            // Recover any swap files that may belong to the 'rebalancing' partition
            partitionNamesToRecover.remove(rebalancingPartition.getSwapPartitionName());
            final SwapSummary rebalancingSwapSummary = rebalancingPartition.recoverSwappedFlowFiles();
            summaries.add(rebalancingSwapSummary);

            // If there is any Partition that has swapped FlowFiles but for which we don't have a Queue Partition created, we need to recover those swap locations
            // and get their swap summaries now. We then transfer any Swap Files that existed for that partition to the 'rebalancing' partition so that the data
            // will be rebalanced against the existing partitions. We do this to handle the following scenario:
            //   - NiFi is running in a cluster with 5 nodes.
            //   - A queue is load balanced across the cluster, with all partitions having data swapped out.
            //   - NiFi is shutdown and upgraded to a new version.
            //   - Admin failed to copy over the Managed State for the nodes from the old version to the new version.
            //   - Upon restart, NiFi does not know about any of the nodes in the cluster.
            //   - When a node joins and recovers swap locations, it is the only known node.
            //   - NiFi will not know that it needs a Remote Partition for nodes 2-5.
            //   - If we don't recover those partitions here, then we'll end up not recovering the Swap Files at all, which will result in the Content Claims
            //     have their Claimant Counts decremented, which could lead to loss of the data from the Content Repository.
            for (final String partitionName : partitionNamesToRecover) {
                logger.info("Found Swap Files for FlowFile Queue with Identifier {} and Partition {} that has not been recovered yet. "
                        + "Will recover Swap Files for this Partition even though no partition exists with this name yet", getIdentifier(), partitionName);

                try {
                    final List<String> swapLocations = swapManager.recoverSwapLocations(this, partitionName);
                    for (final String swapLocation : swapLocations) {
                        final SwapSummary swapSummary = swapManager.getSwapSummary(swapLocation);
                        summaries.add(swapSummary);

                        // Transfer the swap file to the rebalancing partition.
                        final String updatedSwapLocation = swapManager.changePartitionName(swapLocation, rebalancingPartition.getSwapPartitionName());
                        final FlowFileQueueContents queueContents = new FlowFileQueueContents(Collections.emptyList(), Collections.singletonList(updatedSwapLocation), swapSummary.getQueueSize());
                        rebalancingPartition.rebalance(queueContents);
                    }
                } catch (IOException e) {
                    logger.error("Failed to determine whether or not any Swap Files exist for FlowFile Queue {} and Partition {}", getIdentifier(), partitionName, e);
                    if (eventReporter != null) {
                        eventReporter.reportEvent(Severity.ERROR, "FlowFile Swapping", "Failed to determine whether or not any Swap Files exist for FlowFile Queue " +
                                getIdentifier() + "; see logs for more detials");
                    }
                }
            }

            Long maxId = null;
            QueueSize totalQueueSize = new QueueSize(0, 0L);
            final List<ResourceClaim> resourceClaims = new ArrayList<>();

            for (final SwapSummary summary : summaries) {
                Long summaryMaxId = summary.getMaxFlowFileId();
                if (summaryMaxId != null && (maxId == null || summaryMaxId > maxId)) {
                    maxId = summaryMaxId;
                }

                final QueueSize summaryQueueSize = summary.getQueueSize();
                totalQueueSize = totalQueueSize.add(summaryQueueSize);

                final List<ResourceClaim> summaryResourceClaims = summary.getResourceClaims();
                resourceClaims.addAll(summaryResourceClaims);
            }

            adjustSize(totalQueueSize.getObjectCount(), totalQueueSize.getByteCount());

            return new StandardSwapSummary(totalQueueSize, maxId, resourceClaims);
        } finally {
            partitionReadLock.unlock();
        }
    }

    @Override
    public void purgeSwapFiles() {
        swapManager.purge();
    }

    @Override
    public QueueSize size() {
        return totalSize.get();
    }

    @Override
    public boolean isEmpty() {
        return size().getObjectCount() == 0;
    }

    @Override
    public boolean isActiveQueueEmpty() {
        return localPartition.isActiveQueueEmpty();
    }

    @Override
    public QueueDiagnostics getQueueDiagnostics() {
        partitionReadLock.lock();
        try {
            final LocalQueuePartitionDiagnostics localDiagnostics = localPartition.getQueueDiagnostics();

            final List<RemoteQueuePartitionDiagnostics> remoteDiagnostics = new ArrayList<>(queuePartitions.length - 1);

            for (final QueuePartition partition : queuePartitions) {
                if (partition instanceof RemoteQueuePartition) {
                    final RemoteQueuePartition queuePartition = (RemoteQueuePartition) partition;
                    final RemoteQueuePartitionDiagnostics diagnostics = queuePartition.getDiagnostics();
                    remoteDiagnostics.add(diagnostics);
                }
            }

            return new StandardQueueDiagnostics(localDiagnostics, remoteDiagnostics);
        } finally {
            partitionReadLock.unlock();
        }
    }

    protected LocalQueuePartition getLocalPartition() {
        return localPartition;
    }

    protected int getPartitionCount() {
        partitionReadLock.lock();
        try {
            return queuePartitions.length;
        } finally {
            partitionReadLock.unlock();
        }
    }

    protected QueuePartition getPartition(final int index) {
        partitionReadLock.lock();
        try {
            if (index < 0 || index >= queuePartitions.length) {
                throw new IndexOutOfBoundsException();
            }

            return queuePartitions[index];
        } finally {
            partitionReadLock.unlock();
        }
    }

    private void adjustSize(final int countToAdd, final long bytesToAdd) {
        boolean updated = false;
        while (!updated) {
            final QueueSize queueSize = this.totalSize.get();
            final QueueSize updatedSize = queueSize.add(countToAdd, bytesToAdd);
            updated = totalSize.compareAndSet(queueSize, updatedSize);
        }
    }

    public void onTransfer(final Collection<FlowFileRecord> flowFiles) {
        adjustSize(-flowFiles.size(), -flowFiles.stream().mapToLong(FlowFileRecord::getSize).sum());
    }

    public void onAbort(final Collection<FlowFileRecord> flowFiles) {
        if (flowFiles == null || flowFiles.isEmpty()) {
            return;
        }

        adjustSize(-flowFiles.size(), -flowFiles.stream().mapToLong(FlowFileRecord::getSize).sum());
    }

    @Override
    public boolean isLocalPartitionFull() {
        return isFull(localPartition.size());
    }

    /**
     * Determines which QueuePartition the given FlowFile belongs to. Must be called with partition read lock held.
     *
     * @param flowFile the FlowFile
     * @return the QueuePartition that the FlowFile belongs to
     */
    private QueuePartition getPartition(final FlowFileRecord flowFile) {
        final QueuePartition queuePartition = partitioner.getPartition(flowFile, queuePartitions, localPartition);
        logger.debug("{} Assigning {} to Partition: {}", this, flowFile, queuePartition);
        return queuePartition;
    }

    public void setNodeIdentifiers(final Set<NodeIdentifier> updatedNodeIdentifiers, final boolean forceUpdate) {
        partitionWriteLock.lock();
        try {
            // If nothing is changing, then just return
            if (!forceUpdate && this.nodeIdentifiers.equals(updatedNodeIdentifiers)) {
                logger.debug("{} Not going to rebalance Queue even though setNodeIdentifiers was called, because the new set of Node Identifiers is the same as the existing set", this);
                return;
            }

            logger.debug("{} Stopping the {} queue partitions in order to change node identifiers from {} to {}", this, queuePartitions.length, this.nodeIdentifiers, updatedNodeIdentifiers);
            for (final QueuePartition queuePartition : queuePartitions) {
                queuePartition.stop();
            }

            // Determine which Node Identifiers, if any, were removed.
            final Set<NodeIdentifier> removedNodeIds = new HashSet<>(this.nodeIdentifiers);
            removedNodeIds.removeAll(updatedNodeIdentifiers);
            logger.debug("{} The following Node Identifiers were removed from the cluster: {}", this, removedNodeIds);

            // Build up a Map of Node ID to Queue Partition so that we can easily pull over the existing
            // QueuePartition objects instead of having to create new ones.
            final Map<NodeIdentifier, QueuePartition> partitionMap = new HashMap<>();
            for (final QueuePartition partition : this.queuePartitions) {
                final Optional<NodeIdentifier> nodeIdOption = partition.getNodeIdentifier();
                nodeIdOption.ifPresent(nodeIdentifier -> partitionMap.put(nodeIdentifier, partition));
            }

            // Re-define 'queuePartitions' array
            final List<NodeIdentifier> sortedNodeIdentifiers = new ArrayList<>(updatedNodeIdentifiers);
            sortedNodeIdentifiers.sort(Comparator.comparing(nodeId -> nodeId.getApiAddress() + ":" + nodeId.getApiPort()));

            QueuePartition[] updatedQueuePartitions;
            if (sortedNodeIdentifiers.isEmpty()) {
                updatedQueuePartitions = new QueuePartition[] { localPartition };
            } else {
                updatedQueuePartitions = new QueuePartition[sortedNodeIdentifiers.size()];
            }

            // Populate the new QueuePartitions.
            boolean localPartitionIncluded = false;
            for (int i = 0; i < sortedNodeIdentifiers.size(); i++) {
                final NodeIdentifier nodeId = sortedNodeIdentifiers.get(i);
                if (nodeId.equals(clusterCoordinator.getLocalNodeIdentifier())) {
                    updatedQueuePartitions[i] = localPartition;
                    localPartitionIncluded = true;

                    // If we have RemoteQueuePartition with this Node ID with data, that data must be migrated to the local partition.
                    // This can happen if we didn't previously know our Node UUID.
                    final QueuePartition existingPartition = partitionMap.get(nodeId);
                    if (existingPartition != null && existingPartition != localPartition) {
                        final FlowFileQueueContents partitionContents = existingPartition.packageForRebalance(localPartition.getSwapPartitionName());
                        logger.debug("Transferred data from {} to {}", existingPartition, localPartition);
                        localPartition.inheritQueueContents(partitionContents);
                    }

                    continue;
                }

                final QueuePartition existingPartition = partitionMap.get(nodeId);
                updatedQueuePartitions[i] = existingPartition == null ? createRemotePartition(nodeId) : existingPartition;
            }

            if (!localPartitionIncluded) {
                final QueuePartition[] withLocal = new QueuePartition[updatedQueuePartitions.length + 1];
                System.arraycopy(updatedQueuePartitions, 0, withLocal, 0, updatedQueuePartitions.length);
                withLocal[withLocal.length - 1] = localPartition;
                updatedQueuePartitions = withLocal;
            }

            // If the partition requires that all partitions be re-balanced when the number of partitions changes, then do so.
            // Otherwise, just rebalance the data from any Partitions that were removed, if any.
            if (partitioner.isRebalanceOnClusterResize()) {
                for (final QueuePartition queuePartition : this.queuePartitions) {
                    logger.debug("Rebalancing {}", queuePartition);
                    rebalance(queuePartition);
                }
            } else {
                // Not all partitions need to be rebalanced, so just ensure that we rebalance any FlowFiles that are destined
                // for a node that is no longer in the cluster.
                for (final NodeIdentifier removedNodeId : removedNodeIds) {
                    final QueuePartition removedPartition = partitionMap.get(removedNodeId);
                    if (removedPartition == null) {
                        continue;
                    }

                    logger.debug("Rebalancing {}", removedPartition);
                    rebalance(removedPartition);
                }
            }

            // Unregister any client for which the node was removed from the cluster
            for (final NodeIdentifier removedNodeId : removedNodeIds) {
                final QueuePartition removedPartition = partitionMap.get(removedNodeId);
                if (removedPartition instanceof RemoteQueuePartition) {
                    ((RemoteQueuePartition) removedPartition).onRemoved();
                }
            }


            this.nodeIdentifiers.clear();
            this.nodeIdentifiers.addAll(updatedNodeIdentifiers);

            this.queuePartitions = updatedQueuePartitions;

            logger.debug("{} Restarting the {} queue partitions now that node identifiers have been updated", this, queuePartitions.length);
            if (!stopped) {
                for (final QueuePartition queuePartition : updatedQueuePartitions) {
                    queuePartition.start(partitioner);
                }
            }
        } finally {
            partitionWriteLock.unlock();
        }
    }

    protected void rebalance(final QueuePartition partition) {
        logger.debug("Rebalancing Partition {}", partition);
        final FlowFileQueueContents contents = partition.packageForRebalance(rebalancingPartition.getSwapPartitionName());
        rebalancingPartition.rebalance(contents);
    }

    @Override
    public void put(final FlowFileRecord flowFile) {
        putAndGetPartition(flowFile);
    }

    protected QueuePartition putAndGetPartition(final FlowFileRecord flowFile) {
        final QueuePartition partition;

        partitionReadLock.lock();
        try {
            adjustSize(1, flowFile.getSize());

            partition = getPartition(flowFile);
            partition.put(flowFile);
        } finally {
            partitionReadLock.unlock();
        }

        eventListener.triggerDestinationEvent();
        return partition;
    }

    public void receiveFromPeer(final Collection<FlowFileRecord> flowFiles) {
        partitionReadLock.lock();
        try {
            if (partitioner.isRebalanceOnClusterResize()) {
                logger.debug("Received the following FlowFiles from Peer: {}. Will re-partition FlowFiles to ensure proper balancing across the cluster.", flowFiles);
                putAll(flowFiles);
            } else {
                logger.debug("Received the following FlowFiles from Peer: {}. Will accept FlowFiles to the local partition", flowFiles);

                // As explained in the putAllAndGetPartitions() method, we must ensure that we call adjustSize() before we
                // put the FlowFiles on the queue. Otherwise, we will encounter a race condition. Specifically, that race condition
                // can play out like so:
                //
                // Thread 1: Call localPartition.putAll() when the queue is empty (has a queue size of 0) but has not yet adjusted the size.
                // Thread 2: Call poll() to obtain the FlowFile just received.
                // Thread 2: Transfer the FlowFile to some Relationship
                // Thread 2: Commit the session, which will call acknowledge on this queue.
                // Thread 2: The acknowledge() method attempts to decrement the size of the queue to -1.
                //           This causes an Exception to be thrown and the queue size to remain at 0.
                //           However, the FlowFile has already been successfully transferred to the next Queue.
                // Thread 1: Call adjustSize() to increment the size of the queue to 1 FlowFile.
                //
                // In this scenario, we now have no FlowFiles in the queue. However, the queue size is set to 1.
                // We can avoid this race condition by simply ensuring that we call adjustSize() before making the FlowFiles
                // available on the queue. This way, we cannot possibly obtain the FlowFiles and process/acknowledge them before the queue
                // size has been updated to account for them and therefore we will not attempt to assign a negative queue size.
                adjustSize(flowFiles.size(), flowFiles.stream().mapToLong(FlowFileRecord::getSize).sum());
                localPartition.putAll(flowFiles);
            }
        } finally {
            partitionReadLock.unlock();
        }
    }

    @Override
    public void putAll(final Collection<FlowFileRecord> flowFiles) {
        putAllAndGetPartitions(flowFiles);
    }

    protected Map<QueuePartition, List<FlowFileRecord>> putAllAndGetPartitions(final Collection<FlowFileRecord> flowFiles) {
        partitionReadLock.lock();
        try {
            // NOTE WELL: It is imperative that we adjust the size of the queue here before distributing FlowFiles to partitions.
            // If we do it the other way around, we could encounter a race condition where we distribute a FlowFile to the Local Partition,
            // but have not yet adjusted the size. The processor consuming from this queue could then poll() the FlowFile, and acknowledge it.
            // If that happens before we adjust the size, then we can end up with a negative Queue Size, which will throw an IllegalArgumentException,
            // and we end up with the wrong Queue Size.
            final long bytes = flowFiles.stream().mapToLong(FlowFileRecord::getSize).sum();
            adjustSize(flowFiles.size(), bytes);

            final Map<QueuePartition, List<FlowFileRecord>> partitionMap = distributeToPartitionsAndGet(flowFiles);

            return partitionMap;
        } finally {
            partitionReadLock.unlock();

            eventListener.triggerDestinationEvent();
        }
    }

    @Override
    public void distributeToPartitions(final Collection<FlowFileRecord> flowFiles) {
        distributeToPartitionsAndGet(flowFiles);
    }

    public Map<QueuePartition, List<FlowFileRecord>> distributeToPartitionsAndGet(final Collection<FlowFileRecord> flowFiles) {
        if (flowFiles == null || flowFiles.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<QueuePartition, List<FlowFileRecord>> partitionMap;

        partitionReadLock.lock();
        try {
            // Optimize for the most common case (no load balancing) so that we will just call getPartition() for the first FlowFile
            // in the Collection and then put all FlowFiles into that QueuePartition. Is fairly expensive to call stream().collect(#groupingBy).
            if (partitioner.isPartitionStatic()) {
                final QueuePartition partition = getPartition(flowFiles.iterator().next());
                partition.putAll(flowFiles);

                final List<FlowFileRecord> flowFileList = (flowFiles instanceof List) ? (List<FlowFileRecord>) flowFiles : new ArrayList<>(flowFiles);
                partitionMap = Collections.singletonMap(partition, flowFileList);

                logger.debug("Partitioner is static so Partitioned FlowFiles as: {}", partitionMap);
                return partitionMap;
            }

            partitionMap = flowFiles.stream().collect(Collectors.groupingBy(this::getPartition));
            logger.debug("Partitioned FlowFiles as: {}", partitionMap);

            for (final Map.Entry<QueuePartition, List<FlowFileRecord>> entry : partitionMap.entrySet()) {
                final QueuePartition partition = entry.getKey();
                final List<FlowFileRecord> flowFilesForPartition = entry.getValue();

                partition.putAll(flowFilesForPartition);
            }
        } finally {
            partitionReadLock.unlock();
        }

        return partitionMap;
    }

    protected void setFlowFilePartitioner(final FlowFilePartitioner partitioner) {
        partitionWriteLock.lock();
        try {
            if (this.partitioner.equals(partitioner)) {
                return;
            }

            this.partitioner = partitioner;

            for (final QueuePartition partition : this.queuePartitions) {
                rebalance(partition);
            }
        } finally {
            partitionWriteLock.unlock();
        }
    }

    @Override
    public FlowFileRecord poll(final Set<FlowFileRecord> expiredRecords) {
        final FlowFileRecord flowFile = localPartition.poll(expiredRecords);
        onAbort(expiredRecords);
        return flowFile;
    }

    @Override
    public List<FlowFileRecord> poll(int maxResults, Set<FlowFileRecord> expiredRecords) {
        final List<FlowFileRecord> flowFiles = localPartition.poll(maxResults, expiredRecords);
        onAbort(expiredRecords);
        return flowFiles;
    }

    @Override
    public List<FlowFileRecord> poll(FlowFileFilter filter, Set<FlowFileRecord> expiredRecords) {
        final List<FlowFileRecord> flowFiles = localPartition.poll(filter, expiredRecords);
        onAbort(expiredRecords);
        return flowFiles;
    }

    @Override
    public void acknowledge(final FlowFileRecord flowFile) {
        localPartition.acknowledge(flowFile);

        adjustSize(-1, -flowFile.getSize());
        eventListener.triggerSourceEvent();
    }

    @Override
    public void acknowledge(final Collection<FlowFileRecord> flowFiles) {
        localPartition.acknowledge(flowFiles);

        if (!flowFiles.isEmpty()) {
            final long bytes = flowFiles.stream().mapToLong(FlowFileRecord::getSize).sum();
            adjustSize(-flowFiles.size(), -bytes);
        }

        eventListener.triggerSourceEvent();
    }

    @Override
    public boolean isUnacknowledgedFlowFile() {
        return localPartition.isUnacknowledgedFlowFile();
    }

    @Override
    public FlowFileRecord getFlowFile(final String flowFileUuid) throws IOException {
        return localPartition.getFlowFile(flowFileUuid);
    }

    @Override
    public boolean isPropagateBackpressureAcrossNodes() {
        // If offloaded = false, the queue is not offloading; return true to honor backpressure
        // If offloaded = true, the queue is offloading or has finished offloading; return false to ignore backpressure
        return !offloaded;
    }

    @Override
    public void handleExpiredRecords(final Collection<FlowFileRecord> expired) {
        if (expired == null || expired.isEmpty()) {
            return;
        }

        logger.info("{} {} FlowFiles have expired and will be removed", new Object[] {this, expired.size()});
        final List<RepositoryRecord> expiredRecords = new ArrayList<>(expired.size());
        final List<ProvenanceEventRecord> provenanceEvents = new ArrayList<>(expired.size());

        for (final FlowFileRecord flowFile : expired) {
            final StandardRepositoryRecord record = new StandardRepositoryRecord(this, flowFile);
            record.markForDelete();
            expiredRecords.add(record);

            final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder()
                    .fromFlowFile(flowFile)
                    .setEventType(ProvenanceEventType.EXPIRE)
                    .setDetails("Expiration Threshold = " + getFlowFileExpiration())
                    .setComponentType("Load-Balanced Connection")
                    .setComponentId(getIdentifier())
                    .setEventTime(System.currentTimeMillis());

            final ContentClaim contentClaim = flowFile.getContentClaim();
            if (contentClaim != null) {
                final ResourceClaim resourceClaim = contentClaim.getResourceClaim();
                builder.setCurrentContentClaim(resourceClaim.getContainer(), resourceClaim.getSection(), resourceClaim.getId(),
                        contentClaim.getOffset() + flowFile.getContentClaimOffset(), flowFile.getSize());

                builder.setPreviousContentClaim(resourceClaim.getContainer(), resourceClaim.getSection(), resourceClaim.getId(),
                        contentClaim.getOffset() + flowFile.getContentClaimOffset(), flowFile.getSize());
            }

            final ProvenanceEventRecord provenanceEvent = builder.build();
            provenanceEvents.add(provenanceEvent);

            final long flowFileLife = System.currentTimeMillis() - flowFile.getEntryDate();
            logger.info("{} terminated due to FlowFile expiration; life of FlowFile = {} ms", new Object[] {flowFile, flowFileLife});
        }

        try {
            flowFileRepo.updateRepository(expiredRecords);

            for (final RepositoryRecord expiredRecord : expiredRecords) {
                contentRepo.decrementClaimantCount(expiredRecord.getCurrentClaim());
            }

            provRepo.registerEvents(provenanceEvents);

            adjustSize(-expired.size(), -expired.stream().mapToLong(FlowFileRecord::getSize).sum());
        } catch (IOException e) {
            logger.warn("Encountered {} expired FlowFiles but failed to update FlowFile Repository. This FlowFiles may re-appear in the queue after NiFi is restarted and will be expired again at " +
                    "that point.", expiredRecords.size(), e);
        }
    }


    @Override
    protected List<FlowFileRecord> getListableFlowFiles() {
        return localPartition.getListableFlowFiles();
    }

    @Override
    protected void dropFlowFiles(final DropFlowFileRequest dropRequest, final String requestor) {
        partitionReadLock.lock();
        try {
            dropRequest.setOriginalSize(size());
            dropRequest.setState(DropFlowFileState.DROPPING_FLOWFILES);

            int droppedCount = 0;
            long droppedBytes = 0;

            try {
                for (final QueuePartition partition : queuePartitions) {
                    final DropFlowFileRequest partitionRequest = new DropFlowFileRequest(dropRequest.getRequestIdentifier() + "-" + localPartition.getNodeIdentifier());

                    partition.dropFlowFiles(partitionRequest, requestor);

                    adjustSize(-partitionRequest.getDroppedSize().getObjectCount(), -partitionRequest.getDroppedSize().getByteCount());
                    dropRequest.setDroppedSize(new QueueSize(dropRequest.getDroppedSize().getObjectCount() + partitionRequest.getDroppedSize().getObjectCount(),
                            dropRequest.getDroppedSize().getByteCount() + partitionRequest.getDroppedSize().getByteCount()));

                    droppedCount += partitionRequest.getDroppedSize().getObjectCount();
                    droppedBytes += partitionRequest.getDroppedSize().getByteCount();

                    dropRequest.setDroppedSize(new QueueSize(droppedCount, droppedBytes));
                    dropRequest.setCurrentSize(size());

                    if (partitionRequest.getState() == DropFlowFileState.CANCELED) {
                        dropRequest.cancel();
                        break;
                    } else if (partitionRequest.getState() == DropFlowFileState.FAILURE) {
                        dropRequest.setState(DropFlowFileState.FAILURE, partitionRequest.getFailureReason());
                        break;
                    }
                }

                if (dropRequest.getState() == DropFlowFileState.DROPPING_FLOWFILES) {
                    dropRequest.setState(DropFlowFileState.COMPLETE);
                }
            } catch (final Exception e) {
                logger.error("Failed to drop FlowFiles for {}", this, e);
                dropRequest.setState(DropFlowFileState.FAILURE, "Failed to drop FlowFiles due to " + e.getMessage() + ". See log for more details.");
            }
        } finally {
            partitionReadLock.unlock();
        }
    }

    @Override
    public void lock() {
        partitionReadLock.lock();
    }

    @Override
    public void unlock() {
        partitionReadLock.unlock();
    }

    private class ClusterEventListener implements ClusterTopologyEventListener {
        @Override
        public void onNodeAdded(final NodeIdentifier nodeId) {
            partitionWriteLock.lock();
            try {
                final Set<NodeIdentifier> updatedNodeIds = new HashSet<>(nodeIdentifiers);
                updatedNodeIds.add(nodeId);

                logger.debug("Node Identifier {} added to cluster. Node ID's changing from {} to {}", nodeId, nodeIdentifiers, updatedNodeIds);
                setNodeIdentifiers(updatedNodeIds, false);
            } finally {
                partitionWriteLock.unlock();
            }
        }

        @Override
        public void onNodeRemoved(final NodeIdentifier nodeId) {
            partitionWriteLock.lock();
            try {
                final Set<NodeIdentifier> updatedNodeIds = new HashSet<>(nodeIdentifiers);
                final boolean removed = updatedNodeIds.remove(nodeId);
                if (!removed) {
                    return;
                }

                logger.debug("Node Identifier {} removed from cluster. Node ID's changing from {} to {}", nodeId, nodeIdentifiers, updatedNodeIds);
                setNodeIdentifiers(updatedNodeIds, false);
            } finally {
                partitionWriteLock.unlock();
            }
        }

        @Override
        public void onLocalNodeIdentifierSet(final NodeIdentifier localNodeId) {
            partitionWriteLock.lock();
            try {
                if (localNodeId == null) {
                    return;
                }

                if (!nodeIdentifiers.contains(localNodeId)) {
                    final Set<NodeIdentifier> updatedNodeIds = new HashSet<>(nodeIdentifiers);
                    updatedNodeIds.add(localNodeId);

                    logger.debug("Local Node Identifier has now been determined to be {}. Adding to set of Node Identifiers for {}", localNodeId, SocketLoadBalancedFlowFileQueue.this);
                    setNodeIdentifiers(updatedNodeIds, false);
                }

                logger.debug("Local Node Identifier set to {}; current partitions = {}", localNodeId, queuePartitions);

                for (final QueuePartition partition : queuePartitions) {
                    final Optional<NodeIdentifier> nodeIdentifierOption = partition.getNodeIdentifier();
                    if (!nodeIdentifierOption.isPresent()) {
                        continue;
                    }

                    final NodeIdentifier nodeIdentifier = nodeIdentifierOption.get();
                    if (nodeIdentifier.equals(localNodeId)) {
                        if (partition instanceof LocalQueuePartition) {
                            logger.debug("{} Local Node Identifier set to {} and QueuePartition with this identifier is already a Local Queue Partition", SocketLoadBalancedFlowFileQueue.this,
                                    localNodeId);
                            break;
                        }

                        logger.debug("{} Local Node Identifier set to {} and found Queue Partition {} with that Node Identifier. Will force update of partitions",
                                SocketLoadBalancedFlowFileQueue.this, localNodeId, partition);

                        final Set<NodeIdentifier> updatedNodeIds = new HashSet<>(nodeIdentifiers);
                        updatedNodeIds.add(localNodeId);
                        setNodeIdentifiers(updatedNodeIds, true);
                        return;
                    }
                }

                logger.debug("{} Local Node Identifier set to {} but found no Queue Partition with that Node Identifier.", SocketLoadBalancedFlowFileQueue.this, localNodeId);
            } finally {
                partitionWriteLock.unlock();
            }
        }

        @Override
        public void onNodeStateChange(final NodeIdentifier nodeId, final NodeConnectionState newState) {
            partitionWriteLock.lock();
            try {
                if (!offloaded) {
                    return;
                }

                switch (newState) {
                    case CONNECTED:
                        if (nodeId != null && nodeId.equals(clusterCoordinator.getLocalNodeIdentifier())) {
                            // the node with this queue was connected to the cluster, make sure the queue is not offloaded
                            resetOffloadedQueue();
                        }
                        break;
                    case OFFLOADED:
                    case OFFLOADING:
                    case DISCONNECTED:
                    case DISCONNECTING:
                        onNodeRemoved(nodeId);
                        break;
                }
            } finally {
                partitionWriteLock.unlock();
            }
        }
    }
}

