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
package org.apache.nifi.web.api;

import jakarta.annotation.PreDestroy;
import jakarta.ws.rs.HttpMethod;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.bulkreplay.BulkReplayItemStatus;
import org.apache.nifi.web.api.dto.bulkreplay.BulkReplayJobItemDTO;
import org.apache.nifi.web.api.dto.bulkreplay.BulkReplayJobStatus;
import org.apache.nifi.web.api.entity.SubmitReplayRequestEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.context.SecurityContextImpl;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Executes bulk replay jobs asynchronously on the primary node.
 *
 * <p>Items are processed sequentially within each job. The worker checks
 * {@link BulkReplayJob#isCancelRequested()} and primary-node status between items,
 * stopping early if cancellation is requested or the primary role is lost.
 * Progress is written directly into the thread-safe {@link BulkReplayJob}
 * object, so any poll of {@code GET /bulk-replay/jobs/{id}} returns live status.</p>
 *
 * <p>When a cluster node is disconnected, items whose content resides on that
 * node cannot be replayed. On first encounter, the worker waits up to
 * {@code nifi.bulk.replay.node.disconnect.timeout} (default 5 min) for the
 * node to reconnect. If still disconnected after the timeout, all remaining
 * items for that node are immediately marked FAILED.</p>
 *
 * <p>The thread pool size is controlled by {@code nifi.bulk.replay.max.concurrent}
 * (default {@value NiFiProperties#DEFAULT_BULK_REPLAY_MAX_CONCURRENT}). Each thread
 * processes one job at a time, so the value caps the number of concurrently executing jobs.</p>
 */
@Service
public class BulkReplayExecutionService {

    private static final Logger logger = LoggerFactory.getLogger(BulkReplayExecutionService.class);

    private long nodeCheckPollIntervalMs = 5000;

    private volatile NiFiServiceFacade serviceFacade;
    private ExecutorService executorService;
    private volatile Supplier<Boolean> primaryNodeChecker = () -> true;
    private volatile ClusterCoordinator clusterCoordinator;
    private volatile RequestReplicator requestReplicator;
    private volatile long disconnectTimeoutMs = TimeUnit.MINUTES.toMillis(5);
    private String apiScheme = "https";

    @Autowired
    public void setProperties(final NiFiProperties properties) {
        final int maxConcurrent = properties.getBulkReplayMaxConcurrent();
        this.executorService = Executors.newFixedThreadPool(maxConcurrent);
        logger.info("Bulk replay executor configured with {} worker thread(s)", maxConcurrent);

        final String timeoutStr = properties.getBulkReplayNodeDisconnectTimeout();
        try {
            this.disconnectTimeoutMs = FormatUtils.getTimeDuration(timeoutStr, TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            logger.warn("Invalid bulk replay disconnect timeout [{}], using default 5 mins", timeoutStr);
            this.disconnectTimeoutMs = TimeUnit.MINUTES.toMillis(5);
        }
        logger.info("Bulk replay node disconnect timeout: {} ms", disconnectTimeoutMs);

        this.apiScheme = properties.getSslPort() != null ? "https" : "http";
    }

    @Autowired(required = false)
    public void setClusterCoordinator(final ClusterCoordinator clusterCoordinator) {
        if (clusterCoordinator != null) {
            this.clusterCoordinator = clusterCoordinator;
            this.primaryNodeChecker = () -> {
                final NodeIdentifier primary = clusterCoordinator.getPrimaryNode();
                final NodeIdentifier local = clusterCoordinator.getLocalNodeIdentifier();
                return primary != null && primary.equals(local);
            };
        }
    }

    @PreDestroy
    void shutdown() {
        if (executorService == null) {
            return;
        }
        executorService.shutdownNow();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("Bulk replay executor did not terminate in time");
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Starts asynchronous execution of the given job. Returns immediately.
     * Only called on the primary node after the job is registered in the store.
     */
    public void executeJob(final BulkReplayJob job) {
        final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        executorService.submit(() -> {
            SecurityContextHolder.setContext(new SecurityContextImpl(authentication));
            try {
                runJob(job);
            } finally {
                SecurityContextHolder.clearContext();
            }
        });
    }

    private void runJob(final BulkReplayJob job) {
        try {
            job.setStatus(BulkReplayJobStatus.RUNNING);
            if (job.getStartTime() == null) {
                job.setStartTime(Instant.now());
            }
            job.setStatusMessage("Running");
            job.setCancelRequested(false);

            // On resume after failover, item statuses may already reflect progress
            // synced from the previous primary. Recalculate counters to match.
            job.initializeCountersFromItems();

            final List<BulkReplayJobItem> items = job.getItems();
            final int total = items.size();

            if (total == 0) {
                job.setStatus(BulkReplayJobStatus.COMPLETED);
                job.setStatusMessage("Completed — no items to replay");
                job.setEndTime(Instant.now());
                return;
            }

            // When resuming after interruption, reset SKIPPED items back to QUEUED
            // so they are picked up in this run. Already SUCCEEDED/FAILED items are
            // left as-is and skipped below.
            for (final BulkReplayJobItem item : items) {
                if (item.getStatus() == BulkReplayItemStatus.SKIPPED) {
                    item.setStatus(BulkReplayItemStatus.QUEUED);
                }
            }

            // Pass 1: process items on connected nodes, defer disconnected-node items.
            final List<BulkReplayJobItem> deferredItems = new ArrayList<>();
            final Set<String> disconnectedNodes = new HashSet<>();
            long disconnectDeadlineMs = 0;

            for (int i = 0; i < total; i++) {
                if (checkCancelOrPrimaryLoss(job, items, i)) {
                    return;
                }

                final BulkReplayJobItem item = items.get(i);

                // Skip items already processed in a previous run (resume after interruption).
                if (item.getStatus() == BulkReplayItemStatus.SUCCEEDED || item.getStatus() == BulkReplayItemStatus.FAILED) {
                    continue;
                }

                final String nodeId = item.getClusterNodeId();

                // Defer items whose content node is disconnected (cluster mode only).
                if (nodeId != null && clusterCoordinator != null && !isNodeConnected(nodeId)) {
                    disconnectedNodes.add(nodeId);
                    deferredItems.add(item);

                    // Start the timeout on first detection so the countdown is visible in the UI.
                    if (disconnectDeadlineMs == 0) {
                        disconnectDeadlineMs = System.currentTimeMillis() + disconnectTimeoutMs;
                        job.setDisconnectWaitDeadline(Instant.ofEpochMilli(disconnectDeadlineMs));
                        logger.info("Bulk replay job {}: node {} is disconnected, timeout deadline set",
                                job.getJobId(), nodeLabel(nodeId));
                    }
                    continue;
                }

                replayItem(job, item, i + 1, total);
                if (!deferredItems.isEmpty()) {
                    // Append deferred count after replayItem sets its own status message.
                    job.setStatusMessage(job.getStatusMessage()
                            + " — " + deferredItems.size() + " item(s) waiting for disconnected node");
                }
            }

            // Pass 2: handle deferred items.
            if (!deferredItems.isEmpty()) {
                final Set<String> timedOutNodes = new HashSet<>();

                for (final String nodeId : disconnectedNodes) {
                    if (job.isCancelRequested()) {
                        break;
                    }
                    if (isNodeConnected(nodeId)) {
                        logger.info("Bulk replay job {}: content node {} reconnected during pass 1", job.getJobId(), nodeLabel(nodeId));
                        continue;
                    }

                    // Check if the timeout already expired during pass 1.
                    if (System.currentTimeMillis() >= disconnectDeadlineMs) {
                        timedOutNodes.add(nodeId);
                        logger.warn("Bulk replay job {}: content node {} did not reconnect within timeout", job.getJobId(), nodeLabel(nodeId));
                        continue;
                    }

                    final String label = nodeLabel(nodeId);
                    logger.info("Bulk replay job {}: waiting for node {} ({}ms remaining)",
                            job.getJobId(), label, disconnectDeadlineMs - System.currentTimeMillis());
                    job.setStatusMessage("Waiting for disconnected node " + label + " to rejoin cluster");

                    while (!isNodeConnected(nodeId) && System.currentTimeMillis() < disconnectDeadlineMs) {
                        if (job.isCancelRequested()) {
                            break;
                        }
                        try {
                            Thread.sleep(nodeCheckPollIntervalMs);
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }

                    if (!isNodeConnected(nodeId)) {
                        timedOutNodes.add(nodeId);
                        logger.warn("Bulk replay job {}: content node {} did not reconnect within timeout", job.getJobId(), label);
                    } else {
                        logger.info("Bulk replay job {}: content node {} reconnected", job.getJobId(), label);
                    }
                }

                job.setDisconnectWaitDeadline(null);

                // Replay or fail each deferred item
                for (int d = 0; d < deferredItems.size(); d++) {
                    if (checkCancelOrPrimaryLoss(job, deferredItems, d)) {
                        return;
                    }

                    final BulkReplayJobItem item = deferredItems.get(d);
                    final String nodeId = item.getClusterNodeId();
                    if (timedOutNodes.contains(nodeId)) {
                        item.setStatus(BulkReplayItemStatus.FAILED);
                        item.setErrorMessage("Content node " + nodeLabel(nodeId) + " disconnected — timed out waiting for reconnection");
                        item.setEndTime(Instant.now());
                        job.recordItemProcessed(false);
                        syncItemToCluster(job.getJobId(), item);
                    } else {
                        replayItem(job, item, total - deferredItems.size() + d + 1, total);
                    }
                }
            }

            final int failed = job.getFailedItems();
            final int succeeded = job.getSucceededItems();
            if (failed == 0) {
                job.setStatus(BulkReplayJobStatus.COMPLETED);
            } else if (succeeded == 0) {
                job.setStatus(BulkReplayJobStatus.FAILED);
            } else {
                job.setStatus(BulkReplayJobStatus.PARTIAL_SUCCESS);
            }
            job.setStatusMessage("Completed: " + succeeded + " succeeded, " + failed + " failed");
            job.setEndTime(Instant.now());

        } catch (final Exception e) {
            logger.error("Bulk replay job {} failed unexpectedly: {}", job.getJobId(), e.getMessage(), e);
            job.setStatus(BulkReplayJobStatus.FAILED);
            job.setStatusMessage("Internal error: " + e.getMessage());
            job.setEndTime(Instant.now());
        }
    }

    /**
     * Checks for cancellation or primary-node loss. If either condition is detected,
     * marks all remaining items as SKIPPED and sets the appropriate terminal status.
     *
     * @return {@code true} if the job was terminated and the caller should return
     */
    private boolean checkCancelOrPrimaryLoss(final BulkReplayJob job, final List<? extends BulkReplayJobItem> items,
                                             final int currentIndex) {
        if (job.isCancelRequested()) {
            for (int j = currentIndex; j < items.size(); j++) {
                if (items.get(j).getStatus() == BulkReplayItemStatus.QUEUED) {
                    items.get(j).setStatus(BulkReplayItemStatus.SKIPPED);
                }
            }
            job.setStatus(BulkReplayJobStatus.CANCELLED);
            job.setStatusMessage("Cancelled");
            job.setEndTime(Instant.now());
            return true;
        }

        if (!primaryNodeChecker.get()) {
            logger.info("Bulk replay job {} interrupted — this node is no longer primary", job.getJobId());
            for (int j = currentIndex; j < items.size(); j++) {
                if (items.get(j).getStatus() == BulkReplayItemStatus.QUEUED) {
                    items.get(j).setStatus(BulkReplayItemStatus.SKIPPED);
                }
            }
            job.setStatus(BulkReplayJobStatus.INTERRUPTED);
            job.setStatusMessage("Interrupted — primary node role was lost");
            job.setSubmitted(false);
            job.setEndTime(Instant.now());
            return true;
        }

        return false;
    }

    /**
     * Replays a single item, updating its status and the job's progress counters.
     *
     * <p>In a cluster, replay must execute on the node that holds the original content.
     * If the item's {@code clusterNodeId} matches the local (primary) node or is null
     * (standalone mode), the replay runs locally. Otherwise, the request is forwarded
     * to the target node via the {@link RequestReplicator}.</p>
     */
    private void replayItem(final BulkReplayJob job, final BulkReplayJobItem item, final int itemNumber, final int total) {
        item.setStatus(BulkReplayItemStatus.RUNNING);
        item.setStartTime(Instant.now());
        job.setStatusMessage("Replaying item " + itemNumber + " of " + total);

        boolean succeeded = false;
        try {
            if (shouldReplayLocally(item)) {
                serviceFacade.submitReplay(item.getProvenanceEventId());
            } else {
                replayOnRemoteNode(item);
            }
            item.setStatus(BulkReplayItemStatus.SUCCEEDED);
            item.setEndTime(Instant.now());
            succeeded = true;
        } catch (final Exception e) {
            logger.warn("Bulk replay: failed to replay event {} in job {}: {}",
                    item.getProvenanceEventId(), job.getJobId(), e.getMessage());
            item.setStatus(BulkReplayItemStatus.FAILED);
            item.setErrorMessage(e.getMessage() != null ? e.getMessage() : "Replay failed");
            item.setEndTime(Instant.now());
        }

        job.recordItemProcessed(succeeded);
        syncItemToCluster(job.getJobId(), item);
    }

    /**
     * Returns {@code true} if this item should be replayed on the local (primary) node.
     * This is the case when running standalone or when the item originated on this node.
     */
    private boolean shouldReplayLocally(final BulkReplayJobItem item) {
        final String nodeId = item.getClusterNodeId();
        if (nodeId == null || clusterCoordinator == null) {
            return true;
        }
        final NodeIdentifier localNode = clusterCoordinator.getLocalNodeIdentifier();
        return localNode != null && localNode.getId().equals(nodeId);
    }

    /**
     * Forwards a replay request to a remote cluster node via the {@link RequestReplicator}.
     */
    private void replayOnRemoteNode(final BulkReplayJobItem item) {
        final NodeIdentifier targetNode = clusterCoordinator.getNodeIdentifier(item.getClusterNodeId());
        if (targetNode == null) {
            throw new IllegalStateException("Unknown cluster node: " + item.getClusterNodeId());
        }

        final SubmitReplayRequestEntity entity = new SubmitReplayRequestEntity();
        entity.setEventId(item.getProvenanceEventId());
        entity.setClusterNodeId(item.getClusterNodeId());

        final URI replayUri = URI.create(apiScheme + "://"
                + targetNode.getApiAddress() + ":" + targetNode.getApiPort()
                + "/nifi-api/provenance-events/replays");

        final NodeResponse nodeResponse;
        try {
            nodeResponse = requestReplicator.replicate(
                    Collections.singleton(targetNode),
                    HttpMethod.POST,
                    replayUri,
                    entity,
                    Collections.emptyMap(),
                    true,
                    false
            ).awaitMergedResponse();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for remote replay response from node " + item.getClusterNodeId(), e);
        }

        if (nodeResponse.getStatus() < 200 || nodeResponse.getStatus() >= 300) {
            final String detail = nodeResponse.hasThrowable()
                    ? ": " + nodeResponse.getThrowable().getMessage()
                    : "";
            if (nodeResponse.hasThrowable()) {
                logger.warn("Remote replay failed on node {} with status {}",
                        item.getClusterNodeId(), nodeResponse.getStatus(), nodeResponse.getThrowable());
            }
            throw new RuntimeException("Remote replay failed on node " + item.getClusterNodeId()
                    + " with status " + nodeResponse.getStatus() + detail);
        }
    }

    /**
     * Replicates the completed item status to all other cluster nodes for failover recovery.
     */
    private void syncItemToCluster(final String jobId, final BulkReplayJobItem item) {
        if (clusterCoordinator == null || requestReplicator == null) {
            return;
        }

        final Map<NodeConnectionState, List<NodeIdentifier>> stateMap = clusterCoordinator.getConnectionStates();
        final List<NodeIdentifier> connectedNodes = stateMap.get(NodeConnectionState.CONNECTED);
        if (connectedNodes == null || connectedNodes.isEmpty()) {
            return;
        }

        final NodeIdentifier localNode = clusterCoordinator.getLocalNodeIdentifier();
        final Set<NodeIdentifier> otherNodes = connectedNodes.stream()
                .filter(n -> !n.equals(localNode))
                .collect(Collectors.toSet());

        if (otherNodes.isEmpty()) {
            return;
        }

        final BulkReplayJobItemDTO dto = new BulkReplayJobItemDTO();
        dto.setItemIndex(item.getItemIndex());
        dto.setStatus(item.getStatus());
        dto.setErrorMessage(item.getErrorMessage());

        final URI syncUri = URI.create(apiScheme + "://localhost/nifi-api/bulk-replay/jobs/"
                + jobId + "/items/" + item.getItemIndex() + "/status");

        try {
            requestReplicator.replicate(
                    otherNodes,
                    HttpMethod.PUT,
                    syncUri,
                    dto,
                    Collections.emptyMap(),
                    true,
                    false
            ).awaitMergedResponse();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while syncing item {} of job {} to cluster", item.getItemIndex(), jobId);
        } catch (final Exception e) {
            logger.warn("Failed to sync item {} of job {} to cluster: {}", item.getItemIndex(), jobId, e.getMessage());
        }
    }

    /**
     * Returns a human-readable label for a cluster node, preferring hostname:port over the raw UUID.
     */
    private String nodeLabel(final String nodeId) {
        if (clusterCoordinator == null) {
            return nodeId;
        }
        final NodeIdentifier node = clusterCoordinator.getNodeIdentifier(nodeId);
        return node != null ? node.toString() : nodeId;
    }

    /**
     * Checks whether the cluster node with the given ID is currently connected.
     */
    private boolean isNodeConnected(final String nodeId) {
        if (clusterCoordinator == null) {
            return true;
        }
        final NodeIdentifier node = clusterCoordinator.getNodeIdentifier(nodeId);
        if (node == null) {
            return false;
        }
        final NodeConnectionStatus status = clusterCoordinator.getConnectionStatus(node);
        return status != null && status.getState() == NodeConnectionState.CONNECTED;
    }

    /**
     * Returns the current node disconnect timeout as a human-readable NiFi duration string.
     */
    public String getNodeDisconnectTimeout() {
        final long ms = disconnectTimeoutMs;
        if (ms % 60000 == 0) {
            return (ms / 60000) + " mins";
        } else if (ms % 1000 == 0) {
            return (ms / 1000) + " secs";
        }
        return ms + " ms";
    }

    /**
     * Updates the node disconnect timeout at runtime (not persisted to nifi.properties).
     *
     * @param timeout a NiFi duration string such as "5 mins" or "30 secs"
     * @throws IllegalArgumentException if the string cannot be parsed
     */
    public void setNodeDisconnectTimeout(final String timeout) {
        final long ms = FormatUtils.getTimeDuration(timeout, TimeUnit.MILLISECONDS);
        this.disconnectTimeoutMs = ms;
        logger.info("Bulk replay node disconnect timeout updated to: {} ({}ms)", timeout, ms);
    }

    @Autowired
    public void setServiceFacade(final NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    @Autowired(required = false)
    public void setRequestReplicator(final RequestReplicator requestReplicator) {
        this.requestReplicator = requestReplicator;
    }
}
