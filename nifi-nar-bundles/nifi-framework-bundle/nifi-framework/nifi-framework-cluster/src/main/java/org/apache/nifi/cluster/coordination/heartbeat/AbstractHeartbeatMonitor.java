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
package org.apache.nifi.cluster.coordination.heartbeat;

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.DisconnectionCode;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public abstract class AbstractHeartbeatMonitor implements HeartbeatMonitor {

    private final int heartbeatIntervalMillis;
    private static final Logger logger = LoggerFactory.getLogger(AbstractHeartbeatMonitor.class);
    protected final ClusterCoordinator clusterCoordinator;
    protected final FlowEngine flowEngine = new FlowEngine(1, "Heartbeat Monitor", true);

    private volatile ScheduledFuture<?> future;
    private volatile boolean stopped = true;

    public AbstractHeartbeatMonitor(final ClusterCoordinator clusterCoordinator, final NiFiProperties nifiProperties) {
        this.clusterCoordinator = clusterCoordinator;
        final String heartbeatInterval = nifiProperties.getProperty(NiFiProperties.CLUSTER_PROTOCOL_HEARTBEAT_INTERVAL,
                NiFiProperties.DEFAULT_CLUSTER_PROTOCOL_HEARTBEAT_INTERVAL);
        this.heartbeatIntervalMillis = (int) FormatUtils.getTimeDuration(heartbeatInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized final void start() {
        if (!stopped) {
            logger.info("Attempted to start Heartbeat Monitor but it is already started. Stopping heartbeat monitor and re-starting it.");
            stop();
        }

        stopped = false;
        logger.info("Heartbeat Monitor started");

        try {
            onStart();
        } catch (final Exception e) {
            logger.error("Failed to start Heartbeat Monitor", e);
        }

        this.future = flowEngine.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    monitorHeartbeats();
                } catch (final Exception e) {
                    clusterCoordinator.reportEvent(null, Severity.ERROR, "Failed to process heartbeats from nodes due to " + e.toString());
                    logger.error("Failed to process heartbeats", e);
                }
            }
        }, heartbeatIntervalMillis, heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized final void stop() {
        if (stopped) {
            return;
        }

        this.stopped = true;
        logger.info("Heartbeat Monitor stopped");

        try {
            if (future != null) {
                future.cancel(true);
            }
        } finally {
            onStop();
        }
    }

    protected boolean isStopped() {
        return stopped;
    }

    @Override
    public NodeHeartbeat getLatestHeartbeat(final NodeIdentifier nodeId) {
        return getLatestHeartbeats().get(nodeId);
    }

    protected ClusterCoordinator getClusterCoordinator() {
        return clusterCoordinator;
    }

    protected long getHeartbeatInterval(final TimeUnit timeUnit) {
        return timeUnit.convert(heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Fetches all of the latest heartbeats and updates the Cluster Coordinator
     * as appropriate, based on the heartbeats received.
     *
     * Visible for testing.
     */
    protected synchronized void monitorHeartbeats() {
        final NodeIdentifier activeCoordinator = clusterCoordinator.getElectedActiveCoordinatorNode();
        if (activeCoordinator != null && !activeCoordinator.equals(clusterCoordinator.getLocalNodeIdentifier())) {
            // Occasionally Curator appears to not notify us that we have lost the elected leader role, or does so
            // on a very large delay. So before we kick the node out of the cluster, we want to first check what the
            // ZNode in ZooKeeper says, and ensure that this is the node that is being advertised as the appropriate
            // destination for heartbeats.
            logger.debug("It appears that this node is no longer the actively elected cluster coordinator. Will not request that node disconnect.");
            return;
        }

        final Map<NodeIdentifier, NodeHeartbeat> latestHeartbeats = getLatestHeartbeats();
        if (latestHeartbeats == null || latestHeartbeats.isEmpty()) {
            logger.debug("Received no new heartbeats. Will not disconnect any nodes due to lack of heartbeat");
            return;
        }

        final StopWatch procStopWatch = new StopWatch(true);
        for (final NodeHeartbeat heartbeat : latestHeartbeats.values()) {
            try {
                processHeartbeat(heartbeat);
            } catch (final Exception e) {
                clusterCoordinator.reportEvent(null, Severity.ERROR,
                        "Received heartbeat from " + heartbeat.getNodeIdentifier() + " but failed to process heartbeat due to " + e);
                logger.error("Failed to process heartbeat from {} due to {}", heartbeat.getNodeIdentifier(), e.toString());
                logger.error("", e);
            }
        }

        procStopWatch.stop();
        logger.info("Finished processing {} heartbeats in {}", latestHeartbeats.size(), procStopWatch.getDuration());

        // Disconnect any node that hasn't sent a heartbeat in a long time (8 times the heartbeat interval)
        final long maxMillis = heartbeatIntervalMillis * 8;
        final long currentTimestamp = System.currentTimeMillis();
        final long threshold = currentTimestamp - maxMillis;

        // consider all connected nodes
        for (final NodeIdentifier nodeIdentifier : clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED)) {
            final NodeHeartbeat heartbeat = latestHeartbeats.get(nodeIdentifier);

            // consider the most recent heartbeat for this node
            if (heartbeat == null) {
                final long purgeTimestamp = getPurgeTimestamp();

                // if there is no heartbeat for this node, see if we purged the heartbeats beyond the allowed heartbeat threshold
                if (purgeTimestamp < threshold) {
                    final long secondsSinceLastPurge = TimeUnit.MILLISECONDS.toSeconds(currentTimestamp - purgeTimestamp);

                    clusterCoordinator.disconnectionRequestedByNode(nodeIdentifier, DisconnectionCode.LACK_OF_HEARTBEAT,
                            "Have not received a heartbeat from node in " + secondsSinceLastPurge + " seconds");
                }
            } else {
                // see if the heartbeat occurred before the allowed heartbeat threshold
                if (heartbeat.getTimestamp() < threshold) {
                    final long secondsSinceLastHeartbeat = TimeUnit.MILLISECONDS.toSeconds(currentTimestamp - heartbeat.getTimestamp());

                    clusterCoordinator.disconnectionRequestedByNode(nodeIdentifier, DisconnectionCode.LACK_OF_HEARTBEAT,
                            "Have not received a heartbeat from node in " + secondsSinceLastHeartbeat + " seconds");

                    try {
                        removeHeartbeat(nodeIdentifier);
                    } catch (final Exception e) {
                        logger.warn("Failed to remove heartbeat for {} due to {}", nodeIdentifier, e.toString());
                        logger.warn("", e);
                    }
                }
            }
        }
    }

    private void processHeartbeat(final NodeHeartbeat heartbeat) {
        final NodeIdentifier nodeId = heartbeat.getNodeIdentifier();

        // Do not process heartbeat if it's blocked by firewall.
        if (clusterCoordinator.isBlockedByFirewall(nodeId.getSocketAddress())) {
            clusterCoordinator.reportEvent(nodeId, Severity.WARNING, "Firewall blocked received heartbeat. Issuing disconnection request.");

            // request node to disconnect
            clusterCoordinator.requestNodeDisconnect(nodeId, DisconnectionCode.BLOCKED_BY_FIREWALL, "Blocked by Firewall");
            removeHeartbeat(nodeId);
            return;
        }

        final NodeConnectionStatus connectionStatus = clusterCoordinator.getConnectionStatus(nodeId);
        if (connectionStatus == null) {
            // Unknown node. Issue reconnect request
            clusterCoordinator.reportEvent(nodeId, Severity.INFO, "Received heartbeat from unknown node. Removing heartbeat and requesting that node connect to cluster.");
            removeHeartbeat(nodeId);

            clusterCoordinator.requestNodeConnect(nodeId, null);
            return;
        }

        final NodeConnectionState connectionState = connectionStatus.getState();
        if (heartbeat.getConnectionStatus().getState() != NodeConnectionState.CONNECTED && connectionState == NodeConnectionState.CONNECTED) {
            // Cluster Coordinator believes that node is connected, but node does not believe so.
            clusterCoordinator.reportEvent(nodeId, Severity.WARNING, "Received heartbeat from node that thinks it is not yet part of the cluster,"
                    + "though the Cluster Coordinator thought it was (node claimed state was " + heartbeat.getConnectionStatus().getState()
                    + "). Marking as Disconnected and requesting that Node reconnect to cluster");
            clusterCoordinator.requestNodeConnect(nodeId, null);
            return;
        }

        if (NodeConnectionState.DISCONNECTED == connectionState) {
            // ignore heartbeats from nodes disconnected by means other than lack of heartbeat, unless it is
            // the only node. We allow it if it is the only node because if we have a one-node cluster, then
            // we cannot manually reconnect it.
            final DisconnectionCode disconnectionCode = connectionStatus.getDisconnectCode();

            // Determine whether or not the node should be allowed to be in the cluster still, depending on its reason for disconnection.
            switch (disconnectionCode) {
                case LACK_OF_HEARTBEAT:
                case UNABLE_TO_COMMUNICATE:
                case NOT_YET_CONNECTED:
                case STARTUP_FAILURE: {
                    clusterCoordinator.reportEvent(nodeId, Severity.INFO, "Received heartbeat from node previously "
                            + "disconnected due to " + disconnectionCode + ". Issuing reconnection request.");

                    clusterCoordinator.requestNodeConnect(nodeId, null);
                    break;
                }
                default: {
                    // disconnected nodes should not heartbeat, so we need to issue a disconnection request.
                    logger.info("Ignoring received heartbeat from disconnected node " + nodeId + ".  Issuing disconnection request.");
                    clusterCoordinator.requestNodeDisconnect(nodeId, disconnectionCode, connectionStatus.getDisconnectReason());
                    removeHeartbeat(nodeId);
                    break;
                }
            }

            return;
        }

        if (NodeConnectionState.DISCONNECTING == connectionStatus.getState()) {
            // ignore spurious heartbeat
            removeHeartbeat(nodeId);
            return;
        }

        // first heartbeat causes status change from connecting to connected
        if (NodeConnectionState.CONNECTING == connectionState) {
            final Long connectionRequestTime = connectionStatus.getConnectionRequestTime();
            if (connectionRequestTime != null && heartbeat.getTimestamp() < connectionRequestTime) {
                clusterCoordinator.reportEvent(nodeId, Severity.INFO, "Received heartbeat but ignoring because it was reported before the node was last asked to reconnect.");
                removeHeartbeat(nodeId);
                return;
            }

            // connection complete
            clusterCoordinator.finishNodeConnection(nodeId);
            clusterCoordinator.reportEvent(nodeId, Severity.INFO, "Received first heartbeat from connecting node. Node connected.");
        }
    }

    /**
     * @return the most recent heartbeat information for each node in the
     * cluster
     */
    protected abstract Map<NodeIdentifier, NodeHeartbeat> getLatestHeartbeats();

    /**
     * Returns when the heartbeats were purged last.
     *
     * @return when the heartbeats were purged last
     */
    protected abstract long getPurgeTimestamp();

    /**
     * This method does nothing in the abstract class but is meant for
     * subclasses to override in order to provide functionality when the monitor
     * is started.
     */
    protected void onStart() {
    }

    /**
     * This method does nothing in the abstract class but is meant for
     * subclasses to override in order to provide functionality when the monitor
     * is stopped.
     */
    protected void onStop() {
    }
}
