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
import java.util.Properties;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public abstract class AbstractHeartbeatMonitor implements HeartbeatMonitor {

    private final int heartbeatIntervalMillis;
    private static final Logger logger = LoggerFactory.getLogger(AbstractHeartbeatMonitor.class);
    protected final ClusterCoordinator clusterCoordinator;
    protected final FlowEngine flowEngine = new FlowEngine(1, "Heartbeat Monitor", true);

    protected volatile long latestHeartbeatTime;
    private volatile ScheduledFuture<?> future;
    private volatile boolean stopped = true;


    public AbstractHeartbeatMonitor(final ClusterCoordinator clusterCoordinator, final Properties properties) {
        this.clusterCoordinator = clusterCoordinator;
        final String heartbeatInterval = properties.getProperty(NiFiProperties.CLUSTER_PROTOCOL_HEARTBEAT_INTERVAL,
            NiFiProperties.DEFAULT_CLUSTER_PROTOCOL_HEARTBEAT_INTERVAL);
        this.heartbeatIntervalMillis = (int) FormatUtils.getTimeDuration(heartbeatInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized final void start() {
        if (!stopped) {
            throw new IllegalStateException("Heartbeat Monitor cannot be started because it is already started");
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
     * Fetches all of the latest heartbeats and updates the Cluster Coordinator as appropriate,
     * based on the heartbeats received.
     *
     * Visible for testing.
     */
    protected synchronized void monitorHeartbeats() {
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
        final long threshold = System.currentTimeMillis() - maxMillis;
        for (final NodeHeartbeat heartbeat : latestHeartbeats.values()) {
            if (heartbeat.getTimestamp() < threshold) {
                clusterCoordinator.requestNodeDisconnect(heartbeat.getNodeIdentifier(), DisconnectionCode.LACK_OF_HEARTBEAT,
                    "Latest heartbeat from Node has expired");

                try {
                    removeHeartbeat(heartbeat.getNodeIdentifier());
                } catch (final Exception e) {
                    logger.warn("Failed to remove heartbeat for {} due to {}", heartbeat.getNodeIdentifier(), e.toString());
                    logger.warn("", e);
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
            if (disconnectionCode == DisconnectionCode.LACK_OF_HEARTBEAT || disconnectionCode == DisconnectionCode.UNABLE_TO_COMMUNICATE) {
                clusterCoordinator.reportEvent(nodeId, Severity.INFO, "Received heartbeat from node previously "
                    + "disconnected due to " + disconnectionCode + ". Issuing reconnection request.");

                clusterCoordinator.requestNodeConnect(nodeId, null);
            } else {
                // disconnected nodes should not heartbeat, so we need to issue a disconnection request.
                logger.info("Ignoring received heartbeat from disconnected node " + nodeId + ".  Issuing disconnection request.");
                clusterCoordinator.requestNodeDisconnect(nodeId, DisconnectionCode.HEARTBEAT_RECEIVED_FROM_DISCONNECTED_NODE, DisconnectionCode.HEARTBEAT_RECEIVED_FROM_DISCONNECTED_NODE.toString());
                removeHeartbeat(nodeId);
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

        clusterCoordinator.updateNodeRoles(nodeId, heartbeat.getRoles());
    }


    /**
     * @return the most recent heartbeat information for each node in the cluster
     */
    protected abstract Map<NodeIdentifier, NodeHeartbeat> getLatestHeartbeats();

    /**
     * This method does nothing in the abstract class but is meant for subclasses to
     * override in order to provide functionality when the monitor is started.
     */
    protected void onStart() {
    }

    /**
     * This method does nothing in the abstract class but is meant for subclasses to
     * override in order to provide functionality when the monitor is stopped.
     */
    protected void onStop() {
    }
}