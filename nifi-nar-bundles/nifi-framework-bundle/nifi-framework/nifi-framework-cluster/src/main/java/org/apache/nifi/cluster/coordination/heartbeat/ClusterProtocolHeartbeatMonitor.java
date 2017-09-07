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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.coordination.node.NodeWorkload;
import org.apache.nifi.cluster.protocol.Heartbeat;
import org.apache.nifi.cluster.protocol.HeartbeatPayload;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.ProtocolHandler;
import org.apache.nifi.cluster.protocol.ProtocolListener;
import org.apache.nifi.cluster.protocol.message.ClusterWorkloadRequestMessage;
import org.apache.nifi.cluster.protocol.message.ClusterWorkloadResponseMessage;
import org.apache.nifi.cluster.protocol.message.HeartbeatMessage;
import org.apache.nifi.cluster.protocol.message.HeartbeatResponseMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage.MessageType;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses Apache ZooKeeper to advertise the address to send heartbeats to, and
 * then relies on the NiFi Cluster Protocol to receive heartbeat messages from
 * nodes in the cluster.
 */
public class ClusterProtocolHeartbeatMonitor extends AbstractHeartbeatMonitor implements HeartbeatMonitor, ProtocolHandler {

    protected static final Logger logger = LoggerFactory.getLogger(ClusterProtocolHeartbeatMonitor.class);

    private final String heartbeatAddress;
    private final ConcurrentMap<NodeIdentifier, NodeHeartbeat> heartbeatMessages = new ConcurrentHashMap<>();

    private volatile long purgeTimestamp = System.currentTimeMillis();

    public ClusterProtocolHeartbeatMonitor(final ClusterCoordinator clusterCoordinator, final ProtocolListener protocolListener, final NiFiProperties nifiProperties) {
        super(clusterCoordinator, nifiProperties);

        protocolListener.addHandler(this);

        String hostname = nifiProperties.getProperty(NiFiProperties.CLUSTER_NODE_ADDRESS);
        if (hostname == null || hostname.trim().isEmpty()) {
            hostname = "localhost";
        }

        final String port = nifiProperties.getProperty(NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT);
        if (port == null || port.trim().isEmpty()) {
            throw new RuntimeException("Unable to determine which port Cluster Coordinator Protocol is listening on because the '"
                    + NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT + "' property is not set");
        }

        try {
            Integer.parseInt(port);
        } catch (final NumberFormatException nfe) {
            throw new RuntimeException("Unable to determine which port Cluster Coordinator Protocol is listening on because the '"
                    + NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT + "' property is set to '" + port + "', which is not a valid port number.");
        }

        heartbeatAddress = hostname + ":" + port;
    }

    @Override
    public String getHeartbeatAddress() {
        return heartbeatAddress;
    }

    @Override
    public void onStart() {
        // We don't know what the heartbeats look like for the nodes, since we were just elected to monitoring
        // them. However, the map may be filled with old heartbeats. So we clear the heartbeats and populate the
        // map with new heartbeats set to the current time and using the currently known status. We do this so
        // that if we go the required amount of time without receiving a heartbeat, we do know to mark the node
        // as disconnected.
        heartbeatMessages.clear();
        for (final NodeIdentifier nodeId : clusterCoordinator.getNodeIdentifiers()) {
            final NodeHeartbeat heartbeat = new StandardNodeHeartbeat(nodeId, System.currentTimeMillis(),
                    clusterCoordinator.getConnectionStatus(nodeId), 0, 0L, 0, System.currentTimeMillis());
            heartbeatMessages.put(nodeId, heartbeat);
        }
    }

    @Override
    public void onStop() {
    }

    @Override
    protected Map<NodeIdentifier, NodeHeartbeat> getLatestHeartbeats() {
        return Collections.unmodifiableMap(heartbeatMessages);
    }

    @Override
    public synchronized void removeHeartbeat(final NodeIdentifier nodeId) {
        logger.debug("Deleting heartbeat for node {}", nodeId);
        heartbeatMessages.remove(nodeId);
    }

    @Override
    public synchronized void purgeHeartbeats() {
        logger.debug("Purging old heartbeats");
        heartbeatMessages.clear();
        purgeTimestamp = System.currentTimeMillis();
    }

    @Override
    public synchronized long getPurgeTimestamp() {
        return purgeTimestamp;
    }

    @Override
    public ProtocolMessage handle(final ProtocolMessage msg) throws ProtocolException {
        switch (msg.getType()) {
            case HEARTBEAT:
                return handleHeartbeat((HeartbeatMessage) msg);
            case CLUSTER_WORKLOAD_REQUEST:
                return handleClusterWorkload((ClusterWorkloadRequestMessage) msg);
            default:
                throw new ProtocolException("Cannot handle message of type " + msg.getType());
        }
    }

    private ProtocolMessage handleHeartbeat(final HeartbeatMessage msg) {
        final HeartbeatMessage heartbeatMsg = msg;
        final Heartbeat heartbeat = heartbeatMsg.getHeartbeat();

        final NodeIdentifier nodeId = heartbeat.getNodeIdentifier();
        final NodeConnectionStatus connectionStatus = heartbeat.getConnectionStatus();
        final byte[] payloadBytes = heartbeat.getPayload();
        final HeartbeatPayload payload = HeartbeatPayload.unmarshal(payloadBytes);
        final int activeThreadCount = payload.getActiveThreadCount();
        final int flowFileCount = (int) payload.getTotalFlowFileCount();
        final long flowFileBytes = payload.getTotalFlowFileBytes();
        final long systemStartTime = payload.getSystemStartTime();

        final NodeHeartbeat nodeHeartbeat = new StandardNodeHeartbeat(nodeId, System.currentTimeMillis(),
                connectionStatus, flowFileCount, flowFileBytes, activeThreadCount, systemStartTime);
        heartbeatMessages.put(heartbeat.getNodeIdentifier(), nodeHeartbeat);
        logger.debug("Received new heartbeat from {}", nodeId);

        // Formulate a List of differences between our view of the cluster topology and the node's view
        // and send that back to the node so that it is in-sync with us
        List<NodeConnectionStatus> nodeStatusList = payload.getClusterStatus();
        if (nodeStatusList == null) {
            nodeStatusList = Collections.emptyList();
        }
        final List<NodeConnectionStatus> updatedStatuses = getUpdatedStatuses(nodeStatusList);

        final HeartbeatResponseMessage responseMessage = new HeartbeatResponseMessage();
        responseMessage.setUpdatedNodeStatuses(updatedStatuses);

        if (!getClusterCoordinator().isFlowElectionComplete()) {
            responseMessage.setFlowElectionMessage(getClusterCoordinator().getFlowElectionStatus());
        }

        return responseMessage;
    }

    private ProtocolMessage handleClusterWorkload(final ClusterWorkloadRequestMessage msg) {

        final ClusterWorkloadResponseMessage response = new ClusterWorkloadResponseMessage();
        final Map<NodeIdentifier, NodeWorkload> workloads = new HashMap<>();
        getLatestHeartbeats().values().stream()
            .filter(hb -> NodeConnectionState.CONNECTED.equals(hb.getConnectionStatus().getState()))
            .forEach(hb -> {
                NodeWorkload wl = new NodeWorkload();
                wl.setReportedTimestamp(hb.getTimestamp());
                wl.setSystemStartTime(hb.getSystemStartTime());
                wl.setActiveThreadCount(hb.getActiveThreadCount());
                wl.setFlowFileCount(hb.getFlowFileCount());
                wl.setFlowFileBytes(hb.getFlowFileBytes());
                workloads.put(hb.getNodeIdentifier(), wl);
            });
        response.setNodeWorkloads(workloads);

        return response;
    }

    private List<NodeConnectionStatus> getUpdatedStatuses(final List<NodeConnectionStatus> nodeStatusList) {
        // Map node's statuses by NodeIdentifier for quick & easy lookup
        final Map<NodeIdentifier, NodeConnectionStatus> nodeStatusMap = nodeStatusList.stream()
                .collect(Collectors.toMap(status -> status.getNodeIdentifier(), Function.identity()));

        // Check if our connection status is the same for each Node Identifier and if not, add our version of the status
        // to a List of updated statuses.
        final List<NodeConnectionStatus> currentStatuses = clusterCoordinator.getConnectionStatuses();
        final List<NodeConnectionStatus> updatedStatuses = new ArrayList<>();
        for (final NodeConnectionStatus currentStatus : currentStatuses) {
            final NodeConnectionStatus nodeStatus = nodeStatusMap.get(currentStatus.getNodeIdentifier());
            if (!currentStatus.equals(nodeStatus)) {
                updatedStatuses.add(currentStatus);
            }
        }

        // If the node has any statuses that we do not have, add a REMOVED status to the update list
        final Set<NodeIdentifier> nodeIds = currentStatuses.stream().map(status -> status.getNodeIdentifier()).collect(Collectors.toSet());
        for (final NodeConnectionStatus nodeStatus : nodeStatusList) {
            if (!nodeIds.contains(nodeStatus.getNodeIdentifier())) {
                updatedStatuses.add(new NodeConnectionStatus(nodeStatus.getNodeIdentifier(), NodeConnectionState.REMOVED, null));
            }
        }

        logger.debug("\n\nCalculated diff between current cluster status and node cluster status as follows:\nNode: {}\nSelf: {}\nDifference: {}\n\n",
                nodeStatusList, currentStatuses, updatedStatuses);

        return updatedStatuses;
    }

    @Override
    public boolean canHandle(ProtocolMessage msg) {
        return msg.getType() == MessageType.HEARTBEAT || msg.getType() == MessageType.CLUSTER_WORKLOAD_REQUEST;
    }
}
