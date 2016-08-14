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

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.nifi.cluster.HeartbeatPayload;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.Heartbeat;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.ProtocolHandler;
import org.apache.nifi.cluster.protocol.ProtocolListener;
import org.apache.nifi.cluster.protocol.message.HeartbeatMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage.MessageType;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses Apache ZooKeeper to advertise the address to send heartbeats to, and then relies on the NiFi Cluster
 * Protocol to receive heartbeat messages from nodes in the cluster.
 */
public class ClusterProtocolHeartbeatMonitor extends AbstractHeartbeatMonitor implements HeartbeatMonitor, ProtocolHandler {
    protected static final Logger logger = LoggerFactory.getLogger(ClusterProtocolHeartbeatMonitor.class);

    private final String heartbeatAddress;
    private final ConcurrentMap<NodeIdentifier, NodeHeartbeat> heartbeatMessages = new ConcurrentHashMap<>();

    protected static final Unmarshaller nodeIdentifierUnmarshaller;

    static {
        try {
            final JAXBContext jaxbContext = JAXBContext.newInstance(NodeIdentifier.class);
            nodeIdentifierUnmarshaller = jaxbContext.createUnmarshaller();
        } catch (final Exception e) {
            throw new RuntimeException("Failed to create an Unmarshaller for unmarshalling Node Identifier", e);
        }
    }


    public ClusterProtocolHeartbeatMonitor(final ClusterCoordinator clusterCoordinator, final ProtocolListener protocolListener, final Properties properties) {
        super(clusterCoordinator, properties);

        protocolListener.addHandler(this);

        String hostname = properties.getProperty(NiFiProperties.CLUSTER_NODE_ADDRESS);
        if (hostname == null || hostname.trim().isEmpty()) {
            hostname = "localhost";
        }

        final String port = properties.getProperty(NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT);
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
    public ProtocolMessage handle(final ProtocolMessage msg) throws ProtocolException {
        if (msg.getType() != MessageType.HEARTBEAT) {
            throw new ProtocolException("Cannot handle message of type " + msg.getType());
        }

        final HeartbeatMessage heartbeatMsg = (HeartbeatMessage) msg;
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

        return null;
    }

    @Override
    public boolean canHandle(ProtocolMessage msg) {
        return msg.getType() == MessageType.HEARTBEAT;
    }
}
