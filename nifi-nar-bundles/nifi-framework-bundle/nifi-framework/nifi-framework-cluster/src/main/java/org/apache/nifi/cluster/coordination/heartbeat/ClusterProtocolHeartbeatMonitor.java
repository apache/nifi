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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.nifi.cluster.HeartbeatPayload;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.Heartbeat;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.ProtocolHandler;
import org.apache.nifi.cluster.protocol.message.HeartbeatMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage.MessageType;
import org.apache.nifi.controller.cluster.ZooKeeperClientConfig;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.util.NiFiProperties;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses Apache Curator to monitor heartbeats from nodes
 */
public class ClusterProtocolHeartbeatMonitor extends AbstractHeartbeatMonitor implements HeartbeatMonitor, ProtocolHandler {
    protected static final Logger logger = LoggerFactory.getLogger(ClusterProtocolHeartbeatMonitor.class);
    private static final String COORDINATOR_ZNODE_NAME = "coordinator";

    private final ZooKeeperClientConfig zkClientConfig;
    private final String clusterNodesPath;

    private volatile CuratorFramework curatorClient;
    private volatile Map<String, NodeIdentifier> clusterNodeIds = new HashMap<>();

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


    public ClusterProtocolHeartbeatMonitor(final ClusterCoordinator clusterCoordinator, final Properties properties) {
        super(clusterCoordinator, properties);

        this.zkClientConfig = ZooKeeperClientConfig.createConfig(properties);
        this.clusterNodesPath = zkClientConfig.resolvePath("cluster/nodes");

        String hostname = properties.getProperty(NiFiProperties.CLUSTER_MANAGER_ADDRESS);
        if (hostname == null) {
            try {
                hostname = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                throw new RuntimeException("Unable to determine local hostname and the '" + NiFiProperties.CLUSTER_MANAGER_ADDRESS + "' property is not set");
            }
        }

        final String port = properties.getProperty(NiFiProperties.CLUSTER_MANAGER_PROTOCOL_PORT);
        if (port == null) {
            throw new RuntimeException("Unable to determine which port Cluster Manager Protocol is listening on because the '"
                + NiFiProperties.CLUSTER_MANAGER_PROTOCOL_PORT + "' property is not set");
        }

        try {
            Integer.parseInt(port);
        } catch (final NumberFormatException nfe) {
            throw new RuntimeException("Unable to determine which port Cluster Manager Protocol is listening on because the '"
                + NiFiProperties.CLUSTER_MANAGER_PROTOCOL_PORT + "' property is set to '" + port + "', which is not a valid port number.");
        }

        heartbeatAddress = hostname + ":" + port;
    }

    @Override
    public void onStart() {
        final RetryPolicy retryPolicy = new RetryForever(5000);
        curatorClient = CuratorFrameworkFactory.newClient(zkClientConfig.getConnectString(),
            zkClientConfig.getSessionTimeoutMillis(), zkClientConfig.getConnectionTimeoutMillis(), retryPolicy);
        curatorClient.start();

        final Thread publishAddress = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!isStopped()) {
                    final String path = clusterNodesPath + "/" + COORDINATOR_ZNODE_NAME;
                    try {
                        try {
                            curatorClient.setData().forPath(path, heartbeatAddress.getBytes(StandardCharsets.UTF_8));
                            return;
                        } catch (final NoNodeException nne) {
                            // ensure that parents are created, using a wide-open ACL because the parents contain no data
                            // and the path is not in any way sensitive.
                            try {
                                curatorClient.create().creatingParentContainersIfNeeded().forPath(path);
                            } catch (final NodeExistsException nee) {
                                // This is okay. Node already exists.
                            }

                            curatorClient.create().withMode(CreateMode.EPHEMERAL).forPath(path, heartbeatAddress.getBytes(StandardCharsets.UTF_8));
                            logger.info("Successfully created node in ZooKeeper with path {}", path);

                            return;
                        }
                    } catch (Exception e) {
                        logger.warn("Failed to update ZooKeeper to notify nodes of the heartbeat address. Will continue to retry.");

                        try {
                            Thread.sleep(2000L);
                        } catch (final InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
            }
        });

        publishAddress.setName("Publish Heartbeat Address");
        publishAddress.setDaemon(true);
        publishAddress.start();
    }

    private CuratorFramework getClient() {
        return curatorClient;
    }

    @Override
    public void onStop() {
        final CuratorFramework client = getClient();
        if (client != null) {
            client.close();
        }
    }

    @Override
    protected Map<NodeIdentifier, NodeHeartbeat> getLatestHeartbeats() {
        return Collections.unmodifiableMap(heartbeatMessages);
    }

    @Override
    public synchronized void removeHeartbeat(final NodeIdentifier nodeId) {
        logger.debug("Deleting heartbeat for node {}", nodeId);
        final String nodeInfoPath = clusterNodesPath + "/" + nodeId.getId();

        heartbeatMessages.remove(nodeId);

        try {
            getClient().delete().forPath(nodeInfoPath);
            logger.info("Removed heartbeat from ZooKeeper for Node {}", nodeId);
        } catch (final NoNodeException e) {
            // node did not exist. Just return.
            logger.debug("Attempted to remove heartbeat for Node with ID {} but no ZNode existed at {}", nodeId, nodeInfoPath);
            return;
        } catch (final Exception e) {
            logger.warn("Failed to remove heartbeat from ZooKeeper for Node {} due to {}", nodeId, e);
            logger.warn("", e);

            clusterCoordinator.reportEvent(nodeId, Severity.WARNING, "Failed to remove node's heartbeat from ZooKeeper due to " + e);
        }
    }

    protected Set<NodeIdentifier> getClusterNodeIds() {
        return new HashSet<>(clusterNodeIds.values());
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
        final boolean primary = heartbeat.isPrimary();
        final byte[] payloadBytes = heartbeat.getPayload();
        final HeartbeatPayload payload = HeartbeatPayload.unmarshal(payloadBytes);
        final int activeThreadCount = payload.getActiveThreadCount();
        final int flowFileCount = (int) payload.getTotalFlowFileCount();
        final long flowFileBytes = payload.getTotalFlowFileBytes();
        final long systemStartTime = payload.getSystemStartTime();

        final NodeHeartbeat nodeHeartbeat = new StandardNodeHeartbeat(nodeId, System.currentTimeMillis(),
            connectionStatus, primary, flowFileCount, flowFileBytes, activeThreadCount, systemStartTime);
        heartbeatMessages.put(heartbeat.getNodeIdentifier(), nodeHeartbeat);
        logger.debug("Received new heartbeat from {}", nodeId);

        return null;
    }

    @Override
    public boolean canHandle(ProtocolMessage msg) {
        return msg.getType() == MessageType.HEARTBEAT;
    }
}
