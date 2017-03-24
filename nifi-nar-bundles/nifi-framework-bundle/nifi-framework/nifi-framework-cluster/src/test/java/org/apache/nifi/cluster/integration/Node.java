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

package org.apache.nifi.cluster.integration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.cluster.ReportedEvent;
import org.apache.nifi.cluster.coordination.flow.FlowElection;
import org.apache.nifi.cluster.coordination.heartbeat.ClusterProtocolHeartbeatMonitor;
import org.apache.nifi.cluster.coordination.heartbeat.HeartbeatMonitor;
import org.apache.nifi.cluster.coordination.node.LeaderElectionNodeProtocolSender;
import org.apache.nifi.cluster.coordination.node.NodeClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.ClusterCoordinationProtocolSender;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.NodeProtocolSender;
import org.apache.nifi.cluster.protocol.ProtocolContext;
import org.apache.nifi.cluster.protocol.ProtocolListener;
import org.apache.nifi.cluster.protocol.impl.ClusterCoordinationProtocolSenderListener;
import org.apache.nifi.cluster.protocol.impl.NodeProtocolSenderListener;
import org.apache.nifi.cluster.protocol.impl.SocketProtocolListener;
import org.apache.nifi.cluster.protocol.impl.StandardClusterCoordinationProtocolSender;
import org.apache.nifi.cluster.protocol.jaxb.JaxbProtocolContext;
import org.apache.nifi.cluster.protocol.jaxb.message.JaxbProtocolUtils;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.StandardFlowService;
import org.apache.nifi.controller.leader.election.CuratorLeaderElectionManager;
import org.apache.nifi.controller.leader.election.LeaderElectionManager;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.io.socket.ServerSocketConfiguration;
import org.apache.nifi.io.socket.SocketConfiguration;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.revision.RevisionManager;
import org.junit.Assert;
import org.mockito.Mockito;

public class Node {
    private final NodeIdentifier nodeId;
    private final NiFiProperties nodeProperties;

    private final List<ReportedEvent> reportedEvents = Collections.synchronizedList(new ArrayList<ReportedEvent>());
    private final RevisionManager revisionManager;
    private final FlowElection flowElection;

    private NodeClusterCoordinator clusterCoordinator;
    private NodeProtocolSender protocolSender;
    private FlowController flowController;
    private StandardFlowService flowService;
    private LeaderElectionManager electionManager;

    private ProtocolListener protocolListener;

    private volatile boolean running = false;

    private ScheduledExecutorService executor = new FlowEngine(8, "Node tasks", true);


    public Node(final NiFiProperties properties, final FlowElection flowElection) {
        this(createNodeId(), properties, flowElection);
    }

    public Node(final NodeIdentifier nodeId, final NiFiProperties properties, final FlowElection flowElection) {
        this.nodeId = nodeId;
        this.nodeProperties = new NiFiProperties() {
            @Override
            public String getProperty(String key) {
                if(key.equals(NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT)){
                    return String.valueOf(nodeId.getSocketPort());
                }else if(key.equals(NiFiProperties.WEB_HTTP_PORT)){
                    return String.valueOf(nodeId.getApiPort());
                }else {
                    return properties.getProperty(key);
                }
            }

            @Override
            public Set<String> getPropertyKeys() {
                final Set<String> keys = new HashSet<>(properties.getPropertyKeys());
                keys.add(NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT);
                keys.add(NiFiProperties.WEB_HTTP_PORT);
                return keys;
            }
        };

        final Bundle systemBundle = SystemBundle.create(properties);
        ExtensionManager.discoverExtensions(systemBundle, Collections.emptySet());

        revisionManager = Mockito.mock(RevisionManager.class);
        Mockito.when(revisionManager.getAllRevisions()).thenReturn(Collections.emptyList());

        electionManager = new CuratorLeaderElectionManager(4, nodeProperties);
        this.flowElection = flowElection;
    }


    private static NodeIdentifier createNodeId() {
        return new NodeIdentifier(UUID.randomUUID().toString(), "localhost", createPort(), "localhost", createPort(), "localhost", null, null, false, null);
    }

    public synchronized void start() {
        running = true;

        protocolSender = createNodeProtocolSender();
        clusterCoordinator = createClusterCoordinator();
        clusterCoordinator.setLocalNodeIdentifier(nodeId);
        //        clusterCoordinator.setConnected(true);

        final HeartbeatMonitor heartbeatMonitor = createHeartbeatMonitor();
        flowController = FlowController.createClusteredInstance(Mockito.mock(FlowFileEventRepository.class), nodeProperties,
            null, null, StringEncryptor.createEncryptor(nodeProperties), protocolSender, Mockito.mock(BulletinRepository.class), clusterCoordinator,
            heartbeatMonitor, electionManager, VariableRegistry.EMPTY_REGISTRY);

        try {
            flowController.initializeFlow();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final NodeProtocolSenderListener senderListener = new NodeProtocolSenderListener(protocolSender, protocolListener);
        try {
            flowController.getStateManagerProvider().getStateManager("Cluster Node Configuration").setState(Collections.singletonMap("Node UUID", nodeId.getId()), Scope.LOCAL);

            flowService = StandardFlowService.createClusteredInstance(flowController, nodeProperties, senderListener, clusterCoordinator,
                StringEncryptor.createEncryptor(nodeProperties), revisionManager, Mockito.mock(Authorizer.class));

            flowService.start();

            flowService.load(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() throws IOException {
        running = false;

        flowController.shutdown(true);
        flowService.stop(true);

        clusterCoordinator.shutdown();
        executor.shutdownNow();

        // protocol listener is closed by flow controller
    }

    public void suspendHeartbeating() {
        flowController.suspendHeartbeats();
    }

    public void resumeHeartbeating() {
        flowController.resumeHeartbeats();
    }

    public NodeIdentifier getIdentifier() {
        return nodeId;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(nodeId).build();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof Node)) {
            return false;
        }

        return getIdentifier().equals(((Node) obj).getIdentifier());
    }

    @Override
    public String toString() {
        return "Node[id=" + getIdentifier() + ", started=" + isRunning() + "]";
    }

    public boolean isRunning() {
        return running;
    }

    private static int createPort() {
        // get an unused port
        while (true) {
            try (ServerSocket ss = new ServerSocket(0)) {
                return ss.getLocalPort();
            } catch (final IOException ioe) {
            }
        }
    }

    public NodeConnectionStatus getConnectionStatus() {
        return clusterCoordinator.getConnectionStatus(nodeId);
    }

    @SuppressWarnings("unchecked")
    private NodeProtocolSender createNodeProtocolSender() {
        final SocketConfiguration socketConfig = new SocketConfiguration();
        socketConfig.setSocketTimeout(3000);
        socketConfig.setReuseAddress(true);

        final ProtocolContext<ProtocolMessage> protocolContext = new JaxbProtocolContext<>(JaxbProtocolUtils.JAXB_CONTEXT);
        final NodeProtocolSender protocolSender = new LeaderElectionNodeProtocolSender(socketConfig, protocolContext, electionManager);
        return protocolSender;
    }

    @SuppressWarnings("unchecked")
    private ClusterCoordinationProtocolSender createCoordinatorProtocolSender() {
        final SocketConfiguration socketConfig = new SocketConfiguration();
        socketConfig.setSocketTimeout(3000);
        socketConfig.setReuseAddress(true);

        final ProtocolContext<ProtocolMessage> protocolContext = new JaxbProtocolContext<>(JaxbProtocolUtils.JAXB_CONTEXT);
        return new StandardClusterCoordinationProtocolSender(socketConfig, protocolContext, 1);
    }

    private HeartbeatMonitor createHeartbeatMonitor() {
        return new ClusterProtocolHeartbeatMonitor(clusterCoordinator, protocolListener, nodeProperties);
    }

    @SuppressWarnings("unchecked")
    private NodeClusterCoordinator createClusterCoordinator() {
        final EventReporter eventReporter = new EventReporter() {
            @Override
            public void reportEvent(Severity severity, String category, String message) {
                reportedEvents.add(new ReportedEvent(nodeId, severity, message));
            }
        };

        final ServerSocketConfiguration serverSocketConfiguration = new ServerSocketConfiguration();
        serverSocketConfiguration.setSocketTimeout(5000);
        final ProtocolContext<ProtocolMessage> protocolContext = new JaxbProtocolContext<>(JaxbProtocolUtils.JAXB_CONTEXT);

        protocolListener = new SocketProtocolListener(3, Integer.parseInt(nodeProperties.getProperty(NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT)), serverSocketConfiguration, protocolContext);
        try {
            protocolListener.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final ClusterCoordinationProtocolSenderListener protocolSenderListener = new ClusterCoordinationProtocolSenderListener(createCoordinatorProtocolSender(), protocolListener);
        return new NodeClusterCoordinator(protocolSenderListener, eventReporter, electionManager, flowElection, null,
                revisionManager, nodeProperties, protocolSender);
    }


    public NodeClusterCoordinator getClusterCoordinator() {
        return clusterCoordinator;
    }


    //
    // Methods for checking conditions
    //
    public boolean isConnected() {
        final NodeConnectionStatus status = getConnectionStatus();
        if (status == null) {
            return false;
        }

        return status.getState() == NodeConnectionState.CONNECTED;
    }

    //
    // Methods to wait for conditions
    //
    public void waitUntilConnected(final long time, final TimeUnit timeUnit) {
        ClusterUtils.waitUntilConditionMet(time, timeUnit, () -> isConnected());
    }

    private String getClusterAddress() {
        final InetSocketAddress address = nodeProperties.getClusterNodeProtocolAddress();
        return address.getHostName() + ":" + address.getPort();
    }

    public boolean hasRole(final String roleName) {
        final String leaderAddress = electionManager.getLeader(roleName);
        if (leaderAddress == null) {
            return false;
        }

        return leaderAddress.equals(getClusterAddress());
    }

    public void waitUntilElectedForRole(final String roleName, final long time, final TimeUnit timeUnit) {
        ClusterUtils.waitUntilConditionMet(time, timeUnit, () -> hasRole(roleName));
    }

    // Assertions
    /**
     * Assert that the node with the given ID connects (According to this node!) within the given amount of time
     *
     * @param nodeId id of the node
     * @param time how long to wait
     * @param timeUnit unit of time provided by the 'time' argument
     */
    public void assertNodeConnects(final NodeIdentifier nodeId, final long time, final TimeUnit timeUnit) {
        ClusterUtils.waitUntilConditionMet(time, timeUnit,
            () -> getClusterCoordinator().getConnectionStatus(nodeId).getState() == NodeConnectionState.CONNECTED,
            () -> "Connection Status is " + getClusterCoordinator().getConnectionStatus(nodeId).toString());
    }


    /**
     * Assert that the node with the given ID disconnects (According to this node!) within the given amount of time
     *
     * @param nodeId id of the node
     * @param time how long to wait
     * @param timeUnit unit of time provided by the 'time' argument
     */
    public void assertNodeDisconnects(final NodeIdentifier nodeId, final long time, final TimeUnit timeUnit) {
        ClusterUtils.waitUntilConditionMet(time, timeUnit,
            () -> getClusterCoordinator().getConnectionStatus(nodeId).getState() == NodeConnectionState.DISCONNECTED,
            () -> "Connection Status is " + getClusterCoordinator().getConnectionStatus(nodeId).toString());
    }


    /**
     * Asserts that the node with the given ID is currently connected (According to this node!)
     *
     * @param nodeId id of the node
     */
    public void assertNodeIsConnected(final NodeIdentifier nodeId) {
        Assert.assertEquals(NodeConnectionState.CONNECTED, getClusterCoordinator().getConnectionStatus(nodeId).getState());
    }
}
