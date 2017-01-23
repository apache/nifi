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
package org.apache.nifi.cluster.coordination.node;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.nifi.cluster.coordination.flow.FlowElection;
import org.apache.nifi.cluster.manager.exception.IllegalNodeDisconnectionException;
import org.apache.nifi.cluster.protocol.ConnectionRequest;
import org.apache.nifi.cluster.protocol.ConnectionResponse;
import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.StandardDataFlow;
import org.apache.nifi.cluster.protocol.impl.ClusterCoordinationProtocolSenderListener;
import org.apache.nifi.cluster.protocol.message.ConnectionRequestMessage;
import org.apache.nifi.cluster.protocol.message.ConnectionResponseMessage;
import org.apache.nifi.cluster.protocol.message.NodeStatusChangeMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.cluster.protocol.message.ReconnectionRequestMessage;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.revision.RevisionManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestNodeClusterCoordinator {

    private NodeClusterCoordinator coordinator;
    private ClusterCoordinationProtocolSenderListener senderListener;
    private List<NodeConnectionStatus> nodeStatuses;

    private NiFiProperties createProperties() {
        final Map<String,String> addProps = new HashMap<>();
        addProps.put("nifi.zookeeper.connect.string", "localhost:2181");
        return NiFiProperties.createBasicNiFiProperties(null, addProps);
    }

    @Before
    public void setup() throws IOException {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, "src/test/resources/conf/nifi.properties");

        senderListener = Mockito.mock(ClusterCoordinationProtocolSenderListener.class);
        nodeStatuses = Collections.synchronizedList(new ArrayList<>());

        final EventReporter eventReporter = Mockito.mock(EventReporter.class);
        final RevisionManager revisionManager = Mockito.mock(RevisionManager.class);
        Mockito.when(revisionManager.getAllRevisions()).thenReturn(Collections.emptyList());

        coordinator = new NodeClusterCoordinator(senderListener, eventReporter, null, new FirstVoteWinsFlowElection(), null, revisionManager, createProperties(), null) {
            @Override
            void notifyOthersOfNodeStatusChange(NodeConnectionStatus updatedStatus, boolean notifyAllNodes, boolean waitForCoordinator) {
                nodeStatuses.add(updatedStatus);
            }
        };

        final FlowService flowService = Mockito.mock(FlowService.class);
        final StandardDataFlow dataFlow = new StandardDataFlow(new byte[50], new byte[50], new byte[50], new HashSet<>());
        Mockito.when(flowService.createDataFlow()).thenReturn(dataFlow);
        coordinator.setFlowService(flowService);
    }

    @Test
    public void testConnectionResponseIndicatesAllNodes() throws IOException {
        // Add a disconnected node
        coordinator.updateNodeStatus(new NodeConnectionStatus(createNodeId(1), DisconnectionCode.LACK_OF_HEARTBEAT));
        coordinator.updateNodeStatus(new NodeConnectionStatus(createNodeId(2), NodeConnectionState.DISCONNECTING));
        coordinator.updateNodeStatus(new NodeConnectionStatus(createNodeId(3), NodeConnectionState.CONNECTING));
        coordinator.updateNodeStatus(new NodeConnectionStatus(createNodeId(4), NodeConnectionState.CONNECTED));
        coordinator.updateNodeStatus(new NodeConnectionStatus(createNodeId(5), NodeConnectionState.CONNECTED));

        // Create a connection request message and send to the coordinator
        final NodeIdentifier requestedNodeId = createNodeId(6);
        final ProtocolMessage protocolResponse = requestConnection(requestedNodeId, coordinator);
        assertNotNull(protocolResponse);
        assertTrue(protocolResponse instanceof ConnectionResponseMessage);

        final ConnectionResponse response = ((ConnectionResponseMessage) protocolResponse).getConnectionResponse();
        assertNotNull(response);

        assertEquals(requestedNodeId, response.getNodeIdentifier());
        assertNull(response.getRejectionReason());

        final List<NodeConnectionStatus> statuses = response.getNodeConnectionStatuses();
        assertNotNull(statuses);
        assertEquals(6, statuses.size());
        final Map<NodeIdentifier, NodeConnectionStatus> statusMap = statuses.stream().collect(
                Collectors.toMap(status -> status.getNodeIdentifier(), status -> status));

        assertEquals(DisconnectionCode.LACK_OF_HEARTBEAT, statusMap.get(createNodeId(1)).getDisconnectCode());
        assertEquals(NodeConnectionState.DISCONNECTING, statusMap.get(createNodeId(2)).getState());
        assertEquals(NodeConnectionState.CONNECTING, statusMap.get(createNodeId(3)).getState());
        assertEquals(NodeConnectionState.CONNECTED, statusMap.get(createNodeId(4)).getState());
        assertEquals(NodeConnectionState.CONNECTED, statusMap.get(createNodeId(5)).getState());
        assertEquals(NodeConnectionState.CONNECTING, statusMap.get(createNodeId(6)).getState());
    }

    @Test
    public void testTryAgainIfNoFlowServiceSet() {
        final ClusterCoordinationProtocolSenderListener senderListener = Mockito.mock(ClusterCoordinationProtocolSenderListener.class);
        final EventReporter eventReporter = Mockito.mock(EventReporter.class);
        final RevisionManager revisionManager = Mockito.mock(RevisionManager.class);
        Mockito.when(revisionManager.getAllRevisions()).thenReturn(Collections.emptyList());

        final NodeClusterCoordinator coordinator = new NodeClusterCoordinator(senderListener, eventReporter, null, new FirstVoteWinsFlowElection(),
                null, revisionManager, createProperties(), null) {
            @Override
            void notifyOthersOfNodeStatusChange(NodeConnectionStatus updatedStatus, boolean notifyAllNodes, boolean waitForCoordinator) {
            }
        };

        final NodeIdentifier requestedNodeId = createNodeId(6);
        final ConnectionRequest request = new ConnectionRequest(requestedNodeId, new StandardDataFlow(new byte[0], new byte[0], new byte[0], new HashSet<>()));
        final ConnectionRequestMessage requestMsg = new ConnectionRequestMessage();
        requestMsg.setConnectionRequest(request);

        coordinator.setConnected(true);

        final ProtocolMessage protocolResponse = coordinator.handle(requestMsg);
        assertNotNull(protocolResponse);
        assertTrue(protocolResponse instanceof ConnectionResponseMessage);

        final ConnectionResponse response = ((ConnectionResponseMessage) protocolResponse).getConnectionResponse();
        assertNotNull(response);
        assertEquals(5, response.getTryLaterSeconds());
    }

    @Test(timeout = 5000)
    public void testUnknownNodeAskedToConnectOnAttemptedConnectionComplete() throws IOException, InterruptedException {
        final ClusterCoordinationProtocolSenderListener senderListener = Mockito.mock(ClusterCoordinationProtocolSenderListener.class);
        final AtomicReference<ReconnectionRequestMessage> requestRef = new AtomicReference<>();

        Mockito.when(senderListener.requestReconnection(Mockito.any(ReconnectionRequestMessage.class))).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                final ReconnectionRequestMessage msg = invocation.getArgumentAt(0, ReconnectionRequestMessage.class);
                requestRef.set(msg);
                return null;
            }
        });

        final EventReporter eventReporter = Mockito.mock(EventReporter.class);
        final RevisionManager revisionManager = Mockito.mock(RevisionManager.class);
        Mockito.when(revisionManager.getAllRevisions()).thenReturn(Collections.emptyList());

        final NodeClusterCoordinator coordinator = new NodeClusterCoordinator(senderListener, eventReporter, null, new FirstVoteWinsFlowElection(),
                null, revisionManager, createProperties(), null) {
            @Override
            void notifyOthersOfNodeStatusChange(NodeConnectionStatus updatedStatus, boolean notifyAllNodes, boolean waitForCoordinator) {
            }
        };

        final FlowService flowService = Mockito.mock(FlowService.class);
        final StandardDataFlow dataFlow = new StandardDataFlow(new byte[50], new byte[50], new byte[50], new HashSet<>());
        Mockito.when(flowService.createDataFlowFromController()).thenReturn(dataFlow);
        coordinator.setFlowService(flowService);
        coordinator.setConnected(true);

        final NodeIdentifier nodeId = createNodeId(1);
        coordinator.finishNodeConnection(nodeId);

        while (requestRef.get() == null) {
            Thread.sleep(10L);
        }

        final ReconnectionRequestMessage msg = requestRef.get();
        assertEquals(nodeId, msg.getNodeId());

        final StandardDataFlow df = msg.getDataFlow();
        assertNotNull(df);
        assertTrue(Arrays.equals(dataFlow.getFlow(), df.getFlow()));
        assertTrue(Arrays.equals(dataFlow.getSnippets(), df.getSnippets()));
    }

    @Test(timeout = 5000)
    public void testFinishNodeConnectionResultsInConnectedState() throws IOException, InterruptedException {
        final NodeIdentifier nodeId = createNodeId(1);

        // Create a connection request message and send to the coordinator
        requestConnection(createNodeId(1), coordinator);

        while (nodeStatuses.isEmpty()) {
            Thread.sleep(20L);
        }
        assertEquals(NodeConnectionState.CONNECTING, nodeStatuses.get(0).getState());
        nodeStatuses.clear();

        // Finish connecting. This should notify all that the status is now 'CONNECTED'
        coordinator.finishNodeConnection(nodeId);

        while (nodeStatuses.isEmpty()) {
            Thread.sleep(20L);
        }
        assertEquals(NodeConnectionState.CONNECTED, nodeStatuses.get(0).getState());
        assertEquals(NodeConnectionState.CONNECTED, coordinator.getConnectionStatus(nodeId).getState());
    }

    @Test(timeout = 5000)
    public void testStatusChangesReplicated() throws InterruptedException, IOException {
        final RevisionManager revisionManager = Mockito.mock(RevisionManager.class);
        Mockito.when(revisionManager.getAllRevisions()).thenReturn(Collections.emptyList());

        // Create a connection request message and send to the coordinator
        final NodeIdentifier requestedNodeId = createNodeId(1);
        requestConnection(requestedNodeId, coordinator);

        // The above connection request should trigger a 'CONNECTING' state transition to be replicated
        while (nodeStatuses.isEmpty()) {
            Thread.sleep(20L);
        }
        final NodeConnectionStatus connectingStatus = nodeStatuses.get(0);
        assertEquals(NodeConnectionState.CONNECTING, connectingStatus.getState());
        assertEquals(requestedNodeId, connectingStatus.getNodeIdentifier());

        // set node status to connected
        coordinator.finishNodeConnection(requestedNodeId);

        // the above method will result in the node identifier becoming 'CONNECTED'. Wait for this to happen and clear the map
        while (nodeStatuses.isEmpty()) {
            Thread.sleep(20L);
        }
        nodeStatuses.clear();

        coordinator.disconnectionRequestedByNode(requestedNodeId, DisconnectionCode.NODE_SHUTDOWN, "Unit Test");

        while (nodeStatuses.isEmpty()) {
            Thread.sleep(20L);
        }

        assertEquals(1, nodeStatuses.size());
        final NodeConnectionStatus statusChange = nodeStatuses.get(0);
        assertNotNull(statusChange);
        assertEquals(createNodeId(1), statusChange.getNodeIdentifier());
        assertEquals(DisconnectionCode.NODE_SHUTDOWN, statusChange.getDisconnectCode());
        assertEquals("Unit Test", statusChange.getDisconnectReason());
    }

    @Test
    public void testGetConnectionStates() throws IOException {
        // Add a disconnected node
        coordinator.updateNodeStatus(new NodeConnectionStatus(createNodeId(1), DisconnectionCode.LACK_OF_HEARTBEAT));
        coordinator.updateNodeStatus(new NodeConnectionStatus(createNodeId(2), NodeConnectionState.DISCONNECTING));
        coordinator.updateNodeStatus(new NodeConnectionStatus(createNodeId(3), NodeConnectionState.CONNECTING));
        coordinator.updateNodeStatus(new NodeConnectionStatus(createNodeId(4), NodeConnectionState.CONNECTED));
        coordinator.updateNodeStatus(new NodeConnectionStatus(createNodeId(5), NodeConnectionState.CONNECTED));

        final Map<NodeConnectionState, List<NodeIdentifier>> stateMap = coordinator.getConnectionStates();
        assertEquals(4, stateMap.size());

        final List<NodeIdentifier> connectedIds = stateMap.get(NodeConnectionState.CONNECTED);
        assertEquals(2, connectedIds.size());
        assertTrue(connectedIds.contains(createNodeId(4)));
        assertTrue(connectedIds.contains(createNodeId(5)));

        final List<NodeIdentifier> connectingIds = stateMap.get(NodeConnectionState.CONNECTING);
        assertEquals(1, connectingIds.size());
        assertTrue(connectingIds.contains(createNodeId(3)));

        final List<NodeIdentifier> disconnectingIds = stateMap.get(NodeConnectionState.DISCONNECTING);
        assertEquals(1, disconnectingIds.size());
        assertTrue(disconnectingIds.contains(createNodeId(2)));

        final List<NodeIdentifier> disconnectedIds = stateMap.get(NodeConnectionState.DISCONNECTED);
        assertEquals(1, disconnectedIds.size());
        assertTrue(disconnectedIds.contains(createNodeId(1)));
    }

    @Test
    public void testGetNodeIdentifiers() throws IOException {
        // Add a disconnected node
        coordinator.updateNodeStatus(new NodeConnectionStatus(createNodeId(1), DisconnectionCode.LACK_OF_HEARTBEAT));
        coordinator.updateNodeStatus(new NodeConnectionStatus(createNodeId(2), NodeConnectionState.DISCONNECTING));
        coordinator.updateNodeStatus(new NodeConnectionStatus(createNodeId(3), NodeConnectionState.CONNECTING));
        coordinator.updateNodeStatus(new NodeConnectionStatus(createNodeId(4), NodeConnectionState.CONNECTED));
        coordinator.updateNodeStatus(new NodeConnectionStatus(createNodeId(5), NodeConnectionState.CONNECTED));

        final Set<NodeIdentifier> connectedIds = coordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED);
        assertEquals(2, connectedIds.size());
        assertTrue(connectedIds.contains(createNodeId(4)));
        assertTrue(connectedIds.contains(createNodeId(5)));

        final Set<NodeIdentifier> connectingIds = coordinator.getNodeIdentifiers(NodeConnectionState.CONNECTING);
        assertEquals(1, connectingIds.size());
        assertTrue(connectingIds.contains(createNodeId(3)));

        final Set<NodeIdentifier> disconnectingIds = coordinator.getNodeIdentifiers(NodeConnectionState.DISCONNECTING);
        assertEquals(1, disconnectingIds.size());
        assertTrue(disconnectingIds.contains(createNodeId(2)));

        final Set<NodeIdentifier> disconnectedIds = coordinator.getNodeIdentifiers(NodeConnectionState.DISCONNECTED);
        assertEquals(1, disconnectedIds.size());
        assertTrue(disconnectedIds.contains(createNodeId(1)));
    }

    @Test(timeout = 5000)
    public void testRequestNodeDisconnect() throws InterruptedException {
        // Add a connected node
        final NodeIdentifier nodeId1 = createNodeId(1);
        coordinator.updateNodeStatus(new NodeConnectionStatus(nodeId1, NodeConnectionState.CONNECTED));
        coordinator.updateNodeStatus(new NodeConnectionStatus(createNodeId(2), NodeConnectionState.CONNECTED));

        // wait for the status change message and clear it
        while (nodeStatuses.isEmpty()) {
            Thread.sleep(10L);
        }
        nodeStatuses.clear();

        coordinator.requestNodeDisconnect(nodeId1, DisconnectionCode.USER_DISCONNECTED, "Unit Test");
        assertEquals(NodeConnectionState.DISCONNECTED, coordinator.getConnectionStatus(nodeId1).getState());

        while (nodeStatuses.isEmpty()) {
            Thread.sleep(10L);
        }
        final NodeConnectionStatus status = nodeStatuses.get(0);
        assertEquals(nodeId1, status.getNodeIdentifier());
        assertEquals(NodeConnectionState.DISCONNECTED, status.getState());
    }

    @Test(timeout = 5000)
    public void testCannotDisconnectLastNode() throws InterruptedException {
        // Add a connected node
        final NodeIdentifier nodeId1 = createNodeId(1);
        final NodeIdentifier nodeId2 = createNodeId(2);
        coordinator.updateNodeStatus(new NodeConnectionStatus(nodeId1, NodeConnectionState.CONNECTED));
        coordinator.updateNodeStatus(new NodeConnectionStatus(nodeId2, NodeConnectionState.CONNECTED));

        // wait for the status change message and clear it
        while (nodeStatuses.isEmpty()) {
            Thread.sleep(10L);
        }
        nodeStatuses.clear();

        coordinator.requestNodeDisconnect(nodeId2, DisconnectionCode.USER_DISCONNECTED, "Unit Test");

        try {
            coordinator.requestNodeDisconnect(nodeId1, DisconnectionCode.USER_DISCONNECTED, "Unit Test");
            Assert.fail("Expected an IllegalNodeDisconnectionException when trying to disconnect last node but it wasn't thrown");
        } catch (final IllegalNodeDisconnectionException inde) {
            // expected
        }

        // Should still be able to request that node 2 disconnect, since it's not the node that is connected
        coordinator.requestNodeDisconnect(nodeId2, DisconnectionCode.USER_DISCONNECTED, "Unit Test");
    }

    @Test(timeout = 5000)
    public void testUpdateNodeStatusOutOfOrder() throws InterruptedException {
        // Add a connected node
        final NodeIdentifier nodeId1 = createNodeId(1);
        final NodeIdentifier nodeId2 = createNodeId(2);

        coordinator.updateNodeStatus(new NodeConnectionStatus(nodeId1, NodeConnectionState.CONNECTED));
        coordinator.updateNodeStatus(new NodeConnectionStatus(nodeId2, NodeConnectionState.CONNECTED));

        // wait for the status change message and clear it
        while (nodeStatuses.size() < 2) {
            Thread.sleep(10L);
        }
        nodeStatuses.clear();

        final NodeConnectionStatus oldStatus = new NodeConnectionStatus(-1L, nodeId1, NodeConnectionState.DISCONNECTED,
            DisconnectionCode.BLOCKED_BY_FIREWALL, null, 0L);
        final NodeStatusChangeMessage msg = new NodeStatusChangeMessage();
        msg.setNodeId(nodeId1);
        msg.setNodeConnectionStatus(oldStatus);
        coordinator.handle(msg);

        // Ensure that no status change message was send
        Thread.sleep(1000);
        assertTrue(nodeStatuses.isEmpty());
    }

    @Test
    public void testProposedIdentifierResolvedIfConflict() {
        final NodeIdentifier id1 = new NodeIdentifier("1234", "localhost", 8000, "localhost", 9000, "localhost", 10000, 11000, false);
        final NodeIdentifier conflictingId = new NodeIdentifier("1234", "localhost", 8001, "localhost", 9000, "localhost", 10000, 11000, false);

        final ConnectionRequest connectionRequest = new ConnectionRequest(id1, new StandardDataFlow(new byte[0], new byte[0], new byte[0], new HashSet<>()));
        final ConnectionRequestMessage crm = new ConnectionRequestMessage();
        crm.setConnectionRequest(connectionRequest);

        final ProtocolMessage response = coordinator.handle(crm);
        assertNotNull(response);
        assertTrue(response instanceof ConnectionResponseMessage);
        final ConnectionResponseMessage responseMessage = (ConnectionResponseMessage) response;
        final NodeIdentifier resolvedNodeId = responseMessage.getConnectionResponse().getNodeIdentifier();
        assertEquals(id1, resolvedNodeId);

        final ConnectionRequest conRequest2 = new ConnectionRequest(conflictingId, new StandardDataFlow(new byte[0], new byte[0], new byte[0], new HashSet<>()));
        final ConnectionRequestMessage crm2 = new ConnectionRequestMessage();
        crm2.setConnectionRequest(conRequest2);

        final ProtocolMessage conflictingResponse = coordinator.handle(crm2);
        assertNotNull(conflictingResponse);
        assertTrue(conflictingResponse instanceof ConnectionResponseMessage);
        final ConnectionResponseMessage conflictingResponseMessage = (ConnectionResponseMessage) conflictingResponse;
        final NodeIdentifier conflictingNodeId = conflictingResponseMessage.getConnectionResponse().getNodeIdentifier();
        assertNotSame(id1.getId(), conflictingNodeId.getId());
        assertEquals(conflictingId.getApiAddress(), conflictingNodeId.getApiAddress());
        assertEquals(conflictingId.getApiPort(), conflictingNodeId.getApiPort());
        assertEquals(conflictingId.getSiteToSiteAddress(), conflictingNodeId.getSiteToSiteAddress());
        assertEquals(conflictingId.getSiteToSitePort(), conflictingNodeId.getSiteToSitePort());
        assertEquals(conflictingId.getSocketAddress(), conflictingNodeId.getSocketAddress());
        assertEquals(conflictingId.getSocketPort(), conflictingNodeId.getSocketPort());
    }

    private NodeIdentifier createNodeId(final int index) {
        return new NodeIdentifier(String.valueOf(index), "localhost", 8000 + index, "localhost", 9000 + index, "localhost", 10000 + index, 11000 + index, false);
    }

    private ProtocolMessage requestConnection(final NodeIdentifier requestedNodeId, final NodeClusterCoordinator coordinator) {
        final ConnectionRequest request = new ConnectionRequest(requestedNodeId, new StandardDataFlow(new byte[0], new byte[0], new byte[0], new HashSet<>()));
        final ConnectionRequestMessage requestMsg = new ConnectionRequestMessage();
        requestMsg.setConnectionRequest(request);
        return coordinator.handle(requestMsg);
    }


    private static class FirstVoteWinsFlowElection implements FlowElection {
        private DataFlow dataFlow;
        private NodeIdentifier voter;

        @Override
        public boolean isElectionComplete() {
            return dataFlow != null;
        }

        @Override
        public synchronized DataFlow castVote(DataFlow candidate, NodeIdentifier nodeIdentifier) {
            if (dataFlow == null) {
                dataFlow = candidate;
                voter = nodeIdentifier;
            }

            return dataFlow;
        }

        @Override
        public DataFlow getElectedDataFlow() {
            return dataFlow;
        }

        @Override
        public String getStatusDescription() {
            return "First Vote Wins";
        }

        @Override
        public boolean isVoteCounted(NodeIdentifier nodeIdentifier) {
            return voter != null && voter.equals(nodeIdentifier);
        }
    }
}
