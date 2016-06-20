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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.nifi.cluster.protocol.ConnectionRequest;
import org.apache.nifi.cluster.protocol.ConnectionResponse;
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
import org.apache.nifi.web.revision.RevisionManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestNodeClusterCoordinator {
    private NodeClusterCoordinator coordinator;
    private ClusterCoordinationProtocolSenderListener senderListener;
    private List<NodeStatusChangeMessage> nodeStatusChangeMessages;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() throws IOException {
        senderListener = Mockito.mock(ClusterCoordinationProtocolSenderListener.class);
        nodeStatusChangeMessages = Collections.synchronizedList(new ArrayList<>());

        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                final NodeStatusChangeMessage statusChangeMessage = invocation.getArgumentAt(1, NodeStatusChangeMessage.class);
                nodeStatusChangeMessages.add(statusChangeMessage);
                return null;
            }
        }).when(senderListener).notifyNodeStatusChange(Mockito.any(Set.class), Mockito.any(NodeStatusChangeMessage.class));

        final EventReporter eventReporter = Mockito.mock(EventReporter.class);
        final RevisionManager revisionManager = Mockito.mock(RevisionManager.class);
        Mockito.when(revisionManager.getAllRevisions()).thenReturn(Collections.emptyList());

        coordinator = new NodeClusterCoordinator(senderListener, eventReporter, null, revisionManager);

        final FlowService flowService = Mockito.mock(FlowService.class);
        final StandardDataFlow dataFlow = new StandardDataFlow(new byte[50], new byte[50], new byte[50]);
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

        final NodeClusterCoordinator coordinator = new NodeClusterCoordinator(senderListener, eventReporter, null, revisionManager);

        final NodeIdentifier requestedNodeId = createNodeId(6);
        final ConnectionRequest request = new ConnectionRequest(requestedNodeId);
        final ConnectionRequestMessage requestMsg = new ConnectionRequestMessage();
        requestMsg.setConnectionRequest(request);

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

        final NodeClusterCoordinator coordinator = new NodeClusterCoordinator(senderListener, eventReporter, null, revisionManager);

        final FlowService flowService = Mockito.mock(FlowService.class);
        final StandardDataFlow dataFlow = new StandardDataFlow(new byte[50], new byte[50], new byte[50]);
        Mockito.when(flowService.createDataFlow()).thenReturn(dataFlow);
        coordinator.setFlowService(flowService);

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

        while (nodeStatusChangeMessages.isEmpty()) {
            Thread.sleep(20L);
        }
        assertEquals(NodeConnectionState.CONNECTING, nodeStatusChangeMessages.get(0).getNodeConnectionStatus().getState());
        nodeStatusChangeMessages.clear();

        // Finish connecting. This should notify all that the status is now 'CONNECTED'
        coordinator.finishNodeConnection(nodeId);

        while (nodeStatusChangeMessages.isEmpty()) {
            Thread.sleep(20L);
        }
        assertEquals(NodeConnectionState.CONNECTED, nodeStatusChangeMessages.get(0).getNodeConnectionStatus().getState());
        assertEquals(NodeConnectionState.CONNECTED, coordinator.getConnectionStatus(nodeId).getState());
    }

    @Test(timeout = 5000)
    @SuppressWarnings("unchecked")
    public void testStatusChangesReplicated() throws InterruptedException, IOException {
        final ClusterCoordinationProtocolSenderListener senderListener = Mockito.mock(ClusterCoordinationProtocolSenderListener.class);
        final List<NodeStatusChangeMessage> msgs = Collections.synchronizedList(new ArrayList<>());

        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                final NodeStatusChangeMessage statusChangeMessage = invocation.getArgumentAt(1, NodeStatusChangeMessage.class);
                msgs.add(statusChangeMessage);
                return null;
            }
        }).when(senderListener).notifyNodeStatusChange(Mockito.any(Set.class), Mockito.any(NodeStatusChangeMessage.class));

        final EventReporter eventReporter = Mockito.mock(EventReporter.class);
        final RevisionManager revisionManager = Mockito.mock(RevisionManager.class);
        Mockito.when(revisionManager.getAllRevisions()).thenReturn(Collections.emptyList());
        final NodeClusterCoordinator coordinator = new NodeClusterCoordinator(senderListener, eventReporter, null, revisionManager);

        final FlowService flowService = Mockito.mock(FlowService.class);
        final StandardDataFlow dataFlow = new StandardDataFlow(new byte[50], new byte[50], new byte[50]);
        Mockito.when(flowService.createDataFlow()).thenReturn(dataFlow);
        coordinator.setFlowService(flowService);

        // Create a connection request message and send to the coordinator
        final NodeIdentifier requestedNodeId = createNodeId(1);
        requestConnection(requestedNodeId, coordinator);

        // The above connection request should trigger a 'CONNECTING' state transition to be replicated
        while (msgs.isEmpty()) {
            Thread.sleep(20L);
        }
        final NodeStatusChangeMessage connectingMsg = msgs.get(0);
        assertEquals(NodeConnectionState.CONNECTING, connectingMsg.getNodeConnectionStatus().getState());
        assertEquals(requestedNodeId, connectingMsg.getNodeId());

        // set node status to connected
        coordinator.finishNodeConnection(requestedNodeId);

        // the above method will result in the node identifier becoming 'CONNECTED'. Wait for this to happen and clear the map
        while (msgs.isEmpty()) {
            Thread.sleep(20L);
        }
        msgs.clear();

        coordinator.disconnectionRequestedByNode(requestedNodeId, DisconnectionCode.NODE_SHUTDOWN, "Unit Test");

        while (msgs.isEmpty()) {
            Thread.sleep(20L);
        }

        assertEquals(1, msgs.size());
        final NodeStatusChangeMessage statusChangeMsg = msgs.get(0);
        assertNotNull(statusChangeMsg);
        assertEquals(createNodeId(1), statusChangeMsg.getNodeId());
        assertEquals(DisconnectionCode.NODE_SHUTDOWN, statusChangeMsg.getNodeConnectionStatus().getDisconnectCode());
        assertEquals("Unit Test", statusChangeMsg.getNodeConnectionStatus().getDisconnectReason());
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
        final NodeIdentifier nodeId = createNodeId(1);
        coordinator.updateNodeStatus(new NodeConnectionStatus(nodeId, NodeConnectionState.CONNECTED));

        // wait for the status change message and clear it
        while (nodeStatusChangeMessages.isEmpty()) {
            Thread.sleep(10L);
        }
        nodeStatusChangeMessages.clear();

        coordinator.requestNodeDisconnect(nodeId, DisconnectionCode.USER_DISCONNECTED, "Unit Test");
        assertEquals(NodeConnectionState.DISCONNECTED, coordinator.getConnectionStatus(nodeId).getState());

        while (nodeStatusChangeMessages.isEmpty()) {
            Thread.sleep(10L);
        }
        final NodeStatusChangeMessage msg = nodeStatusChangeMessages.get(0);
        assertEquals(nodeId, msg.getNodeId());
        assertEquals(NodeConnectionState.DISCONNECTED, msg.getNodeConnectionStatus().getState());
    }


    @Test(timeout = 5000)
    public void testUpdateNodeStatusOutOfOrder() throws InterruptedException {
        // Add a connected node
        final NodeIdentifier nodeId1 = createNodeId(1);
        final NodeIdentifier nodeId2 = createNodeId(2);

        coordinator.updateNodeStatus(new NodeConnectionStatus(nodeId1, NodeConnectionState.CONNECTED));
        coordinator.updateNodeStatus(new NodeConnectionStatus(nodeId2, NodeConnectionState.CONNECTED));

        // wait for the status change message and clear it
        while (nodeStatusChangeMessages.size() < 2) {
            Thread.sleep(10L);
        }
        nodeStatusChangeMessages.clear();

        final NodeConnectionStatus oldStatus = new NodeConnectionStatus(-1L, nodeId1, NodeConnectionState.DISCONNECTED,
            DisconnectionCode.BLOCKED_BY_FIREWALL, null, 0L, null);
        final NodeStatusChangeMessage msg = new NodeStatusChangeMessage();
        msg.setNodeId(nodeId1);
        msg.setNodeConnectionStatus(oldStatus);
        coordinator.handle(msg);

        // Ensure that no status change message was send
        Thread.sleep(1000);
        assertTrue(nodeStatusChangeMessages.isEmpty());

        // Status should not have changed because our status id is too small.
        NodeConnectionStatus curStatus = coordinator.getConnectionStatus(nodeId1);
        assertEquals(NodeConnectionState.CONNECTED, curStatus.getState());

        // Verify that resetMap updates only the newer statuses
        final NodeConnectionStatus node2Disconnecting = new NodeConnectionStatus(nodeId2, NodeConnectionState.DISCONNECTING);
        final Map<NodeIdentifier, NodeConnectionStatus> resetMap = new HashMap<>();
        resetMap.put(nodeId1, oldStatus);
        resetMap.put(nodeId2, node2Disconnecting);
        coordinator.resetNodeStatuses(resetMap);

        curStatus = coordinator.getConnectionStatus(nodeId1);
        assertEquals(NodeConnectionState.CONNECTED, curStatus.getState());
        assertEquals(NodeConnectionState.DISCONNECTING, coordinator.getConnectionStatus(nodeId2).getState());
    }

    @Test(timeout = 5000)
    public void testUpdateNodeRoles() throws InterruptedException {
        // Add a connected node
        final NodeIdentifier nodeId1 = createNodeId(1);
        final NodeIdentifier nodeId2 = createNodeId(2);

        coordinator.updateNodeStatus(new NodeConnectionStatus(nodeId1, NodeConnectionState.CONNECTED));
        // wait for the status change message and clear it
        while (nodeStatusChangeMessages.isEmpty()) {
            Thread.sleep(10L);
        }
        nodeStatusChangeMessages.clear();

        coordinator.updateNodeStatus(new NodeConnectionStatus(nodeId2, NodeConnectionState.CONNECTED));
        // wait for the status change message and clear it
        while (nodeStatusChangeMessages.isEmpty()) {
            Thread.sleep(10L);
        }
        nodeStatusChangeMessages.clear();

        // Update role of node 1 to primary node
        coordinator.updateNodeRoles(nodeId1, Collections.singleton(ClusterRoles.PRIMARY_NODE));

        // wait for the status change message
        while (nodeStatusChangeMessages.isEmpty()) {
            Thread.sleep(10L);
        }
        // verify the message
        final NodeStatusChangeMessage msg = nodeStatusChangeMessages.get(0);
        assertNotNull(msg);
        assertEquals(nodeId1, msg.getNodeId());
        assertEquals(NodeConnectionState.CONNECTED, msg.getNodeConnectionStatus().getState());
        assertEquals(Collections.singleton(ClusterRoles.PRIMARY_NODE), msg.getNodeConnectionStatus().getRoles());
        nodeStatusChangeMessages.clear();

        // Update role of node 2 to primary node. This should trigger 2 status changes -
        // node 1 should lose primary role & node 2 should gain it
        coordinator.updateNodeRoles(nodeId2, Collections.singleton(ClusterRoles.PRIMARY_NODE));

        // wait for the status change message
        while (nodeStatusChangeMessages.size() < 2) {
            Thread.sleep(10L);
        }

        final NodeStatusChangeMessage msg1 = nodeStatusChangeMessages.get(0);
        final NodeStatusChangeMessage msg2 = nodeStatusChangeMessages.get(1);
        final NodeStatusChangeMessage id1Msg = (msg1.getNodeId().equals(nodeId1)) ? msg1 : msg2;
        final NodeStatusChangeMessage id2Msg = (msg1.getNodeId().equals(nodeId2)) ? msg1 : msg2;

        assertNotSame(id1Msg, id2Msg);

        assertTrue(id1Msg.getNodeConnectionStatus().getRoles().isEmpty());
        assertEquals(Collections.singleton(ClusterRoles.PRIMARY_NODE), id2Msg.getNodeConnectionStatus().getRoles());
    }


    @Test
    public void testProposedIdentifierResolvedIfConflict() {
        final NodeIdentifier id1 = new NodeIdentifier("1234", "localhost", 8000, "localhost", 9000, "localhost", 10000, 11000, false);
        final NodeIdentifier conflictingId = new NodeIdentifier("1234", "localhost", 8001, "localhost", 9000, "localhost", 10000, 11000, false);

        final ConnectionRequest connectionRequest = new ConnectionRequest(id1);
        final ConnectionRequestMessage crm = new ConnectionRequestMessage();
        crm.setConnectionRequest(connectionRequest);

        final ProtocolMessage response = coordinator.handle(crm);
        assertNotNull(response);
        assertTrue(response instanceof ConnectionResponseMessage);
        final ConnectionResponseMessage responseMessage = (ConnectionResponseMessage) response;
        final NodeIdentifier resolvedNodeId = responseMessage.getConnectionResponse().getNodeIdentifier();
        assertEquals(id1, resolvedNodeId);

        final ConnectionRequest conRequest2 = new ConnectionRequest(conflictingId);
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
        final ConnectionRequest request = new ConnectionRequest(requestedNodeId);
        final ConnectionRequestMessage requestMsg = new ConnectionRequestMessage();
        requestMsg.setConnectionRequest(request);
        return coordinator.handle(requestMsg);
    }

}
