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
package org.apache.nifi.remote.protocol.socket;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.PortAuthorizationResult;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.remote.cluster.ClusterNodeInformation;
import org.apache.nifi.remote.cluster.NodeInformation;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.io.socket.SocketChannelCommunicationsSession;
import org.apache.nifi.remote.io.socket.SocketChannelInput;
import org.apache.nifi.remote.io.socket.SocketChannelOutput;
import org.apache.nifi.remote.protocol.HandshakeProperties;
import org.apache.nifi.remote.protocol.HandshakeProperty;
import org.apache.nifi.remote.protocol.Response;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.apache.nifi.util.NiFiProperties;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestSocketFlowFileServerProtocol {

    @BeforeClass
    public static void setup() throws Exception {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, "src/test/resources/nifi.properties");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.remote", "DEBUG");
    }

    private Peer getDefaultPeer(final HandshakeProperties handshakeProperties, final OutputStream outputStream) throws IOException {
        final Map<HandshakeProperty, String> defaultProps = new HashMap<>();
        defaultProps.put(HandshakeProperty.GZIP, String.valueOf(handshakeProperties.isUseGzip()));
        return getCustomPeer(handshakeProperties, outputStream, defaultProps);
    }

    private Peer getCustomPeer(final HandshakeProperties handshakeProperties, final OutputStream outputStream, final Map<HandshakeProperty, String> props) throws IOException {
        final PeerDescription description = new PeerDescription("peer-host", 8080, false);

        final byte[] inputBytes;
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
             final DataOutputStream dos = new DataOutputStream(bos)) {

            dos.writeUTF(handshakeProperties.getCommsIdentifier());
            dos.writeUTF(handshakeProperties.getTransitUriPrefix());
            dos.writeInt(props.size()); // num of properties
            for (Map.Entry<HandshakeProperty, String> prop : props.entrySet()) {
                dos.writeUTF(prop.getKey().name());
                dos.writeUTF(prop.getValue());
            }
            dos.flush();

            inputBytes = bos.toByteArray();
        }

        final InputStream inputStream = new ByteArrayInputStream(inputBytes);

        final SocketChannelCommunicationsSession commsSession = mock(SocketChannelCommunicationsSession.class);
        final SocketChannelInput channelInput = mock(SocketChannelInput.class);
        final SocketChannelOutput channelOutput = mock(SocketChannelOutput.class);
        when(commsSession.getInput()).thenReturn(channelInput);
        when(commsSession.getOutput()).thenReturn(channelOutput);

        when(channelInput.getInputStream()).thenReturn(inputStream);
        when(channelOutput.getOutputStream()).thenReturn(outputStream);

        final String peerUrl = "http://peer-host:8080/";
        final String clusterUrl = "cluster-url";
        return new Peer(description, commsSession, peerUrl, clusterUrl);
    }

    private SocketFlowFileServerProtocol getDefaultSocketFlowFileServerProtocol() {
        return spy(new SocketFlowFileServerProtocol());
    }

    @Test
    public void testSendPeerListStandalone() throws Exception {
        final SocketFlowFileServerProtocol protocol = getDefaultSocketFlowFileServerProtocol();
        final Optional<ClusterNodeInformation> clusterNodeInfo = Optional.empty();
        final String siteToSiteHostname = "node1.example.com";
        final Integer siteToSitePort = 8081;
        final Integer siteToSiteHttpPort = null;
        final int apiPort = 8080;
        final boolean isSiteToSiteSecure = true;
        final int numOfQueuedFlowFiles = 100;
        final NodeInformation self = new NodeInformation(siteToSiteHostname, siteToSitePort, siteToSiteHttpPort,
                apiPort, isSiteToSiteSecure, numOfQueuedFlowFiles);

        final HandshakeProperties handshakeProperties = new HandshakeProperties();
        handshakeProperties.setCommsIdentifier("communication-identifier");
        handshakeProperties.setTransitUriPrefix("uri-prefix");

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final Peer peer = getDefaultPeer(handshakeProperties, outputStream);

        protocol.handshake(peer);
        protocol.sendPeerList(peer, clusterNodeInfo, self);

        try (final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()))) {
            final Response handshakeResponse = Response.read(dis);
            assertEquals(ResponseCode.PROPERTIES_OK, handshakeResponse.getCode());

            final int numPeers = dis.readInt();
            assertEquals(1, numPeers);

            assertEquals(siteToSiteHostname, dis.readUTF());
            assertEquals(siteToSitePort.intValue(), dis.readInt());
            assertEquals(isSiteToSiteSecure, dis.readBoolean());
            assertEquals(numOfQueuedFlowFiles, dis.readInt());

        }
    }

    @Test
    public void testSendPeerListCluster() throws Exception {
        final SocketFlowFileServerProtocol protocol = getDefaultSocketFlowFileServerProtocol();
        final List<NodeInformation> nodeInfoList = new ArrayList<>();
        final ClusterNodeInformation clusterNodeInformation = new ClusterNodeInformation();
        clusterNodeInformation.setNodeInformation(nodeInfoList);
        final Optional<ClusterNodeInformation> clusterNodeInfo = Optional.of(clusterNodeInformation);

        for (int i = 0; i < 3; i++) {
            final String siteToSiteHostname = String.format("node%d.example.com", i);
            final Integer siteToSitePort = 8081;
            final Integer siteToSiteHttpPort = null;
            final int apiPort = 8080;
            final boolean isSiteToSiteSecure = true;
            final int numOfQueuedFlowFiles = 100 + i;
            final NodeInformation nodeInformation = new NodeInformation(siteToSiteHostname, siteToSitePort, siteToSiteHttpPort,
                    apiPort, isSiteToSiteSecure, numOfQueuedFlowFiles);
            nodeInfoList.add(nodeInformation);
        }

        final NodeInformation self = nodeInfoList.get(0);

        final HandshakeProperties handshakeProperties = new HandshakeProperties();
        handshakeProperties.setCommsIdentifier("communication-identifier");
        handshakeProperties.setTransitUriPrefix("uri-prefix");

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final Peer peer = getDefaultPeer(handshakeProperties, outputStream);

        protocol.handshake(peer);
        protocol.sendPeerList(peer, clusterNodeInfo, self);

        try (final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()))) {
            final Response handshakeResponse = Response.read(dis);
            assertEquals(ResponseCode.PROPERTIES_OK, handshakeResponse.getCode());

            final int numPeers = dis.readInt();
            assertEquals(nodeInfoList.size(), numPeers);

            for (final NodeInformation node : nodeInfoList) {
                assertEquals(node.getSiteToSiteHostname(), dis.readUTF());
                assertEquals(node.getSiteToSitePort().intValue(), dis.readInt());
                assertEquals(node.isSiteToSiteSecure(), dis.readBoolean());
                assertEquals(node.getTotalFlowFiles(), dis.readInt());
            }
        }
    }
    @Test
    public void testPortDestinationFull() throws Exception {
        final SocketFlowFileServerProtocol serverProtocol = getDefaultSocketFlowFileServerProtocol();
        final HandshakeProperties handshakeProperties = new HandshakeProperties();
        handshakeProperties.setCommsIdentifier("communication-identifier");
        handshakeProperties.setTransitUriPrefix("uri-prefix");
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final Map<HandshakeProperty, String> customProps = new HashMap<>();
        customProps.put(HandshakeProperty.GZIP, "false");
        customProps.put(HandshakeProperty.PORT_IDENTIFIER, handshakeProperties.getCommsIdentifier());
        final Peer peer = getCustomPeer(handshakeProperties, outputStream, customProps);

        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final RootGroupPort port = mock(RootGroupPort.class);
        final PortAuthorizationResult authResult = mock(PortAuthorizationResult.class);
        doReturn(true).when(processGroup).isRootGroup();
        doReturn(port).when(processGroup).getOutputPort(handshakeProperties.getCommsIdentifier());
        doReturn(true).when(port).isValid();
        doReturn(true).when(port).isRunning();
        doReturn(authResult).when(port).checkUserAuthorization(any(String.class));
        doReturn(true).when(authResult).isAuthorized();
        final Set<Connection> connections = new HashSet<>();
        final Connection connection = mock(Connection.class);
        connections.add(connection);
        doReturn(connections).when(port).getConnections();
        final FlowFileQueue flowFileQueue = mock(FlowFileQueue.class);
        doReturn(flowFileQueue).when(connection).getFlowFileQueue();
        doReturn(true).when(flowFileQueue).isFull();

        serverProtocol.setRootProcessGroup(processGroup);
        try {
            serverProtocol.handshake(peer);
            fail();
        } catch (final HandshakeException e) {
            assertEquals(ResponseCode.PORTS_DESTINATION_FULL, e.getResponseCode());
        }

        assertFalse(serverProtocol.isHandshakeSuccessful());
    }

    @Test
    public void testPortNotInValidState() throws Exception {
        final SocketFlowFileServerProtocol serverProtocol = getDefaultSocketFlowFileServerProtocol();
        final HandshakeProperties handshakeProperties = new HandshakeProperties();
        handshakeProperties.setCommsIdentifier("communication-identifier");
        handshakeProperties.setTransitUriPrefix("uri-prefix");
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final Map<HandshakeProperty, String> customProps = new HashMap<>();
        customProps.put(HandshakeProperty.GZIP, "false");
        customProps.put(HandshakeProperty.PORT_IDENTIFIER, handshakeProperties.getCommsIdentifier());
        final Peer peer = getCustomPeer(handshakeProperties, outputStream, customProps);

        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final RootGroupPort port = mock(RootGroupPort.class);
        final PortAuthorizationResult authResult = mock(PortAuthorizationResult.class);
        doReturn(true).when(processGroup).isRootGroup();
        doReturn(port).when(processGroup).getOutputPort(handshakeProperties.getCommsIdentifier());
        doReturn(false).when(port).isValid();
        doReturn(true).when(port).isRunning();
        doReturn(authResult).when(port).checkUserAuthorization(any(String.class));
        doReturn(true).when(authResult).isAuthorized();

        serverProtocol.setRootProcessGroup(processGroup);
        try {
            serverProtocol.handshake(peer);
            fail();
        } catch (final HandshakeException e) {
            assertEquals(ResponseCode.PORT_NOT_IN_VALID_STATE, e.getResponseCode());
        }

        assertFalse(serverProtocol.isHandshakeSuccessful());
    }

    @Test
    public void testIllegalHandshakeProperty() throws Exception {
        final SocketFlowFileServerProtocol serverProtocol = getDefaultSocketFlowFileServerProtocol();
        final HandshakeProperties handshakeProperties = new HandshakeProperties();
        handshakeProperties.setCommsIdentifier("communication-identifier");
        handshakeProperties.setTransitUriPrefix("uri-prefix");
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final Map<HandshakeProperty, String> customProps = new HashMap<>();
//      specifically NOT including the HandshakeProperty.GZIP entry
        customProps.put(HandshakeProperty.PORT_IDENTIFIER, handshakeProperties.getCommsIdentifier());
        final Peer peer = getCustomPeer(handshakeProperties, outputStream, customProps);

        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final RootGroupPort port = mock(RootGroupPort.class);
        final PortAuthorizationResult authResult = mock(PortAuthorizationResult.class);
        doReturn(true).when(processGroup).isRootGroup();
        doReturn(port).when(processGroup).getOutputPort(handshakeProperties.getCommsIdentifier());
        doReturn(true).when(port).isValid();
        doReturn(true).when(port).isRunning();
        doReturn(authResult).when(port).checkUserAuthorization(any(String.class));
        doReturn(true).when(authResult).isAuthorized();

        serverProtocol.setRootProcessGroup(processGroup);
        try {
            serverProtocol.handshake(peer);
            fail();
        } catch (final HandshakeException e) {
            assertEquals(ResponseCode.MISSING_PROPERTY, e.getResponseCode());
        }

        assertFalse(serverProtocol.isHandshakeSuccessful());
    }

    @Test
    public void testUnauthorized() throws Exception {
        final SocketFlowFileServerProtocol serverProtocol = getDefaultSocketFlowFileServerProtocol();
        final HandshakeProperties handshakeProperties = new HandshakeProperties();
        handshakeProperties.setCommsIdentifier("communication-identifier");
        handshakeProperties.setTransitUriPrefix("uri-prefix");
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final Map<HandshakeProperty, String> customProps = new HashMap<>();
        customProps.put(HandshakeProperty.GZIP, "false");
        customProps.put(HandshakeProperty.PORT_IDENTIFIER, handshakeProperties.getCommsIdentifier());
        final Peer peer = getCustomPeer(handshakeProperties, outputStream, customProps);

        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final RootGroupPort port = mock(RootGroupPort.class);
        final PortAuthorizationResult authResult = mock(PortAuthorizationResult.class);
        doReturn(true).when(processGroup).isRootGroup();
        doReturn(port).when(processGroup).getOutputPort(handshakeProperties.getCommsIdentifier());
        doReturn(true).when(port).isValid();
        doReturn(true).when(port).isRunning();
        doReturn(authResult).when(port).checkUserAuthorization(any(String.class));
        doReturn(false).when(authResult).isAuthorized();
        doReturn("test failure").when(authResult).getExplanation();

        serverProtocol.setRootProcessGroup(processGroup);
        try {
            serverProtocol.handshake(peer);
            fail();
        } catch (final HandshakeException e) {
            assertEquals(ResponseCode.UNAUTHORIZED, e.getResponseCode());
        }

        assertFalse(serverProtocol.isHandshakeSuccessful());
    }

    @Test
    public void testUnknownPort() throws Exception {
        final SocketFlowFileServerProtocol serverProtocol = getDefaultSocketFlowFileServerProtocol();
        final HandshakeProperties handshakeProperties = new HandshakeProperties();
        handshakeProperties.setCommsIdentifier("communication-identifier");
        handshakeProperties.setTransitUriPrefix("uri-prefix");
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final Map<HandshakeProperty, String> customProps = new HashMap<>();
        customProps.put(HandshakeProperty.GZIP, "false");
        customProps.put(HandshakeProperty.PORT_IDENTIFIER, handshakeProperties.getCommsIdentifier());
        final Peer peer = getCustomPeer(handshakeProperties, outputStream, customProps);

        final ProcessGroup processGroup = mock(ProcessGroup.class);
        doReturn(true).when(processGroup).isRootGroup();

        serverProtocol.setRootProcessGroup(processGroup);
        try {
            serverProtocol.handshake(peer);
            fail();
        } catch (final HandshakeException e) {
            assertEquals(ResponseCode.UNKNOWN_PORT, e.getResponseCode());
        }

        assertFalse(serverProtocol.isHandshakeSuccessful());
    }

}
