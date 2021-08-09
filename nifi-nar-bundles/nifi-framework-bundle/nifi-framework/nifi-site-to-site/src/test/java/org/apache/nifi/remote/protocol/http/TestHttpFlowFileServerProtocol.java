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
package org.apache.nifi.remote.protocol.http;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.SiteToSiteAttributes;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.remote.HttpRemoteSiteListener;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.PortAuthorizationResult;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.codec.StandardFlowFileCodec;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.io.http.HttpInput;
import org.apache.nifi.remote.io.http.HttpServerCommunicationsSession;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.protocol.HandshakeProperty;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.apache.nifi.remote.util.StandardDataPacket;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.SharedSessionState;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHttpFlowFileServerProtocol {

    private SharedSessionState sessionState;
    private MockProcessSession processSession;
    private MockProcessContext processContext;

    @BeforeClass
    public static void setup() throws Exception {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, "src/test/resources/nifi.properties");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.remote", "DEBUG");
    }

    private Peer getDefaultPeer() {
        return getDefaultPeer(null);
    }

    private Peer getDefaultPeer(final String transactionId) {
        final PeerDescription description = new PeerDescription("peer-host", 8080, false);
        final InputStream inputStream = new ByteArrayInputStream(new byte[]{});
        final OutputStream outputStream = new ByteArrayOutputStream();
        final HttpServerCommunicationsSession commsSession = new HttpServerCommunicationsSession(inputStream, outputStream, transactionId, "user");
        commsSession.putHandshakeParam(HandshakeProperty.GZIP, "false");
        commsSession.putHandshakeParam(HandshakeProperty.REQUEST_EXPIRATION_MILLIS, "1234");
        final String peerUrl = "http://peer-host:8080/";
        final String clusterUrl = "cluster-url";
        return new Peer(description, commsSession, peerUrl, clusterUrl);
    }

    private HttpFlowFileServerProtocol getDefaultHttpFlowFileServerProtocol() {
        final StandardVersionNegotiator versionNegotiator = new StandardVersionNegotiator(5, 4, 3, 2, 1);
        return new StandardHttpFlowFileServerProtocol(versionNegotiator, new NiFiProperties());
    }

    @Test
    public void testIllegalHandshakeProperty() throws Exception {
        final HttpFlowFileServerProtocol serverProtocol = getDefaultHttpFlowFileServerProtocol();
        final Peer peer = getDefaultPeer();
        ((HttpServerCommunicationsSession)peer.getCommunicationsSession()).getHandshakeParams().clear();
        try {
            serverProtocol.handshake(peer);
            fail();
        } catch (final HandshakeException e) {
            assertEquals(ResponseCode.MISSING_PROPERTY, e.getResponseCode());
        }

        assertFalse(serverProtocol.isHandshakeSuccessful());
    }

    @Test
    public void testUnknownPort() throws Exception {
        final HttpFlowFileServerProtocol serverProtocol = getDefaultHttpFlowFileServerProtocol();
        final Peer peer = getDefaultPeer();
        ((HttpServerCommunicationsSession)peer.getCommunicationsSession())
                .putHandshakeParam(HandshakeProperty.PORT_IDENTIFIER, "port-identifier");

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

    @Test
    public void testUnauthorized() throws Exception {
        final HttpFlowFileServerProtocol serverProtocol = getDefaultHttpFlowFileServerProtocol();
        final Peer peer = getDefaultPeer();
        ((HttpServerCommunicationsSession)peer.getCommunicationsSession())
                .putHandshakeParam(HandshakeProperty.PORT_IDENTIFIER, "port-identifier");

        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final PublicPort port = mock(PublicPort.class);
        final PortAuthorizationResult authResult = mock(PortAuthorizationResult.class);
        doReturn(true).when(processGroup).isRootGroup();
        doReturn(port).when(processGroup).findOutputPort("port-identifier");
        doReturn(authResult).when(port).checkUserAuthorization(any(String.class));

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
    public void testPortNotInValidState() throws Exception {
        final HttpFlowFileServerProtocol serverProtocol = getDefaultHttpFlowFileServerProtocol();
        final Peer peer = getDefaultPeer();
        ((HttpServerCommunicationsSession)peer.getCommunicationsSession())
                .putHandshakeParam(HandshakeProperty.PORT_IDENTIFIER, "port-identifier");

        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final PublicPort port = mock(PublicPort.class);
        final PortAuthorizationResult authResult = mock(PortAuthorizationResult.class);
        doReturn(true).when(processGroup).isRootGroup();
        doReturn(port).when(processGroup).findInputPort(eq("port-identifier"));
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
    public void testPortDestinationFull() throws Exception {
        final HttpFlowFileServerProtocol serverProtocol = getDefaultHttpFlowFileServerProtocol();
        final Peer peer = getDefaultPeer();
        ((HttpServerCommunicationsSession)peer.getCommunicationsSession())
                .putHandshakeParam(HandshakeProperty.PORT_IDENTIFIER, "port-identifier");

        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final PublicPort port = mock(PublicPort.class);
        final PortAuthorizationResult authResult = mock(PortAuthorizationResult.class);
        doReturn(true).when(processGroup).isRootGroup();
        doReturn(port).when(processGroup).findOutputPort("port-identifier");
        doReturn(authResult).when(port).checkUserAuthorization(any(String.class));
        doReturn(true).when(authResult).isAuthorized();
        doReturn(true).when(port).isValid();
        doReturn(true).when(port).isRunning();
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
    public void testShutdown() throws Exception {
        final HttpFlowFileServerProtocol serverProtocol = getDefaultHttpFlowFileServerProtocol();

        final Peer peer = getDefaultPeer();
        serverProtocol.handshake(peer);

        assertTrue(serverProtocol.isHandshakeSuccessful());

        final FlowFileCodec negotiatedCoded = serverProtocol.negotiateCodec(peer);
        assertTrue(negotiatedCoded instanceof StandardFlowFileCodec);

        assertEquals(negotiatedCoded, serverProtocol.getPreNegotiatedCodec());
        assertEquals(1234, serverProtocol.getRequestExpiration());

        serverProtocol.shutdown(peer);

        final ProcessContext context = null;
        final ProcessSession processSession = null;
        try {
            serverProtocol.transferFlowFiles(peer, context, processSession, negotiatedCoded);
            fail("transferFlowFiles should fail since it's already shutdown.");
        } catch (final IllegalStateException e) {
        }

        try {
            serverProtocol.receiveFlowFiles(peer, context, processSession, negotiatedCoded);
            fail("receiveFlowFiles should fail since it's already shutdown.");
        } catch (final IllegalStateException e) {
        }
    }

    @Test
    public void testTransferZeroFile() throws Exception {
        final HttpFlowFileServerProtocol serverProtocol = getDefaultHttpFlowFileServerProtocol();

        final Peer peer = getDefaultPeer();
        serverProtocol.handshake(peer);

        assertTrue(serverProtocol.isHandshakeSuccessful());

        final FlowFileCodec negotiatedCoded = serverProtocol.negotiateCodec(peer);
        final ProcessContext context = null;
        final ProcessSession processSession = mock(ProcessSession.class);

        // Execute test using mock
        final int flowFileSent = serverProtocol.transferFlowFiles(peer, context, processSession, negotiatedCoded);
        assertEquals(0, flowFileSent);
    }

    @Test
    public void testTransferOneFile() throws Exception {
        final HttpFlowFileServerProtocol serverProtocol = getDefaultHttpFlowFileServerProtocol();

        final String transactionId = "testTransferOneFile";
        final Peer peer = getDefaultPeer(transactionId);
        final HttpServerCommunicationsSession commsSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
        final String endpointUri = "https://remote-host:8443/nifi-api/output-ports/port-id/transactions/"
                + transactionId + "/flow-files";
        commsSession.putHandshakeParam(HandshakeProperty.BATCH_COUNT, "1");
        commsSession.setUserDn("unit-test");
        commsSession.setDataTransferUrl(endpointUri);

        transferFlowFiles(serverProtocol, transactionId, peer, processSession -> {
            final MockFlowFile flowFile = processSession.createFlowFile("Server content".getBytes());
            final HashMap<String, String> attributes = new HashMap<>();
            attributes.put("uuid", "server-uuid");
            attributes.put("filename", "server-filename");
            attributes.put("server-attr-1", "server-attr-1-value");
            attributes.put("server-attr-2", "server-attr-2-value");
            flowFile.putAttributes(attributes);

            return Arrays.asList(flowFile);
        });

        // Commit transaction
        final int flowFileSent = serverProtocol.commitTransferTransaction(peer, "3229577812");
        assertEquals(1, flowFileSent);

        // Assert provenance
        final List<ProvenanceEventRecord> provenanceEvents = sessionState.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord provenanceEvent = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.SEND, provenanceEvent.getEventType());
        assertEquals(endpointUri, provenanceEvent.getTransitUri());
        assertEquals("Remote Host=peer-host, Remote DN=unit-test", provenanceEvent.getDetails());
    }

    @Test
    public void testTransferOneFileBadChecksum() throws Exception {
        final HttpFlowFileServerProtocol serverProtocol = getDefaultHttpFlowFileServerProtocol();

        final String transactionId = "testTransferOneFileBadChecksum";
        final Peer peer = getDefaultPeer(transactionId);
        final HttpServerCommunicationsSession commsSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
        final String endpointUri = "https://remote-host:8443/nifi-api/output-ports/port-id/transactions/"
                + transactionId + "/flow-files";
        commsSession.putHandshakeParam(HandshakeProperty.BATCH_COUNT, "1");
        commsSession.setUserDn("unit-test");
        commsSession.setDataTransferUrl(endpointUri);

        transferFlowFiles(serverProtocol, transactionId, peer, processSession -> {
            final MockFlowFile flowFile = processSession.createFlowFile("Server content".getBytes());
            final HashMap<String, String> attributes = new HashMap<>();
            attributes.put("uuid", "server-uuid");
            attributes.put("filename", "server-filename");
            attributes.put("server-attr-1", "server-attr-1-value");
            attributes.put("server-attr-2", "server-attr-2-value");
            flowFile.putAttributes(attributes);

            return Arrays.asList(flowFile);
        });

        // Commit transaction
        try {
            serverProtocol.commitTransferTransaction(peer, "client-sent-wrong-checksum");
            fail();
        } catch (final IOException e) {
            assertTrue(e.getMessage().contains("CRC32 Checksum"));
        }
    }

    private Peer transferFlowFiles(final HttpFlowFileServerProtocol serverProtocol, final String transactionId,
                                   final Peer peer, final Function<MockProcessSession,
            Collection<MockFlowFile>> flowFileGenerator) throws IOException {
        setupMockProcessSession();

        // Enqueue flow files to be transferred.
        final Collection<MockFlowFile> flowFiles = flowFileGenerator.apply(processSession);
        for (final MockFlowFile flowFile : flowFiles) {
            sessionState.getFlowFileQueue().offer(flowFile);
        }

        final HttpRemoteSiteListener remoteSiteListener = HttpRemoteSiteListener.getInstance(new NiFiProperties());

        serverProtocol.handshake(peer);
        assertTrue(serverProtocol.isHandshakeSuccessful());

        final FlowFileCodec negotiatedCoded = serverProtocol.negotiateCodec(peer);

        // Execute test using mock
        final int flowFileSent = serverProtocol.transferFlowFiles(peer, processContext, processSession, negotiatedCoded);
        assertEquals(flowFiles.size(), flowFileSent);

        assertTrue(remoteSiteListener.isTransactionActive(transactionId));
        return peer;
    }

    @Test
    public void testTransferTwoFiles() throws Exception {
        final String transactionId = "testTransferTwoFiles";
        final Peer peer = getDefaultPeer(transactionId);
        final String endpointUri = "https://remote-host:8443/nifi-api/output-ports/port-id/transactions/"
                + transactionId + "/flow-files";
        final HttpFlowFileServerProtocol serverProtocol = getDefaultHttpFlowFileServerProtocol();
        final HttpServerCommunicationsSession commsSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
        commsSession.putHandshakeParam(HandshakeProperty.BATCH_COUNT, "2");
        commsSession.setUserDn("unit-test");
        commsSession.setDataTransferUrl(endpointUri);

        transferFlowFiles(serverProtocol, transactionId, peer, processSession ->
            IntStream.of(1, 2).mapToObj(i -> {
                final MockFlowFile flowFile = processSession.createFlowFile(("Server content " + i).getBytes());
                final HashMap<String, String> attributes = new HashMap<>();
                attributes.put("uuid", "server-uuid-" + i);
                attributes.put("filename", "server-filename-" + i);
                attributes.put("server-attr-" + i + "-1", "server-attr-" + i + "-1-value");
                attributes.put("server-attr-" + i + "-2", "server-attr-" + i + "-2-value");
                flowFile.putAttributes(attributes);
                return flowFile;
            }).collect(Collectors.toList())
        );

        // Commit transaction
        final int flowFileSent = serverProtocol.commitTransferTransaction(peer, "3058746557");
        assertEquals(2, flowFileSent);

        // Assert provenance
        final List<ProvenanceEventRecord> provenanceEvents = sessionState.getProvenanceEvents();
        assertEquals(2, provenanceEvents.size());
        for (final ProvenanceEventRecord provenanceEvent : provenanceEvents) {
            assertEquals(ProvenanceEventType.SEND, provenanceEvent.getEventType());
            assertEquals(endpointUri, provenanceEvent.getTransitUri());
            assertEquals("Remote Host=peer-host, Remote DN=unit-test", provenanceEvent.getDetails());
        }

    }

    private DataPacket createClientDataPacket() {
        final String contents = "Content from client.";
        final byte[] bytes = contents.getBytes();
        final InputStream in = new ByteArrayInputStream(bytes);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.UUID.key(), "client-flow-file-uuid");
        attributes.put("client-attr-1", "client-attr-1-value");
        attributes.put("client-attr-2", "client-attr-2-value");
        return new StandardDataPacket(attributes, in, bytes.length);
    }

    @Test
    public void testReceiveZeroFile() throws Exception {
        final HttpFlowFileServerProtocol serverProtocol = getDefaultHttpFlowFileServerProtocol();

        final Peer peer = getDefaultPeer("testReceiveZeroFile");
        final HttpServerCommunicationsSession commsSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
        commsSession.setUserDn("unit-test");

        serverProtocol.handshake(peer);

        assertTrue(serverProtocol.isHandshakeSuccessful());

        final FlowFileCodec negotiatedCoded = serverProtocol.negotiateCodec(peer);
        final ProcessContext context = null;
        final ProcessSession processSession = mock(ProcessSession.class);


        final InputStream httpInputStream = new ByteArrayInputStream(new byte[]{});

        ((HttpInput)commsSession.getInput()).setInputStream(httpInputStream);

        // Execute test using mock
        final int flowFileReceived = serverProtocol.receiveFlowFiles(peer, context, processSession, negotiatedCoded);
        assertEquals(0, flowFileReceived);
    }

    @Test
    public void testReceiveOneFile() throws Exception {
        final HttpFlowFileServerProtocol serverProtocol = getDefaultHttpFlowFileServerProtocol();

        final String transactionId = "testReceiveOneFile";
        final String endpointUri = "https://remote-host:8443/nifi-api/input-ports/port-id/transactions/"
                + transactionId + "/flow-files";

        final Peer peer = getDefaultPeer(transactionId);

        final HttpServerCommunicationsSession commsSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
        commsSession.putHandshakeParam(HandshakeProperty.BATCH_COUNT, "1");
        commsSession.setUserDn("unit-test");
        commsSession.setDataTransferUrl(endpointUri);

        final DataPacket dataPacket = createClientDataPacket();
        receiveFlowFiles(serverProtocol, transactionId, peer, dataPacket);

        // Commit transaction
        commsSession.setResponseCode(ResponseCode.CONFIRM_TRANSACTION);
        final int flowFileReceived = serverProtocol.commitReceiveTransaction(peer);
        assertEquals(1, flowFileReceived);

        // Assert provenance.
        final List<ProvenanceEventRecord> provenanceEvents = sessionState.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord provenanceEvent = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent.getEventType());
        assertEquals(endpointUri, provenanceEvent.getTransitUri());
        assertEquals("Remote Host=peer-host, Remote DN=unit-test", provenanceEvent.getDetails());

        // Assert received flow files.
        processSession.assertAllFlowFilesTransferred(Relationship.ANONYMOUS);
        final List<MockFlowFile> flowFiles = processSession.getFlowFilesForRelationship(Relationship.ANONYMOUS);
        assertEquals(1, flowFiles.size());
        final MockFlowFile flowFile = flowFiles.get(0);
        flowFile.assertAttributeEquals(SiteToSiteAttributes.S2S_HOST.key(), peer.getHost());
        flowFile.assertAttributeEquals(SiteToSiteAttributes.S2S_ADDRESS.key(), peer.getHost() + ":" + peer.getPort());
        flowFile.assertAttributeEquals("client-attr-1", "client-attr-1-value");
        flowFile.assertAttributeEquals("client-attr-2", "client-attr-2-value");

    }

    @Test
    public void testReceiveOneFileBadChecksum() throws Exception {
        final HttpFlowFileServerProtocol serverProtocol = getDefaultHttpFlowFileServerProtocol();

        final String transactionId = "testReceiveOneFileBadChecksum";
        final Peer peer = getDefaultPeer(transactionId);
        final HttpServerCommunicationsSession commsSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
        receiveFlowFiles(serverProtocol, transactionId, peer, createClientDataPacket());

        // Commit transaction
        commsSession.setResponseCode(ResponseCode.BAD_CHECKSUM);
        try {
            serverProtocol.commitReceiveTransaction(peer);
            fail();
        } catch (final IOException e) {
            assertTrue(e.getMessage().contains("Received a BadChecksum response"));
        }
    }

    private void receiveFlowFiles(final HttpFlowFileServerProtocol serverProtocol, final String transactionId, final Peer peer, final DataPacket ... dataPackets) throws IOException {
        final HttpRemoteSiteListener remoteSiteListener = HttpRemoteSiteListener.getInstance(new NiFiProperties());
        final HttpServerCommunicationsSession commsSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();

        serverProtocol.handshake(peer);
        assertTrue(serverProtocol.isHandshakeSuccessful());

        setupMockProcessSession();

        // Emulate dataPackets sent from a Site-to-Site client.
        final FlowFileCodec negotiatedCoded = serverProtocol.negotiateCodec(peer);
        final ByteArrayOutputStream testDataOs = new ByteArrayOutputStream();
        for (final DataPacket dataPacket : dataPackets) {
            negotiatedCoded.encode(dataPacket, testDataOs);
        }
        final InputStream httpInputStream = new ByteArrayInputStream(testDataOs.toByteArray());
        ((HttpInput)commsSession.getInput()).setInputStream(httpInputStream);

        // Execute test using mock
        final int flowFileReceived = serverProtocol.receiveFlowFiles(peer, processContext, processSession, negotiatedCoded);
        assertEquals(dataPackets.length, flowFileReceived);

        assertTrue(remoteSiteListener.isTransactionActive(transactionId));
    }

    private void setupMockProcessSession() {
        // Construct a RootGroupPort as a processor to use NiFi mock library.
        final Processor rootGroupPort = mock(Processor.class);
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(Relationship.ANONYMOUS);
        when(rootGroupPort.getRelationships()).thenReturn(relationships);
        when(rootGroupPort.getIdentifier()).thenReturn("root-group-port-id");

        sessionState = new SharedSessionState(rootGroupPort, new AtomicLong(0));
        processSession = new MockProcessSession(sessionState, rootGroupPort, true, new MockStateManager(rootGroupPort), true);
        processContext = new MockProcessContext(rootGroupPort);
    }

    @Test
    public void testReceiveTwoFiles() throws Exception {
        final String transactionId = "testReceiveTwoFile";
        final String endpointUri = "https://remote-host:8443/nifi-api/input-ports/port-id/transactions/"
                + transactionId + "/flow-files";
        final HttpFlowFileServerProtocol serverProtocol = getDefaultHttpFlowFileServerProtocol();
        final Peer peer = getDefaultPeer(transactionId);
        final HttpServerCommunicationsSession commsSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
        commsSession.putHandshakeParam(HandshakeProperty.BATCH_COUNT, "2");
        commsSession.setUserDn("unit-test");
        commsSession.setDataTransferUrl(endpointUri);

        receiveFlowFiles(serverProtocol, transactionId, peer, createClientDataPacket(), createClientDataPacket());

        // Commit transaction
        commsSession.setResponseCode(ResponseCode.CONFIRM_TRANSACTION);
        final int flowFileReceived = serverProtocol.commitReceiveTransaction(peer);
        assertEquals(2, flowFileReceived);

        // Assert provenance.
        final List<ProvenanceEventRecord> provenanceEvents = sessionState.getProvenanceEvents();
        assertEquals(2, provenanceEvents.size());
        for (final ProvenanceEventRecord provenanceEvent : provenanceEvents) {
            assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent.getEventType());
            assertEquals(endpointUri, provenanceEvent.getTransitUri());
            assertEquals("Remote Host=peer-host, Remote DN=unit-test", provenanceEvent.getDetails());
        }

        // Assert received flow files.
        processSession.assertAllFlowFilesTransferred(Relationship.ANONYMOUS);
        final List<MockFlowFile> flowFiles = processSession.getFlowFilesForRelationship(Relationship.ANONYMOUS);
        assertEquals(2, flowFiles.size());
        for (final MockFlowFile flowFile : flowFiles) {
            flowFile.assertAttributeEquals(SiteToSiteAttributes.S2S_HOST.key(), peer.getHost());
            flowFile.assertAttributeEquals(SiteToSiteAttributes.S2S_ADDRESS.key(), peer.getHost() + ":" + peer.getPort());
            flowFile.assertAttributeEquals("client-attr-1", "client-attr-1-value");
            flowFile.assertAttributeEquals("client-attr-2", "client-attr-2-value");
        }
    }


}
