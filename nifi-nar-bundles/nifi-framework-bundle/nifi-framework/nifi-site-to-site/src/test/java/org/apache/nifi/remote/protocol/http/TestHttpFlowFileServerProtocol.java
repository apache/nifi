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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.apache.nifi.remote.HttpRemoteSiteListener;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.PortAuthorizationResult;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.codec.StandardFlowFileCodec;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.io.http.HttpInput;
import org.apache.nifi.remote.io.http.HttpServerCommunicationsSession;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.apache.nifi.remote.protocol.HandshakeProperty;
import org.apache.nifi.remote.util.StandardDataPacket;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.util.NiFiProperties;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TestHttpFlowFileServerProtocol {

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
        return new StandardHttpFlowFileServerProtocol(versionNegotiator, NiFiProperties.createBasicNiFiProperties(null, null));
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
        final RootGroupPort port = mock(RootGroupPort.class);
        final PortAuthorizationResult authResult = mock(PortAuthorizationResult.class);
        doReturn(true).when(processGroup).isRootGroup();
        doReturn(port).when(processGroup).getOutputPort("port-identifier");
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
        final RootGroupPort port = mock(RootGroupPort.class);
        final PortAuthorizationResult authResult = mock(PortAuthorizationResult.class);
        doReturn(true).when(processGroup).isRootGroup();
        doReturn(port).when(processGroup).getOutputPort("port-identifier");
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
        final RootGroupPort port = mock(RootGroupPort.class);
        final PortAuthorizationResult authResult = mock(PortAuthorizationResult.class);
        doReturn(true).when(processGroup).isRootGroup();
        doReturn(port).when(processGroup).getOutputPort("port-identifier");
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
        final Peer peer = transferOneFile(serverProtocol, transactionId);

        // Commit transaction
        final int flowFileSent = serverProtocol.commitTransferTransaction(peer, "2077607535");
        assertEquals(1, flowFileSent);
    }

    @Test
    public void testTransferOneFileBadChecksum() throws Exception {
        final HttpFlowFileServerProtocol serverProtocol = getDefaultHttpFlowFileServerProtocol();

        final String transactionId = "testTransferOneFileBadChecksum";
        final Peer peer = transferOneFile(serverProtocol, transactionId);

        // Commit transaction
        try {
            serverProtocol.commitTransferTransaction(peer, "client-sent-wrong-checksum");
            fail();
        } catch (final IOException e) {
            assertTrue(e.getMessage().contains("CRC32 Checksum"));
        }
    }

    private Peer transferOneFile(final HttpFlowFileServerProtocol serverProtocol, final String transactionId) throws IOException {
        final HttpRemoteSiteListener remoteSiteListener = HttpRemoteSiteListener.getInstance(NiFiProperties.createBasicNiFiProperties(null, null));
        final Peer peer = getDefaultPeer(transactionId);
        final HttpServerCommunicationsSession commsSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
        final String endpointUri = "https://peer-host:8443/nifi-api/output-ports/port-id/transactions/"
                + transactionId + "/flow-files";
        commsSession.putHandshakeParam(HandshakeProperty.BATCH_COUNT, "1");
        commsSession.setUserDn("unit-test");
        commsSession.setDataTransferUrl(endpointUri);

        serverProtocol.handshake(peer);

        assertTrue(serverProtocol.isHandshakeSuccessful());

        final FlowFileCodec negotiatedCoded = serverProtocol.negotiateCodec(peer);
        final ProcessContext context = mock(ProcessContext.class);
        final ProcessSession processSession = mock(ProcessSession.class);
        final ProvenanceReporter provenanceReporter = mock(ProvenanceReporter.class);
        final FlowFile flowFile = mock(FlowFile.class);
        doReturn(flowFile).when(processSession).get();
        doReturn(provenanceReporter).when(processSession).getProvenanceReporter();
        doAnswer(invocation -> {
            final String transitUri = (String)invocation.getArguments()[1];
            final String detail = (String)invocation.getArguments()[2];
            assertEquals(endpointUri, transitUri);
            assertEquals("Remote Host=peer-host, Remote DN=unit-test", detail);
            return null;
        }).when(provenanceReporter).send(eq(flowFile), any(String.class), any(String.class), any(Long.class), any(Boolean.class));

        doAnswer(invocation -> {
            final InputStreamCallback callback = (InputStreamCallback)invocation.getArguments()[1];
            callback.process(new java.io.ByteArrayInputStream("Server content".getBytes()));
            return null;
        }).when(processSession).read(any(FlowFile.class), any(InputStreamCallback.class));

        // Execute test using mock
        final int flowFileSent = serverProtocol.transferFlowFiles(peer, context, processSession, negotiatedCoded);
        assertEquals(1, flowFileSent);

        assertTrue(remoteSiteListener.isTransactionActive(transactionId));
        return peer;
    }

    @Test
    public void testTransferTwoFiles() throws Exception {
        final HttpRemoteSiteListener remoteSiteListener = HttpRemoteSiteListener.getInstance(NiFiProperties.createBasicNiFiProperties(null, null));

        final String transactionId = "testTransferTwoFiles";
        final Peer peer = getDefaultPeer(transactionId);
        final String endpointUri = "https://peer-host:8443/nifi-api/output-ports/port-id/transactions/"
                + transactionId + "/flow-files";
        final HttpFlowFileServerProtocol serverProtocol = getDefaultHttpFlowFileServerProtocol();
        final HttpServerCommunicationsSession commsSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
        commsSession.putHandshakeParam(HandshakeProperty.BATCH_COUNT, "2");
        commsSession.setUserDn("unit-test");
        commsSession.setDataTransferUrl(endpointUri);

        serverProtocol.handshake(peer);

        assertTrue(serverProtocol.isHandshakeSuccessful());

        final FlowFileCodec negotiatedCoded = serverProtocol.negotiateCodec(peer);
        final ProcessContext context = mock(ProcessContext.class);
        final ProcessSession processSession = mock(ProcessSession.class);
        final ProvenanceReporter provenanceReporter = mock(ProvenanceReporter.class);
        final FlowFile flowFile1 = mock(FlowFile.class);
        final FlowFile flowFile2 = mock(FlowFile.class);
        doReturn(flowFile1)
                .doReturn(flowFile2)
                .when(processSession).get();

        doReturn(provenanceReporter).when(processSession).getProvenanceReporter();
        doAnswer(invocation -> {
            final String transitUri = (String)invocation.getArguments()[1];
            final String detail = (String)invocation.getArguments()[2];
            assertEquals(endpointUri, transitUri);
            assertEquals("Remote Host=peer-host, Remote DN=unit-test", detail);
            return null;
        }).when(provenanceReporter).send(eq(flowFile1), any(String.class), any(String.class), any(Long.class), any(Boolean.class));

        doReturn(provenanceReporter).when(processSession).getProvenanceReporter();
        doAnswer(invocation -> {
            final String transitUri = (String)invocation.getArguments()[1];
            final String detail = (String)invocation.getArguments()[2];
            assertEquals(endpointUri, transitUri);
            assertEquals("Remote Host=peer-host, Remote DN=unit-test", detail);
            return null;
        }).when(provenanceReporter).send(eq(flowFile2), any(String.class), any(String.class), any(Long.class), any(Boolean.class));

        doAnswer(invocation -> {
            final InputStreamCallback callback = (InputStreamCallback)invocation.getArguments()[1];
            callback.process(new java.io.ByteArrayInputStream("Server content".getBytes()));
            return null;
        }).when(processSession).read(any(FlowFile.class), any(InputStreamCallback.class));

        // Execute test using mock
        int flowFileSent = serverProtocol.transferFlowFiles(peer, context, processSession, negotiatedCoded);
        assertEquals(2, flowFileSent);

        assertTrue(remoteSiteListener.isTransactionActive(transactionId));

        // Commit transaction
        flowFileSent = serverProtocol.commitTransferTransaction(peer, "2747386400");
        assertEquals(2, flowFileSent);
    }

    private DataPacket createClientDataPacket() {
        final String contents = "Content from client.";
        final byte[] bytes = contents.getBytes();
        final InputStream in = new ByteArrayInputStream(bytes);
        final Map<String, String> attributes = new HashMap<>();
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
        final Peer peer = getDefaultPeer(transactionId);
        final HttpServerCommunicationsSession commsSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
        receiveOneFile(serverProtocol, transactionId, peer);

        // Commit transaction
        commsSession.setResponseCode(ResponseCode.CONFIRM_TRANSACTION);
        final int flowFileReceived = serverProtocol.commitReceiveTransaction(peer);
        assertEquals(1, flowFileReceived);
    }

    @Test
    public void testReceiveOneFileBadChecksum() throws Exception {
        final HttpFlowFileServerProtocol serverProtocol = getDefaultHttpFlowFileServerProtocol();

        final String transactionId = "testReceiveOneFileBadChecksum";
        final Peer peer = getDefaultPeer(transactionId);
        final HttpServerCommunicationsSession commsSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
        receiveOneFile(serverProtocol, transactionId, peer);

        // Commit transaction
        commsSession.setResponseCode(ResponseCode.BAD_CHECKSUM);
        try {
            serverProtocol.commitReceiveTransaction(peer);
            fail();
        } catch (final IOException e) {
            assertTrue(e.getMessage().contains("Received a BadChecksum response"));
        }
    }

    private void receiveOneFile(final HttpFlowFileServerProtocol serverProtocol, final String transactionId, final Peer peer) throws IOException {
        final HttpRemoteSiteListener remoteSiteListener = HttpRemoteSiteListener.getInstance(NiFiProperties.createBasicNiFiProperties(null, null));
        final String endpointUri = "https://peer-host:8443/nifi-api/input-ports/port-id/transactions/"
                + transactionId + "/flow-files";
        final HttpServerCommunicationsSession commsSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
        commsSession.putHandshakeParam(HandshakeProperty.BATCH_COUNT, "1");
        commsSession.setUserDn("unit-test");
        commsSession.setDataTransferUrl(endpointUri);

        serverProtocol.handshake(peer);

        assertTrue(serverProtocol.isHandshakeSuccessful());

        final FlowFileCodec negotiatedCoded = serverProtocol.negotiateCodec(peer);
        final ProcessContext context = mock(ProcessContext.class);
        final ProcessSession processSession = mock(ProcessSession.class);
        final ProvenanceReporter provenanceReporter = mock(ProvenanceReporter.class);
        final FlowFile flowFile = mock(FlowFile.class);

        final DataPacket dataPacket = createClientDataPacket();

        final ByteArrayOutputStream testDataOs = new ByteArrayOutputStream();
        negotiatedCoded.encode(dataPacket, testDataOs);
        final InputStream httpInputStream = new ByteArrayInputStream(testDataOs.toByteArray());

        ((HttpInput)commsSession.getInput()).setInputStream(httpInputStream);

        doAnswer(invocation -> {
            final InputStream is = (InputStream) invocation.getArguments()[0];
            for (int b; (b = is.read()) >= 0;) {
                // consume stream.
            }
            return flowFile;
        }).when(processSession).importFrom(any(InputStream.class), any(FlowFile.class));
        // AbstractFlowFileServerProtocol adopts builder pattern and putAttribute is the last execution
        // which returns flowFile instance used later.
        doReturn(flowFile).when(processSession).putAttribute(any(FlowFile.class), any(String.class), any(String.class));
        doReturn(provenanceReporter).when(processSession).getProvenanceReporter();
        doAnswer(invocation -> {
            final String transitUri = (String)invocation.getArguments()[1];
            final String detail = (String)invocation.getArguments()[3];
            assertEquals(endpointUri, transitUri);
            assertEquals("Remote Host=peer-host, Remote DN=unit-test", detail);
            return null;
        }).when(provenanceReporter)
                .receive(any(FlowFile.class), any(String.class), any(String.class), any(String.class), any(Long.class));

        final Set<Relationship> relations = new HashSet<>();
        final Relationship relationship = new Relationship.Builder().build();
        relations.add(relationship);
        doReturn(relations).when(context).getAvailableRelationships();

        // Execute test using mock
        final int flowFileReceived = serverProtocol.receiveFlowFiles(peer, context, processSession, negotiatedCoded);
        assertEquals(1, flowFileReceived);

        assertTrue(remoteSiteListener.isTransactionActive(transactionId));
    }

    @Test
    public void testReceiveTwoFiles() throws Exception {
        final HttpRemoteSiteListener remoteSiteListener = HttpRemoteSiteListener.getInstance(NiFiProperties.createBasicNiFiProperties(null, null));

        final String transactionId = "testReceiveTwoFile";
        final String endpointUri = "https://peer-host:8443/nifi-api/input-ports/port-id/transactions/"
                + transactionId + "/flow-files";
        final HttpFlowFileServerProtocol serverProtocol = getDefaultHttpFlowFileServerProtocol();
        final Peer peer = getDefaultPeer(transactionId);
        final HttpServerCommunicationsSession commsSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
        commsSession.putHandshakeParam(HandshakeProperty.BATCH_COUNT, "2");
        commsSession.setUserDn("unit-test");
        commsSession.setDataTransferUrl(endpointUri);

        serverProtocol.handshake(peer);

        assertTrue(serverProtocol.isHandshakeSuccessful());

        final FlowFileCodec negotiatedCoded = serverProtocol.negotiateCodec(peer);
        final ProcessContext context = mock(ProcessContext.class);
        final ProcessSession processSession = mock(ProcessSession.class);
        final ProvenanceReporter provenanceReporter = mock(ProvenanceReporter.class);
        final FlowFile flowFile1 = mock(FlowFile.class);
        final FlowFile flowFile2 = mock(FlowFile.class);

        final ByteArrayOutputStream testDataOs = new ByteArrayOutputStream();
        negotiatedCoded.encode(createClientDataPacket(), testDataOs);
        negotiatedCoded.encode(createClientDataPacket(), testDataOs);
        final InputStream httpInputStream = new ByteArrayInputStream(testDataOs.toByteArray());

        ((HttpInput)commsSession.getInput()).setInputStream(httpInputStream);

        doAnswer(invocation -> {
            final InputStream is = (InputStream) invocation.getArguments()[0];
            for (int b; (b = is.read()) >= 0;) {
                // consume stream.
            }
            return flowFile1;
        }).when(processSession).importFrom(any(InputStream.class), any(FlowFile.class));
        // AbstractFlowFileServerProtocol adopts builder pattern and putAttribute is the last execution
        // which returns flowFile instance used later.
        doReturn(flowFile1)
                .doReturn(flowFile2)
                .when(processSession).putAttribute(any(FlowFile.class), any(String.class), any(String.class));
        doReturn(provenanceReporter).when(processSession).getProvenanceReporter();
        doAnswer(invocation -> {
            final String transitUri = (String)invocation.getArguments()[1];
            final String detail = (String)invocation.getArguments()[3];
            assertEquals(endpointUri, transitUri);
            assertEquals("Remote Host=peer-host, Remote DN=unit-test", detail);
            return null;
        }).when(provenanceReporter)
                .receive(any(FlowFile.class), any(String.class), any(String.class), any(String.class), any(Long.class));

        final Set<Relationship> relations = new HashSet<>();
        doReturn(relations).when(context).getAvailableRelationships();

        // Execute test using mock
        int flowFileReceived = serverProtocol.receiveFlowFiles(peer, context, processSession, negotiatedCoded);
        assertEquals(2, flowFileReceived);

        assertTrue(remoteSiteListener.isTransactionActive(transactionId));

        // Commit transaction
        commsSession.setResponseCode(ResponseCode.CONFIRM_TRANSACTION);
        flowFileReceived = serverProtocol.commitReceiveTransaction(peer);
        assertEquals(2, flowFileReceived);
    }


}
