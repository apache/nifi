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
package org.apache.nifi.remote;

import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.io.http.HttpCommunicationsSession;
import org.apache.nifi.remote.io.socket.SocketChannelCommunicationsSession;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.remote.util.StandardDataPacket;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.InputStream;
import java.net.URI;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import org.apache.nifi.util.NiFiProperties;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class TestStandardRemoteGroupPort {

    private static final String ID = "remote-group-port-id";
    private static final String NAME = "remote-group-port-name";

    private RemoteProcessGroup remoteGroup;
    private ProcessScheduler scheduler;
    private SiteToSiteClient siteToSiteClient;
    private Transaction transaction;
    private EventReporter eventReporter;
    private ProcessGroup processGroup;
    public static final String REMOTE_CLUSTER_URL = "http://node0.example.com:8080/nifi";
    private StandardRemoteGroupPort port;
    private ProcessContext context;
    private ProcessSession session;
    private ProvenanceReporter provenanceReporter;

    @BeforeClass
    public static void setup() throws Exception {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, "src/test/resources/nifi.properties");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.remote", "DEBUG");
    }

    private void setupMock(final SiteToSiteTransportProtocol protocol,
            final TransferDirection direction) throws Exception {
        setupMock(protocol, direction, mock(Transaction.class));
    }

    private void setupMock(final SiteToSiteTransportProtocol protocol,
            final TransferDirection direction,
            final Transaction transaction) throws Exception {
        processGroup = null;
        remoteGroup = mock(RemoteProcessGroup.class);
        scheduler = null;
        siteToSiteClient = mock(SiteToSiteClient.class);
        this.transaction = transaction;

        eventReporter = mock(EventReporter.class);

        final ConnectableType connectableType;
        switch (direction) {
            case SEND:
                connectableType = ConnectableType.REMOTE_INPUT_PORT;
                break;
            case RECEIVE:
                connectableType = ConnectableType.OUTPUT_PORT;
                break;
            default:
                connectableType = null;
                break;
        }
        port = spy(new StandardRemoteGroupPort(ID, NAME,
                processGroup, remoteGroup, direction, connectableType, null, scheduler, NiFiProperties.createBasicNiFiProperties(null, null)));

        doReturn(true).when(remoteGroup).isTransmitting();
        doReturn(protocol).when(remoteGroup).getTransportProtocol();
        doReturn(new URI(REMOTE_CLUSTER_URL)).when(remoteGroup).getTargetUri();
        doReturn(siteToSiteClient).when(port).getSiteToSiteClient();
        doReturn(transaction).when(siteToSiteClient).createTransaction(eq(direction));
        doReturn(eventReporter).when(remoteGroup).getEventReporter();

        context = null;
        session = mock(ProcessSession.class);
        provenanceReporter = mock(ProvenanceReporter.class);
        doReturn(provenanceReporter).when(session).getProvenanceReporter();

    }

    @Test
    public void testSendRaw() throws Exception {

        setupMock(SiteToSiteTransportProtocol.RAW, TransferDirection.SEND);

        final String peerUrl = "nifi://node1.example.com:9090";
        final PeerDescription peerDescription = new PeerDescription("node1.example.com", 9090, false);
        try (final SocketChannel socketChannel = SocketChannel.open()) {
            final CommunicationsSession commsSession = new SocketChannelCommunicationsSession(socketChannel);
            final Peer peer = new Peer(peerDescription, commsSession, peerUrl, REMOTE_CLUSTER_URL);

            doReturn(peer).when(transaction).getCommunicant();

            final QueueSize queueSize = new QueueSize(1, 10);
            final FlowFile flowFile = mock(FlowFile.class);

            doReturn(queueSize).when(session).getQueueSize();
            // Return null when it gets called second time.
            doReturn(flowFile).doReturn(null).when(session).get();

            final String flowFileUuid = "flowfile-uuid";
            doReturn(flowFileUuid).when(flowFile).getAttribute(eq(CoreAttributes.UUID.key()));

            port.onTrigger(context, session);

            // Transit uri can be customized if necessary.
            verify(provenanceReporter).send(eq(flowFile), eq(peerUrl + "/" + flowFileUuid), any(String.class),
                    any(Long.class), eq(false));
        }
    }

    @Test
    public void testReceiveRaw() throws Exception {

        setupMock(SiteToSiteTransportProtocol.RAW, TransferDirection.RECEIVE);

        final String peerUrl = "nifi://node1.example.com:9090";
        final PeerDescription peerDescription = new PeerDescription("node1.example.com", 9090, false);
        try (final SocketChannel socketChannel = SocketChannel.open()) {
            final CommunicationsSession commsSession = new SocketChannelCommunicationsSession(socketChannel);
            final Peer peer = new Peer(peerDescription, commsSession, peerUrl, REMOTE_CLUSTER_URL);

            doReturn(peer).when(transaction).getCommunicant();

            final FlowFile flowFile = mock(FlowFile.class);
            final String sourceFlowFileUuid = "flowfile-uuid";
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.UUID.key(), sourceFlowFileUuid);
            final byte[] dataPacketContents = "DataPacket Contents".getBytes();
            final ByteArrayInputStream dataPacketInputStream = new ByteArrayInputStream(dataPacketContents);
            final DataPacket dataPacket = new StandardDataPacket(attributes,
                    dataPacketInputStream, dataPacketContents.length);

            doReturn(flowFile).when(session).create();
            // Return null when it gets called second time.
            doReturn(dataPacket).doReturn(null).when(this.transaction).receive();

            doReturn(flowFile).when(session).putAllAttributes(eq(flowFile), eq(attributes));
            doReturn(flowFile).when(session).importFrom(any(InputStream.class), eq(flowFile));

            port.onTrigger(context, session);

            // Transit uri can be customized if necessary.
            verify(provenanceReporter).receive(eq(flowFile), eq(peerUrl + "/" + sourceFlowFileUuid), any(String.class),
                    any(String.class), any(Long.class));
        }

    }

    @Test
    public void testSendHttp() throws Exception {

        setupMock(SiteToSiteTransportProtocol.HTTP, TransferDirection.SEND);

        final String peerUrl = "http://node1.example.com:8080/nifi";
        final PeerDescription peerDescription = new PeerDescription("node1.example.com", 8080, false);
        final HttpCommunicationsSession commsSession = new HttpCommunicationsSession();
        final Peer peer = new Peer(peerDescription, commsSession, peerUrl, REMOTE_CLUSTER_URL);

        final String flowFileEndpointUri = "http://node1.example.com:8080/nifi-api/output-ports/port-id/transactions/transaction-id/flow-files";

        doReturn(peer).when(transaction).getCommunicant();
        commsSession.setDataTransferUrl(flowFileEndpointUri);

        final QueueSize queueSize = new QueueSize(1, 10);
        final FlowFile flowFile = mock(FlowFile.class);

        doReturn(queueSize).when(session).getQueueSize();
        // Return null when it's called second time.
        doReturn(flowFile).doReturn(null).when(session).get();

        port.onTrigger(context, session);

        // peerUrl should be used as the transit url.
        verify(provenanceReporter).send(eq(flowFile), eq(flowFileEndpointUri), any(String.class),
                any(Long.class), eq(false));

    }

    @Test
    public void testReceiveHttp() throws Exception {

        setupMock(SiteToSiteTransportProtocol.HTTP, TransferDirection.RECEIVE);

        final String peerUrl = "http://node1.example.com:8080/nifi";
        final PeerDescription peerDescription = new PeerDescription("node1.example.com", 8080, false);
        final HttpCommunicationsSession commsSession = new HttpCommunicationsSession();
        final Peer peer = new Peer(peerDescription, commsSession, peerUrl, REMOTE_CLUSTER_URL);

        final String flowFileEndpointUri = "http://node1.example.com:8080/nifi-api/output-ports/port-id/transactions/transaction-id/flow-files";

        doReturn(peer).when(transaction).getCommunicant();
        commsSession.setDataTransferUrl(flowFileEndpointUri);

        final FlowFile flowFile = mock(FlowFile.class);
        final Map<String, String> attributes = new HashMap<>();
        final byte[] dataPacketContents = "DataPacket Contents".getBytes();
        final ByteArrayInputStream dataPacketInputStream = new ByteArrayInputStream(dataPacketContents);
        final DataPacket dataPacket = new StandardDataPacket(attributes,
                dataPacketInputStream, dataPacketContents.length);

        doReturn(flowFile).when(session).create();
        // Return null when it's called second time.
        doReturn(dataPacket).doReturn(null).when(transaction).receive();

        doReturn(flowFile).when(session).putAllAttributes(eq(flowFile), eq(attributes));
        doReturn(flowFile).when(session).importFrom(any(InputStream.class), eq(flowFile));

        port.onTrigger(context, session);

        // peerUrl should be used as the transit url.
        verify(provenanceReporter).receive(eq(flowFile), eq(flowFileEndpointUri), any(String.class),
                any(String.class), any(Long.class));

    }
}
