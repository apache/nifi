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
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.SiteToSiteAttributes;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.remote.io.http.HttpCommunicationsSession;
import org.apache.nifi.remote.io.socket.SocketChannelCommunicationsSession;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.remote.util.StandardDataPacket;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.nifi.util.NiFiProperties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestStandardRemoteGroupPort {

    private static final String ID = "remote-group-port-id";
    private static final String NAME = "remote-group-port-name";

    private RemoteProcessGroup remoteGroup;
    private ProcessScheduler scheduler;
    private SiteToSiteClient siteToSiteClient;
    private Transaction transaction;
    private EventReporter eventReporter;
    private ProcessGroup processGroup;
    private static final String REMOTE_CLUSTER_URL = "http://node0.example.com:8080/nifi";
    private StandardRemoteGroupPort port;
    private SharedSessionState sessionState;
    private MockProcessSession processSession;
    private MockProcessContext processContext;

    @BeforeClass
    public static void setup() throws Exception {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, "src/test/resources/nifi.properties");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.remote", "DEBUG");
    }

    private void setupMock(final SiteToSiteTransportProtocol protocol,
            final TransferDirection direction) throws Exception {
        final SiteToSiteClientConfig siteToSiteClientConfig = new SiteToSiteClient.Builder().buildConfig();
        setupMock(protocol, direction, siteToSiteClientConfig);
    }

    private void setupMock(final SiteToSiteTransportProtocol protocol,
            final TransferDirection direction,
           final SiteToSiteClientConfig siteToSiteClientConfig) throws Exception {
        processGroup = null;
        remoteGroup = mock(RemoteProcessGroup.class);
        scheduler = null;
        siteToSiteClient = mock(SiteToSiteClient.class);
        this.transaction = mock(Transaction.class);

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

        port = spy(new StandardRemoteGroupPort(ID, ID, NAME,
                processGroup, remoteGroup, direction, connectableType, null, scheduler, NiFiProperties.createBasicNiFiProperties(null, null)));

        doReturn(true).when(remoteGroup).isTransmitting();
        doReturn(protocol).when(remoteGroup).getTransportProtocol();
        doReturn(REMOTE_CLUSTER_URL).when(remoteGroup).getTargetUri();
        doReturn(siteToSiteClient).when(port).getSiteToSiteClient();
        doReturn(transaction).when(siteToSiteClient).createTransaction(eq(direction));
        doReturn(siteToSiteClientConfig).when(siteToSiteClient).getConfig();
        doReturn(eventReporter).when(remoteGroup).getEventReporter();

    }

    private void setupMockProcessSession() {
        // Construct a RemoteGroupPort as a processor to use NiFi mock library.
        final Processor remoteGroupPort = mock(Processor.class);
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(Relationship.ANONYMOUS);
        when(remoteGroupPort.getRelationships()).thenReturn(relationships);
        when(remoteGroupPort.getIdentifier()).thenReturn("remote-group-port-id");

        sessionState = new SharedSessionState(remoteGroupPort, new AtomicLong(0));
        processSession = new MockProcessSession(sessionState, remoteGroupPort);
        processContext = new MockProcessContext(remoteGroupPort);
    }

    @Test
    public void testSendRaw() throws Exception {

        setupMock(SiteToSiteTransportProtocol.RAW, TransferDirection.SEND);
        setupMockProcessSession();

        final String peerUrl = "nifi://node1.example.com:9090";
        final PeerDescription peerDescription = new PeerDescription("node1.example.com", 9090, true);
        try (final SocketChannel socketChannel = SocketChannel.open()) {
            final CommunicationsSession commsSession = new SocketChannelCommunicationsSession(socketChannel);
            commsSession.setUserDn("nifi.node1.example.com");
            final Peer peer = new Peer(peerDescription, commsSession, peerUrl, REMOTE_CLUSTER_URL);

            doReturn(peer).when(transaction).getCommunicant();

            final MockFlowFile flowFile = processSession.createFlowFile("0123456789".getBytes());
            sessionState.getFlowFileQueue().offer(flowFile);

            port.onTrigger(processContext, processSession);

            // Assert provenance.
            final List<ProvenanceEventRecord> provenanceEvents = sessionState.getProvenanceEvents();
            assertEquals(1, provenanceEvents.size());
            final ProvenanceEventRecord provenanceEvent = provenanceEvents.get(0);
            assertEquals(ProvenanceEventType.SEND, provenanceEvent.getEventType());
            assertEquals(peerUrl + "/" + flowFile.getAttribute(CoreAttributes.UUID.key()), provenanceEvent.getTransitUri());
            assertEquals("Remote DN=nifi.node1.example.com", provenanceEvent.getDetails());
            assertEquals("remote-group-port-id", provenanceEvent.getAttribute(SiteToSiteAttributes.S2S_PORT_ID.key()));

        }
    }

    @Test
    public void testReceiveRaw() throws Exception {

        setupMock(SiteToSiteTransportProtocol.RAW, TransferDirection.RECEIVE);
        setupMockProcessSession();

        final String peerUrl = "nifi://node1.example.com:9090";
        final PeerDescription peerDescription = new PeerDescription("node1.example.com", 9090, true);
        try (final SocketChannel socketChannel = SocketChannel.open()) {
            final CommunicationsSession commsSession = new SocketChannelCommunicationsSession(socketChannel);
            commsSession.setUserDn("nifi.node1.example.com");
            final Peer peer = new Peer(peerDescription, commsSession, peerUrl, REMOTE_CLUSTER_URL);

            doReturn(peer).when(transaction).getCommunicant();

            final String sourceFlowFileUuid = "flowfile-uuid";
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.UUID.key(), sourceFlowFileUuid);
            final byte[] dataPacketContents = "DataPacket Contents".getBytes();
            final ByteArrayInputStream dataPacketInputStream = new ByteArrayInputStream(dataPacketContents);
            final DataPacket dataPacket = new StandardDataPacket(attributes,
                    dataPacketInputStream, dataPacketContents.length);

            // Return null when it gets called second time.
            doReturn(dataPacket).doReturn(null).when(this.transaction).receive();

            port.onTrigger(processContext, processSession);

            // Assert provenance.
            final List<ProvenanceEventRecord> provenanceEvents = sessionState.getProvenanceEvents();
            assertEquals(1, provenanceEvents.size());
            final ProvenanceEventRecord provenanceEvent = provenanceEvents.get(0);
            assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent.getEventType());
            assertEquals(peerUrl + "/" + sourceFlowFileUuid, provenanceEvent.getTransitUri());
            assertEquals("Remote DN=nifi.node1.example.com", provenanceEvent.getDetails());

            // Assert received flow files.
            processSession.assertAllFlowFilesTransferred(Relationship.ANONYMOUS);
            final List<MockFlowFile> flowFiles = processSession.getFlowFilesForRelationship(Relationship.ANONYMOUS);
            assertEquals(1, flowFiles.size());
            final MockFlowFile flowFile = flowFiles.get(0);
            flowFile.assertAttributeEquals(SiteToSiteAttributes.S2S_HOST.key(), peer.getHost());
            flowFile.assertAttributeEquals(SiteToSiteAttributes.S2S_ADDRESS.key(), peer.getHost() + ":" + peer.getPort());
            flowFile.assertAttributeEquals(SiteToSiteAttributes.S2S_PORT_ID.key(), "remote-group-port-id");
        }

    }

    @Test
    public void testSendHttp() throws Exception {

        setupMock(SiteToSiteTransportProtocol.HTTP, TransferDirection.SEND);
        setupMockProcessSession();

        final String peerUrl = "https://node1.example.com:8080/nifi";
        final PeerDescription peerDescription = new PeerDescription("node1.example.com", 8080, true);
        final HttpCommunicationsSession commsSession = new HttpCommunicationsSession();
        commsSession.setUserDn("nifi.node1.example.com");
        final Peer peer = new Peer(peerDescription, commsSession, peerUrl, REMOTE_CLUSTER_URL);

        final String flowFileEndpointUri = "https://node1.example.com:8080/nifi-api/output-ports/port-id/transactions/transaction-id/flow-files";

        doReturn(peer).when(transaction).getCommunicant();
        commsSession.setDataTransferUrl(flowFileEndpointUri);

        final MockFlowFile flowFile = processSession.createFlowFile("0123456789".getBytes());
        sessionState.getFlowFileQueue().offer(flowFile);

        port.onTrigger(processContext, processSession);

        // Assert provenance.
        final List<ProvenanceEventRecord> provenanceEvents = sessionState.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord provenanceEvent = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.SEND, provenanceEvent.getEventType());
        assertEquals(flowFileEndpointUri, provenanceEvent.getTransitUri());
        assertEquals("Remote DN=nifi.node1.example.com", provenanceEvent.getDetails());
        assertEquals("remote-group-port-id", provenanceEvent.getAttribute(SiteToSiteAttributes.S2S_PORT_ID.key()));
    }

    @Test
    public void testSendBatchByCount() throws Exception {

        final SiteToSiteClientConfig siteToSiteClientConfig = new SiteToSiteClient.Builder()
                .requestBatchCount(2)
                .buildConfig();
        setupMock(SiteToSiteTransportProtocol.HTTP, TransferDirection.SEND, siteToSiteClientConfig);

        // t1 = {0, 1}, t2 = {2, 3}, t3 = {4}
        final int[] expectedNumberOfPackets = {2, 2, 1};
        testSendBatch(expectedNumberOfPackets);

    }

    @Test
    public void testSendBatchBySize() throws Exception {

        final SiteToSiteClientConfig siteToSiteClientConfig = new SiteToSiteClient.Builder()
                .requestBatchSize(30)
                .buildConfig();
        setupMock(SiteToSiteTransportProtocol.HTTP, TransferDirection.SEND, siteToSiteClientConfig);

        // t1 = {10, 11, 12}, t2 = {13, 14}
        final int[] expectedNumberOfPackets = {3, 2};
        testSendBatch(expectedNumberOfPackets);

    }

    @Test
    public void testSendBatchByDuration() throws Exception {

        final SiteToSiteClientConfig siteToSiteClientConfig = new SiteToSiteClient.Builder()
                .requestBatchDuration(1, TimeUnit.NANOSECONDS)
                .buildConfig();
        setupMock(SiteToSiteTransportProtocol.HTTP, TransferDirection.SEND, siteToSiteClientConfig);

        // t1 = {1}, t2 = {2} .. and so on.
        final int[] expectedNumberOfPackets = {1, 1, 1, 1, 1};
        testSendBatch(expectedNumberOfPackets);

    }

    /**
     * Generate flow files to be sent, and execute port's onTrigger method.
     * Finally, this method verifies whether packets are sent as expected.
     * @param expectedNumberOfPackets Specify how many packets should be sent by each transaction.
     *                                E.g. passing {2, 2, 1}, would generate 5 flow files in total.
     *                                Based on the siteToSiteClientConfig batch parameters,
     *                                it's expected to be sent via 3 transactions,
     *                                transaction 0 will send flow file 0 and 1,
     *                                transaction 1 will send flow file 2 and 3,
     *                                and transaction 2 will send flow file 4.
     *                                Each flow file has different content size generated automatically.
     *                                The content size starts with 10, and increases as more flow files are generated.
     *                                E.g. flow file 1 will have 10 bytes, flow file 2 has 11 bytes, f3 has 12 and so on.
     *
     */
    private void testSendBatch(final int[] expectedNumberOfPackets) throws Exception {

        setupMockProcessSession();

        final String peerUrl = "http://node1.example.com:8080/nifi";
        final PeerDescription peerDescription = new PeerDescription("node1.example.com", 8080, false);
        final HttpCommunicationsSession commsSession = new HttpCommunicationsSession();
        final Peer peer = new Peer(peerDescription, commsSession, peerUrl, REMOTE_CLUSTER_URL);

        final String flowFileEndpointUri = "http://node1.example.com:8080/nifi-api/output-ports/port-id/transactions/transaction-id/flow-files";

        doReturn(peer).when(transaction).getCommunicant();
        commsSession.setDataTransferUrl(flowFileEndpointUri);

        // Capture packets being sent to the remote peer
        final AtomicInteger totalPacketsSent = new AtomicInteger(0);
        final List<List<DataPacket>> sentPackets = new ArrayList<>(expectedNumberOfPackets.length);
        final List<DataPacket> sentPacketsPerTransaction = new ArrayList<>();
        doAnswer(invocation -> {
            sentPacketsPerTransaction.add((DataPacket)invocation.getArguments()[0]);
            totalPacketsSent.incrementAndGet();
            return null;
        }).when(transaction).send(any(DataPacket.class));
        doAnswer(invocation -> {
            sentPackets.add(new ArrayList<>(sentPacketsPerTransaction));
            sentPacketsPerTransaction.clear();
            return null;
        }).when(transaction).confirm();


        // Execute onTrigger while offering new flow files.
        final List<MockFlowFile> flowFiles = new ArrayList<>();
        for (int i = 0; i < expectedNumberOfPackets.length; i++) {
            int numOfPackets = expectedNumberOfPackets[i];
            int startF = flowFiles.size();
            int endF = startF + numOfPackets;
            IntStream.range(startF, endF).forEach(f -> {
                final StringBuilder flowFileContents = new StringBuilder("0123456789");
                for (int c = 0; c < f; c++) {
                    flowFileContents.append(c);
                }
                final byte[] bytes = flowFileContents.toString().getBytes();
                final MockFlowFile flowFile = spy(processSession.createFlowFile(bytes));
                when(flowFile.getSize()).then(invocation -> {
                    Thread.sleep(1); // For testSendBatchByDuration
                    return bytes.length;
                });
                sessionState.getFlowFileQueue().offer(flowFile);
                flowFiles.add(flowFile);
            });
            port.onTrigger(processContext, processSession);
        }

        // Verify transactions, sent packets, and provenance events.
        assertEquals(flowFiles.size(), totalPacketsSent.get());
        assertEquals("The number of transactions should match as expected.", expectedNumberOfPackets.length, sentPackets.size());
        final List<ProvenanceEventRecord> provenanceEvents = sessionState.getProvenanceEvents();
        assertEquals(flowFiles.size(), provenanceEvents.size());

        int f = 0;
        for (int i = 0; i < expectedNumberOfPackets.length; i++) {
            final List<DataPacket> dataPackets = sentPackets.get(i);
            assertEquals(expectedNumberOfPackets[i], dataPackets.size());

            for (int p = 0; p < dataPackets.size(); p++) {
                final FlowFile flowFile = flowFiles.get(f);

                // Assert sent packet
                final DataPacket dataPacket = dataPackets.get(p);
                assertEquals(flowFile.getSize(), dataPacket.getSize());

                // Assert provenance event
                final ProvenanceEventRecord provenanceEvent = provenanceEvents.get(f);
                assertEquals(ProvenanceEventType.SEND, provenanceEvent.getEventType());
                assertEquals(flowFileEndpointUri, provenanceEvent.getTransitUri());

                f++;
            }
        }
    }

    @Test
    public void testReceiveHttp() throws Exception {

        setupMock(SiteToSiteTransportProtocol.HTTP, TransferDirection.RECEIVE);
        setupMockProcessSession();

        final String peerUrl = "https://node1.example.com:8080/nifi";
        final PeerDescription peerDescription = new PeerDescription("node1.example.com", 8080, true);
        final HttpCommunicationsSession commsSession = new HttpCommunicationsSession();
        commsSession.setUserDn("nifi.node1.example.com");
        final Peer peer = new Peer(peerDescription, commsSession, peerUrl, REMOTE_CLUSTER_URL);

        final String flowFileEndpointUri = "https://node1.example.com:8080/nifi-api/output-ports/port-id/transactions/transaction-id/flow-files";

        doReturn(peer).when(transaction).getCommunicant();
        commsSession.setDataTransferUrl(flowFileEndpointUri);

        final Map<String, String> attributes = new HashMap<>();
        final byte[] dataPacketContents = "DataPacket Contents".getBytes();
        final ByteArrayInputStream dataPacketInputStream = new ByteArrayInputStream(dataPacketContents);
        final DataPacket dataPacket = new StandardDataPacket(attributes,
                dataPacketInputStream, dataPacketContents.length);

        // Return null when it gets called second time.
        doReturn(dataPacket).doReturn(null).when(transaction).receive();

        port.onTrigger(processContext, processSession);

        // Assert provenance.
        final List<ProvenanceEventRecord> provenanceEvents = sessionState.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord provenanceEvent = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent.getEventType());
        assertEquals(flowFileEndpointUri, provenanceEvent.getTransitUri());
        assertEquals("Remote DN=nifi.node1.example.com", provenanceEvent.getDetails());

        // Assert received flow files.
        processSession.assertAllFlowFilesTransferred(Relationship.ANONYMOUS);
        final List<MockFlowFile> flowFiles = processSession.getFlowFilesForRelationship(Relationship.ANONYMOUS);
        assertEquals(1, flowFiles.size());
        final MockFlowFile flowFile = flowFiles.get(0);
        flowFile.assertAttributeEquals(SiteToSiteAttributes.S2S_HOST.key(), peer.getHost());
        flowFile.assertAttributeEquals(SiteToSiteAttributes.S2S_ADDRESS.key(), peer.getHost() + ":" + peer.getPort());
        flowFile.assertAttributeEquals(SiteToSiteAttributes.S2S_PORT_ID.key(), "remote-group-port-id");

    }
}
