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

import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.SiteToSiteEventReporter;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.codec.StandardFlowFileCodec;
import org.apache.nifi.remote.io.http.HttpCommunicationsSession;
import org.apache.nifi.remote.io.http.HttpInput;
import org.apache.nifi.remote.io.http.HttpOutput;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.apache.nifi.remote.util.SiteToSiteRestApiClient;
import org.apache.nifi.web.api.entity.TransactionResultEntity;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.apache.nifi.remote.protocol.ResponseCode.CONFIRM_TRANSACTION;
import static org.apache.nifi.remote.protocol.SiteToSiteTestUtils.createDataPacket;
import static org.apache.nifi.remote.protocol.SiteToSiteTestUtils.execReceiveOneFlowFile;
import static org.apache.nifi.remote.protocol.SiteToSiteTestUtils.execReceiveTwoFlowFiles;
import static org.apache.nifi.remote.protocol.SiteToSiteTestUtils.execReceiveWithInvalidChecksum;
import static org.apache.nifi.remote.protocol.SiteToSiteTestUtils.execReceiveZeroFlowFile;
import static org.apache.nifi.remote.protocol.SiteToSiteTestUtils.execSendButDestinationFull;
import static org.apache.nifi.remote.protocol.SiteToSiteTestUtils.execSendOneFlowFile;
import static org.apache.nifi.remote.protocol.SiteToSiteTestUtils.execSendTwoFlowFiles;
import static org.apache.nifi.remote.protocol.SiteToSiteTestUtils.execSendWithInvalidChecksum;
import static org.apache.nifi.remote.protocol.SiteToSiteTestUtils.execSendZeroFlowFile;
import static org.apache.nifi.remote.protocol.SiteToSiteTestUtils.readContents;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TestHttpClientTransaction {

    private static final Logger logger = LoggerFactory.getLogger(TestHttpClientTransaction.class);
    private final FlowFileCodec codec = new StandardFlowFileCodec();

    private HttpClientTransaction getClientTransaction(final InputStream is, final OutputStream os, final SiteToSiteRestApiClient apiClient,
            final TransferDirection direction, final String transactionUrl) throws IOException {
        final PeerDescription description = null;
        final String peerUrl = "";

        final HttpCommunicationsSession commsSession = new HttpCommunicationsSession();
        ((HttpInput) commsSession.getInput()).setInputStream(is);
        ((HttpOutput) commsSession.getOutput()).setOutputStream(os);

        final String clusterUrl = "";
        final Peer peer = new Peer(description, commsSession, peerUrl, clusterUrl);
        final String portId = "portId";
        final boolean useCompression = false;
        final int penaltyMillis = 1000;
        final SiteToSiteEventReporter eventReporter = (severity, category, message) -> logger.info("Reporting event... severity={}, category={}, message={}", severity, category, message);
        final int protocolVersion = 5;

        final HttpClientTransaction transaction = new HttpClientTransaction(protocolVersion, peer, direction, useCompression, portId, penaltyMillis, eventReporter);
        transaction.initialize(apiClient, transactionUrl);

        return transaction;
    }

    @Test
    public void testReceiveZeroFlowFile() throws IOException {

        final SiteToSiteRestApiClient apiClient = mock(SiteToSiteRestApiClient.class);
        final String transactionUrl = "http://www.example.com/data-transfer/input-ports/portId/transactions/transactionId";
        doReturn(false).when(apiClient).openConnectionForReceive(eq(transactionUrl), any(Peer.class));

        final ByteArrayInputStream serverResponse = new ByteArrayInputStream(new byte[0]);
        final ByteArrayOutputStream clientRequest = new ByteArrayOutputStream();
        final HttpClientTransaction transaction = getClientTransaction(serverResponse, clientRequest, apiClient, TransferDirection.RECEIVE, transactionUrl);

        execReceiveZeroFlowFile(transaction);

        assertEquals(0, clientRequest.toByteArray().length, "Client sends nothing as payload to receive flow files.");
    }

    @Test
    public void testReceiveOneFlowFile() throws IOException {

        final SiteToSiteRestApiClient apiClient = mock(SiteToSiteRestApiClient.class);
        final String transactionUrl = "http://www.example.com/data-transfer/input-ports/portId/transactions/transactionId";
        doReturn(true).when(apiClient).openConnectionForReceive(eq(transactionUrl), any(Peer.class));
        final TransactionResultEntity resultEntity = new TransactionResultEntity();
        resultEntity.setResponseCode(CONFIRM_TRANSACTION.getCode());
        doReturn(resultEntity).when(apiClient).commitReceivingFlowFiles(eq(transactionUrl), eq(CONFIRM_TRANSACTION), eq("3680976076"));

        final ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        codec.encode(createDataPacket("contents on server 1"), serverResponseBos);
        final ByteArrayInputStream serverResponse = new ByteArrayInputStream(serverResponseBos.toByteArray());
        final ByteArrayOutputStream clientRequest = new ByteArrayOutputStream();
        final HttpClientTransaction transaction = getClientTransaction(serverResponse, clientRequest, apiClient, TransferDirection.RECEIVE, transactionUrl);

        execReceiveOneFlowFile(transaction);

        assertEquals(0, clientRequest.toByteArray().length, "Client sends nothing as payload to receive flow files.");
        verify(apiClient).commitReceivingFlowFiles(transactionUrl, CONFIRM_TRANSACTION, "3680976076");
    }

    @Test
    public void testReceiveTwoFlowFiles() throws IOException {

        final SiteToSiteRestApiClient apiClient = mock(SiteToSiteRestApiClient.class);
        final String transactionUrl = "http://www.example.com/data-transfer/input-ports/portId/transactions/transactionId";
        doReturn(true).when(apiClient).openConnectionForReceive(eq(transactionUrl), any(Peer.class));
        final TransactionResultEntity resultEntity = new TransactionResultEntity();
        resultEntity.setResponseCode(CONFIRM_TRANSACTION.getCode());
        doReturn(resultEntity).when(apiClient).commitReceivingFlowFiles(eq(transactionUrl), eq(CONFIRM_TRANSACTION), eq("2969091230"));

        final ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        codec.encode(createDataPacket("contents on server 1"), serverResponseBos);
        codec.encode(createDataPacket("contents on server 2"), serverResponseBos);
        final ByteArrayInputStream serverResponse = new ByteArrayInputStream(serverResponseBos.toByteArray());
        final ByteArrayOutputStream clientRequest = new ByteArrayOutputStream();
        final HttpClientTransaction transaction = getClientTransaction(serverResponse, clientRequest, apiClient, TransferDirection.RECEIVE, transactionUrl);

        execReceiveTwoFlowFiles(transaction);

        assertEquals(0, clientRequest.toByteArray().length, "Client sends nothing as payload to receive flow files.");
        verify(apiClient).commitReceivingFlowFiles(transactionUrl, CONFIRM_TRANSACTION, "2969091230");
    }

    @Test
    public void testReceiveWithInvalidChecksum() throws IOException {

        final SiteToSiteRestApiClient apiClient = mock(SiteToSiteRestApiClient.class);
        final String transactionUrl = "http://www.example.com/data-transfer/input-ports/portId/transactions/transactionId";
        doReturn(true).when(apiClient).openConnectionForReceive(eq(transactionUrl), any(Peer.class));
        // The checksum is correct, but here we simulate as if it's wrong, BAD_CHECKSUM.
        final TransactionResultEntity resultEntity = new TransactionResultEntity();
        resultEntity.setResponseCode(ResponseCode.BAD_CHECKSUM.getCode());
        doReturn(resultEntity).when(apiClient).commitReceivingFlowFiles(eq(transactionUrl), eq(CONFIRM_TRANSACTION), eq("2969091230"));

        final ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        codec.encode(createDataPacket("contents on server 1"), serverResponseBos);
        codec.encode(createDataPacket("contents on server 2"), serverResponseBos);
        final ByteArrayInputStream serverResponse = new ByteArrayInputStream(serverResponseBos.toByteArray());
        final ByteArrayOutputStream clientRequest = new ByteArrayOutputStream();
        final HttpClientTransaction transaction = getClientTransaction(serverResponse, clientRequest, apiClient, TransferDirection.RECEIVE, transactionUrl);

        execReceiveWithInvalidChecksum(transaction);

        assertEquals(0, clientRequest.toByteArray().length, "Client sends nothing as payload to receive flow files.");
        verify(apiClient).commitReceivingFlowFiles(transactionUrl, CONFIRM_TRANSACTION, "2969091230");
    }

    @Test
    public void testSendZeroFlowFile() throws IOException {

        final SiteToSiteRestApiClient apiClient = mock(SiteToSiteRestApiClient.class);
        final String transactionUrl = "http://www.example.com/data-transfer/input-ports/portId/transactions/transactionId";
        doNothing().when(apiClient).openConnectionForSend(eq(transactionUrl), any(Peer.class));

        final ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        final ByteArrayInputStream serverResponse = new ByteArrayInputStream(serverResponseBos.toByteArray());
        final ByteArrayOutputStream clientRequest = new ByteArrayOutputStream();
        final HttpClientTransaction transaction = getClientTransaction(serverResponse, clientRequest, apiClient, TransferDirection.SEND, transactionUrl);

        execSendZeroFlowFile(transaction);

        assertEquals(0, clientRequest.toByteArray().length, "Client didn't send anything");
    }

    @Test
    public void testSendOneFlowFile() throws IOException {

        final SiteToSiteRestApiClient apiClient = mock(SiteToSiteRestApiClient.class);
        final String transactionUrl = "http://www.example.com/data-transfer/input-ports/portId/transactions/transactionId";
        doNothing().when(apiClient).openConnectionForSend(eq(transactionUrl), any(Peer.class));
        // Emulate that server returns correct checksum.
        doAnswer(invocation -> {
            final HttpCommunicationsSession commSession = (HttpCommunicationsSession) invocation.getArguments()[0];
            commSession.setChecksum("2946083981");
            return null;
        }).when(apiClient).finishTransferFlowFiles(any(CommunicationsSession.class));
        final TransactionResultEntity resultEntity = new TransactionResultEntity();
        resultEntity.setResponseCode(ResponseCode.TRANSACTION_FINISHED.getCode());
        doReturn(resultEntity).when(apiClient).commitTransferFlowFiles(eq(transactionUrl), eq(CONFIRM_TRANSACTION));

        final ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        final ByteArrayInputStream serverResponse = new ByteArrayInputStream(serverResponseBos.toByteArray());
        final ByteArrayOutputStream clientRequest = new ByteArrayOutputStream();
        final HttpClientTransaction transaction = getClientTransaction(serverResponse, clientRequest, apiClient, TransferDirection.SEND, transactionUrl);

        execSendOneFlowFile(transaction);

        final InputStream sentByClient = new ByteArrayInputStream(clientRequest.toByteArray());
        final DataPacket packetByClient = codec.decode(sentByClient);
        assertEquals("contents on client 1", readContents(packetByClient));
        assertEquals(-1, sentByClient.read());

        verify(apiClient).commitTransferFlowFiles(transactionUrl, CONFIRM_TRANSACTION);
    }

    @Test
    public void testSendTwoFlowFiles() throws IOException {

        final SiteToSiteRestApiClient apiClient = mock(SiteToSiteRestApiClient.class);
        final String transactionUrl = "http://www.example.com/data-transfer/input-ports/portId/transactions/transactionId";
        doNothing().when(apiClient).openConnectionForSend(eq("portId"), any(Peer.class));
        // Emulate that server returns correct checksum.
        doAnswer(invocation -> {
            final HttpCommunicationsSession commSession = (HttpCommunicationsSession) invocation.getArguments()[0];
            commSession.setChecksum("3359812065");
            return null;
        }).when(apiClient).finishTransferFlowFiles(any(CommunicationsSession.class));
        final TransactionResultEntity resultEntity = new TransactionResultEntity();
        resultEntity.setResponseCode(ResponseCode.TRANSACTION_FINISHED.getCode());
        doReturn(resultEntity).when(apiClient).commitTransferFlowFiles(eq(transactionUrl), eq(CONFIRM_TRANSACTION));

        final ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        final ByteArrayInputStream serverResponse = new ByteArrayInputStream(serverResponseBos.toByteArray());
        final ByteArrayOutputStream clientRequest = new ByteArrayOutputStream();
        final HttpClientTransaction transaction = getClientTransaction(serverResponse, clientRequest, apiClient, TransferDirection.SEND, transactionUrl);

        execSendTwoFlowFiles(transaction);

        final InputStream sentByClient = new ByteArrayInputStream(clientRequest.toByteArray());
        DataPacket packetByClient = codec.decode(sentByClient);
        assertEquals("contents on client 1", readContents(packetByClient));
        packetByClient = codec.decode(sentByClient);
        assertEquals("contents on client 2", readContents(packetByClient));
        assertEquals(-1, sentByClient.read());

        verify(apiClient).commitTransferFlowFiles(transactionUrl, CONFIRM_TRANSACTION);
    }

    @Test
    public void testSendWithInvalidChecksum() throws IOException {
        final SiteToSiteRestApiClient apiClient = mock(SiteToSiteRestApiClient.class);
        final String transactionUrl = "http://www.example.com/data-transfer/input-ports/portId/transactions/transactionId";
        doNothing().when(apiClient).openConnectionForSend(eq(transactionUrl), any(Peer.class));
        // Emulate that server returns incorrect checksum.
        doAnswer(invocation -> {
            final HttpCommunicationsSession commSession = (HttpCommunicationsSession) invocation.getArguments()[0];
            commSession.setChecksum("Different checksum");
            return null;
        }).when(apiClient).finishTransferFlowFiles(any(CommunicationsSession.class));
        doAnswer(invocation -> {
            final TransactionResultEntity serverResult = new TransactionResultEntity();
            serverResult.setResponseCode(ResponseCode.CANCEL_TRANSACTION.getCode());
            return serverResult;
        }).when(apiClient).commitTransferFlowFiles(eq(transactionUrl), eq(ResponseCode.BAD_CHECKSUM));

        final ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        final ByteArrayInputStream serverResponse = new ByteArrayInputStream(serverResponseBos.toByteArray());
        final ByteArrayOutputStream clientRequest = new ByteArrayOutputStream();
        final HttpClientTransaction transaction = getClientTransaction(serverResponse, clientRequest, apiClient, TransferDirection.SEND, transactionUrl);

        execSendWithInvalidChecksum(transaction);

        final InputStream sentByClient = new ByteArrayInputStream(clientRequest.toByteArray());
        DataPacket packetByClient = codec.decode(sentByClient);
        assertEquals("contents on client 1", readContents(packetByClient));
        packetByClient = codec.decode(sentByClient);
        assertEquals("contents on client 2", readContents(packetByClient));
        assertEquals(-1, sentByClient.read());

        verify(apiClient).commitTransferFlowFiles(transactionUrl, ResponseCode.BAD_CHECKSUM);
    }

    @Test
    public void testSendButDestinationFull() throws IOException {

        final SiteToSiteRestApiClient apiClient = mock(SiteToSiteRestApiClient.class);
        final String transactionUrl = "http://www.example.com/data-transfer/input-ports/portId/transactions/transactionId";
        doNothing().when(apiClient).openConnectionForSend(eq("portId"), any(Peer.class));
        // Emulate that server returns correct checksum.
        doAnswer(invocation -> {
            final HttpCommunicationsSession commSession = (HttpCommunicationsSession) invocation.getArguments()[0];
            commSession.setChecksum("3359812065");
            return null;
        }).when(apiClient).finishTransferFlowFiles(any(CommunicationsSession.class));
        final TransactionResultEntity resultEntity = new TransactionResultEntity();
        resultEntity.setResponseCode(ResponseCode.TRANSACTION_FINISHED_BUT_DESTINATION_FULL.getCode());
        doReturn(resultEntity).when(apiClient).commitTransferFlowFiles(eq(transactionUrl), eq(CONFIRM_TRANSACTION));

        final ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        final ByteArrayInputStream serverResponse = new ByteArrayInputStream(serverResponseBos.toByteArray());
        final ByteArrayOutputStream clientRequest = new ByteArrayOutputStream();
        final HttpClientTransaction transaction = getClientTransaction(serverResponse, clientRequest, apiClient, TransferDirection.SEND, transactionUrl);

        execSendButDestinationFull(transaction);

        final InputStream sentByClient = new ByteArrayInputStream(clientRequest.toByteArray());
        DataPacket packetByClient = codec.decode(sentByClient);
        assertEquals("contents on client 1", readContents(packetByClient));
        packetByClient = codec.decode(sentByClient);
        assertEquals("contents on client 2", readContents(packetByClient));
        assertEquals(-1, sentByClient.read());

        verify(apiClient).commitTransferFlowFiles(transactionUrl, CONFIRM_TRANSACTION);
    }
}
