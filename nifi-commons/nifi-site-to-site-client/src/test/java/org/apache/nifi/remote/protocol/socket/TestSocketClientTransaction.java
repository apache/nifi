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

import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.SiteToSiteEventReporter;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.codec.StandardFlowFileCodec;
import org.apache.nifi.remote.io.socket.SocketCommunicationsSession;
import org.apache.nifi.remote.io.socket.SocketInput;
import org.apache.nifi.remote.io.socket.SocketOutput;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.protocol.RequestType;
import org.apache.nifi.remote.protocol.Response;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestSocketClientTransaction {

    private FlowFileCodec codec = new StandardFlowFileCodec();

    private SocketClientTransaction getClientTransaction(final ByteArrayInputStream bis, final ByteArrayOutputStream bos, final TransferDirection direction) throws IOException {
        final PeerDescription description = null;
        final String peerUrl = "";
        final SocketCommunicationsSession commsSession = mock(SocketCommunicationsSession.class);
        final SocketInput socketIn = mock(SocketInput.class);
        final SocketOutput socketOut = mock(SocketOutput.class);
        when(commsSession.getInput()).thenReturn(socketIn);
        when(commsSession.getOutput()).thenReturn(socketOut);

        when(socketIn.getInputStream()).thenReturn(bis);
        when(socketOut.getOutputStream()).thenReturn(bos);

        final String clusterUrl = "";
        final Peer peer = new Peer(description, commsSession, peerUrl, clusterUrl);
        final boolean useCompression = false;
        final int penaltyMillis = 1000;
        final SiteToSiteEventReporter eventReporter = null;
        final int protocolVersion = 5;
        final String destinationId = "destinationId";
        return new SocketClientTransaction(protocolVersion, destinationId, peer, codec, direction, useCompression, penaltyMillis, eventReporter);
    }

    @Test
    public void testReceiveZeroFlowFile() throws IOException {

        final ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        final DataOutputStream serverResponse = new DataOutputStream(serverResponseBos);
        ResponseCode.NO_MORE_DATA.writeResponse(serverResponse);

        final ByteArrayInputStream bis = new ByteArrayInputStream(serverResponseBos.toByteArray());
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();

        final SocketClientTransaction transaction = getClientTransaction(bis, bos, TransferDirection.RECEIVE);

        execReceiveZeroFlowFile(transaction);

        // Verify what client has sent.
        final DataInputStream sentByClient = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
        assertEquals(RequestType.RECEIVE_FLOWFILES, RequestType.readRequestType(sentByClient));
        assertEquals(-1, sentByClient.read());
    }

    @Test
    public void testReceiveOneFlowFile() throws IOException {

        final ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        final DataOutputStream serverResponse = new DataOutputStream(serverResponseBos);
        ResponseCode.MORE_DATA.writeResponse(serverResponse);
        codec.encode(createDataPacket("contents on server 1"), serverResponse);
        ResponseCode.FINISH_TRANSACTION.writeResponse(serverResponse);
        ResponseCode.CONFIRM_TRANSACTION.writeResponse(serverResponse, "Checksum has been verified at server.");

        final ByteArrayInputStream bis = new ByteArrayInputStream(serverResponseBos.toByteArray());
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();

        final SocketClientTransaction transaction = getClientTransaction(bis, bos, TransferDirection.RECEIVE);

        execReceiveOneFlowFile(transaction);

        // Verify what client has sent.
        final DataInputStream sentByClient = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
        assertEquals(RequestType.RECEIVE_FLOWFILES, RequestType.readRequestType(sentByClient));
        final Response confirmResponse = Response.read(sentByClient);
        assertEquals(ResponseCode.CONFIRM_TRANSACTION, confirmResponse.getCode());
        assertEquals("3680976076", confirmResponse.getMessage(), "Checksum should be calculated at client");
        final Response completeResponse = Response.read(sentByClient);
        assertEquals(ResponseCode.TRANSACTION_FINISHED, completeResponse.getCode());
        assertEquals(-1, sentByClient.read());
    }

    @Test
    public void testReceiveTwoFlowFiles() throws IOException {

        final ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        final DataOutputStream serverResponse = new DataOutputStream(serverResponseBos);
        ResponseCode.MORE_DATA.writeResponse(serverResponse);
        codec.encode(createDataPacket("contents on server 1"), serverResponse);
        ResponseCode.CONTINUE_TRANSACTION.writeResponse(serverResponse);
        codec.encode(createDataPacket("contents on server 2"), serverResponse);
        ResponseCode.FINISH_TRANSACTION.writeResponse(serverResponse);
        ResponseCode.CONFIRM_TRANSACTION.writeResponse(serverResponse, "Checksum has been verified at server.");

        final ByteArrayInputStream bis = new ByteArrayInputStream(serverResponseBos.toByteArray());
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();

        final SocketClientTransaction transaction = getClientTransaction(bis, bos, TransferDirection.RECEIVE);

        assertEquals(Transaction.TransactionState.TRANSACTION_STARTED, transaction.getState());

        execReceiveTwoFlowFiles(transaction);

        // Verify what client has sent.
        final DataInputStream sentByClient = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
        assertEquals(RequestType.RECEIVE_FLOWFILES, RequestType.readRequestType(sentByClient));
        final Response confirmResponse = Response.read(sentByClient);
        assertEquals(ResponseCode.CONFIRM_TRANSACTION, confirmResponse.getCode());
        assertEquals("2969091230", confirmResponse.getMessage(), "Checksum should be calculated at client");
        final Response completeResponse = Response.read(sentByClient);
        assertEquals(ResponseCode.TRANSACTION_FINISHED, completeResponse.getCode());
        assertEquals(-1, sentByClient.read());
    }

    @Test
    public void testReceiveWithInvalidChecksum() throws IOException {

        final ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        final DataOutputStream serverResponse = new DataOutputStream(serverResponseBos);
        ResponseCode.MORE_DATA.writeResponse(serverResponse);
        codec.encode(createDataPacket("contents on server 1"), serverResponse);
        ResponseCode.CONTINUE_TRANSACTION.writeResponse(serverResponse);
        codec.encode(createDataPacket("contents on server 2"), serverResponse);
        ResponseCode.FINISH_TRANSACTION.writeResponse(serverResponse);
        ResponseCode.BAD_CHECKSUM.writeResponse(serverResponse);

        final ByteArrayInputStream bis = new ByteArrayInputStream(serverResponseBos.toByteArray());
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();

        final SocketClientTransaction transaction = getClientTransaction(bis, bos, TransferDirection.RECEIVE);

        execReceiveWithInvalidChecksum(transaction);

        // Verify what client has sent.
        final DataInputStream sentByClient = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
        assertEquals(RequestType.RECEIVE_FLOWFILES, RequestType.readRequestType(sentByClient));
        final Response confirmResponse = Response.read(sentByClient);
        assertEquals(ResponseCode.CONFIRM_TRANSACTION, confirmResponse.getCode());
        assertEquals("2969091230", confirmResponse.getMessage(), "Checksum should be calculated at client");
        assertEquals(-1, sentByClient.read());
    }

    @Test
    public void testSendZeroFlowFile() throws IOException {

        final ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();

        final ByteArrayInputStream bis = new ByteArrayInputStream(serverResponseBos.toByteArray());
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();

        final SocketClientTransaction transaction = getClientTransaction(bis, bos, TransferDirection.SEND);

        execSendZeroFlowFile(transaction);

        // Verify what client has sent.
        final DataInputStream sentByClient = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
        assertEquals(RequestType.SEND_FLOWFILES, RequestType.readRequestType(sentByClient));
        assertEquals(-1, sentByClient.read());
    }

    @Test
    public void testSendOneFlowFile() throws IOException {

        final ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        final DataOutputStream serverResponse = new DataOutputStream(serverResponseBos);
        ResponseCode.CONFIRM_TRANSACTION.writeResponse(serverResponse, "2946083981");
        ResponseCode.TRANSACTION_FINISHED.writeResponse(serverResponse);

        final ByteArrayInputStream bis = new ByteArrayInputStream(serverResponseBos.toByteArray());
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();

        final SocketClientTransaction transaction = getClientTransaction(bis, bos, TransferDirection.SEND);

        execSendOneFlowFile(transaction);

        // Verify what client has sent.
        final DataInputStream sentByClient = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
        assertEquals(RequestType.SEND_FLOWFILES, RequestType.readRequestType(sentByClient));
        final DataPacket packetByClient = codec.decode(sentByClient);
        assertEquals("contents on client 1", readContents(packetByClient));
        final Response endOfDataResponse = Response.read(sentByClient);
        assertEquals(ResponseCode.FINISH_TRANSACTION, endOfDataResponse.getCode());
        final Response confirmResponse = Response.read(sentByClient);
        assertEquals(ResponseCode.CONFIRM_TRANSACTION, confirmResponse.getCode());
        assertEquals(-1, sentByClient.read());
    }

    @Test
    public void testSendTwoFlowFiles() throws IOException {

        final ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        final DataOutputStream serverResponse = new DataOutputStream(serverResponseBos);
        ResponseCode.CONFIRM_TRANSACTION.writeResponse(serverResponse, "3359812065");
        ResponseCode.TRANSACTION_FINISHED.writeResponse(serverResponse);

        final ByteArrayInputStream bis = new ByteArrayInputStream(serverResponseBos.toByteArray());
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();

        final SocketClientTransaction transaction = getClientTransaction(bis, bos, TransferDirection.SEND);

        execSendTwoFlowFiles(transaction);

        // Verify what client has sent.
        final DataInputStream sentByClient = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
        assertEquals(RequestType.SEND_FLOWFILES, RequestType.readRequestType(sentByClient));
        DataPacket packetByClient = codec.decode(sentByClient);
        assertEquals("contents on client 1", readContents(packetByClient));
        final Response continueDataResponse = Response.read(sentByClient);
        assertEquals(ResponseCode.CONTINUE_TRANSACTION, continueDataResponse.getCode());
        packetByClient = codec.decode(sentByClient);
        assertEquals("contents on client 2", readContents(packetByClient));
        final Response endOfDataResponse = Response.read(sentByClient);
        assertEquals(ResponseCode.FINISH_TRANSACTION, endOfDataResponse.getCode());
        final Response confirmResponse = Response.read(sentByClient);
        assertEquals(ResponseCode.CONFIRM_TRANSACTION, confirmResponse.getCode());
        assertEquals(-1, sentByClient.read());
    }

    @Test
    public void testSendWithInvalidChecksum() throws IOException {

        final ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        final DataOutputStream serverResponse = new DataOutputStream(serverResponseBos);
        ResponseCode.CONFIRM_TRANSACTION.writeResponse(serverResponse, "Different checksum");

        final ByteArrayInputStream bis = new ByteArrayInputStream(serverResponseBos.toByteArray());
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();

        final SocketClientTransaction transaction = getClientTransaction(bis, bos, TransferDirection.SEND);

        execSendWithInvalidChecksum(transaction);

        // Verify what client has sent.
        final DataInputStream sentByClient = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
        assertEquals(RequestType.SEND_FLOWFILES, RequestType.readRequestType(sentByClient));
        DataPacket packetByClient = codec.decode(sentByClient);
        assertEquals("contents on client 1", readContents(packetByClient));
        final Response continueDataResponse = Response.read(sentByClient);
        assertEquals(ResponseCode.CONTINUE_TRANSACTION, continueDataResponse.getCode());
        packetByClient = codec.decode(sentByClient);
        assertEquals("contents on client 2", readContents(packetByClient));
        final Response endOfDataResponse = Response.read(sentByClient);
        assertEquals(ResponseCode.FINISH_TRANSACTION, endOfDataResponse.getCode());
        final Response confirmResponse = Response.read(sentByClient);
        assertEquals(ResponseCode.BAD_CHECKSUM, confirmResponse.getCode());
        assertEquals(-1, sentByClient.read());
    }

    @Test
    public void testSendButDestinationFull() throws IOException {

        final ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        final DataOutputStream serverResponse = new DataOutputStream(serverResponseBos);
        ResponseCode.CONFIRM_TRANSACTION.writeResponse(serverResponse, "3359812065");
        ResponseCode.TRANSACTION_FINISHED_BUT_DESTINATION_FULL.writeResponse(serverResponse);

        final ByteArrayInputStream bis = new ByteArrayInputStream(serverResponseBos.toByteArray());
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();

        final SocketClientTransaction transaction = getClientTransaction(bis, bos, TransferDirection.SEND);

        execSendButDestinationFull(transaction);

        // Verify what client has sent.
        final DataInputStream sentByClient = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
        assertEquals(RequestType.SEND_FLOWFILES, RequestType.readRequestType(sentByClient));
        DataPacket packetByClient = codec.decode(sentByClient);
        assertEquals("contents on client 1", readContents(packetByClient));
        final Response continueDataResponse = Response.read(sentByClient);
        assertEquals(ResponseCode.CONTINUE_TRANSACTION, continueDataResponse.getCode());
        packetByClient = codec.decode(sentByClient);
        assertEquals("contents on client 2", readContents(packetByClient));
        final Response endOfDataResponse = Response.read(sentByClient);
        assertEquals(ResponseCode.FINISH_TRANSACTION, endOfDataResponse.getCode());
        final Response confirmResponse = Response.read(sentByClient);
        assertEquals(ResponseCode.CONFIRM_TRANSACTION, confirmResponse.getCode());
        assertEquals(-1, sentByClient.read());
    }

}
