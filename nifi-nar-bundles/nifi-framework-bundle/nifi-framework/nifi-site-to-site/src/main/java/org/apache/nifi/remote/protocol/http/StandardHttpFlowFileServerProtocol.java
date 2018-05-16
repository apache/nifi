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

import org.apache.nifi.remote.HttpRemoteSiteListener;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.cluster.ClusterNodeInformation;
import org.apache.nifi.remote.cluster.NodeInformation;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.codec.StandardFlowFileCodec;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.io.http.HttpServerCommunicationsSession;
import org.apache.nifi.remote.protocol.AbstractFlowFileServerProtocol;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.FlowFileTransaction;
import org.apache.nifi.remote.protocol.HandshakeProperties;
import org.apache.nifi.remote.protocol.RequestType;
import org.apache.nifi.remote.protocol.Response;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.util.StringUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Optional;
import org.apache.nifi.util.NiFiProperties;

public class StandardHttpFlowFileServerProtocol extends AbstractFlowFileServerProtocol implements HttpFlowFileServerProtocol {

    public static final String RESOURCE_NAME = "HttpFlowFileProtocol";

    private final FlowFileCodec codec = new StandardFlowFileCodec();
    private final VersionNegotiator versionNegotiator;
    private final HttpRemoteSiteListener transactionManager;

    public StandardHttpFlowFileServerProtocol(final VersionNegotiator versionNegotiator, final NiFiProperties nifiProperties) {
        super();
        this.versionNegotiator = versionNegotiator;
        this.transactionManager = HttpRemoteSiteListener.getInstance(nifiProperties);
    }

    @Override
    public FlowFileCodec negotiateCodec(final Peer peer) throws IOException {
        return codec;
    }

    @Override
    public FlowFileCodec getPreNegotiatedCodec() {
        return codec;
    }

    @Override
    protected HandshakeProperties doHandshake(Peer peer) throws IOException, HandshakeException {

        HttpServerCommunicationsSession commsSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
        final String transactionId = commsSession.getTransactionId();

        HandshakeProperties confirmed = null;
        if (!StringUtils.isEmpty(transactionId)) {
            // If handshake is already done, use it.
            confirmed = transactionManager.getHandshakenProperties(transactionId);
        }
        if (confirmed == null) {
            // If it's not, then do handshake.
            confirmed = new HandshakeProperties();
            confirmed.setCommsIdentifier(transactionId);
            validateHandshakeRequest(confirmed, peer, commsSession.getHandshakeParams());
        }

        logger.debug("{} Done handshake, confirmed={}", this, confirmed);
        return confirmed;
    }

    @Override
    protected void writeTransactionResponse(boolean isTransfer, ResponseCode response, CommunicationsSession commsSession, String explanation) throws IOException {
        HttpServerCommunicationsSession commSession = (HttpServerCommunicationsSession) commsSession;

        commSession.setResponseCode(response);
        if(isTransfer){
            switch (response) {
                case NO_MORE_DATA:
                    logger.debug("{} There's no data to send.", this);
                    break;
                case CONTINUE_TRANSACTION:
                    logger.debug("{} Continue transaction... expecting more flow files.", this);
                    commSession.setStatus(Transaction.TransactionState.DATA_EXCHANGED);
                    break;
                case BAD_CHECKSUM:
                    logger.debug("{} Received BAD_CHECKSUM.", this);
                    commSession.setStatus(Transaction.TransactionState.ERROR);
                    break;
                case CONFIRM_TRANSACTION:
                    logger.debug("{} Transaction is confirmed.", this);
                    commSession.setStatus(Transaction.TransactionState.TRANSACTION_CONFIRMED);
                    break;
                case FINISH_TRANSACTION:
                    logger.debug("{} transaction is completed.", this);
                    commSession.setStatus(Transaction.TransactionState.TRANSACTION_COMPLETED);
                    break;
            }
        } else {
            switch (response) {
                case CONFIRM_TRANSACTION:
                    logger.debug("{} Confirming transaction. checksum={}", this, explanation);
                    commSession.setChecksum(explanation);
                    commSession.setStatus(Transaction.TransactionState.DATA_EXCHANGED);
                    break;
                case TRANSACTION_FINISHED:
                case TRANSACTION_FINISHED_BUT_DESTINATION_FULL:
                    logger.debug("{} Transaction is completed. responseCode={}", this, response);
                    commSession.setStatus(Transaction.TransactionState.TRANSACTION_COMPLETED);
                    break;
            }
        }
    }

    @Override
    protected Response readTransactionResponse(boolean isTransfer, CommunicationsSession commsSession) throws IOException {
        // Returns Response based on current status.
        HttpServerCommunicationsSession commSession = (HttpServerCommunicationsSession) commsSession;

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Transaction.TransactionState currentStatus = commSession.getStatus();
        if(isTransfer){
            switch (currentStatus){
                case DATA_EXCHANGED:
                    String clientChecksum = commSession.getChecksum();
                    logger.debug("readTransactionResponse. clientChecksum={}", clientChecksum);
                    ResponseCode.CONFIRM_TRANSACTION.writeResponse(new DataOutputStream(bos), clientChecksum);
                    break;
                case TRANSACTION_CONFIRMED:
                    logger.debug("readTransactionResponse. finishing.");
                    ResponseCode.TRANSACTION_FINISHED.writeResponse(new DataOutputStream(bos));
                    break;
            }
        } else {
            switch (currentStatus){
                case TRANSACTION_STARTED:
                    logger.debug("readTransactionResponse. returning CONTINUE_TRANSACTION.");
                    // We don't know if there's more data to receive, so just continue it.
                    ResponseCode.CONTINUE_TRANSACTION.writeResponse(new DataOutputStream(bos));
                    break;
                case TRANSACTION_CONFIRMED:
                    // Checksum was successfully validated at client side, or BAD_CHECKSUM is returned.
                    ResponseCode responseCode = commSession.getResponseCode();
                    logger.debug("readTransactionResponse. responseCode={}", responseCode);
                    if(responseCode.containsMessage()){
                        responseCode.writeResponse(new DataOutputStream(bos), "");
                    } else {
                        responseCode.writeResponse(new DataOutputStream(bos));
                    }
                    break;
            }
        }

        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        return Response.read(new DataInputStream(bis));
    }

    private int holdTransaction(Peer peer, FlowFileTransaction transaction) {
        // We don't commit the session here yet,
        // to avoid losing sent flow files in case some issue happens at client side while it is processing,
        // hold the transaction until we confirm additional request from client.
        HttpServerCommunicationsSession commSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
        String transactionId = commSession.getTransactionId();
        logger.debug("{} Holding transaction. transactionId={}", this, transactionId);
        transactionManager.holdTransaction(transactionId, transaction, handshakeProperties);

        return transaction.getFlowFilesSent().size();
    }

    @Override
    protected int commitTransferTransaction(Peer peer, FlowFileTransaction transaction) throws IOException {
        return holdTransaction(peer, transaction);
    }

    @Override
    public int commitTransferTransaction(Peer peer, String clientChecksum) throws IOException, IllegalStateException {
        logger.debug("{} Committing the transfer transaction. peer={} clientChecksum={}", this, peer, clientChecksum);
        HttpServerCommunicationsSession commSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
        String transactionId = commSession.getTransactionId();
        FlowFileTransaction transaction = transactionManager.finalizeTransaction(transactionId);
        commSession.setChecksum(clientChecksum);
        commSession.setStatus(Transaction.TransactionState.DATA_EXCHANGED);
        return super.commitTransferTransaction(peer, transaction);
    }

    @Override
    protected int commitReceiveTransaction(Peer peer, FlowFileTransaction transaction) throws IOException {
        return holdTransaction(peer, transaction);
    }

    @Override
    public int commitReceiveTransaction(Peer peer) throws IOException, IllegalStateException {
        logger.debug("{} Committing the receive transaction. peer={}", this, peer);
        HttpServerCommunicationsSession commSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
        String transactionId = commSession.getTransactionId();
        FlowFileTransaction transaction = transactionManager.finalizeTransaction(transactionId);
        commSession.setStatus(Transaction.TransactionState.TRANSACTION_CONFIRMED);
        return super.commitReceiveTransaction(peer, transaction);
    }

    @Override
    public RequestType getRequestType(final Peer peer) throws IOException {
        return null;
    }

    @Override
    public VersionNegotiator getVersionNegotiator() {
        return versionNegotiator;
    }

    @Override
    public void sendPeerList(Peer peer, Optional<ClusterNodeInformation> clusterNodeInfo, final NodeInformation self) throws IOException {
    }

    @Override
    public String getResourceName() {
        return RESOURCE_NAME;
    }

}
