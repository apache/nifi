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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.remote.AbstractTransaction;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.codec.StandardFlowFileCodec;
import org.apache.nifi.remote.io.http.HttpCommunicationsSession;
import org.apache.nifi.remote.protocol.Response;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.apache.nifi.remote.util.SiteToSiteRestApiClient;
import org.apache.nifi.web.api.entity.TransactionResultEntity;

public class HttpClientTransaction extends AbstractTransaction {

    private SiteToSiteRestApiClient apiClient;
    private String transactionUrl;

    public HttpClientTransaction(final int protocolVersion, final Peer peer, TransferDirection direction,
                                 final boolean useCompression, final String portId, int penaltyMillis, EventReporter eventReporter) throws IOException {
        super(peer, direction, useCompression, new StandardFlowFileCodec(), eventReporter, protocolVersion, penaltyMillis, portId);
    }

    public void initialize(SiteToSiteRestApiClient apiUtil, String transactionUrl) throws IOException {
        this.transactionUrl = transactionUrl;
        this.apiClient = apiUtil;
        if(TransferDirection.RECEIVE.equals(direction)){
            dataAvailable = apiUtil.openConnectionForReceive(transactionUrl, peer);
        } else {
            apiUtil.openConnectionForSend(transactionUrl, peer);
        }
    }

    @Override
    protected Response readTransactionResponse() throws IOException {
        HttpCommunicationsSession commSession = (HttpCommunicationsSession) peer.getCommunicationsSession();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        if(TransferDirection.RECEIVE.equals(direction)){
            switch (state){
                case TRANSACTION_STARTED:
                case DATA_EXCHANGED:
                    logger.debug("{} {} readTransactionResponse. checksum={}", this, peer, commSession.getChecksum());
                    if(StringUtils.isEmpty(commSession.getChecksum())){
                        // We don't know if there's more data to receive, so just continue it.
                        ResponseCode.CONTINUE_TRANSACTION.writeResponse(dos);
                    } else {
                        // We got a checksum to send to server.
                        if (TransactionState.TRANSACTION_STARTED.equals(state)) {
                            logger.debug("{} {} There's no transaction to confirm.", this, peer);
                            ResponseCode.CONFIRM_TRANSACTION.writeResponse(dos, "");
                        } else {
                            TransactionResultEntity transactionResult
                                    = apiClient.commitReceivingFlowFiles(transactionUrl, ResponseCode.CONFIRM_TRANSACTION, commSession.getChecksum());
                            ResponseCode responseCode = ResponseCode.fromCode(transactionResult.getResponseCode());
                            if(responseCode.containsMessage()){
                                String message = transactionResult.getMessage();
                                responseCode.writeResponse(dos, message == null ? "" : message);
                            } else {
                                responseCode.writeResponse(dos);
                            }
                        }
                    }
                    break;
            }
        } else {
            switch (state){
                case DATA_EXCHANGED:
                    // Some flow files have been sent via stream, finish transferring.
                    apiClient.finishTransferFlowFiles(commSession);
                    ResponseCode.CONFIRM_TRANSACTION.writeResponse(dos, commSession.getChecksum());
                    break;
                case TRANSACTION_CONFIRMED:
                    TransactionResultEntity resultEntity = apiClient.commitTransferFlowFiles(transactionUrl, ResponseCode.CONFIRM_TRANSACTION);
                    ResponseCode responseCode = ResponseCode.fromCode(resultEntity.getResponseCode());
                    if(responseCode.containsMessage()){
                        responseCode.writeResponse(dos, resultEntity.getMessage());
                    } else {
                        responseCode.writeResponse(dos);
                    }
                    break;
            }
        }
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        return Response.read(new DataInputStream(bis));
    }

    @Override
    protected void writeTransactionResponse(ResponseCode response, String explanation) throws IOException {
        HttpCommunicationsSession commSession = (HttpCommunicationsSession) peer.getCommunicationsSession();
        if(TransferDirection.RECEIVE.equals(direction)){
            switch (response) {
                case CONFIRM_TRANSACTION:
                    logger.debug("{} Confirming transaction. checksum={}", this, explanation);
                    commSession.setChecksum(explanation);
                    break;
                case TRANSACTION_FINISHED:
                    logger.debug("{} Finishing transaction.", this);
                    break;
                case CANCEL_TRANSACTION:
                    logger.debug("{} Canceling transaction. explanation={}", this, explanation);
                    TransactionResultEntity resultEntity = apiClient.commitReceivingFlowFiles(transactionUrl, ResponseCode.CANCEL_TRANSACTION, null);
                    ResponseCode cancelResponse = ResponseCode.fromCode(resultEntity.getResponseCode());
                    switch (cancelResponse) {
                        case CANCEL_TRANSACTION:
                            logger.debug("{} CANCEL_TRANSACTION, The transaction is canceled on server properly.", this);
                            break;
                        default:
                            logger.warn("{} CANCEL_TRANSACTION, Expected the transaction is canceled on server, but received {}.", this, cancelResponse);
                            break;
                    }
                    break;
            }
        } else {
            switch (response) {
                case FINISH_TRANSACTION:
                    // The actual HTTP request will be sent in readTransactionResponse.
                    logger.debug("{} Finished sending flow files.", this);
                    break;
                case BAD_CHECKSUM: {
                        TransactionResultEntity resultEntity = apiClient.commitTransferFlowFiles(transactionUrl, ResponseCode.BAD_CHECKSUM);
                        ResponseCode badChecksumCancelResponse = ResponseCode.fromCode(resultEntity.getResponseCode());
                        switch (badChecksumCancelResponse) {
                            case CANCEL_TRANSACTION:
                                logger.debug("{} BAD_CHECKSUM, The transaction is canceled on server properly.", this);
                                break;
                            default:
                                logger.warn("{} BAD_CHECKSUM, Expected the transaction is canceled on server, but received {}.", this, badChecksumCancelResponse);
                                break;
                        }

                    }
                    break;
                case CONFIRM_TRANSACTION:
                    // The actual HTTP request will be sent in readTransactionResponse.
                    logger.debug("{} Transaction is confirmed.", this);
                    break;
                case CANCEL_TRANSACTION: {
                        logger.debug("{} Canceling transaction.", this);
                        TransactionResultEntity resultEntity = apiClient.commitTransferFlowFiles(transactionUrl, ResponseCode.CANCEL_TRANSACTION);
                        ResponseCode cancelResponse = ResponseCode.fromCode(resultEntity.getResponseCode());
                        switch (cancelResponse) {
                            case CANCEL_TRANSACTION:
                                logger.debug("{} CANCEL_TRANSACTION, The transaction is canceled on server properly.", this);
                                break;
                            default:
                                logger.warn("{} CANCEL_TRANSACTION, Expected the transaction is canceled on server, but received {}.", this, cancelResponse);
                                break;
                        }
                    }
                    break;
            }
        }
    }


    @Override
    protected void close() throws IOException {
        if (apiClient != null) {
            apiClient.close();
        }
    }
}

