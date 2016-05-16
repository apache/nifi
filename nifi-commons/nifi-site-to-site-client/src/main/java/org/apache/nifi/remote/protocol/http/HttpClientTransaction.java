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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.remote.AbstractTransaction;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.http.SiteToSiteRestApiUtil;
import org.apache.nifi.remote.codec.StandardFlowFileCodec;
import org.apache.nifi.remote.io.http.HttpCommunicationsSession;
import org.apache.nifi.remote.protocol.socket.Response;
import org.apache.nifi.remote.protocol.socket.ResponseCode;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.web.api.entity.TransactionResultEntity;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class HttpClientTransaction extends AbstractTransaction {

    private SiteToSiteRestApiUtil apiUtil;
    private String holdUri;

    public HttpClientTransaction(final int protocolVersion, final Peer peer, TransferDirection direction,
                                 final boolean useCompression, final String portId, int penaltyMillis, EventReporter eventReporter) throws IOException {
        super(peer, direction, useCompression, new StandardFlowFileCodec(), eventReporter, protocolVersion, penaltyMillis, portId);
    }

    public void initialize(SiteToSiteRestApiUtil apiUtil, String transactionUrl) throws IOException {
        this.apiUtil = apiUtil;
        if(TransferDirection.RECEIVE.equals(direction)){
            holdUri = apiUtil.openConnectionForReceive(transactionUrl, peer.getCommunicationsSession());
            dataAvailable = (holdUri != null);
        } else {
            apiUtil.openConnectionForSend(transactionUrl, peer.getCommunicationsSession());
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
                        if(holdUri == null){
                            logger.debug("{} {} There's no transaction to confirm.", this, peer);
                            ResponseCode.CONFIRM_TRANSACTION.writeResponse(dos, "");
                        } else {
                            TransactionResultEntity transactionResult = apiUtil.commitReceivingFlowFiles(holdUri, commSession.getChecksum());
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
                    holdUri = apiUtil.finishTransferFlowFiles(commSession);
                    ResponseCode.CONFIRM_TRANSACTION.writeResponse(dos, commSession.getChecksum());
                    break;
                case TRANSACTION_CONFIRMED:
                    TransactionResultEntity resultEntity = apiUtil.commitTransferFlowFiles(holdUri, ResponseCode.CONFIRM_TRANSACTION);
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
            }
        } else {
            switch (response) {
                case FINISH_TRANSACTION:
                    // The actual HTTP request will be sent in readTransactionResponse.
                    logger.debug("{} Finished sending flow files.", this);
                    break;
                case BAD_CHECKSUM:
                    apiUtil.commitTransferFlowFiles(holdUri, ResponseCode.BAD_CHECKSUM);
                    break;
                case CONFIRM_TRANSACTION:
                    // The actual HTTP request will be sent in readTransactionResponse.
                    logger.debug("{} Transaction is confirmed.", this);
                    break;
            }
        }
    }

}

