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

import org.apache.nifi.events.EventReporter;
import org.apache.nifi.remote.AbstractTransaction;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.protocol.RequestType;
import org.apache.nifi.remote.protocol.Response;
import org.apache.nifi.remote.protocol.ResponseCode;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class SocketClientTransaction extends AbstractTransaction {


    private final DataInputStream dis;
    private final DataOutputStream dos;

    SocketClientTransaction(final int protocolVersion, final String destinationId, final Peer peer, final FlowFileCodec codec,
            final TransferDirection direction, final boolean useCompression, final int penaltyMillis, final EventReporter eventReporter) throws IOException {
        super(peer, direction, useCompression, codec, eventReporter, protocolVersion, penaltyMillis, destinationId);
        this.dis = new DataInputStream(peer.getCommunicationsSession().getInput().getInputStream());
        this.dos = new DataOutputStream(peer.getCommunicationsSession().getOutput().getOutputStream());

        initialize();
    }

    private void initialize() throws IOException {
        try {
            if (direction == TransferDirection.RECEIVE) {
                // Indicate that we would like to have some data
                RequestType.RECEIVE_FLOWFILES.writeRequestType(dos);
                dos.flush();

                final Response dataAvailableCode = Response.read(dis);
                switch (dataAvailableCode.getCode()) {
                    case MORE_DATA:
                        logger.debug("{} {} Indicates that data is available", this, peer);
                        this.dataAvailable = true;
                        break;
                    case NO_MORE_DATA:
                        logger.debug("{} No data available from {}", this, peer);
                        this.dataAvailable = false;
                        return;
                    default:
                        throw new ProtocolException("Got unexpected response when asking for data: " + dataAvailableCode);
                }

            } else {
                // Indicate that we would like to have some data
                RequestType.SEND_FLOWFILES.writeRequestType(dos);
                dos.flush();
            }
        } catch (final Exception e) {
            error();
            throw e;
        }
    }

    @Override
    protected Response readTransactionResponse() throws IOException {
        return Response.read(dis);
    }

    @Override
    protected void writeTransactionResponse(ResponseCode response, String explanation, boolean flush) throws IOException {
        if(explanation == null){
            response.writeResponse(dos, flush);
        } else {
            response.writeResponse(dos, explanation, flush);
        }
    }
}
