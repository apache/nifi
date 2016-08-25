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

import org.apache.nifi.events.EventReporter;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.io.CompressionInputStream;
import org.apache.nifi.remote.io.CompressionOutputStream;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.protocol.Response;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.apache.nifi.remote.util.StandardDataPacket;
import org.apache.nifi.reporting.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

public abstract class AbstractTransaction implements Transaction {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected final Peer peer;
    protected final TransferDirection direction;
    private final CRC32 crc = new CRC32();
    private final boolean compress;
    protected final FlowFileCodec codec;
    protected final EventReporter eventReporter;
    protected final int protocolVersion;
    private final int penaltyMillis;
    protected final String destinationId;
    protected TransactionState state;
    protected boolean dataAvailable = false;
    private final long creationNanoTime = System.nanoTime();
    private int transfers = 0;
    private long contentBytes = 0;

    public AbstractTransaction(final Peer peer, final TransferDirection direction, final boolean useCompression,
                               final FlowFileCodec codec, final EventReporter eventReporter, final int protocolVersion,
                               final int penaltyMillis, final String destinationId) {
        this.peer = peer;
        this.state = TransactionState.TRANSACTION_STARTED;
        this.direction = direction;
        this.compress = useCompression;
        this.codec = codec;
        this.eventReporter = eventReporter;
        this.protocolVersion = protocolVersion;
        this.penaltyMillis = penaltyMillis;
        this.destinationId = destinationId;
    }

    protected void close() throws IOException {
    }

    @Override
    public void send(final byte[] content, final Map<String, String> attributes) throws IOException {
        send(new StandardDataPacket(attributes, new ByteArrayInputStream(content), content.length));
    }

    @Override
    public void error() {
        this.state = TransactionState.ERROR;
        try {
            close();
        } catch (IOException e) {
            logger.warn("Failed to close transaction due to {}", e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.warn("", e);
            }
        }
    }

    @Override
    public TransactionState getState() {
        return state;
    }

    @Override
    public Peer getCommunicant() {
        return peer;
    }

    @Override
    public final DataPacket receive() throws IOException {
        try {
            try {
                if (state != TransactionState.DATA_EXCHANGED && state != TransactionState.TRANSACTION_STARTED) {
                    throw new IllegalStateException("Cannot receive data from " + peer + " because Transaction State is " + state);
                }

                if (direction == TransferDirection.SEND) {
                    throw new IllegalStateException("Attempting to receive data from " + peer + " but started a SEND Transaction");
                }

                // if we already know there's no data, just return null
                if (!dataAvailable) {
                    return null;
                }

                // if we have already received a packet, check if another is available.
                if (transfers > 0) {
                    // Determine if Peer will send us data or has no data to send us
                    final Response dataAvailableCode = readTransactionResponse();
                    switch (dataAvailableCode.getCode()) {
                        case CONTINUE_TRANSACTION:
                            logger.debug("{} {} Indicates Transaction should continue", this, peer);
                            this.dataAvailable = true;
                            break;
                        case FINISH_TRANSACTION:
                            logger.debug("{} {} Indicates Transaction should finish", this, peer);
                            this.dataAvailable = false;
                            break;
                        default:
                            throw new ProtocolException("Got unexpected response from " + peer + " when asking for data: " + dataAvailableCode);
                    }
                }

                // if no data available, return null
                if (!dataAvailable) {
                    return null;
                }

                logger.debug("{} Receiving data from {}", this, peer);
                final InputStream is = peer.getCommunicationsSession().getInput().getInputStream();
                final InputStream dataIn = compress ? new CompressionInputStream(is) : is;
                final DataPacket packet = codec.decode(new CheckedInputStream(dataIn, crc));

                if (packet == null) {
                    this.dataAvailable = false;
                } else {
                    transfers++;
                    contentBytes += packet.getSize();
                }

                this.state = TransactionState.DATA_EXCHANGED;
                return packet;
            } catch (final IOException ioe) {
                throw new IOException("Failed to receive data from " + peer + " due to " + ioe, ioe);
            }
        } catch (final Exception e) {
            error();
            throw e;
        }
    }

    abstract protected Response readTransactionResponse() throws IOException;

    protected final void writeTransactionResponse(ResponseCode response) throws IOException {
        writeTransactionResponse(response, null);
    }
    abstract protected void writeTransactionResponse(ResponseCode response, String explanation) throws IOException;

    @Override
    public final void confirm() throws IOException {
        try {
            try {
                if (state == TransactionState.TRANSACTION_STARTED && !dataAvailable && direction == TransferDirection.RECEIVE) {
                    // client requested to receive data but no data available. no need to confirm.
                    state = TransactionState.TRANSACTION_CONFIRMED;
                    return;
                }

                if (state != TransactionState.DATA_EXCHANGED) {
                    throw new IllegalStateException("Cannot confirm Transaction because state is " + state
                            + "; Transaction can only be confirmed when state is " + TransactionState.DATA_EXCHANGED);
                }

                final CommunicationsSession commsSession = peer.getCommunicationsSession();
                if (direction == TransferDirection.RECEIVE) {
                    if (dataAvailable) {
                        throw new IllegalStateException("Cannot complete transaction because the sender has already sent more data than client has consumed.");
                    }

                    // we received a FINISH_TRANSACTION indicator. Send back a CONFIRM_TRANSACTION message
                    // to peer so that we can verify that the connection is still open. This is a two-phase commit,
                    // which helps to prevent the chances of data duplication. Without doing this, we may commit the
                    // session and then when we send the response back to the peer, the peer may have timed out and may not
                    // be listening. As a result, it will re-send the data. By doing this two-phase commit, we narrow the
                    // Critical Section involved in this transaction so that rather than the Critical Section being the
                    // time window involved in the entire transaction, it is reduced to a simple round-trip conversation.
                    logger.trace("{} Sending CONFIRM_TRANSACTION Response Code to {}", this, peer);
                    final String calculatedCRC = String.valueOf(crc.getValue());
                    writeTransactionResponse(ResponseCode.CONFIRM_TRANSACTION, calculatedCRC);

                    final Response confirmTransactionResponse;
                    try {
                        confirmTransactionResponse = readTransactionResponse();
                    } catch (final IOException ioe) {
                        logger.error("Failed to receive response code from {} when expecting confirmation of transaction", peer);
                        if (eventReporter != null) {
                            eventReporter.reportEvent(Severity.ERROR, "Site-to-Site", "Failed to receive response code from " + peer + " when expecting confirmation of transaction");
                        }
                        throw ioe;
                    }

                    logger.trace("{} Received {} from {}", this, confirmTransactionResponse, peer);

                    switch (confirmTransactionResponse.getCode()) {
                        case CONFIRM_TRANSACTION:
                            break;
                        case BAD_CHECKSUM:
                            throw new IOException(this + " Received a BadChecksum response from peer " + peer);
                        default:
                            throw new ProtocolException(this + " Received unexpected Response from peer " + peer + " : "
                                    + confirmTransactionResponse + "; expected 'Confirm Transaction' Response Code");
                    }

                    state = TransactionState.TRANSACTION_CONFIRMED;
                } else {
                    logger.debug("{} Sent FINISH_TRANSACTION indicator to {}", this, peer);
                    writeTransactionResponse(ResponseCode.FINISH_TRANSACTION);

                    final String calculatedCRC = String.valueOf(crc.getValue());

                    // we've sent a FINISH_TRANSACTION. Now we'll wait for the peer to send a 'Confirm Transaction' response
                    final Response transactionConfirmationResponse = readTransactionResponse();
                    if (transactionConfirmationResponse.getCode() == ResponseCode.CONFIRM_TRANSACTION) {
                        // Confirm checksum and echo back the confirmation.
                        logger.trace("{} Received {} from {}", this, transactionConfirmationResponse, peer);
                        final String receivedCRC = transactionConfirmationResponse.getMessage();

                        // CRC was not used before version 4
                        if (protocolVersion > 3) {
                            if (!receivedCRC.equals(calculatedCRC)) {
                                writeTransactionResponse(ResponseCode.BAD_CHECKSUM);
                                throw new IOException(this + " Sent data to peer " + peer + " but calculated CRC32 Checksum as "
                                        + calculatedCRC + " while peer calculated CRC32 Checksum as "
                                        + receivedCRC + "; canceling transaction and rolling back session");
                            }
                        }

                        writeTransactionResponse(ResponseCode.CONFIRM_TRANSACTION, "");
                    } else {
                        throw new ProtocolException("Expected to receive 'Confirm Transaction' response from peer "
                                + peer + " but received " + transactionConfirmationResponse);
                    }

                    state = TransactionState.TRANSACTION_CONFIRMED;
                }
            } catch (final IOException ioe) {
                throw new IOException("Failed to confirm transaction with " + peer + " due to " + ioe, ioe);
            }
        } catch (final Exception e) {
            error();
            throw e;
        }
    }

    @Override
    public final TransactionCompletion complete() throws IOException {
        try {
            try {
                if (state != TransactionState.TRANSACTION_CONFIRMED) {
                    throw new IllegalStateException("Cannot complete transaction with " + peer + " because state is " + state
                            + "; Transaction can only be completed when state is " + TransactionState.TRANSACTION_CONFIRMED);
                }

                boolean backoff = false;
                if (direction == TransferDirection.RECEIVE) {
                    if (transfers == 0) {
                        state = TransactionState.TRANSACTION_COMPLETED;
                        return new ClientTransactionCompletion(false, 0, 0L, System.nanoTime() - creationNanoTime);
                    }

                    // Confirm that we received the data and the peer can now discard it
                    logger.debug("{} Sending TRANSACTION_FINISHED to {}", this, peer);
                    writeTransactionResponse(ResponseCode.TRANSACTION_FINISHED);

                    state = TransactionState.TRANSACTION_COMPLETED;
                } else {
                    final Response transactionResponse;
                    try {
                        transactionResponse = readTransactionResponse();
                    } catch (final IOException e) {
                        throw new IOException(this + " Failed to receive a response from " + peer + " when expecting a TransactionFinished Indicator. "
                                + "It is unknown whether or not the peer successfully received/processed the data. " + e, e);
                    }

                    logger.debug("{} Received {} from {}", this, transactionResponse, peer);
                    if (transactionResponse.getCode() == ResponseCode.TRANSACTION_FINISHED_BUT_DESTINATION_FULL) {
                        peer.penalize(destinationId, penaltyMillis);
                        backoff = true;
                    } else if (transactionResponse.getCode() != ResponseCode.TRANSACTION_FINISHED) {
                        throw new ProtocolException("After sending data to " + peer + ", expected TRANSACTION_FINISHED response but got " + transactionResponse);
                    }

                    state = TransactionState.TRANSACTION_COMPLETED;
                }

                return new ClientTransactionCompletion(backoff, transfers, contentBytes, System.nanoTime() - creationNanoTime);
            } catch (final IOException ioe) {
                throw new IOException("Failed to complete transaction with " + peer + " due to " + ioe, ioe);
            }
        } catch (final Exception e) {
            error();
            throw e;
        } finally {
            close();
        }
    }

    @Override
    public final void cancel(final String explanation) throws IOException {
        if (state == TransactionState.TRANSACTION_CANCELED || state == TransactionState.TRANSACTION_COMPLETED || state == TransactionState.ERROR) {
            throw new IllegalStateException("Cannot cancel transaction because state is already " + state);
        }

        try {
            writeTransactionResponse(ResponseCode.CANCEL_TRANSACTION, explanation == null ? "<No explanation given>" : explanation);
            state = TransactionState.TRANSACTION_CANCELED;
        } catch (final IOException ioe) {
            error();
            throw new IOException("Failed to send 'cancel transaction' message to " + peer + " due to " + ioe, ioe);
        } finally {
            close();
        }
    }

    @Override
    public final String toString() {
        return getClass().getSimpleName() + "[Url=" + peer.getUrl() + ", TransferDirection=" + direction + ", State=" + state + "]";
    }

    @Override
    public final void send(final DataPacket dataPacket) throws IOException {
        try {
            try {
                if (state != TransactionState.DATA_EXCHANGED && state != TransactionState.TRANSACTION_STARTED) {
                    throw new IllegalStateException("Cannot send data to " + peer + " because Transaction State is " + state);
                }

                if (direction == TransferDirection.RECEIVE) {
                    throw new IllegalStateException("Attempting to send data to " + peer + " but started a RECEIVE Transaction");
                }

                if (transfers > 0) {
                    writeTransactionResponse(ResponseCode.CONTINUE_TRANSACTION);
                }

                logger.debug("{} Sending data to {}", this, peer);

                final OutputStream os = peer.getCommunicationsSession().getOutput().getOutputStream();
                final OutputStream dataOut = compress ? new CompressionOutputStream(os) : os;
                final OutputStream out = new CheckedOutputStream(dataOut, crc);

                codec.encode(dataPacket, out);

                // need to close the CompressionOutputStream in order to force it write out any remaining bytes.
                // Otherwise, do NOT close it because we don't want to close the underlying stream
                // (CompressionOutputStream will not close the underlying stream when it's closed)
                if (compress) {
                    out.close();
                }

                transfers++;
                contentBytes += dataPacket.getSize();
                this.state = TransactionState.DATA_EXCHANGED;
            } catch (final IOException ioe) {
                throw new IOException("Failed to send data to " + peer + " due to " + ioe, ioe);
            }
        } catch (final Exception e) {
            error();
            throw e;
        }
    }
}
