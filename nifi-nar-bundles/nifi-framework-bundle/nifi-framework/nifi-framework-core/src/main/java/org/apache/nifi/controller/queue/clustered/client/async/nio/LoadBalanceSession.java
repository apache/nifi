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

package org.apache.nifi.controller.queue.clustered.client.async.nio;

import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.clustered.FlowFileContentAccess;
import org.apache.nifi.controller.queue.clustered.TransactionThreshold;
import org.apache.nifi.controller.queue.clustered.client.LoadBalanceFlowFileCodec;
import org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants;
import org.apache.nifi.controller.queue.clustered.server.TransactionAbortedException;
import org.apache.nifi.controller.repository.ContentNotFoundException;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.apache.nifi.stream.io.GZIPOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.ABORT_PROTOCOL_NEGOTIATION;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.ABORT_TRANSACTION;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.CONFIRM_CHECKSUM;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.CONFIRM_COMPLETE_TRANSACTION;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.QUEUE_FULL;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.REJECT_CHECKSUM;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.REQEUST_DIFFERENT_VERSION;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.SPACE_AVAILABLE;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.VERSION_ACCEPTED;


public class LoadBalanceSession {
    private static final Logger logger = LoggerFactory.getLogger(LoadBalanceSession.class);
    static final int MAX_DATA_FRAME_SIZE = 65535;
    private static final long PENALTY_MILLIS = TimeUnit.SECONDS.toMillis(2L);

    private final RegisteredPartition partition;
    private final Supplier<FlowFileRecord> flowFileSupplier;
    private final FlowFileContentAccess flowFileContentAccess;
    private final LoadBalanceFlowFileCodec flowFileCodec;
    private final PeerChannel channel;
    private final int timeoutMillis;
    private final String peerDescription;
    private final String connectionId;
    private final TransactionThreshold transactionThreshold;

    final VersionNegotiator negotiator = new StandardVersionNegotiator(1);
    private int protocolVersion = 1;

    private final Checksum checksum = new CRC32();

    // guarded by synchronizing on 'this'
    private ByteBuffer preparedFrame;
    private FlowFileRecord currentFlowFile;
    private List<FlowFileRecord> flowFilesSent = new ArrayList<>();
    private TransactionPhase phase = TransactionPhase.RECOMMEND_PROTOCOL_VERSION;
    private InputStream flowFileInputStream;
    private byte[] byteBuffer = new byte[MAX_DATA_FRAME_SIZE];
    private boolean complete = false;
    private long readTimeout;
    private long penaltyExpiration = -1L;

    public LoadBalanceSession(final RegisteredPartition partition, final FlowFileContentAccess contentAccess, final LoadBalanceFlowFileCodec flowFileCodec, final PeerChannel peerChannel,
                              final int timeoutMillis, final TransactionThreshold transactionThreshold) {
        this.partition = partition;
        this.flowFileSupplier = partition.getFlowFileRecordSupplier();
        this.connectionId = partition.getConnectionId();
        this.flowFileContentAccess = contentAccess;
        this.flowFileCodec = flowFileCodec;
        this.channel = peerChannel;
        this.peerDescription = peerChannel.getPeerDescription();

        if (timeoutMillis < 1) {
            throw new IllegalArgumentException();
        }
        this.timeoutMillis = timeoutMillis;
        this.transactionThreshold = transactionThreshold;
    }

    public RegisteredPartition getPartition() {
        return partition;
    }

    public synchronized int getDesiredReadinessFlag() {
        return phase.getRequiredSelectionKey();
    }

    public synchronized List<FlowFileRecord> getFlowFilesSent() {
        return Collections.unmodifiableList(flowFilesSent);
    }

    public synchronized boolean isComplete() {
        return complete;
    }

    public synchronized boolean communicate() throws IOException {
        if (isComplete()) {
            return false;
        }

        if (isPenalized()) {
            logger.debug("Will not communicate with Peer {} for Connection {} because session is penalized", peerDescription, connectionId);
            return false;
        }

        // If there's already a data frame prepared for writing, just write to the channel.
        if (preparedFrame != null && preparedFrame.hasRemaining()) {
            logger.trace("Current Frame is already available. Will continue writing current frame to channel");
            final int bytesWritten = channel.write(preparedFrame);
            return bytesWritten > 0;
        }

        try {
            // Check if the phase is one that needs to receive data and if so, call the appropriate method.
            switch (phase) {
                case RECEIVE_SPACE_RESPONSE:
                    return receiveSpaceAvailableResponse();
                case VERIFY_CHECKSUM:
                    return verifyChecksum();
                case CONFIRM_TRANSACTION_COMPLETE:
                    return confirmTransactionComplete();
                case RECEIVE_PROTOCOL_VERSION_ACKNOWLEDGMENT:
                    return receiveProtocolVersionAcknowledgment();
                case RECEIVE_RECOMMENDED_PROTOCOL_VERSION:
                    return receiveRecommendedProtocolVersion();
            }

            // Otherwise, we need to send something so get the data frame that should be sent and write it to the channel
            final ByteBuffer byteBuffer = getDataFrame();
            preparedFrame = channel.prepareForWrite(byteBuffer); // Prepare data frame for writing. E.g., encrypt the data, etc.

            final int bytesWritten = channel.write(preparedFrame);
            return bytesWritten > 0;
        } catch (final Exception e) {
            complete = true;
            throw e;
        }
    }


    private boolean confirmTransactionComplete() throws IOException {
        logger.debug("Confirming Transaction Complete for Peer {}", peerDescription);

        final OptionalInt transactionResponse = channel.read();
        if (!transactionResponse.isPresent()) {
            if (System.currentTimeMillis() > readTimeout) {
                throw new SocketTimeoutException("Timed out waiting for Peer " + peerDescription + " to confirm the transaction is complete");
            }

            return false;
        }

        final int response = transactionResponse.getAsInt();
        if (response < 0) {
            throw new EOFException("Confirmed checksum when writing data to Peer " + peerDescription + " but encountered End-of-File when expecting a Transaction Complete confirmation");
        }

        if (response == ABORT_TRANSACTION) {
            throw new TransactionAbortedException("Confirmed checksum when writing data to Peer " + peerDescription + " but Peer aborted transaction instead of completing it");
        }
        if (response != CONFIRM_COMPLETE_TRANSACTION) {
            throw new IOException("Expected a CONFIRM_COMPLETE_TRANSACTION response from Peer " + peerDescription + " but received a value of " + response);
        }

        complete = true;
        logger.debug("Successfully completed Transaction to send {} FlowFiles to Peer {} for Connection {}", flowFilesSent.size(), peerDescription, connectionId);

        return true;
    }


    private boolean verifyChecksum() throws IOException {
        logger.debug("Verifying Checksum for Peer {}", peerDescription);

        final OptionalInt checksumResponse = channel.read();
        if (!checksumResponse.isPresent()) {
            if (System.currentTimeMillis() > readTimeout) {
                throw new SocketTimeoutException("Timed out waiting for Peer " + peerDescription + " to verify the checksum");
            }

            return false;
        }

        final int response = checksumResponse.getAsInt();
        if (response < 0) {
            throw new EOFException("Encountered End-of-File when trying to verify Checksum with Peer " + peerDescription);
        }

        if (response == REJECT_CHECKSUM) {
            throw new TransactionAbortedException("After transferring FlowFiles to Peer " + peerDescription + " received a REJECT_CHECKSUM response. Aborting transaction.");
        }
        if (response != CONFIRM_CHECKSUM) {
            throw new TransactionAbortedException("After transferring FlowFiles to Peer " + peerDescription + " received an unexpected response code " + response
                    + ". Aborting transaction.");
        }

        logger.debug("Checksum confirmed. Writing COMPLETE_TRANSACTION flag");
        phase = TransactionPhase.SEND_TRANSACTION_COMPLETE;

        return true;
    }



    private ByteBuffer getDataFrame() throws IOException {
        switch (phase) {
            case RECOMMEND_PROTOCOL_VERSION:
                return recommendProtocolVersion();
            case ABORT_PROTOCOL_NEGOTIATION:
                return abortProtocolNegotiation();
            case SEND_CONNECTION_ID:
                return getConnectionId();
            case CHECK_SPACE:
                return checkSpace();
            case GET_NEXT_FLOWFILE:
                return getNextFlowFile();
            case SEND_FLOWFILE_DEFINITION:
            case SEND_FLOWFILE_CONTENTS:
                return getFlowFileContent();
            case SEND_CHECKSUM:
                return getChecksum();
            case SEND_TRANSACTION_COMPLETE:
                return getTransactionComplete();
            default:
                logger.debug("Phase of {}, returning null ByteBuffer", phase);
                return null;
        }
    }


    private ByteBuffer getTransactionComplete() {
        logger.debug("Sending Transaction Complete Indicator to Peer {}", peerDescription);

        final ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put((byte) LoadBalanceProtocolConstants.COMPLETE_TRANSACTION);
        buffer.rewind();

        readTimeout = System.currentTimeMillis() + timeoutMillis;
        phase = TransactionPhase.CONFIRM_TRANSACTION_COMPLETE;
        return buffer;
    }

    private ByteBuffer getChecksum() {
        logger.debug("Sending Checksum of {} to Peer {}", checksum.getValue(), peerDescription);

        // No more FlowFiles.
        final ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(checksum.getValue());

        readTimeout = System.currentTimeMillis() + timeoutMillis;
        phase = TransactionPhase.VERIFY_CHECKSUM;
        buffer.rewind();
        return buffer;
    }

    private ByteBuffer getFlowFileContent() throws IOException {
        // This method is fairly inefficient, copying lots of byte[]. Can do better. But keeping it simple for
        // now to get this working. Revisit with optimizations later.
        try {
            if (flowFileInputStream == null) {
                flowFileInputStream = flowFileContentAccess.read(currentFlowFile);
            }

            final int bytesRead = StreamUtils.fillBuffer(flowFileInputStream, byteBuffer, false);
            if (bytesRead < 1) {
                // If no data available, close the stream and move on to the next phase, returning a NO_DATA_FRAME buffer.
                flowFileInputStream.close();
                flowFileInputStream = null;
                phase = TransactionPhase.GET_NEXT_FLOWFILE;

                final ByteBuffer buffer = ByteBuffer.allocate(1);
                buffer.put((byte) LoadBalanceProtocolConstants.NO_DATA_FRAME);
                buffer.rewind();

                checksum.update(LoadBalanceProtocolConstants.NO_DATA_FRAME);

                logger.debug("Sending NO_DATA_FRAME indicator to Peer {}", peerDescription);

                return buffer;
            }

            logger.trace("Sending Data Frame that is {} bytes long to Peer {}", bytesRead, peerDescription);
            final ByteBuffer buffer;

            if (partition.getCompression() == LoadBalanceCompression.COMPRESS_ATTRIBUTES_AND_CONTENT) {
                final byte[] compressed = compressDataFrame(byteBuffer, bytesRead);
                final int compressedMaxLen = compressed.length;

                buffer = ByteBuffer.allocate(3 + compressedMaxLen);
                buffer.put((byte) LoadBalanceProtocolConstants.DATA_FRAME_FOLLOWS);
                buffer.putShort((short) compressedMaxLen);

                buffer.put(compressed, 0, compressedMaxLen);

            } else {
                buffer = ByteBuffer.allocate(3 + bytesRead);
                buffer.put((byte) LoadBalanceProtocolConstants.DATA_FRAME_FOLLOWS);
                buffer.putShort((short) bytesRead);

                buffer.put(byteBuffer, 0, bytesRead);
            }

            final byte[] frameArray = buffer.array();
            checksum.update(frameArray, 0, frameArray.length);

            phase = TransactionPhase.SEND_FLOWFILE_CONTENTS;
            buffer.rewind();
            return buffer;
        } catch (final ContentNotFoundException cnfe) {
            throw new ContentNotFoundException(currentFlowFile, cnfe.getMissingClaim(), cnfe.getMessage());
        }
    }

    private byte[] compressDataFrame(final byte[] uncompressed, final int byteCount) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final OutputStream gzipOut = new GZIPOutputStream(baos, 1)) {

            gzipOut.write(uncompressed, 0, byteCount);
            gzipOut.close();

            return baos.toByteArray();
        }
    }

    private ByteBuffer getNextFlowFile() throws IOException {
        if (transactionThreshold.isThresholdMet()) {
            currentFlowFile = null;
            logger.debug("Transaction Threshold reached sending to Peer {}; Transitioning phase to SEND_CHECKSUM", peerDescription);
        } else {
            currentFlowFile = flowFileSupplier.get();

            if (currentFlowFile == null) {
                logger.debug("No more FlowFiles to send to Peer {}; Transitioning phase to SEND_CHECKSUM", peerDescription);
            }
        }

        if (currentFlowFile == null) {
            phase = TransactionPhase.SEND_CHECKSUM;
            return noMoreFlowFiles();
        }

        transactionThreshold.adjust(1, currentFlowFile.getSize());
        logger.debug("Next FlowFile to send to Peer {} is {}", peerDescription, currentFlowFile);
        flowFilesSent.add(currentFlowFile);

        final LoadBalanceCompression compression = partition.getCompression();
        final boolean compressAttributes = compression != LoadBalanceCompression.DO_NOT_COMPRESS;
        logger.debug("Compression to use for sending to Peer {} is {}", peerDescription, compression);

        final byte[] flowFileEncoded;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            if (compressAttributes) {
                try (final OutputStream gzipOut = new GZIPOutputStream(baos, 1);
                     final ByteCountingOutputStream out = new ByteCountingOutputStream(gzipOut)) {

                    flowFileCodec.encode(currentFlowFile, out);
                }
            } else {
                flowFileCodec.encode(currentFlowFile, baos);
            }

            flowFileEncoded = baos.toByteArray();
        }

        final int metadataLength = flowFileEncoded.length;
        final ByteBuffer buffer = ByteBuffer.allocate(flowFileEncoded.length + 5);
        buffer.put((byte) LoadBalanceProtocolConstants.MORE_FLOWFILES);
        checksum.update(LoadBalanceProtocolConstants.MORE_FLOWFILES);

        buffer.putInt(metadataLength);
        checksum.update((metadataLength >> 24) & 0xFF);
        checksum.update((metadataLength >> 16) & 0xFF);
        checksum.update((metadataLength >> 8) & 0xFF);
        checksum.update(metadataLength & 0xFF);

        buffer.put(flowFileEncoded);
        checksum.update(flowFileEncoded, 0, flowFileEncoded.length);

        phase = TransactionPhase.SEND_FLOWFILE_DEFINITION;
        buffer.rewind();
        return buffer;
    }


    private ByteBuffer recommendProtocolVersion() {
        logger.debug("Recommending to Peer {} that Protocol Version {} be used", peerDescription, protocolVersion);

        final ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put((byte) protocolVersion);
        buffer.rewind();

        readTimeout = System.currentTimeMillis() + timeoutMillis;
        phase = TransactionPhase.RECEIVE_PROTOCOL_VERSION_ACKNOWLEDGMENT;
        return buffer;
    }

    private boolean receiveProtocolVersionAcknowledgment() throws IOException {
        logger.debug("Confirming Transaction Complete for Peer {}", peerDescription);

        final OptionalInt ackResponse = channel.read();
        if (!ackResponse.isPresent()) {
            if (System.currentTimeMillis() > readTimeout) {
                throw new SocketTimeoutException("Timed out waiting for Peer " + peerDescription + " to acknowledge Protocol Version");
            }

            return false;
        }

        final int response = ackResponse.getAsInt();
        if (response < 0) {
            throw new EOFException("Encounter End-of-File with Peer " + peerDescription + " when expecting a Protocol Version Acknowledgment");
        }

        if (response == VERSION_ACCEPTED) {
            logger.debug("Peer {} accepted Protocol Version {}", peerDescription, protocolVersion);
            phase = TransactionPhase.SEND_CONNECTION_ID;
            return true;
        }

        if (response == REQEUST_DIFFERENT_VERSION) {
            logger.debug("Recommended using Protocol Version of {} with Peer {} but received REQUEST_DIFFERENT_VERSION response", protocolVersion, peerDescription);
            readTimeout = System.currentTimeMillis() + timeoutMillis;
            phase = TransactionPhase.RECEIVE_RECOMMENDED_PROTOCOL_VERSION;
            return true;
        }

        throw new IOException("Failed to negotiate Protocol Version with Peer " + peerDescription + ". Recommended version " + protocolVersion + " but instead of an ACCEPT or REJECT " +
                "response got back a response of " + response);
    }

    private boolean receiveRecommendedProtocolVersion() throws IOException {
        logger.debug("Receiving Protocol Version from Peer {}", peerDescription);

        final OptionalInt recommendationResponse = channel.read();
        if (!recommendationResponse.isPresent()) {
            if (System.currentTimeMillis() > readTimeout) {
                throw new SocketTimeoutException("Timed out waiting for Peer " + peerDescription + " to recommend Protocol Version");
            }

            return false;
        }

        final int requestedVersion = recommendationResponse.getAsInt();
        if (requestedVersion < 0) {
            throw new EOFException("Encounter End-of-File with Peer " + peerDescription + " when expecting a Protocol Version Recommendation");
        }

        if (negotiator.isVersionSupported(requestedVersion)) {
            protocolVersion = requestedVersion;
            phase = TransactionPhase.SEND_CONNECTION_ID;
            logger.debug("Peer {} recommended Protocol Version of {}. Accepting version.", peerDescription, requestedVersion);

            return true;
        } else {
            final Integer preferred = negotiator.getPreferredVersion(requestedVersion);
            if (preferred == null) {
                logger.debug("Peer {} requested version {} of the Load Balance Protocol. This version is not acceptable. Aborting communications.", peerDescription, requestedVersion);
                phase = TransactionPhase.ABORT_PROTOCOL_NEGOTIATION;
                return true;
            } else {
                logger.debug("Peer {} requested version {} of the Protocol. Recommending version {} instead", peerDescription, requestedVersion, preferred);
                protocolVersion = preferred;
                phase = TransactionPhase.RECOMMEND_PROTOCOL_VERSION;
                return true;
            }
        }
    }

    private ByteBuffer noMoreFlowFiles() {
        final ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put((byte) LoadBalanceProtocolConstants.NO_MORE_FLOWFILES);
        buffer.rewind();

        checksum.update(LoadBalanceProtocolConstants.NO_MORE_FLOWFILES);
        return buffer;
    }

    private ByteBuffer abortProtocolNegotiation() {
        final ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put((byte) ABORT_PROTOCOL_NEGOTIATION);
        buffer.rewind();

        return buffer;
    }

    private ByteBuffer getConnectionId() {
        logger.debug("Sending Connection ID {} to Peer {}", connectionId, peerDescription);

        final ByteBuffer buffer = ByteBuffer.allocate(connectionId.length() + 2);
        buffer.putShort((short) connectionId.length());
        buffer.put(connectionId.getBytes(StandardCharsets.UTF_8));
        buffer.rewind();

        final byte[] frameBytes = buffer.array();
        checksum.update(frameBytes, 0, frameBytes.length);

        phase = TransactionPhase.CHECK_SPACE;
        return buffer;
    }

    private ByteBuffer checkSpace() {
        logger.debug("Sending a 'Check Space' request to Peer {} to determine if there is space in the queue for more FlowFiles", peerDescription);

        final ByteBuffer buffer = ByteBuffer.allocate(1);

        if (partition.isHonorBackpressure()) {
            buffer.put((byte) LoadBalanceProtocolConstants.CHECK_SPACE);
            checksum.update(LoadBalanceProtocolConstants.CHECK_SPACE);

            readTimeout = System.currentTimeMillis() + timeoutMillis;
            phase = TransactionPhase.RECEIVE_SPACE_RESPONSE;
        } else {
            buffer.put((byte) LoadBalanceProtocolConstants.SKIP_SPACE_CHECK);
            checksum.update(LoadBalanceProtocolConstants.SKIP_SPACE_CHECK);

            phase = TransactionPhase.GET_NEXT_FLOWFILE;
        }

        buffer.rewind();
        return buffer;
    }


    private boolean receiveSpaceAvailableResponse() throws IOException {
        logger.debug("Receiving response from Peer {} to determine whether or not space is available in queue {}", peerDescription, connectionId);

        final OptionalInt spaceAvailableResponse = channel.read();
        if (!spaceAvailableResponse.isPresent()) {
            if (System.currentTimeMillis() > readTimeout) {
                throw new SocketTimeoutException("Timed out waiting for Peer " + peerDescription + " to verify whether or not space is available for Connection " + connectionId);
            }

            return false;
        }

        final int response = spaceAvailableResponse.getAsInt();
        if (response < 0) {
            throw new EOFException("Encountered End-of-File when trying to verify with Peer " + peerDescription + " whether or not space is available in Connection " + connectionId);
        }

        if (response == SPACE_AVAILABLE) {
            logger.debug("Peer {} has confirmed that space is available in Connection {}", peerDescription, connectionId);
            phase = TransactionPhase.GET_NEXT_FLOWFILE;
        } else if (response == QUEUE_FULL) {
            logger.debug("Peer {} has confirmed that the queue is full for Connection {}", peerDescription, connectionId);
            phase = TransactionPhase.RECOMMEND_PROTOCOL_VERSION;
            checksum.reset(); // We are restarting the session entirely so we need to reset our checksum
            penalize();
        } else {
            throw new TransactionAbortedException("After requesting to know whether or not Peer " + peerDescription + " has space available in Connection " + connectionId
                + ", received unexpected response of " + response + ". Aborting transaction.");
        }

        return true;
    }

    private void penalize() {
        penaltyExpiration = System.currentTimeMillis() + PENALTY_MILLIS;
    }

    private boolean isPenalized() {
        // check for penaltyExpiration > -1L is not strictly necessary as it's implied by the second check but is still
        // here because it's more efficient to check this than to make the system call to System.currentTimeMillis().
        return penaltyExpiration > -1L && System.currentTimeMillis() < penaltyExpiration;
    }


    private enum TransactionPhase {
        RECOMMEND_PROTOCOL_VERSION(SelectionKey.OP_WRITE),

        RECEIVE_PROTOCOL_VERSION_ACKNOWLEDGMENT(SelectionKey.OP_READ),

        RECEIVE_RECOMMENDED_PROTOCOL_VERSION(SelectionKey.OP_READ),

        ABORT_PROTOCOL_NEGOTIATION(SelectionKey.OP_WRITE),

        SEND_CONNECTION_ID(SelectionKey.OP_WRITE),

        CHECK_SPACE(SelectionKey.OP_WRITE),

        RECEIVE_SPACE_RESPONSE(SelectionKey.OP_READ),

        SEND_FLOWFILE_DEFINITION(SelectionKey.OP_WRITE),

        SEND_FLOWFILE_CONTENTS(SelectionKey.OP_WRITE),

        GET_NEXT_FLOWFILE(SelectionKey.OP_WRITE),

        SEND_CHECKSUM(SelectionKey.OP_WRITE),

        VERIFY_CHECKSUM(SelectionKey.OP_READ),

        SEND_TRANSACTION_COMPLETE(SelectionKey.OP_WRITE),

        CONFIRM_TRANSACTION_COMPLETE(SelectionKey.OP_READ);


        private final int requiredSelectionKey;

        TransactionPhase(final int requiredSelectionKey) {
            this.requiredSelectionKey = requiredSelectionKey;
        }

        public int getRequiredSelectionKey() {
            return requiredSelectionKey;
        }
    }
}
