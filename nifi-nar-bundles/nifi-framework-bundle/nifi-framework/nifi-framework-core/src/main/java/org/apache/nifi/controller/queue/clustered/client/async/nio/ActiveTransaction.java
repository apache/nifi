package org.apache.nifi.controller.queue.clustered.client.async.nio;

import org.apache.nifi.controller.queue.clustered.FlowFileContentAccess;
import org.apache.nifi.controller.queue.clustered.SimpleLimitThreshold;
import org.apache.nifi.controller.queue.clustered.TransactionThreshold;
import org.apache.nifi.controller.queue.clustered.client.LoadBalanceFlowFileCodec;
import org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants;
import org.apache.nifi.controller.queue.clustered.server.TransactionAbortedException;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.stream.io.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.function.Supplier;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.ABORT_TRANSACTION;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.CONFIRM_CHECKSUM;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.CONFIRM_COMPLETE_TRANSACTION;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.REJECT_CHECKSUM;


public class ActiveTransaction {
    private static final Logger logger = LoggerFactory.getLogger(ActiveTransaction.class);
    static final int MAX_DATA_FRAME_SIZE = 65535;

    private final RegisteredPartition partition;
    private final Supplier<FlowFileRecord> flowFileSupplier;
    private final FlowFileContentAccess flowFileContentAccess;
    private final LoadBalanceFlowFileCodec flowFileCodec;
    private final PeerChannel channel;
    private final int timeoutMillis;
    private final String peerDescription;
    private final String connectionId;
    private final TransactionThreshold transactionThreshold = new SimpleLimitThreshold(1000, 10_000_000L);

    private final Checksum checksum = new CRC32();

    // guarded by synchronizing on 'this'
    private ByteBuffer preparedFrame;
    private FlowFileRecord currentFlowFile;
    private List<FlowFileRecord> flowFilesSent = new ArrayList<>();
    private TransactionPhase phase = TransactionPhase.SEND_CONNECTION_ID;
    private InputStream flowFileInputStream;
    private byte[] byteBuffer = new byte[MAX_DATA_FRAME_SIZE];
    private boolean complete = false;
    private long readTimeout;

    public ActiveTransaction(final RegisteredPartition partition, final FlowFileContentAccess contentAccess, final LoadBalanceFlowFileCodec flowFileCodec, final PeerChannel peerChannel,
                             final int timeoutMillis) {
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
    }

    public RegisteredPartition getPartition() {
        return partition;
    }

    public synchronized int getDesiredReadinessFlag() {
        switch (phase) {
            case SEND_CONNECTION_ID:
            case GET_NEXT_FLOWFILE:
            case SEND_FLOWFILE_DEFINITION:
            case SEND_FLOWFILE_CONTENTS:
            case SEND_CHECKSUM:
            case SEND_TRANSACTION_COMPLETE:
                return SelectionKey.OP_WRITE;
            default:
                return SelectionKey.OP_READ;
        }
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

        if (preparedFrame != null && preparedFrame.hasRemaining()) {
            logger.trace("Current Frame is already available. Will continue writing current frame to channel");
            final int bytesWritten = channel.write(preparedFrame);
            return bytesWritten > 0;
        }

        try {
            switch (phase) {
                case VERIFY_CHECKSUM:
                    return verifyChecksum();
                case CONFIRM_TRANSACTION_COMPLETE:
                    return confirmTransactionComplete();
            }


            final ByteBuffer dataFrame = getDataFrame();
            if (dataFrame == null) {
                return false;
            }

            preparedFrame = channel.prepareForWrite(dataFrame);

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
        logger.info("Successfully completed Transaction to send {} FlowFiles to Peer {} for Connection {}", flowFilesSent.size(), peerDescription, connectionId);

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
            case SEND_CONNECTION_ID:
                return getConnectionId();
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
        // TODO: This method is rather inefficient, copying lots of byte[]. Can do better. But keeping it simple for
        // now to get this working. Revisit with optimizations later.
        if (flowFileInputStream == null) {
            flowFileInputStream = flowFileContentAccess.read(currentFlowFile);
        }

        final int bytesRead = StreamUtils.fillBuffer(flowFileInputStream, byteBuffer,false);
        if (bytesRead < 1) {
            // If no data available, close the stream and move on to the next phase, returning a NO_DATA_FRAME buffer.
            flowFileInputStream.close();
            flowFileInputStream = null;
            phase = TransactionPhase.GET_NEXT_FLOWFILE;

            final ByteBuffer buffer = ByteBuffer.allocate(1);
            buffer.put((byte) LoadBalanceProtocolConstants.NO_DATA_FRAME);
            buffer.rewind();

            checksum.update(LoadBalanceProtocolConstants.NO_DATA_FRAME);
            return buffer;
        }

        logger.trace("Sending Data Frame that is {} bytes long to Peer {}", bytesRead, peerDescription);
        final ByteBuffer buffer = ByteBuffer.allocate(3 + bytesRead);
        buffer.put((byte) LoadBalanceProtocolConstants.DATA_FRAME_FOLLOWS);
        buffer.putShort((short) bytesRead);
        buffer.put(byteBuffer, 0, bytesRead);

        final byte[] frameArray = buffer.array();
        checksum.update(frameArray, 0, frameArray.length);

        phase = TransactionPhase.SEND_FLOWFILE_CONTENTS;
        buffer.rewind();
        return buffer;
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

        final byte[] flowFileEncoded;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            flowFileCodec.encode(currentFlowFile, baos);
            flowFileEncoded = baos.toByteArray();
        }

        final ByteBuffer buffer = ByteBuffer.allocate(flowFileEncoded.length + 1);
        buffer.put((byte) LoadBalanceProtocolConstants.MORE_FLOWFILES);
        checksum.update(LoadBalanceProtocolConstants.MORE_FLOWFILES);

        buffer.put(flowFileEncoded);
        checksum.update(flowFileEncoded, 0, flowFileEncoded.length);

        phase = TransactionPhase.SEND_FLOWFILE_DEFINITION;
        buffer.rewind();
        return buffer;
    }


    private ByteBuffer noMoreFlowFiles() {
        final ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put((byte) LoadBalanceProtocolConstants.NO_MORE_FLOWFILES);
        buffer.rewind();

        checksum.update(LoadBalanceProtocolConstants.NO_MORE_FLOWFILES);
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

        phase = TransactionPhase.GET_NEXT_FLOWFILE;
        return buffer;
    }

    private enum TransactionPhase {
        SEND_CONNECTION_ID,

        SEND_FLOWFILE_DEFINITION,

        SEND_FLOWFILE_CONTENTS,

        GET_NEXT_FLOWFILE,

        SEND_CHECKSUM,

        VERIFY_CHECKSUM,

        SEND_TRANSACTION_COMPLETE,

        CONFIRM_TRANSACTION_COMPLETE;
    }
}
