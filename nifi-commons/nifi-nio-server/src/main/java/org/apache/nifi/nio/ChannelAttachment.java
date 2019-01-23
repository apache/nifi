package org.apache.nifi.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import java.nio.ByteBuffer;

class ChannelAttachment<T extends MessageSequence> {
    private static final Logger LOG = LoggerFactory.getLogger(ChannelAttachment.class);

    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

    private ByteBuffer appReadBuffer;
    private ByteBuffer appWriteBuffer;

    private ByteBuffer netReadBuffer;
    private ByteBuffer netWriteBuffer;

    private final MessageSequence sequence;
    private final SSLEngine sslEngine;

    private MessageAction previousAction;

    private volatile long lastCommunicationTimestamp;

    ChannelAttachment(T sequence, SSLEngine sslEngine) {
        this.sequence = sequence;
        this.sslEngine = sslEngine;

        appReadBuffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        appWriteBuffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);

        if (sslEngine != null) {
            final int packetBufferSize = sslEngine.getSession().getPacketBufferSize();
            netReadBuffer = ByteBuffer.allocate(packetBufferSize);
            netWriteBuffer = ByteBuffer.allocate(packetBufferSize);

        } else {
            netReadBuffer = appReadBuffer;
            netWriteBuffer = appWriteBuffer;
        }
    }

    /**
     * @return The buffer for application to read data sent by the peer.
     */
    ByteBuffer getAppReadBuffer() {
        return appReadBuffer;
    }

    /**
     * @return The buffer for application to write data to sent to the peer.
     */
    ByteBuffer getAppWriteBuffer() {
        return appWriteBuffer;
    }

    ByteBuffer getNetReadBuffer() {
        return netReadBuffer;
    }

    ByteBuffer getNetWriteBuffer() {
        return netWriteBuffer;
    }

    SSLEngine getSslEngine() {
        return sslEngine;
    }

    MessageSequence getSequence() {
        return sequence;
    }

    void prepareAction(MessageAction action) {
        if (previousAction == null || !previousAction.equals(action)) {
            LOG.debug("Calling prepare for {} of {}", action.getClass().getSimpleName(), sequence);
            action.prepare(sequence);
        }
        previousAction = action;
    }

    void onSequenceSuccess() {
        previousAction = null;
    }

    public long getLastCommunicationTimestamp() {
        return lastCommunicationTimestamp;
    }

    public void setLastCommunicationTimestamp(long lastCommunicationTimestamp) {
        this.lastCommunicationTimestamp = lastCommunicationTimestamp;
    }
}
