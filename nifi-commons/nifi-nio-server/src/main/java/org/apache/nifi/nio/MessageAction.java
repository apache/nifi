package org.apache.nifi.nio;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface MessageAction extends Clearable {

    default void prepare(final MessageSequence sequence) {
    }

    interface SSLAction extends MessageAction {
        boolean execute(final SSLEngine sslEngine)throws IOException;
    }

    interface ReadFromChannel {
        default boolean hasRemainingReadData() {
            return false;
        }
    }

    interface Read extends MessageAction, ReadFromChannel {
        boolean isBufferReady(final ByteBuffer buffer);
        boolean execute(final ByteBuffer buffer) throws IOException;
    }

    interface Write extends MessageAction {
        boolean isBufferReady(final ByteBuffer buffer);
        boolean execute(final ByteBuffer buffer) throws IOException;
    }

    interface Both extends MessageAction, ReadFromChannel {
        boolean isBufferReady(final ByteBuffer readBuffer, ByteBuffer writeBuffer);
        boolean execute(final ByteBuffer readBuffer, ByteBuffer writeBuffer) throws IOException;
    }
}
