package org.apache.nifi.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

public class WriteBytes implements MessageAction.Write {

    private static final Logger LOG = LoggerFactory.getLogger(WriteBytes.class);

    private final Supplier<byte[]> supplier;
    private final IORunnable written;

    private byte[] bytes;
    private int offset = 0;

    public WriteBytes(Supplier<byte[]> supplier, IORunnable written) {
        this.supplier = supplier;
        this.written = written;
    }

    @Override
    public boolean isBufferReady(ByteBuffer buffer) {
        return buffer.hasRemaining();
    }

    @Override
    public void prepare(MessageSequence sequence) {
        bytes = supplier.get();
        offset = 0;
    }

    @Override
    public void clear() {
        bytes = null;
    }

    public boolean execute(final ByteBuffer buffer) throws IOException {
        final int bytesToWrite = Math.min(buffer.remaining(), bytes.length - offset);
        buffer.put(bytes, offset, bytesToWrite);
        offset += bytesToWrite;

        if (bytes.length - offset == 0) {
            LOG.debug("Calling written.");
            written.run();
            return true;
        }

        return false;
    }
}
