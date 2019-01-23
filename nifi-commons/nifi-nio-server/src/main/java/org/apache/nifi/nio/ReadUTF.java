package org.apache.nifi.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ReadUTF implements MessageAction.Read {

    private static final Logger LOG = LoggerFactory.getLogger(ReadUTF.class);

    private final IOConsumer<String> consumer;

    private short utfLength = -1;
    private byte[] utfBytes;
    private int utfOffset = 0;
    private int utfRemaining = 0;

    public ReadUTF(IOConsumer<String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public boolean isBufferReady(ByteBuffer buffer) {
        final boolean ready;
        if (utfLength < 0) {
            // At least need 2 bytes to read the short.
            ready = buffer.remaining() > 1;
        } else {
            ready = buffer.hasRemaining();
        }
        return ready;
    }

    @Override
    public void clear() {
        utfLength = -1;
        utfBytes = null;
        utfOffset = 0;
        utfRemaining = 0;
    }

    @Override
    public boolean execute(ByteBuffer buffer) throws IOException {
        if (utfLength < 0) {
            utfLength = buffer.getShort();
            utfBytes = new byte[utfLength];
            utfOffset = 0;
            utfRemaining = utfLength;
            LOG.trace("Initialized, utfLength={}, utfOffset={}, utfRemaining={}, buffer={}", utfLength, utfOffset, utfRemaining, buffer);
        }

        final int bytesToRead = Math.min(buffer.remaining(), utfRemaining);
        buffer.get(utfBytes, utfOffset, bytesToRead);

        utfOffset += bytesToRead;
        utfRemaining -= bytesToRead;

        LOG.trace("read {} bytes, utfLength={}, utfOffset={}, utfRemaining={}, buffer={}", bytesToRead, utfLength, utfOffset, utfRemaining, buffer);

        if (utfRemaining == 0) {
            String value = new String(utfBytes, StandardCharsets.UTF_8);
            consumer.accept(value);
            return true;
        }

        return false;
    }

}
