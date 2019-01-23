package org.apache.nifi.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class ReadData<T> implements MessageAction.Read {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final IOConsumer<T> consumer;
    protected final int length;

    public ReadData(int length, IOConsumer<T> consumer) {
        this.consumer = consumer;
        this.length = length;
    }

    @Override
    public boolean isBufferReady(ByteBuffer buffer) {
        return buffer.remaining() >= length;
    }

    public boolean execute(final ByteBuffer buffer) throws IOException {
        consumer.accept(fromBuffer(buffer));
        return true;
    }

    abstract protected T fromBuffer(ByteBuffer buffer);
}
