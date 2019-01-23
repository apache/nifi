package org.apache.nifi.nio;

import java.nio.ByteBuffer;

public class ReadLong extends ReadData<Long> {

    public ReadLong(IOConsumer<Long> consumer) {
        super(Long.BYTES, consumer);
    }

    @Override
    protected Long fromBuffer(ByteBuffer buffer) {
        return buffer.getLong();
    }
}
