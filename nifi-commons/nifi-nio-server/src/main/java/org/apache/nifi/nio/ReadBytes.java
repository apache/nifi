package org.apache.nifi.nio;

import java.nio.ByteBuffer;

public class ReadBytes extends ReadData<byte[]> {

    public ReadBytes(int length, IOConsumer<byte[]> consumer) {
        super(length, consumer);
    }

    @Override
    protected byte[] fromBuffer(ByteBuffer buffer) {
        final byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }
}
