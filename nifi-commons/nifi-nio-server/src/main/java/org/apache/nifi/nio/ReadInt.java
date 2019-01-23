package org.apache.nifi.nio;

import java.nio.ByteBuffer;

public class ReadInt extends ReadData<Integer> {

    public ReadInt(IOConsumer<Integer> consumer) {
        super(Integer.BYTES, consumer);
    }

    @Override
    protected Integer fromBuffer(ByteBuffer buffer) {
        return buffer.getInt();
    }
}
