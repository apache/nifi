package org.apache.nifi.nio;

import java.nio.ByteBuffer;

public class ByteBufferUtil {

    /**
     * Adjust the buffer for next fill operation.
     * If the buffer has some remaining bytes, compact it. Otherwise, clear it.
     * @param buffer right after a buffer consumption operation is done
     */
    public static void adjustBufferForNextFill(ByteBuffer buffer) {
        if (buffer.hasRemaining()) {
            // Move markers so that more data can be filled after the remaining bytes to process.
            buffer.compact();
        } else {
            buffer.clear();
        }
    }
}
