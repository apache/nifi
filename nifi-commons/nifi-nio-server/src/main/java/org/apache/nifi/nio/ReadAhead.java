package org.apache.nifi.nio;

import java.nio.ByteBuffer;

public interface ReadAhead {
    default int getRewindLength() {
        return 0;
    }

    void rewind(ByteBuffer buffer);
}
