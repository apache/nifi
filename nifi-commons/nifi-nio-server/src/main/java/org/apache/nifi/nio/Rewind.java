package org.apache.nifi.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Rewind implements MessageAction.Read {

    private static final Logger LOG = LoggerFactory.getLogger(Rewind.class);
    private final ReadAhead readAhead;

    private boolean done;

    public Rewind(ReadAhead readAhead) {
        this.readAhead = readAhead;
    }

    @Override
    public void clear() {
        done = false;
    }

    @Override
    public boolean isBufferReady(ByteBuffer buffer) {
        return true;
    }

    @Override
    public boolean execute(ByteBuffer buffer) throws IOException {
        final int rewindLength = readAhead.getRewindLength();
        if (rewindLength > 0) {
            // Prepare for back-fill.
            ByteBufferUtil.adjustBufferForNextFill(buffer);
            LOG.debug("Rewinding {} bytes, before buffer={}", rewindLength, buffer);

            // If there's already some bytes in the buffer,
            // need to insert the back-fill at the beginning.
            byte[] existingBytes = null;
            if (buffer.position() > 0) {
                LOG.debug("Copying {} existing bytes. {}", buffer.position(), buffer);
                existingBytes = new byte[buffer.position()];
                buffer.flip();
                buffer.get(existingBytes);
                buffer.clear();
                LOG.debug("Cleared the buffer {}", buffer);
            }

            readAhead.rewind(buffer);

            if (existingBytes != null) {
                buffer.put(existingBytes);
                LOG.debug("Put the existing bytes back. {}", buffer);
            }

            // Flip to consumption mode.
            buffer.flip();
            LOG.debug("Rewound {} bytes, after buffer={}", rewindLength, buffer);
        }
        done = true;
        return true;
    }

    @Override
    public boolean hasRemainingReadData() {
        return readAhead.getRewindLength() > 0;
    }

    public boolean isDone() {
        return done;
    }
}
