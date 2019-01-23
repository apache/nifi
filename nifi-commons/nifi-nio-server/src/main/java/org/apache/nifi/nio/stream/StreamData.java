package org.apache.nifi.nio.stream;

import org.apache.nifi.nio.MessageAction;
import org.apache.nifi.nio.MessageSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class StreamData implements MessageAction.Both {

    private static final Logger LOG = LoggerFactory.getLogger(StreamData.class);
    private final StreamToChannel writeAction;
    private final ChannelToStream readAction;
    private final Runnable onStreamReady;

    public StreamData(ChannelToStream readAction, StreamToChannel writeAction, Runnable onStreamReady) {
        this.writeAction = writeAction;
        this.readAction = readAction;
        this.onStreamReady = onStreamReady;
    }

    @Override
    public boolean isBufferReady(ByteBuffer readBuffer, ByteBuffer writeBuffer) {
        return true;
    }

    @Override
    public boolean execute(ByteBuffer readBuffer, ByteBuffer writeBuffer) throws IOException {
        final boolean doneRead = readAction.execute(readBuffer);
        final boolean doneWrite = writeAction.execute(writeBuffer);
        return doneWrite && doneRead;
    }

    @Override
    public void prepare(MessageSequence sequence) {
        writeAction.prepare(sequence);
        readAction.prepare(sequence);
        onStreamReady.run();
    }

    @Override
    public void clear() {
        writeAction.clear();
        readAction.clear();
    }

    public boolean isDone() {
        return writeAction.isDone() && readAction.isDone();
    }

    @Override
    public boolean hasRemainingReadData() {
        return readAction.hasRemainingReadData();
    }

    public void terminate() {
        readAction.terminate();
    }
}
