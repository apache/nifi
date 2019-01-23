package org.apache.nifi.nio.stream;

import org.apache.nifi.nio.AutoCleared;
import org.apache.nifi.nio.MessageSequence;

import java.util.function.Function;

public class ChannelToBoundStream<T extends MessageSequence> extends AbstractChannelToStream {

    private final Function<T, Integer> getDataSize;

    @AutoCleared
    private Long dataSize;

    public ChannelToBoundStream(int bufferSize, long timeoutMillis, Function<T, Integer> getDataSize) {
        super(bufferSize, timeoutMillis);
        this.getDataSize = getDataSize;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void prepare(MessageSequence sequence) {
        super.prepare(sequence);
        dataSize = Long.valueOf(getDataSize.apply((T) sequence));
    }

    @Override
    protected boolean shouldStopReadingFromChannel() {
        return dataSize != null && getTotalPiped() == dataSize;
    }

    @Override
    protected boolean isStreamingDone() {
        return dataSize != null && getTotalStreamed() == dataSize;
    }
}
