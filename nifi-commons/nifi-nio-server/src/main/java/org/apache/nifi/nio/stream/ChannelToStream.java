package org.apache.nifi.nio.stream;

import org.apache.nifi.nio.AutoCleared;
import org.apache.nifi.nio.ByteBufferUtil;
import org.apache.nifi.nio.MessageAction;
import org.apache.nifi.nio.MessageSequence;
import org.apache.nifi.nio.ReadAhead;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class ChannelToStream extends AbstractChannelToStream {

    @AutoCleared
    private volatile boolean done = false;

    public ChannelToStream(int bufferSize, long timeoutMillis) {
        super(bufferSize, timeoutMillis);
    }

    @Override
    protected boolean shouldStopReadingFromChannel() {
        return done;
    }

    @Override
    protected boolean isStreamingDone() {
        return done;
    }

    public void complete() {
        done = true;
    }

}
