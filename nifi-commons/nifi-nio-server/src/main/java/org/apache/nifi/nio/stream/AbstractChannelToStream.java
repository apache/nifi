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
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractChannelToStream implements MessageAction.Read, ReadAhead {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final long timeoutMillis;
    @AutoCleared
    private volatile boolean terminated = false;
    @AutoCleared
    private long totalPiped = 0;
    @AutoCleared
    private long totalStreamed = 0;
    @AutoCleared
    private int rewindLength = 0;

    @AutoCleared
    private byte[] backfillBytes;
    @AutoCleared
    private boolean backfillCaptured;

    @AutoCleared
    private long startedTimestamp;

    private final BufferedInputStream stream;

    private class BufferedInputStream extends InputStream {
        private final ByteBuffer readBuffer;
        private final ReentrantLock readBufferLock = new ReentrantLock(false);

        private BufferedInputStream(int bufferSize) {
            readBuffer = ByteBuffer.allocate(bufferSize);
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            if (isDone()) {
                return -1;
            }

            if (b == null) {
                throw new NullPointerException();
            } else if (off < 0 || len < 0 || len > b.length - off) {
                throw new IndexOutOfBoundsException();
            } else if (len == 0) {
                return 0;
            }

            readBufferLock.lock();
            try {
                final int bytesToRead = Math.min(len, readBuffer.position());
                readBuffer.flip();
                readBuffer.get(b, off, bytesToRead);
                ByteBufferUtil.adjustBufferForNextFill(readBuffer);
                totalStreamed += bytesToRead;
                log.trace("Streamed {} bytes, {}", bytesToRead, readBuffer);
                return bytesToRead;
            } finally {
                readBufferLock.unlock();
            }
        }

        @Override
        public int read() throws IOException {
            if (isDone()) {
                return -1;
            }

            final long started = System.currentTimeMillis();
            while (readBuffer.position() == 0) {
                try {
                    Thread.sleep(1);
                    log.trace("waiting for bytes to read {}", readBuffer);
                } catch (InterruptedException e) {
                    throw new IOException("Interrupted while waiting for buffer gets readable.");
                }
                if (isDone()) {
                    return -1;
                }
                if ((System.currentTimeMillis() - started) > timeoutMillis) {
                    throw new IOException("Timed out for reading data from the channel.");
                }
            }

            readBufferLock.lock();
            try {
                readBuffer.flip();
                final byte b = readBuffer.get();
                totalStreamed++;
                ByteBufferUtil.adjustBufferForNextFill(readBuffer);
                return b;
            } finally {
                readBufferLock.unlock();
            }
        }

        private int pipeFrom(ByteBuffer buffer) throws IOException {
            readBufferLock.lock();
            try {
                int piped = 0;
                while (buffer.hasRemaining() && readBuffer.hasRemaining() && !isDone()) {
                    readBuffer.put(buffer.get());
                    piped++;
                }
                return piped;
            } finally {
                readBufferLock.unlock();
            }
        }

        @Override
        public int available() throws IOException {
            if (isDone()) {
                return 0;
            }
            return readBuffer.position();
        }

        /**
         * <p>Captures the remaining bytes in the read buffer
         * that has been read from the channel, but not streamed yet.
         * Such bytes need to be processed at the next action.</p>
         */
        private void captureBackFill() {
            readBufferLock.lock();
            try {
                rewindLength = readBuffer.position();
                if (rewindLength > 0) {
                    backfillBytes = new byte[rewindLength];

                    readBuffer.flip();
                    readBuffer.get(backfillBytes, 0, rewindLength);
                    ByteBufferUtil.adjustBufferForNextFill(readBuffer);
                    log.debug("Captured back-fill {} bytes", backfillBytes);
                }

            } finally {
                readBufferLock.unlock();
            }

        }
    }



    public AbstractChannelToStream(int bufferSize, long timeoutMillis) {
        this.stream = new BufferedInputStream(bufferSize);
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public boolean isBufferReady(ByteBuffer buffer) {
        // If there's a data in the channel, or already finished piping but waiting for the consumer to finish
        return buffer.remaining() > 0 || (shouldStopReadingFromChannel() && !isDone());
    }

    @Override
    public void prepare(MessageSequence sequence) {
        startedTimestamp = System.currentTimeMillis();
    }

    public boolean execute(final ByteBuffer buffer) throws IOException {

        if (isDone()) {
            // execute() method may be called multiple times, but capturing bak-fill should be done only once.
            if (!backfillCaptured) {
                backfillCaptured = true;
                stream.captureBackFill();
                log.debug("Consuming stream data completed. Streamed {} / {} bytes. rewindLength={}, stream.readBuffer={}", totalStreamed, totalPiped, rewindLength, stream.readBuffer);
            }

            return true;
        }

        if (shouldStopReadingFromChannel()) {
            log.trace("Streamed {} / {} bytes so far. Already stopped streaming, and waiting for those to be consumed.",
                totalStreamed, totalPiped);
            return false;
        }

        final int piped = stream.pipeFrom(buffer);
        totalPiped += piped;

        if (piped > 0) {
            log.debug("Piped {} bytes, streamed {} / {} bytes in total so far", piped, totalStreamed, totalPiped);
        }

        log.trace("Streamed {} / {} bytes so far. Waiting for another I/O cycle to get more.", totalStreamed, totalPiped);
        return false;
    }

    public InputStream getInputStream() {
        return stream;
    }

    public boolean isDone() {
        if (terminated) {
            throw new RuntimeException("Streaming data is already terminated.");
        }
        return isStreamingDone();
    }

    protected abstract boolean shouldStopReadingFromChannel();

    protected abstract boolean isStreamingDone();

    protected long getTotalPiped() {
        return totalPiped;
    }

    protected long getTotalStreamed() {
        return totalStreamed;
    }

    @Override
    public int getRewindLength() {
        return rewindLength;
    }

    @Override
    public void rewind(ByteBuffer buffer) {

        if (backfillBytes == null) {
            return;
        }

        // TODO: the buffer may not have enough space. in that case, need to handle partial write.
        buffer.put(backfillBytes);
    }

    @Override
    public boolean hasRemainingReadData() {
        return (totalPiped - totalStreamed) > 0;
    }

    public void terminate() {
        log.debug("Terminating...");
        terminated = true;
    }

}
