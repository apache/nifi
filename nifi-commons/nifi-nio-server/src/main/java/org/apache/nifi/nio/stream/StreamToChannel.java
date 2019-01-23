package org.apache.nifi.nio.stream;

import org.apache.nifi.nio.AutoCleared;
import org.apache.nifi.nio.ByteBufferUtil;
import org.apache.nifi.nio.MessageAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

public class StreamToChannel implements MessageAction.Write {


    private static final Logger LOG = LoggerFactory.getLogger(StreamToChannel.class);

    private final BufferedOutputStream stream;

    private class BufferedOutputStream extends OutputStream {

        private final ByteBuffer writeBuffer;
        private final ReentrantLock writeBufferLock = new ReentrantLock(false);

        private BufferedOutputStream(int bufferSize) {
            writeBuffer = ByteBuffer.allocate(bufferSize);
        }

        @Override
        public void write(byte b[], int off, int len) throws IOException {
            if (b == null) {
                throw new NullPointerException();
            } else if ((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) > b.length) || ((off + len) < 0)) {
                throw new IndexOutOfBoundsException();
            } else if (len == 0) {
                return;
            }

            if (noMoreData) {
                throw new IllegalStateException("Stream is already marked as completed.");
            }

            int written = 0;
            long lastWrite = System.currentTimeMillis();
            while (written < len) {
                final int toWrite = Math.min(len - written, writeBuffer.remaining());
                if (toWrite == 0) {
                    if ((System.currentTimeMillis() - lastWrite) > timeoutMillis) {
                        throw new IOException("Timed out for acquiring write buffer space.");
                    }
                    continue;
                }
                writeBufferLock.lock();
                try {
                    writeBuffer.put(b, off + written, toWrite);
                } finally {
                    writeBufferLock.unlock();
                }
                written += toWrite;
                lastWrite = System.currentTimeMillis();
            }
        }

        @Override
        public void write(int b) throws IOException {

            if (noMoreData) {
                throw new IllegalStateException("Stream is already marked as completed.");
            }

            while (!writeBuffer.hasRemaining()) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new IOException("Interrupted while waiting for buffer space.");
                }
            }

            writeBufferLock.lock(); // TODO: trylock instead.
            try {
                writeBuffer.put((byte) b);
            } finally {
                writeBufferLock.unlock();
            }
        }

        private int drainTo(ByteBuffer buffer) {
            writeBufferLock.lock();
            int drained = 0;
            try {
                writeBuffer.flip();
                final boolean shouldLog = writeBuffer.hasRemaining() && buffer.hasRemaining();
                if (shouldLog) {
                    LOG.trace("Start draining {}, {}", writeBuffer, buffer);
                }
                while (writeBuffer.hasRemaining() && buffer.hasRemaining()) {
                    buffer.put(writeBuffer.get());
                    drained++;
                }
                if (shouldLog) {
                    LOG.trace("Stop draining {}, {}", writeBuffer, buffer);
                }
                ByteBufferUtil.adjustBufferForNextFill(writeBuffer);
            } finally {
                writeBufferLock.unlock();
            }
            return drained;
        }
    }

    private final long timeoutMillis;

    @AutoCleared
    private volatile boolean noMoreData = false;
    @AutoCleared
    private long totalPiped;

    // Clear manually at clear()
    private Completion completion;

    public StreamToChannel(int bufferSize, long timeoutMillis) {
        this.stream = new BufferedOutputStream(bufferSize);
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public boolean isBufferReady(ByteBuffer buffer) {
        return buffer.remaining() > 0;
    }

    public boolean execute(ByteBuffer buffer) throws IOException {

        if (isDone()) {
            // If it's already written, do nothing.
            return true;
        }

        final int drained = stream.drainTo(buffer);
        this.totalPiped += drained;

        if (noMoreData && stream.writeBuffer.position() == 0) {
            LOG.debug("All buffered bytes have been drained to the channel");
            completion.done = true;
        }

        return isDone();
    }

    public OutputStream getOutputStream() {
        return stream;
    }

    @Override
    public void clear() {
        MessageAction.Write.super.clear();
        if (completion != null) {
            // Release the other thread waiting for completion.
            completion.latch.countDown();
            completion = null;
        }
    }

    private class Completion implements Future<Boolean> {
        private boolean done;
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return done;
        }

        @Override
        public Boolean get() throws InterruptedException, ExecutionException {
            latch.await();
            return done;
        }

        @Override
        public Boolean get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return latch.await(timeout, unit);
        }
    }

    public Future<Boolean> complete() {
        // No more data to be written.
        LOG.debug("Stop accepting further data. Piped {} bytes in total so far", totalPiped);
        noMoreData = true;

        completion = new Completion();

        return completion;
    }

    public boolean isDone() {
        return completion != null && completion.isDone();
    }

}
