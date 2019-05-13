/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.stream.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class LeakyBucketStreamThrottler implements StreamThrottler {

    private volatile long bytesRemaining = 0;
    private volatile boolean shutdown = false;
    private final ReentrantLock synch = new ReentrantLock(true);
    private final Condition condition = synch.newCondition();
    private final ScheduledExecutorService executorService;
    private final long idleTimeoutMillis;

    public LeakyBucketStreamThrottler(final long maxBytesPerSecond, final long idleTimeoutMillis) {
        // if idleTimout is set to 0, that means there is no idle time limit.
        this.idleTimeoutMillis = idleTimeoutMillis == 0 ? Long.MAX_VALUE : idleTimeoutMillis;
        executorService = Executors.newSingleThreadScheduledExecutor((r) -> {
            Thread thread = new Thread(r);
            thread.setName("LeakyBucketStreamThrottler Reset Timer");
            thread.setDaemon(true);
            return thread;
        });
        executorService.scheduleAtFixedRate(() -> {
            while (!synch.tryLock()) { // this form of tryLock allows barging in on threads waiting for the lock
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    // shutting down...go ahead and exit
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            try {
                bytesRemaining = maxBytesPerSecond;
                // this wakes up the longest waiting thread...which in turn will wake up the next one. Since the bytes have already been read
                // off the socket, the execution time for each thread is nominal.
                condition.signal();
            } finally {
                synch.unlock();
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);
    }

    public void add(final int numBytes) throws IOException {
        if (numBytes < 1)
            return;
        try {
            if (synch.tryLock(idleTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    long bytesTransferred = Math.min(numBytes, bytesRemaining);
                    long bytesToTransfer = numBytes - bytesTransferred;
                    while (bytesToTransfer > 0) {
                        if (shutdown) {
                            throw new IOException("LeakyBucketStreamThrottler is closed");
                        }
                        // since bytesRemaining was less than numBytes, or bytesToTransfer, the limit has been hit so set bytesRemaining to 0
                        bytesRemaining = 0;
                        // let the InterruptedException fly
                        if (!condition.await(idleTimeoutMillis, TimeUnit.MILLISECONDS)) {
                            throw new IOException("Cannot service I/O request within timeout period of " + idleTimeoutMillis + " ms. Either "
                                + "increase idle timeout, or reduce the number of threads using this throttler");
                        }
                        bytesTransferred = Math.min(bytesToTransfer, bytesRemaining);
                        bytesToTransfer -= bytesTransferred;
                        if (bytesToTransfer == 0) {
                            bytesRemaining -= bytesTransferred;
                            condition.signal();
                            return;
                        }
                        // don't want to signal since there are no bytesRemaining
                    }
                    bytesRemaining -= numBytes;
                    // now notify the next thread
                    condition.signal();
                } finally {
                    synch.unlock();
                }
            } else {
                throw new IOException(
                    "Cannot service I/O request within timeout period of " + idleTimeoutMillis + " ms. Either " + "increase idle timeout, or reduce the number of threads using this throttler");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("LeakyBucketStreamThrottler is closed");
        }
    }

    @Override
    public void close() {
        this.shutdown = true;

        executorService.shutdownNow();
        try {
            // Should not take more than a second because we run every second.
            executorService.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }
    }

    @Override
    public OutputStream newThrottledOutputStream(final OutputStream toWrap) {
        return new OutputStream() {
            @Override
            public void write(final int b) throws IOException {
                write(new byte[] { (byte) b }, 0, 1);
            }

            @Override
            public void write(byte[] b) throws IOException {
                write(b, 0, b.length);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                LeakyBucketStreamThrottler.this.add(len);
                toWrap.write(b, off, len);
            }

            @Override
            public void close() throws IOException {
                toWrap.close();
            }

            @Override
            public void flush() throws IOException {
                toWrap.flush();
            }
        };
    }

    @Override
    public InputStream newThrottledInputStream(final InputStream toWrap) {
        return new InputStream() {

            @Override
            public int read() throws IOException {
                final byte[] buffer = new byte[1];
                if (-1 == toWrap.read(buffer)) {
                    return -1;
                }
                LeakyBucketStreamThrottler.this.add(1);
                return buffer[0] & 0xFF;
            }

            @Override
            public int read(final byte[] b) throws IOException {
                if (b.length == 0) {
                    return 0;
                }
                return read(b, 0, b.length);
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                if (len < 0) {
                    throw new IllegalArgumentException();
                }
                if (len == 0) {
                    return 0;
                }
                final int bytesRead = toWrap.read(b, off, len);
                LeakyBucketStreamThrottler.this.add(bytesRead);
                return bytesRead;
            }

            @Override
            public void close() throws IOException {
                toWrap.close();
            }

            @Override
            public int available() throws IOException {
                return toWrap.available();
            }
        };
    }

}
