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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class LeakyBucketStreamThrottler implements StreamThrottler {

    private final int maxBytesPerSecond;
    private final BlockingQueue<Request> requestQueue = new LinkedBlockingQueue<Request>();
    private final ScheduledExecutorService executorService;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    public LeakyBucketStreamThrottler(final int maxBytesPerSecond) {
        this.maxBytesPerSecond = maxBytesPerSecond;

        executorService = Executors.newSingleThreadScheduledExecutor();
        final Runnable task = new Drain();
        executorService.scheduleAtFixedRate(task, 0, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        this.shutdown.set(true);

        executorService.shutdown();
        try {
            // Should not take more than 2 seconds because we run every second. If it takes more than
            // 2 seconds, it is because the Runnable thread is blocking on a write; in this case,
            // we will just ignore it and return
            executorService.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }
    }

    @Override
    public OutputStream newThrottledOutputStream(final OutputStream toWrap) {
        return new OutputStream() {
            @Override
            public void write(final int b) throws IOException {
                write(new byte[]{(byte) b}, 0, 1);
            }

            @Override
            public void write(byte[] b) throws IOException {
                write(b, 0, b.length);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                final InputStream in = new ByteArrayInputStream(b, off, len);
                LeakyBucketStreamThrottler.this.copy(in, toWrap);
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
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();

            @Override
            public int read() throws IOException {
                final ByteArrayOutputStream baos = new ByteArrayOutputStream(1);
                LeakyBucketStreamThrottler.this.copy(toWrap, baos, 1L);
                if (baos.size() < 1) {
                    return -1;
                }

                return baos.toByteArray()[0] & 0xFF;
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

                baos.reset();
                final int copied = (int) LeakyBucketStreamThrottler.this.copy(toWrap, baos, len);
                if (copied == 0) {
                    return -1;
                }
                System.arraycopy(baos.toByteArray(), 0, b, off, copied);
                return copied;
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

    @Override
    public long copy(final InputStream in, final OutputStream out) throws IOException {
        return copy(in, out, -1);
    }

    @Override
    public long copy(final InputStream in, final OutputStream out, final long maxBytes) throws IOException {
        long totalBytesCopied = 0;
        boolean finished = false;
        while (!finished) {
            final long requestMax = (maxBytes < 0) ? Long.MAX_VALUE : maxBytes - totalBytesCopied;
            final Request request = new Request(in, out, requestMax);
            boolean transferred = false;
            while (!transferred) {
                if (shutdown.get()) {
                    throw new IOException("Throttler shutdown");
                }

                try {
                    transferred = requestQueue.offer(request, 1000, TimeUnit.MILLISECONDS);
                } catch (final InterruptedException e) {
                    throw new IOException("Interrupted", e);
                }
            }

            final BlockingQueue<Response> responseQueue = request.getResponseQueue();
            Response response = null;
            while (response == null) {
                try {
                    if (shutdown.get()) {
                        throw new IOException("Throttler shutdown");
                    }
                    response = responseQueue.poll(1000L, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    throw new IOException("Interrupted", e);
                }
            }

            if (!response.isSuccess()) {
                throw response.getError();
            }

            totalBytesCopied += response.getBytesCopied();
            finished = (response.getBytesCopied() == 0) || (totalBytesCopied >= maxBytes && maxBytes > 0);
        }

        return totalBytesCopied;
    }

    /**
     * This class is responsible for draining water from the leaky bucket. I.e., it actually moves the data
     */
    private class Drain implements Runnable {

        private final byte[] buffer;

        public Drain() {
            final int bufferSize = Math.min(4096, maxBytesPerSecond);
            buffer = new byte[bufferSize];
        }

        @Override
        public void run() {
            final long start = System.currentTimeMillis();

            int bytesTransferred = 0;
            while (bytesTransferred < maxBytesPerSecond) {
                final long maxMillisToWait = 1000 - (System.currentTimeMillis() - start);
                if (maxMillisToWait < 1) {
                    return;
                }

                try {
                    final Request request = requestQueue.poll(maxMillisToWait, TimeUnit.MILLISECONDS);
                    if (request == null) {
                        return;
                    }

                    final BlockingQueue<Response> responseQueue = request.getResponseQueue();

                    final OutputStream out = request.getOutputStream();
                    final InputStream in = request.getInputStream();

                    try {
                        final long requestMax = request.getMaxBytesToCopy();
                        long maxBytesToTransfer;
                        if (requestMax < 0) {
                            maxBytesToTransfer = Math.min(buffer.length, maxBytesPerSecond - bytesTransferred);
                        } else {
                            maxBytesToTransfer = Math.min(requestMax,
                                    Math.min(buffer.length, maxBytesPerSecond - bytesTransferred));
                        }
                        maxBytesToTransfer = Math.max(1L, maxBytesToTransfer);

                        final int bytesCopied = fillBuffer(in, maxBytesToTransfer);
                        out.write(buffer, 0, bytesCopied);

                        final Response response = new Response(true, bytesCopied);
                        responseQueue.put(response);
                        bytesTransferred += bytesCopied;
                    } catch (final IOException e) {
                        final Response response = new Response(e);
                        responseQueue.put(response);
                    }
                } catch (InterruptedException e) {
                }
            }
        }

        private int fillBuffer(final InputStream in, final long maxBytes) throws IOException {
            int bytesRead = 0;
            int len;
            while (bytesRead < maxBytes && (len = in.read(buffer, bytesRead, (int) Math.min(maxBytes - bytesRead, buffer.length - bytesRead))) > 0) {
                bytesRead += len;
            }

            return bytesRead;
        }
    }

    private static class Response {

        private final boolean success;
        private final IOException error;
        private final int bytesCopied;

        public Response(final boolean success, final int bytesCopied) {
            this.success = success;
            this.bytesCopied = bytesCopied;
            this.error = null;
        }

        public Response(final IOException error) {
            this.success = false;
            this.error = error;
            this.bytesCopied = -1;
        }

        public boolean isSuccess() {
            return success;
        }

        public IOException getError() {
            return error;
        }

        public int getBytesCopied() {
            return bytesCopied;
        }
    }

    private static class Request {

        private final OutputStream out;
        private final InputStream in;
        private final long maxBytesToCopy;
        private final BlockingQueue<Response> responseQueue;

        public Request(final InputStream in, final OutputStream out, final long maxBytesToCopy) {
            this.out = out;
            this.in = in;
            this.maxBytesToCopy = maxBytesToCopy;
            this.responseQueue = new LinkedBlockingQueue<Response>(1);
        }

        public BlockingQueue<Response> getResponseQueue() {
            return this.responseQueue;
        }

        public OutputStream getOutputStream() {
            return out;
        }

        public InputStream getInputStream() {
            return in;
        }

        public long getMaxBytesToCopy() {
            return maxBytesToCopy;
        }

        @Override
        public String toString() {
            return "Request[maxBytes=" + maxBytesToCopy + "]";
        }
    }

}
