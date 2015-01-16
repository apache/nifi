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
package org.apache.nifi.remote.io.socket;

import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

public class SocketChannelOutputStream extends OutputStream {

    private static final long CHANNEL_FULL_WAIT_NANOS = TimeUnit.NANOSECONDS.convert(10, TimeUnit.MILLISECONDS);
    private final SocketChannel channel;
    private volatile int timeout = 30000;

    private final ByteBuffer oneByteBuffer = ByteBuffer.allocate(1);

    public SocketChannelOutputStream(final SocketChannel socketChannel) throws IOException {
        // this class expects a non-blocking channel
        socketChannel.configureBlocking(false);
        this.channel = socketChannel;
    }

    public void setTimeout(final int timeoutMillis) {
        this.timeout = timeoutMillis;
    }

    @Override
    public void write(final int b) throws IOException {
        oneByteBuffer.flip();
        oneByteBuffer.clear();
        oneByteBuffer.put((byte) b);
        oneByteBuffer.flip();

        final int timeoutMillis = this.timeout;
        long maxTime = System.currentTimeMillis() + timeoutMillis;
        int bytesWritten;
        while (oneByteBuffer.hasRemaining()) {
            bytesWritten = channel.write(oneByteBuffer);
            if (bytesWritten == 0) {
                if (System.currentTimeMillis() > maxTime) {
                    throw new SocketTimeoutException("Timed out writing to socket");
                }
                try {
                    TimeUnit.NANOSECONDS.sleep(CHANNEL_FULL_WAIT_NANOS);
                } catch (InterruptedException e) {
                    close();
                    Thread.currentThread().interrupt(); // set the interrupt status
                    throw new ClosedByInterruptException(); // simulate an interrupted blocked write operation
                }
            } else {
                return;
            }
        }
    }

    @Override
    public void write(final byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        final ByteBuffer buffer = ByteBuffer.wrap(b, off, len);

        final int timeoutMillis = this.timeout;
        long maxTime = System.currentTimeMillis() + timeoutMillis;
        int bytesWritten;
        while (buffer.hasRemaining()) {
            bytesWritten = channel.write(buffer);
            if (bytesWritten == 0) {
                if (System.currentTimeMillis() > maxTime) {
                    throw new SocketTimeoutException("Timed out writing to socket");
                }
                try {
                    TimeUnit.NANOSECONDS.sleep(CHANNEL_FULL_WAIT_NANOS);
                } catch (InterruptedException e) {
                    close();
                    Thread.currentThread().interrupt(); // set the interrupt status
                    throw new ClosedByInterruptException(); // simulate an interrupted blocked write operation
                }
            } else {
                maxTime = System.currentTimeMillis() + timeoutMillis;
            }
        }
    }

    /**
     * Closes the underlying SocketChannel
     * @throws java.io.IOException
     */
    @Override
    public void close() throws IOException {
        channel.close();
    }
}
