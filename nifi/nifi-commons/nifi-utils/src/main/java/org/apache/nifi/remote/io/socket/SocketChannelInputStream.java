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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

public class SocketChannelInputStream extends InputStream {

    private static final long CHANNEL_EMPTY_WAIT_NANOS = TimeUnit.NANOSECONDS.convert(10, TimeUnit.MILLISECONDS);
    private final SocketChannel channel;
    private volatile int timeoutMillis = 30000;

    private final ByteBuffer oneByteBuffer = ByteBuffer.allocate(1);
    private Byte bufferedByte = null;

    public SocketChannelInputStream(final SocketChannel socketChannel) throws IOException {
        // this class expects a non-blocking channel
        socketChannel.configureBlocking(false);
        this.channel = socketChannel;
    }

    public void setTimeout(final int timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }
    
    public void consume() throws IOException {
        channel.shutdownInput();
        
        final byte[] b = new byte[4096];
        final ByteBuffer buffer = ByteBuffer.wrap(b);
        int bytesRead;
        do {
            bytesRead = channel.read(buffer);
            buffer.flip();
        } while ( bytesRead > 0 );
    }

    @Override
    public int read() throws IOException {
        if (bufferedByte != null) {
            final int retVal = bufferedByte & 0xFF;
            bufferedByte = null;
            return retVal;
        }

        oneByteBuffer.flip();
        oneByteBuffer.clear();

        final long maxTime = System.currentTimeMillis() + timeoutMillis;
        int bytesRead;
        do {
            bytesRead = channel.read(oneByteBuffer);
            if (bytesRead == 0) {
                if (System.currentTimeMillis() > maxTime) {
                    throw new SocketTimeoutException("Timed out reading from socket");
                }
                try {
                    TimeUnit.NANOSECONDS.sleep(CHANNEL_EMPTY_WAIT_NANOS);
                } catch (InterruptedException e) {
                    close();
                    Thread.currentThread().interrupt(); // set the interrupt status
                    throw new ClosedByInterruptException(); // simulate an interrupted blocked read operation
                }
            }
        } while (bytesRead == 0);

        if (bytesRead == -1) {
            return -1;
        }
        oneByteBuffer.flip();
        return oneByteBuffer.get() & 0xFF;
    }

    @Override
    public int read(final byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        if (bufferedByte != null) {
            final byte retVal = bufferedByte;
            bufferedByte = null;
            b[off] = retVal;
            return 1;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(b, off, len);

        final long maxTime = System.currentTimeMillis() + timeoutMillis;
        int bytesRead;
        do {
            bytesRead = channel.read(buffer);
            if (bytesRead == 0) {
                if (System.currentTimeMillis() > maxTime) {
                    throw new SocketTimeoutException("Timed out reading from socket");
                }
                try {
                    TimeUnit.NANOSECONDS.sleep(CHANNEL_EMPTY_WAIT_NANOS);
                } catch (InterruptedException e) {
                    close();
                    Thread.currentThread().interrupt(); // set the interrupt status
                    throw new ClosedByInterruptException(); // simulate an interrupted blocked read operation
                }
            }
        } while (bytesRead == 0);

        return bytesRead;
    }

    @Override
    public int available() throws IOException {
        if (bufferedByte != null) {
            return 1;
        }

        isDataAvailable(); // attempt to read from socket
        return (bufferedByte == null) ? 0 : 1;
    }

    public boolean isDataAvailable() throws IOException {
        if (bufferedByte != null) {
            return true;
        }

        oneByteBuffer.flip();
        oneByteBuffer.clear();
        final int bytesRead = channel.read(oneByteBuffer);
        if (bytesRead == -1) {
            throw new EOFException("Peer has closed the stream");
        }
        if (bytesRead > 0) {
            oneByteBuffer.flip();
            bufferedByte = oneByteBuffer.get();
            return true;
        }
        return false;
    }

    /**
     * Closes the underlying socket channel.
     * @throws java.io.IOException
     */
    @Override
    public void close() throws IOException {
        channel.close();
    }
}
