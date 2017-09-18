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
package org.apache.nifi.processor.util.put.sender;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.remote.io.socket.SocketChannelOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;

/**
 * Sends messages over a SocketChannel.
 */
public class SocketChannelSender extends ChannelSender {

    protected SocketChannel channel;
    protected SocketChannelOutputStream socketChannelOutput;

    public SocketChannelSender(final String host, final int port, final int maxSendBufferSize, final ComponentLog logger) {
        super(host, port, maxSendBufferSize, logger);
    }

    @Override
    public void open() throws IOException {
        try {
            if (channel == null) {
                channel = SocketChannel.open();
                channel.configureBlocking(false);

                if (maxSendBufferSize > 0) {
                    channel.setOption(StandardSocketOptions.SO_SNDBUF, maxSendBufferSize);
                    final int actualSendBufSize = channel.getOption(StandardSocketOptions.SO_SNDBUF);
                    if (actualSendBufSize < maxSendBufferSize) {
                        logger.warn("Attempted to set Socket Send Buffer Size to " + maxSendBufferSize
                                + " bytes but could only set to " + actualSendBufSize + "bytes. You may want to "
                                + "consider changing the Operating System's maximum send buffer");
                    }
                }
            }

            if (!channel.isConnected()) {
                final long startTime = System.currentTimeMillis();
                final InetSocketAddress socketAddress = new InetSocketAddress(InetAddress.getByName(host), port);

                if (!channel.connect(socketAddress)) {
                    while (!channel.finishConnect()) {
                        if (System.currentTimeMillis() > startTime + timeout) {
                            throw new SocketTimeoutException("Timed out connecting to " + host + ":" + port);
                        }

                        try {
                            Thread.sleep(50L);
                        } catch (final InterruptedException e) {
                        }
                    }
                }

                if (logger.isDebugEnabled()) {
                    final SocketAddress localAddress = channel.getLocalAddress();
                    if (localAddress != null && localAddress instanceof InetSocketAddress) {
                        final InetSocketAddress inetSocketAddress = (InetSocketAddress) localAddress;
                        logger.debug("Connected to local port {}", new Object[] {inetSocketAddress.getPort()});
                    }
                }

                socketChannelOutput = new SocketChannelOutputStream(channel);
                socketChannelOutput.setTimeout(timeout);
            }
        } catch (final IOException e) {
            IOUtils.closeQuietly(channel);
            throw e;
        }
    }

    @Override
    protected void write(byte[] data) throws IOException {
        socketChannelOutput.write(data);
    }

    @Override
    public boolean isConnected() {
        return channel != null && channel.isConnected();
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(socketChannelOutput);
        IOUtils.closeQuietly(channel);
        socketChannelOutput = null;
        channel = null;
    }

    public OutputStream getOutputStream() {
        return new OutputStream() {
            @Override
            public void write(int b) throws IOException {
               socketChannelOutput.write(b);
            }

            @Override
            public void write(byte[] b) throws IOException {
                socketChannelOutput.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                socketChannelOutput.write(b, off, len);
            }

            @Override
            public void close() throws IOException {
                socketChannelOutput.close();
            }

            @Override
            public void flush() throws IOException {
                socketChannelOutput.flush();
                updateLastUsed();
            }
        };
    }

    private void updateLastUsed() {
        this.lastUsed = System.currentTimeMillis();
    }

}
