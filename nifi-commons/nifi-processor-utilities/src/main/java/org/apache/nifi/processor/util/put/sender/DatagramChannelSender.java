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
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

/**
 * Sends messages over a DatagramChannel.
 */
public class DatagramChannelSender extends ChannelSender {

    private DatagramChannel channel;

    public DatagramChannelSender(final String host, final int port, final int maxSendBufferSize, final ComponentLog logger) {
        super(host, port, maxSendBufferSize, logger);
    }

    @Override
    public void open() throws IOException {
        if (channel == null) {
            channel = DatagramChannel.open();

            if (maxSendBufferSize > 0) {
                channel.setOption(StandardSocketOptions.SO_SNDBUF, maxSendBufferSize);
                final int actualSendBufSize = channel.getOption(StandardSocketOptions.SO_SNDBUF);
                if (actualSendBufSize < maxSendBufferSize) {
                    logger.warn("Attempted to set Socket Send Buffer Size to " + maxSendBufferSize
                            + " bytes but could only set to " + actualSendBufSize + "bytes. You may want to "
                            + "consider changing the Operating System's maximum receive buffer");
                }
            }
        }

        if (!channel.isConnected()) {
            channel.connect(new InetSocketAddress(InetAddress.getByName(host), port));
        }
    }

    @Override
    protected void write(byte[] data) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    @Override
    public boolean isConnected() {
        return channel != null && channel.isConnected();
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(channel);
        channel = null;
    }

}
