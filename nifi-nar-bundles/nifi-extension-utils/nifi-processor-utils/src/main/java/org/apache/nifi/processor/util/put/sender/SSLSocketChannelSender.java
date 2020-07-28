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
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannel;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannelOutputStream;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Sends messages over an SSLSocketChannel.
 */
public class SSLSocketChannelSender extends SocketChannelSender {

    private SSLContext sslContext;
    private SSLSocketChannel sslChannel;
    private SSLSocketChannelOutputStream sslOutputStream;

    public SSLSocketChannelSender(final String host,
                                  final int port,
                                  final int maxSendBufferSize,
                                  final SSLContext sslContext,
                                  final ComponentLog logger) {
        super(host, port, maxSendBufferSize, logger);
        this.sslContext = sslContext;
    }

    @Override
    public void open() throws IOException {
        if (sslChannel == null) {
            super.open();
            sslChannel = new SSLSocketChannel(sslContext, channel, true);
        }
        sslChannel.setTimeout(timeout);

        // SSLSocketChannel will check if already connected so we can safely call this
        sslChannel.connect();
        sslOutputStream = new SSLSocketChannelOutputStream(sslChannel);
    }

    @Override
    protected void write(byte[] data) throws IOException {
        sslChannel.write(data);
    }

    @Override
    public boolean isConnected() {
        return sslChannel != null && !sslChannel.isClosed();
    }

    @Override
    public void close() {
        super.close();
        IOUtils.closeQuietly(sslOutputStream);
        IOUtils.closeQuietly(sslChannel);
        sslChannel = null;
    }

    @Override
    public OutputStream getOutputStream() {
        return sslOutputStream;
    }

}
