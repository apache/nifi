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
package org.apache.nifi.distributed.cache.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannel;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannelInputStream;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannelOutputStream;

public class SSLCommsSession implements CommsSession {

    private final SSLSocketChannel sslSocketChannel;
    private final SSLContext sslContext;
    private final String hostname;
    private final int port;

    private final SSLSocketChannelInputStream in;
    private final BufferedInputStream bufferedIn;

    private final SSLSocketChannelOutputStream out;
    private final BufferedOutputStream bufferedOut;

    private int protocolVersion;

    public SSLCommsSession(final SSLContext sslContext, final String hostname, final int port, final int timeoutMillis) throws IOException {
        final SocketChannel socketChannel = SocketChannel.open();
        socketChannel.socket().connect(new InetSocketAddress(hostname, port), timeoutMillis);
        socketChannel.configureBlocking(false);

        sslSocketChannel = new SSLSocketChannel(sslContext, socketChannel,true);

        in = new SSLSocketChannelInputStream(sslSocketChannel);
        bufferedIn = new BufferedInputStream(in);

        out = new SSLSocketChannelOutputStream(sslSocketChannel);
        bufferedOut = new BufferedOutputStream(out);

        this.sslContext = sslContext;
        this.hostname = hostname;
        this.port = port;
    }

    @Override
    public void interrupt() {
        sslSocketChannel.interrupt();
    }

    @Override
    public void close() throws IOException {
        sslSocketChannel.close();
    }

    @Override
    public void setTimeout(final long value, final TimeUnit timeUnit) {
        sslSocketChannel.setTimeout((int) TimeUnit.MILLISECONDS.convert(value, timeUnit));
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return bufferedIn;
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        return bufferedOut;
    }

    @Override
    public boolean isClosed() {
        return sslSocketChannel.isClosed();
    }

    @Override
    public String getHostname() {
        return hostname;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public SSLContext getSSLContext() {
        return sslContext;
    }

    @Override
    public long getTimeout(final TimeUnit timeUnit) {
        return timeUnit.convert(sslSocketChannel.getTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int getProtocolVersion() {
        return protocolVersion;
    }

    @Override
    public void setProtocolVersion(final int protocolVersion) {
        this.protocolVersion = protocolVersion;
    }
}
