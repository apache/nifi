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
import org.apache.nifi.remote.io.InterruptableInputStream;
import org.apache.nifi.remote.io.InterruptableOutputStream;
import org.apache.nifi.remote.io.socket.SocketChannelInputStream;
import org.apache.nifi.remote.io.socket.SocketChannelOutputStream;

public class StandardCommsSession implements CommsSession {

    private final SocketChannel socketChannel;
    private final String hostname;
    private final int port;
    private volatile long timeoutMillis;

    private final SocketChannelInputStream in;
    private final InterruptableInputStream bufferedIn;

    private final SocketChannelOutputStream out;
    private final InterruptableOutputStream bufferedOut;

    private int protocolVersion;

    public StandardCommsSession(final String hostname, final int port, final int timeoutMillis) throws IOException {
        socketChannel = SocketChannel.open();
        socketChannel.socket().connect(new InetSocketAddress(hostname, port), timeoutMillis);
        socketChannel.configureBlocking(false);

        in = new SocketChannelInputStream(socketChannel);
        bufferedIn = new InterruptableInputStream(new BufferedInputStream(in));

        out = new SocketChannelOutputStream(socketChannel);
        bufferedOut = new InterruptableOutputStream(new BufferedOutputStream(out));

        this.hostname = hostname;
        this.port = port;
    }

    @Override
    public void interrupt() {
        bufferedIn.interrupt();
        bufferedOut.interrupt();
    }

    @Override
    public void close() throws IOException {
        socketChannel.close();
    }

    @Override
    public void setTimeout(final long value, final TimeUnit timeUnit) {
        in.setTimeout((int) TimeUnit.MILLISECONDS.convert(value, timeUnit));
        out.setTimeout((int) TimeUnit.MILLISECONDS.convert(value, timeUnit));
        timeoutMillis = TimeUnit.MILLISECONDS.convert(value, timeUnit);
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
        boolean closed = !socketChannel.isConnected();
        if (!closed) {
            try {
                this.in.isDataAvailable();
            } catch (IOException e) {
                try {
                    close();
                } catch (IOException e1) {
                }
                closed = true;
            }
        }
        return closed;
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
        return null;
    }

    @Override
    public long getTimeout(final TimeUnit timeUnit) {
        return timeUnit.convert(timeoutMillis, TimeUnit.MILLISECONDS);
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
