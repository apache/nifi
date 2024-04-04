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
package org.apache.nifi.processors.standard.socket;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.util.Objects;

/**
 * Proxy Socket Factory implementation creates Sockets using the configured Proxy
 */
public class ProxySocketFactory extends SocketFactory {
    private final Proxy proxy;

    public ProxySocketFactory(final Proxy proxy) {
        this.proxy = Objects.requireNonNull(proxy, "Proxy required");
    }

    @Override
    public Socket createSocket() {
        return new Socket(proxy);
    }

    @Override
    public Socket createSocket(final String host, final int port) throws IOException {
        final InetSocketAddress socketAddress = new InetSocketAddress(host, port);
        return createSocket(socketAddress);
    }

    @Override
    public Socket createSocket(final String host, final int port, final InetAddress localHost, final int localPort) throws IOException {
        final InetSocketAddress socketAddress = new InetSocketAddress(host, port);
        final InetSocketAddress bindSocketAddress = new InetSocketAddress(localHost, localPort);
        return createSocket(socketAddress, bindSocketAddress);
    }

    @Override
    public Socket createSocket(final InetAddress host, final int port) throws IOException {
        final InetSocketAddress socketAddress = new InetSocketAddress(host, port);
        return createSocket(socketAddress);
    }

    @Override
    public Socket createSocket(final InetAddress host, final int port, final InetAddress localAddress, final int localPort) throws IOException {
        final InetSocketAddress socketAddress = new InetSocketAddress(host, port);
        final InetSocketAddress bindSocketAddress = new InetSocketAddress(localAddress, localPort);
        return createSocket(socketAddress, bindSocketAddress);
    }

    private Socket createSocket(final InetSocketAddress socketAddress, final InetSocketAddress bindSocketAddress) throws IOException {
        final Socket socket = createSocket();
        socket.bind(bindSocketAddress);
        socket.connect(socketAddress);
        return socket;
    }

    private Socket createSocket(final InetSocketAddress socketAddress) throws IOException {
        final Socket socket = createSocket();
        socket.connect(socketAddress);
        return socket;
    }
}
