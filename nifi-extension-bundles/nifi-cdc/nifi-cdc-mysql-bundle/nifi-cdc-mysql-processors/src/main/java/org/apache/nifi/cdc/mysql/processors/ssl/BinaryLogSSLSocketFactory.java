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
package org.apache.nifi.cdc.mysql.processors.ssl;

import com.github.shyiko.mysql.binlog.network.SSLSocketFactory;

import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

/**
 * Binary Log SSLSocketFactory wrapping standard Java SSLSocketFactory
 */
public class BinaryLogSSLSocketFactory implements SSLSocketFactory {
    private static final boolean AUTO_CLOSE_ENABLED = true;

    private final javax.net.ssl.SSLSocketFactory sslSocketFactory;

    public BinaryLogSSLSocketFactory(final javax.net.ssl.SSLSocketFactory sslSocketFactory) {
        this.sslSocketFactory = sslSocketFactory;
    }

    /**
     * Create SSL Socket layers provided Socket using Java SSLSocketFactory
     *
     * @param socket Socket to be layered
     * @return SSL Socket
     * @throws SocketException Thrown when IOException encountered from SSLSocketFactory.createSocket()
     */
    @Override
    public SSLSocket createSocket(final Socket socket) throws SocketException {
        final String hostAddress = socket.getInetAddress().getHostAddress();
        final int port = socket.getPort();
        try {
            return (SSLSocket) sslSocketFactory.createSocket(socket, hostAddress, port, AUTO_CLOSE_ENABLED);
        } catch (final IOException e) {
            final String message = String.format("Create SSL Socket Failed for Host Address [%s] Port [%d]: %s", hostAddress, port, e.getMessage());
            throw new SocketException(message);
        }
    }
}
