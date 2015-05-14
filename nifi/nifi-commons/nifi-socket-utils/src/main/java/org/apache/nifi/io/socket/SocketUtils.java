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
package org.apache.nifi.io.socket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

import org.apache.nifi.logging.NiFiLog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SocketUtils {

    private static final Logger logger = new NiFiLog(LoggerFactory.getLogger(SocketUtils.class));

    public static Socket createSocket(final InetSocketAddress address, final SocketConfiguration config) throws IOException {
        if (address == null) {
            throw new IllegalArgumentException("Socket address may not be null.");
        } else if (config == null) {
            throw new IllegalArgumentException("Configuration may not be null.");
        }

        final Socket socket;

        final SSLContext sslContext;
        try {
            sslContext = config.createSSLContext();
        } catch (final Exception e) {
            throw new IOException("Could not create SSLContext", e);
        }

        if (sslContext == null) {
            socket = new Socket(address.getHostName(), address.getPort());
        } else {
            socket = sslContext.getSocketFactory().createSocket(address.getHostName(), address.getPort());
        }

        if (config.getSocketTimeout() != null) {
            socket.setSoTimeout(config.getSocketTimeout());
        }

        if (config.getReuseAddress() != null) {
            socket.setReuseAddress(config.getReuseAddress());
        }

        if (config.getReceiveBufferSize() != null) {
            socket.setReceiveBufferSize(config.getReceiveBufferSize());
        }

        if (config.getSendBufferSize() != null) {
            socket.setSendBufferSize(config.getSendBufferSize());
        }

        if (config.getTrafficClass() != null) {
            socket.setTrafficClass(config.getTrafficClass());
        }

        if (config.getKeepAlive() != null) {
            socket.setKeepAlive(config.getKeepAlive());
        }

        if (config.getOobInline() != null) {
            socket.setOOBInline(config.getOobInline());
        }

        if (config.getTcpNoDelay() != null) {
            socket.setTcpNoDelay(config.getTcpNoDelay());
        }

        return socket;
    }

    public static ServerSocket createServerSocket(final int port, final ServerSocketConfiguration config)
            throws IOException, KeyManagementException, UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, CertificateException {
        if (config == null) {
            throw new NullPointerException("Configuration may not be null.");
        }

        final SSLContext sslContext = config.createSSLContext();
        final ServerSocket serverSocket;
        if (sslContext == null) {
            serverSocket = new ServerSocket(port);
        } else {
            serverSocket = sslContext.getServerSocketFactory().createServerSocket(port);
            ((SSLServerSocket) serverSocket).setNeedClientAuth(config.getNeedClientAuth());
        }

        if (config.getSocketTimeout() != null) {
            serverSocket.setSoTimeout(config.getSocketTimeout());
        }

        if (config.getReuseAddress() != null) {
            serverSocket.setReuseAddress(config.getReuseAddress());
        }

        if (config.getReceiveBufferSize() != null) {
            serverSocket.setReceiveBufferSize(config.getReceiveBufferSize());
        }

        return serverSocket;
    }

    public static void closeQuietly(final Socket socket) {
        if (socket == null) {
            return;
        }

        try {
            try {
                // can't shudown input/output individually with secure sockets
                if ((socket instanceof SSLSocket) == false) {
                    if (socket.isInputShutdown() == false) {
                        socket.shutdownInput();
                    }
                    if (socket.isOutputShutdown() == false) {
                        socket.shutdownOutput();
                    }
                }
            } finally {
                if (socket.isClosed() == false) {
                    socket.close();
                }
            }
        } catch (final Exception ex) {
            logger.debug("Failed to close socket due to: " + ex, ex);
        }
    }

    public static void closeQuietly(final ServerSocket serverSocket) {
        if (serverSocket == null) {
            return;
        }

        try {
            serverSocket.close();
        } catch (final Exception ex) {
            logger.debug("Failed to close server socket due to: " + ex, ex);
        }
    }

}
