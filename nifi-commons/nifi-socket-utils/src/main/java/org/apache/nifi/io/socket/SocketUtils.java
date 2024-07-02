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
import java.security.cert.CertificateException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.security.util.TlsPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SocketUtils {

    private static final Logger logger = LoggerFactory.getLogger(SocketUtils.class);

    /**
     * Returns a {@link Socket} (effectively used as a client socket) for the given address and configuration.
     *
     * @param address   the {@link InetSocketAddress} for the socket (used for hostname and port)
     * @param config the {@link SocketConfiguration}
     * @return the socket (can be configured for SSL)
     * @throws IOException  if there is a problem creating the socket
     */
    public static Socket createSocket(final InetSocketAddress address, final SocketConfiguration config) throws IOException {
        if (address == null) {
            throw new IllegalArgumentException("Socket address may not be null.");
        } else if (config == null) {
            throw new IllegalArgumentException("Configuration may not be null.");
        }

        final Socket socket;

        final SSLContext sslContext = config.getSslContext();
        if (sslContext == null) {
            socket = new Socket(address.getHostName(), address.getPort());
        } else {
            /* This would ideally be refactored to a shared create method but Socket and ServerSocket
             * do not share a common interface; Socket is effectively "client socket" in this context
             */
            Socket tempSocket = sslContext.getSocketFactory().createSocket(address.getHostName(), address.getPort());
            final SSLSocket sslSocket = (SSLSocket) tempSocket;
            // Set Preferred TLS Protocol Versions
            sslSocket.setEnabledProtocols(TlsPlatform.getPreferredProtocols().toArray(new String[0]));
            socket = sslSocket;
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

    /**
     * Returns a {@link ServerSocket} for the given port and configuration.
     *
     * @param port   the port for the socket
     * @param config the {@link ServerSocketConfiguration}
     * @return the server socket (can be configured for SSL)
     * @throws IOException  if there is a problem creating the socket
     * @throws TlsException if there is a problem creating the socket
     */
    public static ServerSocket createServerSocket(final int port, final ServerSocketConfiguration config)
            throws IOException, TlsException {
        if (config == null) {
            throw new NullPointerException("Configuration may not be null.");
        }

        final SSLContext sslContext = config.getSslContext();
        final ServerSocket serverSocket;
        if (sslContext == null) {
            serverSocket = new ServerSocket(port);
        } else {
            serverSocket = sslContext.getServerSocketFactory().createServerSocket(port);
            final SSLServerSocket sslServerSocket = (SSLServerSocket) serverSocket;
            sslServerSocket.setNeedClientAuth(config.getNeedClientAuth());
            // Set Preferred TLS Protocol Versions
            sslServerSocket.setEnabledProtocols(TlsPlatform.getPreferredProtocols().toArray(new String[0]));
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
                // Can't shutdown input/output individually with secure sockets
                if (!(socket instanceof SSLSocket)) {
                    if (!socket.isInputShutdown()) {
                        socket.shutdownInput();
                    }
                    if (!socket.isOutputShutdown()) {
                        socket.shutdownOutput();
                    }
                }
            } finally {
                if (!socket.isClosed()) {
                    socket.close();
                }
            }
        } catch (final Exception ex) {
            logger.debug("Failed to close socket", ex);
        }
    }

    public static void closeQuietly(final ServerSocket serverSocket) {
        if (serverSocket == null) {
            return;
        }

        try {
            serverSocket.close();
        } catch (final Exception ex) {
            logger.debug("Failed to close server socket", ex);
        }
    }

    /**
     * Returns {@code true} if this exception is due to a TLS problem (either directly or because of its cause, if present). Traverses the cause chain recursively.
     *
     * @param e the exception to evaluate
     * @return true if the direct or indirect cause of this exception was TLS-related
     */
    public static boolean isTlsError(Throwable e) {
        if (e == null) {
            return false;
        } else {
            if (e instanceof CertificateException || e instanceof TlsException || e instanceof SSLException) {
                return true;
            } else if (e.getCause() != null) {
                return isTlsError(e.getCause());
            } else {
                return false;
            }
        }
    }
}
