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
package org.apache.nifi.jetty.configuration.connector.alpn;

import org.eclipse.jetty.alpn.server.ALPNServerConnection;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.ssl.ALPNProcessor;
import org.eclipse.jetty.io.ssl.SslConnection;
import org.eclipse.jetty.io.ssl.SslHandshakeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Standard ALPN Processor supporting JDK 1.8.0-251 and higher based on Jetty JDK9ServerALPNProcessor
 */
public class StandardALPNProcessor implements ALPNProcessor.Server, SslHandshakeListener {
    private static final Logger logger = LoggerFactory.getLogger(StandardALPNProcessor.class);

    /**
     * Applies to SSL Engine instances regardless of implementation
     *
     * @param sslEngine SSL Engine to be evaluated
     * @return Applicable Status
     */
    @Override
    public boolean appliesTo(final SSLEngine sslEngine) {
        return true;
    }

    /**
     * Configure ALPN negotiation for Connection
     *
     * @param sslEngine SSL Engine to be configured
     * @param connection Connection to be configured
     */
    @Override
    public void configure(final SSLEngine sslEngine, final Connection connection) {
        logger.debug("Configuring Connection Remote Address [{}]", connection.getEndPoint().getRemoteAddress());
        final ALPNServerConnection serverConnection = (ALPNServerConnection) connection;
        final ProtocolSelector protocolSelector = new ProtocolSelector(serverConnection);
        sslEngine.setHandshakeApplicationProtocolSelector(protocolSelector);

        final SslConnection.DecryptedEndPoint endPoint = (SslConnection.DecryptedEndPoint) serverConnection.getEndPoint();
        endPoint.getSslConnection().addHandshakeListener(protocolSelector);
    }

    private static final class ProtocolSelector implements BiFunction<SSLEngine, List<String>, String>, SslHandshakeListener {
        private final ALPNServerConnection serverConnection;

        private ProtocolSelector(final ALPNServerConnection connection) {
            serverConnection = connection;
        }

        /**
         * Select supported Application Layer Protocol based on requested protocols
         *
         * @param sslEngine SSL Engine
         * @param protocols Protocols requested
         * @return Protocol selected or null when no supported protocol found
         */
        @Override
        public String apply(final SSLEngine sslEngine, final List<String> protocols) {
            String protocol = null;
            try {
                serverConnection.select(protocols);
                protocol = serverConnection.getProtocol();
                logger.debug("Connection Remote Address [{}] Application Layer Protocol [{}] selected", serverConnection.getEndPoint().getRemoteAddress(), protocol);
            } catch (final Throwable e) {
                logger.debug("Connection Remote Address [{}] Application Layer Protocols {} not supported", serverConnection.getEndPoint().getRemoteAddress(), protocols);
            }
            return protocol;
        }

        /**
         * Handler for successful handshake checks for selected Application Layer Protocol
         *
         * @param event Event is not used
         */
        @Override
        public void handshakeSucceeded(final Event event) {
            final InetSocketAddress remoteAddress = serverConnection.getEndPoint().getRemoteAddress();
            final SSLSession session = event.getSSLEngine().getSession();
            logger.debug("Connection Remote Address [{}] Handshake Succeeded [{}] Cipher Suite [{}]", remoteAddress, session.getProtocol(), session.getCipherSuite());

            final String protocol = serverConnection.getProtocol();
            if (protocol == null) {
                logger.debug("Connection Remote Address [{}] Application Layer Protocol not supported", remoteAddress);
                serverConnection.unsupported();
            }
        }

        /**
         * Handle for failed handshake logs status
         *
         * @param event Event is not used
         * @param failure Failure cause to be logged
         */
        @Override
        public void handshakeFailed(final Event event, final Throwable failure) {
            logger.debug("Connection Remote Address [{}] Handshake Failed", serverConnection.getEndPoint().getRemoteAddress(), failure);
        }
    }
}
