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

package org.apache.nifi.controller.queue.clustered.client.async.nio;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SocketChannel;
import java.util.Objects;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

/**
 * Standard implementation of Peer Channel Provider that configures TLS with HTTPS endpoint identification so that
 * the peer certificate is verified against the Load Balance address of the destination node during the handshake.
 */
class StandardPeerChannelProvider implements PeerChannelProvider {

    private static final Logger logger = LoggerFactory.getLogger(StandardPeerChannelProvider.class);

    private static final String TLS_ENDPOINT_IDENTIFICATION_ALGORITHM = "HTTPS";

    private final SSLContext sslContext;

    private final NodeIdentifier nodeIdentifier;

    StandardPeerChannelProvider(final SSLContext sslContext, final NodeIdentifier nodeIdentifier) {
        this.sslContext = sslContext;
        this.nodeIdentifier = nodeIdentifier;
    }

    @Override
    public PeerChannel getPeerChannel(final SocketChannel socketChannel, final String peerDescription) {
        Objects.requireNonNull(socketChannel, "Socket Channel required");
        Objects.requireNonNull(peerDescription, "Peer Description required");

        final PeerChannel peerChannel;

        if (sslContext == null) {
            logger.debug("SSLContext not configured for Peer Channel [{}]", peerDescription);
            peerChannel = new PeerChannel(socketChannel, null, peerDescription);
        } else {
            logger.debug("Configured TLS for Peer Channel [{}]", peerDescription);
            final SSLEngine sslEngine = createSslEngine();
            peerChannel = new PeerChannel(socketChannel, sslEngine, peerDescription);
        }

        return peerChannel;
    }

    private SSLEngine createSslEngine() {
        // Provide the peer address so endpoint identification can verify the peer certificate against the Load Balance host
        final SSLEngine sslEngine = sslContext.createSSLEngine(nodeIdentifier.getLoadBalanceAddress(), nodeIdentifier.getLoadBalancePort());
        sslEngine.setUseClientMode(true);

        final SSLParameters sslParameters = sslEngine.getSSLParameters();
        sslParameters.setEndpointIdentificationAlgorithm(TLS_ENDPOINT_IDENTIFICATION_ALGORITHM);
        sslEngine.setSSLParameters(sslParameters);

        return sslEngine;
    }
}
