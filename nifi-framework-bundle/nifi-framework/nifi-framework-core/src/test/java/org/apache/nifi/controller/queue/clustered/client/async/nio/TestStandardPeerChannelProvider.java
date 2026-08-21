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
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.ssl.EphemeralKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.security.auth.x500.X500Principal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class TestStandardPeerChannelProvider {

    private static final String KEY_ALGORITHM = "RSA";

    private static final String SUBJECT_FORMAT = "CN=%s";

    private static final String LOAD_BALANCE_ADDRESS = "node-1.nifi.example.com";

    private static final int LOAD_BALANCE_PORT = 6342;

    private static final String NODE_ID = "node-1";

    private static final String LOCALHOST = "localhost";

    private static final int NODE_PORT = 8443;

    private static final String PEER_DESCRIPTION = "local::remote";

    private static final String TLS_ENDPOINT_IDENTIFICATION_ALGORITHM = "HTTPS";

    private static final Duration CERTIFICATE_VALIDITY = Duration.ofHours(1);

    private static final char[] EMPTY_PASSWORD = new char[]{};

    private static SSLContext sslContext;

    private static NodeIdentifier nodeIdentifier;

    @BeforeAll
    static void setConfiguration() throws GeneralSecurityException {
        final KeyPair keyPair = KeyPairGenerator.getInstance(KEY_ALGORITHM).generateKeyPair();
        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, new X500Principal(SUBJECT_FORMAT.formatted(LOAD_BALANCE_ADDRESS)), CERTIFICATE_VALIDITY).build();
        final KeyStore keyStore = new EphemeralKeyStoreBuilder()
                .addPrivateKeyEntry(new KeyStore.PrivateKeyEntry(keyPair.getPrivate(), new Certificate[]{certificate}))
                .build();

        sslContext = new StandardSslContextBuilder()
                .trustStore(keyStore)
                .keyStore(keyStore)
                .keyPassword(EMPTY_PASSWORD)
                .build();

        nodeIdentifier = new NodeIdentifier(
                NODE_ID,
                LOCALHOST,
                NODE_PORT,
                LOCALHOST,
                NODE_PORT,
                LOAD_BALANCE_ADDRESS,
                LOAD_BALANCE_PORT,
                LOCALHOST,
                NODE_PORT,
                NODE_PORT,
                false
        );
    }

    @Test
    void testGetPeerChannelConfiguresEndpointIdentification() {
        final StandardPeerChannelProvider provider = new StandardPeerChannelProvider(sslContext, nodeIdentifier);

        final PeerChannel peerChannel = provider.getPeerChannel(mock(SocketChannel.class), PEER_DESCRIPTION);
        final SSLEngine sslEngine = peerChannel.getSslEngine();
        assertNotNull(sslEngine, "SSLEngine not configured");

        assertTrue(sslEngine.getUseClientMode(), "Client mode not enabled");
        assertEquals(TLS_ENDPOINT_IDENTIFICATION_ALGORITHM, sslEngine.getSSLParameters().getEndpointIdentificationAlgorithm());
        assertEquals(LOAD_BALANCE_ADDRESS, sslEngine.getPeerHost(), "Peer host not matched to Load Balance address");
        assertEquals(LOAD_BALANCE_PORT, sslEngine.getPeerPort(), "Peer port not matched to Load Balance port");
    }

    @Test
    void testGetPeerChannelWithoutSslContext() {
        final StandardPeerChannelProvider provider = new StandardPeerChannelProvider(null, nodeIdentifier);

        final PeerChannel peerChannel = provider.getPeerChannel(mock(SocketChannel.class), PEER_DESCRIPTION);
        assertNull(peerChannel.getSslEngine(), "SSLEngine should not be configured without an SSL Context");
    }
}
