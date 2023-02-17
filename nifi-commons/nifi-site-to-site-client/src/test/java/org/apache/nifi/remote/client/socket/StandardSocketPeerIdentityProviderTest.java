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
package org.apache.nifi.remote.client.socket;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.net.Socket;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardSocketPeerIdentityProviderTest {
    private static final String DISTINGUISHED_NAME = "CN=Common Name,OU=Organizational Unit,O=Organization";

    @Mock
    SSLSocket sslSocket;

    @Mock
    SSLSession sslSession;

    @Mock
    X509Certificate peerCertificate;

    StandardSocketPeerIdentityProvider provider;

    @BeforeEach
    void setProvider() {
        provider = new StandardSocketPeerIdentityProvider();
    }

    @Test
    void testGetPeerIdentityStandardSocket() throws IOException {
        try (Socket socket = new Socket()) {
            final Optional<String> peerIdentity = provider.getPeerIdentity(socket);

            assertFalse(peerIdentity.isPresent());
        }
    }

    @Test
    void testGetPeerIdentitySSLSocketPeerUnverifiedException() throws SSLPeerUnverifiedException {
        when(sslSocket.getSession()).thenReturn(sslSession);
        when(sslSession.getPeerCertificates()).thenThrow(new SSLPeerUnverifiedException(SSLPeerUnverifiedException.class.getSimpleName()));

        final Optional<String> peerIdentity = provider.getPeerIdentity(sslSocket);

        assertFalse(peerIdentity.isPresent());
    }

    @Test
    void testGetPeerIdentitySSLSocketPeerCertificatesNotFound() throws SSLPeerUnverifiedException {
        when(sslSocket.getSession()).thenReturn(sslSession);
        when(sslSession.getPeerCertificates()).thenReturn(new Certificate[]{});

        final Optional<String> peerIdentity = provider.getPeerIdentity(sslSocket);

        assertFalse(peerIdentity.isPresent());
    }

    @Test
    void testGetPeerIdentityFound() throws SSLPeerUnverifiedException {
        when(sslSocket.getSession()).thenReturn(sslSession);
        when(sslSession.getPeerCertificates()).thenReturn(new X509Certificate[]{peerCertificate});

        final X500Principal subjectDistinguishedName = new X500Principal(DISTINGUISHED_NAME);
        when(peerCertificate.getSubjectDN()).thenReturn(subjectDistinguishedName);

        final Optional<String> peerIdentity = provider.getPeerIdentity(sslSocket);

        assertTrue(peerIdentity.isPresent());
        final String identity = peerIdentity.get();
        assertEquals(DISTINGUISHED_NAME, identity);
    }
}
