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

import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.ssl.EphemeralKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSocket;
import javax.security.auth.x500.X500Principal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SocketUtilsTest {

    private static final char[] EMPTY_PASSWORD = new char[]{};

    private static final Duration CERTIFICATE_VALIDITY = Duration.ofHours(1);

    private static final int TIMEOUT_MILLISECONDS = 5000;

    private static final String LOCALHOST = "localhost";

    private static final String NON_MATCHING_HOST = "nifi.apache.org";

    private static final String KEY_ALGORITHM = "RSA";

    private static final String SUBJECT_FORMAT = "CN=%s";

    private ExecutorService executorService;

    @BeforeEach
    void setExecutorService() {
        executorService = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void shutdownExecutorService() {
        executorService.shutdownNow();
    }

    @Test
    void testCreateSocketVerifiesMatchingHostname() throws Exception {
        final KeyPair keyPair = generateKeyPair();
        final X509Certificate certificate = buildCertificate(keyPair, LOCALHOST);

        connectAndHandshake(keyPair, certificate);
    }

    @Test
    void testCreateSocketRejectsMismatchedHostname() throws Exception {
        final KeyPair keyPair = generateKeyPair();
        final X509Certificate certificate = buildCertificate(keyPair, NON_MATCHING_HOST);

        assertThrows(SSLHandshakeException.class, () -> connectAndHandshake(keyPair, certificate));
    }

    private void connectAndHandshake(final KeyPair serverKeyPair, final X509Certificate serverCertificate) throws Exception {
        final SSLContext serverSslContext = buildServerSslContext(serverKeyPair, serverCertificate);
        final SSLContext clientSslContext = buildClientSslContext(serverCertificate);

        final ServerSocketConfiguration serverConfiguration = new ServerSocketConfiguration();
        serverConfiguration.setSslContext(serverSslContext);
        serverConfiguration.setSocketTimeout(TIMEOUT_MILLISECONDS);

        try (ServerSocket serverSocket = SocketUtils.createServerSocket(0, serverConfiguration)) {
            final int port = serverSocket.getLocalPort();

            final Future<?> serverConnection = executorService.submit(() -> {
                try (Socket accepted = serverSocket.accept()) {
                    // Read to initiate handshaking
                    final int read = accepted.getInputStream().read();
                    assertEquals(0, read);
                }
                return null;
            });

            final SocketConfiguration clientConfiguration = new SocketConfiguration();
            clientConfiguration.setSslContext(clientSslContext);
            clientConfiguration.setSocketTimeout(TIMEOUT_MILLISECONDS);

            try (Socket socket = SocketUtils.createSocket(new InetSocketAddress(LOCALHOST, port), clientConfiguration)) {
                final SSLSocket sslSocket = (SSLSocket) socket;
                sslSocket.startHandshake();
                sslSocket.getOutputStream().write(0);
                assertTrue(sslSocket.getSession().isValid());
            } finally {
                serverConnection.cancel(true);
            }
        }
    }

    private SSLContext buildServerSslContext(final KeyPair keyPair, final X509Certificate certificate) {
        final KeyStore keyStore = new EphemeralKeyStoreBuilder()
                .addPrivateKeyEntry(new KeyStore.PrivateKeyEntry(keyPair.getPrivate(), new Certificate[]{certificate}))
                .build();
        return new StandardSslContextBuilder()
                .keyStore(keyStore)
                .keyPassword(EMPTY_PASSWORD)
                .build();
    }

    private SSLContext buildClientSslContext(final X509Certificate certificate) {
        final KeyStore trustStore = new EphemeralKeyStoreBuilder()
                .addCertificate(certificate)
                .build();
        return new StandardSslContextBuilder()
                .trustStore(trustStore)
                .build();
    }

    private X509Certificate buildCertificate(final KeyPair keyPair, final String commonName) {
        final X500Principal subject = new X500Principal(SUBJECT_FORMAT.formatted(commonName));
        return new StandardCertificateBuilder(keyPair, subject, CERTIFICATE_VALIDITY).build();
    }

    private KeyPair generateKeyPair() throws NoSuchAlgorithmException {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM);
        return keyPairGenerator.generateKeyPair();
    }
}
