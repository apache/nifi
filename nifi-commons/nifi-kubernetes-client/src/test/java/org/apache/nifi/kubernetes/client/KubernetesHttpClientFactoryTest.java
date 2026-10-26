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
package org.apache.nifi.kubernetes.client;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.http.HttpClient;
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Base64;
import java.util.Comparator;
import java.util.ServiceLoader;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KubernetesHttpClientFactoryTest {

    private static final int KEY_SIZE = 4096;

    private static final String KEY_ALGORITHM = "RSA";

    private static final String MASTER_URL = "https://kubernetes.default.svc/";

    private static final String OAUTH_TOKEN = "service-account-token";

    private static final String NAMESPACE = "nifi";

    private static final String CERTIFICATE_AUTHORITY_FILE = "ca.crt";

    private static final String CERTIFICATE_AUTHORITY_DATA = "certificate-authority";

    private static final String CERTIFICATE_HEADER = "-----BEGIN CERTIFICATE-----";

    private static final String CERTIFICATE_FOOTER = "-----END CERTIFICATE-----";

    private static final String PRIVATE_KEY_HEADER = "-----BEGIN PRIVATE KEY-----";

    private static final String PRIVATE_KEY_FOOTER = "-----END PRIVATE KEY-----";

    private static final String CLIENT_KEY_FILE = "client.key";

    private static final String CLIENT_CERTIFICATE_DATA = "client-certificate";

    private static final String CLIENT_KEY_DATA = "client-key";

    private static final String TRUST_STORE_FILE = "truststore.p12";

    private static final String LINE_SEPARATOR = "\n";

    private static final String INVALID_CERTIFICATE_AUTHORITY_PEM = CERTIFICATE_HEADER
            + LINE_SEPARATOR
            + "NOT-A-CERTIFICATE"
            + LINE_SEPARATOR
            + CERTIFICATE_FOOTER
            + LINE_SEPARATOR;

    private static final String CERTIFICATE_SUBJECT = "CN=localhost";

    private static final Duration CERTIFICATE_VALIDITY_PERIOD = Duration.ofHours(1);

    private final KubernetesHttpClientFactory factory = new KubernetesHttpClientFactory();

    @Test
    void testPriorityHigherThanOtherFactories() {
        final int highestOtherPriority = ServiceLoader.load(HttpClient.Factory.class)
                .stream()
                .map(ServiceLoader.Provider::get)
                .filter(candidate -> !(candidate instanceof KubernetesHttpClientFactory))
                .mapToInt(HttpClient.Factory::priority)
                .max()
                .orElseThrow();

        assertTrue(factory.priority() > highestOtherPriority);
    }

    @Test
    void testServiceLoaderSelectsHighestPriorityFactory() {
        final HttpClient.Factory highestPriorityFactory = ServiceLoader.load(HttpClient.Factory.class)
                .stream()
                .map(ServiceLoader.Provider::get)
                .max(Comparator.comparingInt(HttpClient.Factory::priority))
                .orElseThrow();

        assertInstanceOf(KubernetesHttpClientFactory.class, highestPriorityFactory);
    }

    @Test
    void testGetConfigWithoutCertificateAuthority() {
        final Config config = getConfigBuilder()
                .withCaCertData(CERTIFICATE_AUTHORITY_DATA)
                .withCaCertFile(CERTIFICATE_AUTHORITY_FILE)
                .withAutoConfigure(true)
                .build();

        final Config configWithoutCertificateAuthority = KubernetesHttpClientFactory.getConfigWithoutCertificateAuthority(config);

        assertNull(configWithoutCertificateAuthority.getCaCertData());
        assertNull(configWithoutCertificateAuthority.getCaCertFile());

        assertEquals(MASTER_URL, configWithoutCertificateAuthority.getMasterUrl());
        assertEquals(OAUTH_TOKEN, configWithoutCertificateAuthority.getAutoOAuthToken());
        assertEquals(NAMESPACE, configWithoutCertificateAuthority.getNamespace());

        assertTrue(configWithoutCertificateAuthority.getAutoConfigure());
    }

    @Test
    void testGetTrustManagersCertificateAuthorityData(@TempDir final Path tempDir) throws GeneralSecurityException, IOException {
        final Config config = getConfigBuilder().withCaCertData(getCertificateAuthorityData(tempDir)).build();

        assertTrustManagersConfigured(config);
    }

    @Test
    void testGetTrustManagersCertificateAuthorityFile(@TempDir final Path tempDir) throws GeneralSecurityException, IOException {
        final Config config = getConfigBuilder().withCaCertFile(getCertificateAuthorityFile(tempDir)).build();

        assertTrustManagersConfigured(config);
    }

    @Test
    void testGetTrustManagersInvalidCertificateAuthorityThrowsKubernetesClientException() {
        final String encodedCertificateAuthority = Base64.getEncoder().encodeToString(INVALID_CERTIFICATE_AUTHORITY_PEM.getBytes(StandardCharsets.UTF_8));
        final Config config = getConfigBuilder().withCaCertData(encodedCertificateAuthority).build();

        assertThrows(KubernetesClientException.class, () -> KubernetesHttpClientFactory.getTrustManagers(config));
    }

    @Test
    void testIsDefaultSslContextRequiredCertificateAuthorityConfigured() {
        final Config config = getConfigBuilder().withCaCertData(CERTIFICATE_AUTHORITY_DATA).build();

        assertFalse(KubernetesHttpClientFactory.isDefaultSslContextRequired(config));
    }

    @Test
    void testIsDefaultSslContextRequiredCertificateAuthorityNotConfigured() {
        final Config config = getConfigBuilder().build();

        assertTrue(KubernetesHttpClientFactory.isDefaultSslContextRequired(config));
    }

    @Test
    void testIsDefaultSslContextRequiredTrustCerts() {
        final Config config = getConfigBuilder()
                .withTrustCerts(true)
                .withCaCertData(CERTIFICATE_AUTHORITY_DATA)
                .build();

        assertTrue(KubernetesHttpClientFactory.isDefaultSslContextRequired(config));
    }

    @Test
    void testIsDefaultSslContextRequiredTrustStoreFile() {
        final Config config = getConfigBuilder()
                .withTrustStoreFile(TRUST_STORE_FILE)
                .withCaCertData(CERTIFICATE_AUTHORITY_DATA)
                .build();

        assertTrue(KubernetesHttpClientFactory.isDefaultSslContextRequired(config));
    }

    @Test
    void testIsDefaultSslContextRequiredClientCertificateConfigured() {
        final Config config = getConfigBuilder()
                .withCaCertData(CERTIFICATE_AUTHORITY_DATA)
                .withClientCertData(CLIENT_CERTIFICATE_DATA)
                .withClientKeyData(CLIENT_KEY_DATA)
                .build();

        assertTrue(KubernetesHttpClientFactory.isDefaultSslContextRequired(config));
    }

    @Test
    void testNewBuilderCertificateAuthorityFile(@TempDir final Path tempDir) throws GeneralSecurityException, IOException {
        final Config config = getConfigBuilder().withCaCertFile(getCertificateAuthorityFile(tempDir)).build();

        final HttpClient.Builder builder = factory.newBuilder(config);

        try (HttpClient client = builder.build()) {
            assertNotNull(client);
        }

        assertTrustManagersConfigured(config);
    }

    @Test
    void testNewBuilderCertificateAuthorityNotConfigured() {
        final Config config = getConfigBuilder().build();

        final HttpClient.Builder builder = factory.newBuilder(config);

        assertNotNull(builder);
        try (HttpClient client = builder.build()) {
            assertNotNull(client);
        }
    }

    @Test
    void testNewBuilderClientCertificateConfigured(@TempDir final Path tempDir) throws GeneralSecurityException, IOException {
        final CertificateMaterial certificateMaterial = writeCertificateMaterial(tempDir);
        final Config config = getConfigBuilder()
                .withCaCertFile(certificateMaterial.certificatePath().toString())
                .withClientCertFile(certificateMaterial.certificatePath().toString())
                .withClientKeyFile(certificateMaterial.privateKeyPath().toString())
                .build();

        final HttpClient.Builder builder = factory.newBuilder(config);

        assertNotNull(builder);
        try (HttpClient client = builder.build()) {
            assertNotNull(client);
        }
    }

    @Test
    void testNewBuilderTrustCerts() {
        final Config config = getConfigBuilder()
                .withTrustCerts(true)
                .withCaCertData(CERTIFICATE_AUTHORITY_DATA)
                .build();

        final HttpClient.Builder builder = factory.newBuilder(config);

        assertNotNull(builder);
        try (HttpClient client = builder.build()) {
            assertNotNull(client);
        }
    }

    private void assertTrustManagersConfigured(final Config config) {
        final KeyStore trustStore = KubernetesHttpClientFactory.getTrustStore(config);
        assertEquals(KeyStore.getDefaultType(), trustStore.getType());

        final TrustManager[] trustManagers = KubernetesHttpClientFactory.getTrustManagers(config);

        assertEquals(1, trustManagers.length);
        final X509TrustManager trustManager = assertInstanceOf(X509TrustManager.class, trustManagers[0]);
        assertEquals(1, trustManager.getAcceptedIssuers().length);
        assertEquals(CERTIFICATE_SUBJECT, trustManager.getAcceptedIssuers()[0].getSubjectX500Principal().getName());
    }

    private ConfigBuilder getConfigBuilder() {
        return new ConfigBuilder()
                .withAutoConfigure(false)
                .withMasterUrl(MASTER_URL)
                .withAutoOAuthToken(OAUTH_TOKEN)
                .withNamespace(NAMESPACE);
    }

    private String getCertificateAuthorityData(final Path tempDir) throws GeneralSecurityException, IOException {
        final String certificateAuthorityPem = Files.readString(Path.of(getCertificateAuthorityFile(tempDir)));
        return Base64.getEncoder().encodeToString(certificateAuthorityPem.getBytes(StandardCharsets.UTF_8));
    }

    private String getCertificateAuthorityFile(final Path tempDir) throws GeneralSecurityException, IOException {
        return writeCertificateMaterial(tempDir).certificatePath().toString();
    }

    private CertificateMaterial writeCertificateMaterial(final Path tempDir) throws GeneralSecurityException, IOException {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM);
        keyPairGenerator.initialize(KEY_SIZE);
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();

        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, new X500Principal(CERTIFICATE_SUBJECT), CERTIFICATE_VALIDITY_PERIOD).build();

        final String encodedCertificate = Base64.getMimeEncoder(64, LINE_SEPARATOR.getBytes(StandardCharsets.US_ASCII))
                .encodeToString(certificate.getEncoded());
        final String certificatePem = CERTIFICATE_HEADER
                + LINE_SEPARATOR
                + encodedCertificate
                + LINE_SEPARATOR
                + CERTIFICATE_FOOTER
                + LINE_SEPARATOR;

        final String encodedPrivateKey = Base64.getMimeEncoder(64, LINE_SEPARATOR.getBytes(StandardCharsets.US_ASCII))
                .encodeToString(keyPair.getPrivate().getEncoded());
        final String privateKeyPem = PRIVATE_KEY_HEADER
                + LINE_SEPARATOR
                + encodedPrivateKey
                + LINE_SEPARATOR
                + PRIVATE_KEY_FOOTER
                + LINE_SEPARATOR;

        final Path certificatePath = tempDir.resolve(CERTIFICATE_AUTHORITY_FILE);
        final Path privateKeyPath = tempDir.resolve(CLIENT_KEY_FILE);
        Files.writeString(certificatePath, certificatePem);
        Files.writeString(privateKeyPath, privateKeyPem);
        return new CertificateMaterial(certificatePath, privateKeyPath);
    }

    private record CertificateMaterial(Path certificatePath, Path privateKeyPath) {
    }
}
