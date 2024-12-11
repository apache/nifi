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
package org.apache.nifi.ssl;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.ssl.PEMEncodedSSLContextProvider.PrivateKeySource;
import org.apache.nifi.ssl.PEMEncodedSSLContextProvider.CertificateAuthoritiesSource;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.util.TlsPlatform;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PEMEncodedSSLContextProviderTest {
    private static final String RSA_ALGORITHM = "RSA";

    private static final int RSA_KEY_SIZE = 3072;

    private static final String SERVICE_ID = PEMEncodedSSLContextProviderTest.class.getSimpleName();

    private static final String PRIVATE_KEY_HEADER = "-----BEGIN PRIVATE KEY-----";

    private static final String PRIVATE_KEY_FOOTER = "-----END PRIVATE KEY-----";

    private static final String CERTIFICATE_HEADER = "-----BEGIN CERTIFICATE-----";

    private static final String CERTIFICATE_FOOTER = "-----END CERTIFICATE-----";

    private static final String CERTIFICATE_INVALID = "%s%nINVALID".formatted(CERTIFICATE_HEADER);

    private static final String PRIVATE_KEY_FILE_PREFIX = "private-key";

    private static final String CERTIFICATE_CHAIN_FILE_PREFIX = "certificate-chain";

    private static final String PEM_EXTENSION = ".pem";

    private static final String LINE_LENGTH_PATTERN = "(?<=\\G.{64})";

    private static final char LINE_FEED = 10;

    private static final Base64.Encoder encoder = Base64.getEncoder();

    private static String privateKeyPemEncoded;

    private static String certificatePemEncoded;

    private static X509Certificate certificate;

    private TestRunner runner;

    private PEMEncodedSSLContextProvider provider;

    @BeforeAll
    static void setKeyCertificate() throws Exception {
        final KeyPairGenerator rsaKeyPairGenerator = KeyPairGenerator.getInstance(RSA_ALGORITHM);
        rsaKeyPairGenerator.initialize(RSA_KEY_SIZE);
        final KeyPair rsaKeyPair = rsaKeyPairGenerator.generateKeyPair();
        final PrivateKey rsaPrivateKey = rsaKeyPair.getPrivate();
        final byte[] rsaPrivateKeyEncoded = rsaPrivateKey.getEncoded();
        privateKeyPemEncoded = getPrivateKeyPemEncoded(rsaPrivateKeyEncoded);

        certificate = new StandardCertificateBuilder(rsaKeyPair, new X500Principal("CN=localhost"), Duration.ofHours(1)).build();
        final byte[] certificateEncoded = certificate.getEncoded();
        certificatePemEncoded = getCertificatePemEncoded(certificateEncoded);
    }

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        provider = new PEMEncodedSSLContextProvider();
        runner.addControllerService(SERVICE_ID, provider);
    }

    @Test
    void testVerifySystemCertificateAuthorities() {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(PEMEncodedSSLContextProvider.PRIVATE_KEY_SOURCE, PrivateKeySource.UNDEFINED.getValue());
        properties.put(PEMEncodedSSLContextProvider.CERTIFICATE_AUTHORITIES_SOURCE, CertificateAuthoritiesSource.SYSTEM.getValue());

        final ControllerServiceLookup serviceLookup = runner.getProcessContext().getControllerServiceLookup();
        final ConfigurationContext context = new MockConfigurationContext(provider, properties, serviceLookup, Collections.emptyMap());

        final List<ConfigVerificationResult> results = provider.verify(context, runner.getLogger(), Map.of());

        assertNotNull(results);
        assertFalse(results.isEmpty());

        for (final ConfigVerificationResult result : results) {
            assertNotEquals(ConfigVerificationResult.Outcome.FAILED, result.getOutcome(), result.getExplanation());
        }
    }

    @Test
    void testVerifyPrivateKeyCertificateChain() {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(PEMEncodedSSLContextProvider.PRIVATE_KEY_SOURCE, PrivateKeySource.PROPERTIES.getValue());
        properties.put(PEMEncodedSSLContextProvider.PRIVATE_KEY, privateKeyPemEncoded);
        properties.put(PEMEncodedSSLContextProvider.CERTIFICATE_CHAIN, certificatePemEncoded);
        properties.put(PEMEncodedSSLContextProvider.CERTIFICATE_AUTHORITIES_SOURCE, CertificateAuthoritiesSource.SYSTEM.getValue());

        final ControllerServiceLookup serviceLookup = runner.getProcessContext().getControllerServiceLookup();
        final ConfigurationContext context = new MockConfigurationContext(provider, properties, serviceLookup, Collections.emptyMap());

        final List<ConfigVerificationResult> results = provider.verify(context, runner.getLogger(), Map.of());

        assertNotNull(results);
        assertFalse(results.isEmpty());

        for (final ConfigVerificationResult result : results) {
            assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, result.getOutcome(), result.getExplanation());
        }
    }

    @Test
    void testCreateContextSystemCertificateAuthorities() {
        runner.setProperty(provider, PEMEncodedSSLContextProvider.PRIVATE_KEY_SOURCE, PrivateKeySource.UNDEFINED);
        runner.setProperty(provider, PEMEncodedSSLContextProvider.CERTIFICATE_AUTHORITIES_SOURCE, CertificateAuthoritiesSource.SYSTEM);

        runner.enableControllerService(provider);

        final SSLContext sslContext = provider.createContext();

        assertNotNull(sslContext);

        runner.disableControllerService(provider);
    }

    @Test
    void testCreateContextProtocolConfigured() {
        runner.setProperty(provider, PEMEncodedSSLContextProvider.PRIVATE_KEY_SOURCE, PrivateKeySource.UNDEFINED);
        runner.setProperty(provider, PEMEncodedSSLContextProvider.CERTIFICATE_AUTHORITIES_SOURCE, CertificateAuthoritiesSource.SYSTEM);

        final String protocol = TlsPlatform.getLatestProtocol();

        runner.setProperty(provider, PEMEncodedSSLContextProvider.TLS_PROTOCOL, protocol);
        runner.enableControllerService(provider);

        final SSLContext sslContext = provider.createContext();

        assertNotNull(sslContext);
        assertEquals(protocol, sslContext.getProtocol());
    }

    @Test
    void testCreateContextPrivateKeyCertificateConfigured() {
        runner.setProperty(provider, PEMEncodedSSLContextProvider.CERTIFICATE_AUTHORITIES_SOURCE, CertificateAuthoritiesSource.SYSTEM);

        runner.setProperty(provider, PEMEncodedSSLContextProvider.PRIVATE_KEY_SOURCE, PrivateKeySource.PROPERTIES);
        runner.setProperty(provider, PEMEncodedSSLContextProvider.PRIVATE_KEY, privateKeyPemEncoded);
        runner.setProperty(provider, PEMEncodedSSLContextProvider.CERTIFICATE_CHAIN, certificatePemEncoded);
        runner.enableControllerService(provider);

        final SSLContext sslContext = provider.createContext();

        assertNotNull(sslContext);
    }

    @Test
    void testCreateContextPrivateKeyCertificateLocationsConfigured(@TempDir final Path tempDir) throws IOException {
        runner.setProperty(provider, PEMEncodedSSLContextProvider.CERTIFICATE_AUTHORITIES_SOURCE, CertificateAuthoritiesSource.SYSTEM);

        runner.setProperty(provider, PEMEncodedSSLContextProvider.PRIVATE_KEY_SOURCE, PrivateKeySource.FILES);

        final Path privateKeyPath = Files.createTempFile(tempDir, PRIVATE_KEY_FILE_PREFIX, PEM_EXTENSION);
        Files.writeString(privateKeyPath, privateKeyPemEncoded);

        final Path certificateChainPath = Files.createTempFile(tempDir, CERTIFICATE_CHAIN_FILE_PREFIX, PEM_EXTENSION);
        Files.writeString(certificateChainPath, certificatePemEncoded);

        runner.setProperty(provider, PEMEncodedSSLContextProvider.PRIVATE_KEY_LOCATION, privateKeyPath.toString());
        runner.setProperty(provider, PEMEncodedSSLContextProvider.CERTIFICATE_CHAIN_LOCATION, certificateChainPath.toString());
        runner.enableControllerService(provider);

        final SSLContext sslContext = provider.createContext();

        assertNotNull(sslContext);
    }

    @Test
    void testCreateKeyManagerSourceUndefined() {
        runner.setProperty(provider, PEMEncodedSSLContextProvider.PRIVATE_KEY_SOURCE, PrivateKeySource.UNDEFINED);
        runner.setProperty(provider, PEMEncodedSSLContextProvider.CERTIFICATE_AUTHORITIES_SOURCE, CertificateAuthoritiesSource.SYSTEM);

        runner.enableControllerService(provider);

        final Optional<X509ExtendedKeyManager> keyManagerCreated = provider.createKeyManager();

        assertTrue(keyManagerCreated.isEmpty());
    }

    @Test
    void testCreateKeyManager() {
        runner.setProperty(provider, PEMEncodedSSLContextProvider.CERTIFICATE_AUTHORITIES_SOURCE, CertificateAuthoritiesSource.SYSTEM);

        runner.setProperty(provider, PEMEncodedSSLContextProvider.PRIVATE_KEY, privateKeyPemEncoded);
        runner.setProperty(provider, PEMEncodedSSLContextProvider.CERTIFICATE_CHAIN, certificatePemEncoded);

        runner.enableControllerService(provider);

        final Optional<X509ExtendedKeyManager> keyManagerCreated = provider.createKeyManager();
        assertTrue(keyManagerCreated.isPresent());

        final X509ExtendedKeyManager keyManager = keyManagerCreated.get();

        final String[] serverAliases = keyManager.getServerAliases(RSA_ALGORITHM, null);
        assertNotNull(serverAliases);

        final String[] clientAliases = keyManager.getClientAliases(RSA_ALGORITHM, null);
        assertNotNull(clientAliases);
    }

    @Test
    void testCreateKeyManagerPrivateKeyInvalid() {
        runner.setProperty(provider, PEMEncodedSSLContextProvider.CERTIFICATE_AUTHORITIES_SOURCE, CertificateAuthoritiesSource.SYSTEM);

        runner.setProperty(provider, PEMEncodedSSLContextProvider.PRIVATE_KEY, certificatePemEncoded);
        runner.setProperty(provider, PEMEncodedSSLContextProvider.CERTIFICATE_CHAIN, certificatePemEncoded);

        assertThrows(AssertionError.class, () -> runner.enableControllerService(provider));
    }

    @Test
    void testCreateTrustManagerSystem() {
        runner.setProperty(provider, PEMEncodedSSLContextProvider.PRIVATE_KEY_SOURCE, PrivateKeySource.UNDEFINED);
        runner.setProperty(provider, PEMEncodedSSLContextProvider.CERTIFICATE_AUTHORITIES_SOURCE, CertificateAuthoritiesSource.SYSTEM);

        runner.enableControllerService(provider);

        final X509TrustManager trustManager = provider.createTrustManager();

        assertNotNull(trustManager);

        final List<X509Certificate> acceptedIssuers = Arrays.asList(trustManager.getAcceptedIssuers());
        final boolean issuerFound = acceptedIssuers.stream().anyMatch(certificate::equals);
        assertFalse(issuerFound);
    }

    @Test
    void testCreateTrustManagerInvalid() {
        runner.setProperty(provider, PEMEncodedSSLContextProvider.PRIVATE_KEY_SOURCE, PrivateKeySource.UNDEFINED);
        runner.setProperty(provider, PEMEncodedSSLContextProvider.CERTIFICATE_AUTHORITIES_SOURCE, CertificateAuthoritiesSource.PROPERTIES);

        runner.setProperty(provider, PEMEncodedSSLContextProvider.CERTIFICATE_AUTHORITIES, CERTIFICATE_INVALID);

        assertThrows(AssertionError.class, () -> runner.enableControllerService(provider));
    }

    @Test
    void testCreateTrustManagerContentEncoded() {
        runner.setProperty(provider, PEMEncodedSSLContextProvider.PRIVATE_KEY_SOURCE, PrivateKeySource.UNDEFINED);
        runner.setProperty(provider, PEMEncodedSSLContextProvider.CERTIFICATE_AUTHORITIES_SOURCE, CertificateAuthoritiesSource.PROPERTIES);
        runner.setProperty(provider, PEMEncodedSSLContextProvider.CERTIFICATE_AUTHORITIES, certificatePemEncoded);

        runner.enableControllerService(provider);

        final X509TrustManager trustManager = provider.createTrustManager();

        assertNotNull(trustManager);

        final List<X509Certificate> acceptedIssuers = Arrays.asList(trustManager.getAcceptedIssuers());
        final boolean issuerFound = acceptedIssuers.stream().anyMatch(certificate::equals);
        assertTrue(issuerFound);
    }

    @Test
    void testCreateTrustManagerFilePath(@TempDir final Path tempDir) throws IOException {
        runner.setProperty(provider, PEMEncodedSSLContextProvider.PRIVATE_KEY_SOURCE, PrivateKeySource.UNDEFINED);
        runner.setProperty(provider, PEMEncodedSSLContextProvider.CERTIFICATE_AUTHORITIES_SOURCE, CertificateAuthoritiesSource.PROPERTIES);

        final Path certificateFile = Files.createTempFile(tempDir, PEMEncodedSSLContextProviderTest.class.getSimpleName(), PEM_EXTENSION);
        Files.writeString(certificateFile, certificatePemEncoded);

        runner.setProperty(provider, PEMEncodedSSLContextProvider.CERTIFICATE_AUTHORITIES, certificateFile.toString());

        runner.enableControllerService(provider);

        final X509TrustManager trustManager = provider.createTrustManager();

        assertNotNull(trustManager);

        final List<X509Certificate> acceptedIssuers = Arrays.asList(trustManager.getAcceptedIssuers());
        final boolean issuerFound = acceptedIssuers.stream().anyMatch(certificate::equals);
        assertTrue(issuerFound);
    }

    private static String getPrivateKeyPemEncoded(final byte[] privateKeyEncoded) {
        final StringBuilder builder = new StringBuilder();
        builder.append(PRIVATE_KEY_HEADER);
        builder.append(LINE_FEED);

        appendLines(privateKeyEncoded, builder);

        builder.append(PRIVATE_KEY_FOOTER);
        builder.append(LINE_FEED);

        return builder.toString();
    }

    private static String getCertificatePemEncoded(final byte[] certificateEncoded) {
        final StringBuilder builder = new StringBuilder();
        builder.append(CERTIFICATE_HEADER);
        builder.append(LINE_FEED);

        appendLines(certificateEncoded, builder);

        builder.append(CERTIFICATE_FOOTER);
        builder.append(LINE_FEED);

        return builder.toString();
    }

    private static void appendLines(final byte[] encoded, final StringBuilder builder) {
        final String formatted = encoder.encodeToString(encoded);
        final String[] lines = formatted.split(LINE_LENGTH_PATTERN);

        for (final String line : lines) {
            builder.append(line);
            builder.append(LINE_FEED);
        }
    }
}
