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
package org.apache.nifi.service.cql.it;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.service.cql.api.service.CQLExecutionService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import javax.security.auth.x500.X500Principal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every {@code verify()} scenario that needs a specially-provisioned server, against one container per
 * backend: {@code PasswordAuthenticator} plus one-way TLS, holding an {@value #ADMIN_ROLE} role with a
 * generated password and the {@value #KEYSPACE} keyspace. This replaces three separate suites - connection
 * verification, authentication, SSL - that each booted their own container, because the session provider
 * runs them all through the same {@code verify()} path and the only thing that differed was server config.
 *
 * <p>The scenarios: a fully valid secure config succeeds every step; a wrong password and an untrusted
 * server certificate each fail {@code Establish Connection}; a bogus datacenter fails {@code Verify
 * Datacenter} (the driver builds a session with an unknown local datacenter and only fails when a statement
 * forces node selection, which {@code verify()} does deliberately); an unknown keyspace fails {@code Verify
 * Keyspace}; and a syntactically valid driver configuration file still connects. The one-way-vs-two-way TLS
 * distinction is not exercised - the provider hands the driver a single {@code SSLContext} and never
 * inspects it - and the {@code buildConfigLoader} composition itself is unit-tested without a container.
 *
 * <p>Gated behind {@link ConnectionTest}: runs only when {@value ConnectionTest#ENABLED_PROPERTY} is
 * {@code true}, since a specially-configured container is a high price for coverage that a change to query
 * or write behaviour cannot affect.
 */
@ConnectionTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractCqlSecureVerificationIT {

    protected static final String ADMIN_ROLE = "admin";

    protected static final String KEYSPACE = "testspace";

    protected static final String LOCAL_DATACENTER = "datacenter1";

    protected static final String STORE_TYPE = "PKCS12";

    private static final String SSL_CONTEXT_SERVICE_ID = "ssl-context-service";

    private static final Path SSL_ARTIFACT_DIRECTORY = createSslArtifactDirectory();

    private SecureServer server;

    private X509Certificate serverCertificate;

    private String adminPassword;

    /**
     * @return a fresh, unconfigured instance of the backend implementation under test
     */
    protected abstract CQLExecutionService newSessionProvider();

    /**
     * Boots one container with {@code PasswordAuthenticator} and one-way TLS enabled using the given
     * already-generated server certificate/key, creates an {@value #ADMIN_ROLE} role with {@code adminPassword}
     * and the {@value #KEYSPACE} keyspace, and returns its contact point once it is ready for the test
     * connections this class will make.
     */
    protected abstract SecureServer startSecureServer(KeyPair serverKeyPair, X509Certificate serverCertificate,
                                                       String adminRole, String adminPassword) throws Exception;

    /**
     * A running container's connection coordinates plus how to stop it, so the shared test methods never
     * need to know the concrete container type.
     */
    public record SecureServer(String contactPoint, Runnable shutdown) {
    }

    /**
     * A generated PKCS12 file together with the random password it was protected with. Reusable by concrete
     * subclasses for the server's own keystore where the backend's server-side TLS is also keystore-based.
     */
    public record SslKeyStoreCredentials(Path path, String password) {
    }

    @BeforeAll
    void startServer() throws Exception {
        final KeyPair serverKeyPair = generateKeyPair();
        serverCertificate = buildSelfSignedCertificate(serverKeyPair, "CN=cql-secure-server");
        adminPassword = UUID.randomUUID().toString();
        server = startSecureServer(serverKeyPair, serverCertificate, ADMIN_ROLE, adminPassword);
    }

    @AfterAll
    void stopServer() {
        if (server != null) {
            server.shutdown().run();
        }
    }

    @Test
    @DisplayName("A fully valid secure configuration reports success for every verification step")
    void testValidSecureConfigurationSucceeds() throws Exception {
        final List<ConfigVerificationResult> results = verify(adminPassword, trustStoreFor(serverCertificate), runner -> { });

        assertAllSuccessful(results);
        assertTrue(results.stream().anyMatch(result -> "Establish Connection".equals(result.getVerificationStepName())));
        assertTrue(results.stream().anyMatch(result -> "Verify Datacenter".equals(result.getVerificationStepName())));
        assertTrue(results.stream().anyMatch(result -> "Verify Keyspace".equals(result.getVerificationStepName())));
    }

    @Test
    @DisplayName("An incorrect password fails the connection step against PasswordAuthenticator")
    void testIncorrectPasswordFailsConnection() throws Exception {
        final List<ConfigVerificationResult> results =
                verify(UUID.randomUUID().toString(), trustStoreFor(serverCertificate), runner -> { });

        assertStepFailed(results, "Establish Connection");
    }

    @Test
    @DisplayName("A truststore that does not trust the server certificate fails the connection step")
    void testUntrustedServerCertificateFailsConnection() throws Exception {
        // A certificate the server never presents - the client's trust anchor is simply wrong.
        final X509Certificate unrelated = buildSelfSignedCertificate(generateKeyPair(), "CN=untrusted-server");

        final List<ConfigVerificationResult> results = verify(adminPassword, trustStoreFor(unrelated), runner -> { });

        assertStepFailed(results, "Establish Connection");
    }

    @Test
    @DisplayName("A nonexistent datacenter fails the datacenter step, caught by the statement verify() forces")
    void testInvalidDatacenterFailsDatacenterStep() throws Exception {
        final List<ConfigVerificationResult> results = verify(adminPassword, trustStoreFor(serverCertificate),
                runner -> runner.withProperty(CQLExecutionService.DATACENTER, "not-a-real-datacenter"));

        assertStepFailed(results, "Verify Datacenter");
    }

    @Test
    @DisplayName("A keyspace that does not exist fails the keyspace step")
    void testNonexistentKeyspaceFailsKeyspaceStep() throws Exception {
        final List<ConfigVerificationResult> results = verify(adminPassword, trustStoreFor(serverCertificate),
                runner -> runner.withProperty(CQLExecutionService.KEYSPACE, "keyspace_that_does_not_exist"));

        assertStepFailed(results, "Verify Keyspace");
    }

    @Test
    @DisplayName("A syntactically valid driver configuration file still connects on top of auth and TLS")
    void testDriverConfigurationFileSucceeds() throws Exception {
        final Path configFile = Files.createTempFile("CqlDriverConfig", ".conf");
        Files.writeString(configFile, """
                datastax-java-driver {
                  basic.request.timeout = 15 seconds
                }
                """, StandardCharsets.UTF_8);
        configFile.toFile().deleteOnExit();

        final List<ConfigVerificationResult> results = verify(adminPassword, trustStoreFor(serverCertificate),
                runner -> runner.withProperty(CQLExecutionService.DRIVER_CONFIGURATION_FILE, configFile.toString()));

        assertAllSuccessful(results);
    }

    /**
     * Runs {@code verify()} with the {@value #ADMIN_ROLE} credentials, the given password, and an SSL
     * context service configured with the given truststore, applying {@code customizer} last so a scenario
     * can override one property before verification.
     */
    private List<ConfigVerificationResult> verify(final String password, final SslKeyStoreCredentials trustStore,
                                                   final Consumer<CqlServiceRunner> customizer) throws Exception {
        final CqlServiceRunner serviceRunner = CqlServiceRunner.forService(newSessionProvider())
                .withConnection(server.contactPoint(), LOCAL_DATACENTER, KEYSPACE)
                .withProperty(CQLExecutionService.USERNAME, ADMIN_ROLE)
                .withProperty(CQLExecutionService.PASSWORD, password);

        final TestRunner runner = serviceRunner.runner();
        final StandardSSLContextService sslContextService = new StandardSSLContextService();
        runner.addControllerService(SSL_CONTEXT_SERVICE_ID, sslContextService);
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, trustStore.path().toString());
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, trustStore.password());
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, STORE_TYPE);
        runner.enableControllerService(sslContextService);
        serviceRunner.withProperty(CQLExecutionService.PROP_SSL_CONTEXT_SERVICE, SSL_CONTEXT_SERVICE_ID);

        customizer.accept(serviceRunner);
        return serviceRunner.verify();
    }

    private SslKeyStoreCredentials trustStoreFor(final X509Certificate certificate) throws Exception {
        return writeTrustStore(certificate, "trusted");
    }

    private static void assertAllSuccessful(final List<ConfigVerificationResult> results) {
        assertFalse(results.isEmpty());
        for (final ConfigVerificationResult result : results) {
            assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, result.getOutcome(), result.getExplanation());
        }
    }

    private static void assertStepFailed(final List<ConfigVerificationResult> results, final String stepName) {
        final ConfigVerificationResult stepResult = results.stream()
                .filter(result -> stepName.equals(result.getVerificationStepName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Expected a '" + stepName + "' verification result in " + results));

        assertEquals(ConfigVerificationResult.Outcome.FAILED, stepResult.getOutcome(), stepResult.getExplanation());
    }

    // ---- TLS material helpers, shared with the concrete subclasses' server provisioning ----------------

    protected static KeyPair generateKeyPair() throws Exception {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048);
        return keyPairGenerator.generateKeyPair();
    }

    protected static X509Certificate buildSelfSignedCertificate(final KeyPair keyPair, final String distinguishedName) {
        return new StandardCertificateBuilder(keyPair, new X500Principal(distinguishedName), Duration.ofDays(1))
                .setDnsSubjectAlternativeNames(List.of("localhost"))
                .build();
    }

    protected static SslKeyStoreCredentials writeKeyStore(final PrivateKey privateKey, final X509Certificate certificate, final String alias) throws Exception {
        final String password = generateStorePassword();
        final KeyStore keyStore = KeyStore.getInstance(STORE_TYPE);
        keyStore.load(null);
        keyStore.setKeyEntry(alias, privateKey, password.toCharArray(), new X509Certificate[]{certificate});

        final Path path = Files.createTempFile(SSL_ARTIFACT_DIRECTORY, alias + "-keystore", ".p12");
        try (OutputStream outputStream = Files.newOutputStream(path)) {
            keyStore.store(outputStream, password.toCharArray());
        }
        return new SslKeyStoreCredentials(path, password);
    }

    protected static SslKeyStoreCredentials writeTrustStore(final X509Certificate certificate, final String alias) throws Exception {
        final String password = generateStorePassword();
        final KeyStore trustStore = KeyStore.getInstance(STORE_TYPE);
        trustStore.load(null);
        trustStore.setCertificateEntry(alias, certificate);

        final Path path = Files.createTempFile(SSL_ARTIFACT_DIRECTORY, alias + "-truststore", ".p12");
        try (OutputStream outputStream = Files.newOutputStream(path)) {
            trustStore.store(outputStream, password.toCharArray());
        }
        return new SslKeyStoreCredentials(path, password);
    }

    private static String generateStorePassword() {
        final SecureRandom secureRandom = new SecureRandom();
        final byte[] bytes = new byte[24];
        secureRandom.nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }

    // Written under the module's own target/ directory so "mvn clean" collects the generated stores rather
    // than relying on deleteOnExit().
    private static Path createSslArtifactDirectory() {
        try {
            return Files.createDirectories(Path.of("target", "cql-secure-it"));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
