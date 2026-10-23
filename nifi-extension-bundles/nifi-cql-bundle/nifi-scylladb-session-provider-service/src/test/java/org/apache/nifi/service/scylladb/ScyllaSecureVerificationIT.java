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

package org.apache.nifi.service.scylladb;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.nifi.service.cql.api.service.CQLExecutionService;
import org.apache.nifi.service.cql.it.AbstractCqlSecureVerificationIT;
import org.apache.nifi.service.cql.it.CqlDdl;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.scylladb.ScyllaDBContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.UUID;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * {@link AbstractCqlSecureVerificationIT} against a single ScyllaDB container configured with
 * {@code PasswordAuthenticator} and one-way (PEM-based) {@code client_encryption_options}. ScyllaDB has no
 * plaintext fallback once TLS is enabled and (as of image 2026.2) provisions no default superuser, so a
 * bootstrap superuser is baked into {@code scylla.yaml} via {@code auth_superuser_*} and used once, over a
 * TLS session, to create the real {@code admin} role and the test keyspace - the same bootstrap material
 * {@code ScyllaAuthenticationIT} and {@code ScyllaSslIT} used before this suite merged them.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScyllaSecureVerificationIT extends AbstractCqlSecureVerificationIT {

    private static final String IMAGE = "scylladb/scylla:2026.2";

    private static final int CQL_PORT = 9042;

    private static final String BASE_CONFIG_RESOURCE = "scylla-ssl-base-config/scylla.yaml";

    private static final String SCYLLA_YAML_CONTAINER_PATH = "/etc/scylla/scylla.yaml";

    private static final String SERVER_CERT_CONTAINER_PATH = "/etc/scylla/ssl/server.crt";

    private static final String SERVER_KEY_CONTAINER_PATH = "/etc/scylla/ssl/server.key";

    private static final String CERTIFICATE_PEM_FORMAT = "-----BEGIN CERTIFICATE-----%n%s%n-----END CERTIFICATE-----%n";

    private static final String PRIVATE_KEY_PEM_FORMAT = "-----BEGIN PRIVATE KEY-----%n%s%n-----END PRIVATE KEY-----%n";

    private static final String BOOTSTRAP_USERNAME = "bootstrap_admin";

    private static final String BOOTSTRAP_PASSWORD = "cassandra-bootstrap-only";

    // The SHA-512-crypt ($6$) hash of BOOTSTRAP_PASSWORD, in the form auth_superuser_salted_password expects.
    private static final String BOOTSTRAP_SALTED_PASSWORD =
            "$6$Yk/rTi56stmYje0c$JfFJrNQa5wUQAV/ZyyHqaAtGs.4CLcHbLh6RANryG6/pxeHvmPO0/eA8mSUY5HHs5w9sNFtwp.008MCHKo5ty.";

    @Override
    protected CQLExecutionService newSessionProvider() {
        return new ScyllaDBCQLExecutionService();
    }

    @Override
    protected SecureServer startSecureServer(final KeyPair serverKeyPair, final X509Certificate serverCertificate,
                                              final String adminRole, final String adminPassword) throws Exception {
        final Path targetDirectory = Files.createDirectories(
                Paths.get("target", "scylla-secure-it", UUID.randomUUID().toString()));

        final Path serverCertPem = writeCertificatePem(serverCertificate, targetDirectory);
        final Path serverKeyPem = writePrivateKeyPem(serverKeyPair.getPrivate(), targetDirectory);
        final Path serverYaml = buildServerYaml(targetDirectory);

        final ScyllaDBContainer container = ScyllaContainerLimits.apply(new ScyllaDBContainer(IMAGE))
                .withCopyFileToContainer(MountableFile.forHostPath(serverYaml), SCYLLA_YAML_CONTAINER_PATH)
                .withCopyFileToContainer(MountableFile.forHostPath(serverCertPem), SERVER_CERT_CONTAINER_PATH)
                .withCopyFileToContainer(MountableFile.forHostPath(serverKeyPem), SERVER_KEY_CONTAINER_PATH);
        container.withExposedPorts(CQL_PORT);
        container.start();

        // The only way to reach this container: TLS (no plaintext fallback), as the bootstrap superuser
        // baked into scylla.yaml. Used once to create the real admin role and the test keyspace.
        final SslKeyStoreCredentials bootstrapTrustStore = writeTrustStore(serverCertificate, "server");
        try (CqlSession bootstrapSession = CqlSession.builder()
                .addContactPoint(container.getContactPoint())
                .withLocalDatacenter(LOCAL_DATACENTER)
                .withAuthCredentials(BOOTSTRAP_USERNAME, BOOTSTRAP_PASSWORD)
                .withSslContext(trustOnlySslContext(bootstrapTrustStore))
                .withConfigLoader(ScyllaDdlTimeouts.longSchemaTimeoutConfigLoader())
                .build()) {
            CqlDdl.executeWithRetry(bootstrapSession, String.format(
                    "CREATE ROLE IF NOT EXISTS %s WITH PASSWORD = '%s' AND LOGIN = true AND SUPERUSER = true",
                    adminRole, adminPassword));
            CqlDdl.executeWithRetry(bootstrapSession, "create keyspace if not exists " + KEYSPACE
                    + " with replication = { 'class': 'NetworkTopologyStrategy', '" + LOCAL_DATACENTER + "': 1};");
        }

        final String contactPoint = container.getContainerIpAddress() + ":" + container.getMappedPort(CQL_PORT);
        return new SecureServer(contactPoint, container::stop);
    }

    /**
     * Assembles {@code scylla.yaml} from the checked-in base template plus a one-way
     * {@code client_encryption_options} block and the {@code authenticator}/{@code auth_superuser_*}
     * settings - the two blocks {@code ScyllaSslIT} and {@code ScyllaAuthenticationIT} appended separately.
     */
    private Path buildServerYaml(final Path targetDirectory) throws IOException {
        final String baseYaml;
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(BASE_CONFIG_RESOURCE)) {
            if (inputStream == null) {
                throw new IOException("Classpath resource not found: " + BASE_CONFIG_RESOURCE);
            }
            baseYaml = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }

        final String encryptionOptions = "client_encryption_options:\n"
                + "  enabled: true\n"
                + "  certificate: " + SERVER_CERT_CONTAINER_PATH + "\n"
                + "  keyfile: " + SERVER_KEY_CONTAINER_PATH + "\n"
                + "  require_client_auth: false\n";

        final String authOptions = "authenticator: PasswordAuthenticator\n"
                + "auth_superuser_name: " + BOOTSTRAP_USERNAME + "\n"
                + "auth_superuser_salted_password: \"" + BOOTSTRAP_SALTED_PASSWORD + "\"\n";

        final Path yamlFile = targetDirectory.resolve("scylla.yaml");
        Files.writeString(yamlFile, baseYaml + "\n" + encryptionOptions + "\n" + authOptions);
        return yamlFile;
    }

    private static SSLContext trustOnlySslContext(final SslKeyStoreCredentials trustStore) throws Exception {
        final KeyStore trustKeyStore = KeyStore.getInstance(STORE_TYPE);
        try (InputStream inputStream = Files.newInputStream(trustStore.path())) {
            trustKeyStore.load(inputStream, trustStore.password().toCharArray());
        }
        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustKeyStore);

        final SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
        return sslContext;
    }

    private static Path writeCertificatePem(final X509Certificate certificate, final Path targetDirectory) throws Exception {
        final String encoded = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.UTF_8)).encodeToString(certificate.getEncoded());
        final Path path = targetDirectory.resolve("server-cert.pem");
        Files.writeString(path, String.format(CERTIFICATE_PEM_FORMAT, encoded));
        return path;
    }

    private static Path writePrivateKeyPem(final PrivateKey privateKey, final Path targetDirectory) throws IOException {
        final String encoded = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.UTF_8)).encodeToString(privateKey.getEncoded());
        final Path path = targetDirectory.resolve("server-key.pem");
        Files.writeString(path, String.format(PRIVATE_KEY_PEM_FORMAT, encoded));
        return path;
    }
}
