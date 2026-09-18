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

package org.apache.nifi.service.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.nifi.service.cql.api.service.CQLExecutionService;
import org.apache.nifi.service.cql.it.AbstractCqlSecureVerificationIT;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.function.UnaryOperator;

/**
 * {@link AbstractCqlSecureVerificationIT} against a single Cassandra 5.0 container configured with
 * {@code PasswordAuthenticator} and one-way {@code client_encryption_options} in {@code optional: true}
 * mode, so the {@code init.cql} bootstrap still runs over plaintext. The {@code admin} role is created once
 * afterwards through the built-in {@code cassandra} superuser. A single fixed version rather than a matrix:
 * what is exercised here is the driver's auth and SSL wiring, not anything version-specific.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CassandraSecureVerificationIT extends AbstractCqlSecureVerificationIT {

    private static final String CASSANDRA_VERSION = "5.0";

    private static final int CQL_PORT = 9042;

    private static final String KEYSTORE_CONTAINER_PATH = "/etc/cassandra/.keystore";

    // The disabled client_encryption_options block as it appears in cassandra-base-config/cassandra.yaml.
    private static final String DISABLED_ENCRYPTION_BLOCK =
            "client_encryption_options:\n\n  enabled: false\n\n  keystore: conf/.keystore\n\n  require_client_auth: false\n";

    @Override
    protected CQLExecutionService newSessionProvider() {
        return new CassandraCQLExecutionService();
    }

    @Override
    protected SecureServer startSecureServer(final KeyPair serverKeyPair, final X509Certificate serverCertificate,
                                              final String adminRole, final String adminPassword) throws Exception {
        final SslKeyStoreCredentials serverKeyStore = writeKeyStore(serverKeyPair.getPrivate(), serverCertificate, "server");

        final CassandraContainer container = CassandraContainerLimits.apply(new CassandraContainer("cassandra:" + CASSANDRA_VERSION))
                .withCopyFileToContainer(MountableFile.forHostPath(writePatchedConfig(serverKeyStore)),
                        CassandraConfigOverrides.CONTAINER_CASSANDRA_YAML_PATH)
                .withCopyFileToContainer(MountableFile.forHostPath(serverKeyStore.path()), KEYSTORE_CONTAINER_PATH)
                .withInitScript("init.cql");
        container.withExposedPorts(CQL_PORT);
        container.start();

        // init.cql runs over the optional-plaintext listener as the built-in "cassandra" superuser, which
        // Testcontainers also uses; reuse it once to create the real, generated-password admin role.
        try (CqlSession bootstrapSession = CqlSession.builder()
                .addContactPoint(container.getContactPoint())
                .withLocalDatacenter(LOCAL_DATACENTER)
                .withAuthCredentials(container.getUsername(), container.getPassword())
                .build()) {
            bootstrapSession.execute(String.format(
                    "CREATE ROLE %s WITH PASSWORD = '%s' AND LOGIN = true", adminRole, adminPassword));
        }

        final String contactPoint = container.getContainerIpAddress() + ":" + container.getMappedPort(CQL_PORT);
        return new SecureServer(contactPoint, container::stop);
    }

    /**
     * Patches {@code cassandra-base-config/cassandra.yaml} to switch on {@code PasswordAuthenticator} and an
     * enabled, one-way, optional {@code client_encryption_options} pointing at the mounted server keystore -
     * rather than checking in a near-duplicate of that ~250-line file.
     */
    private static Path writePatchedConfig(final SslKeyStoreCredentials serverKeyStore) {
        final UnaryOperator<String> enableAuth = requirePatched("authenticator",
                config -> config.replace("authenticator: AllowAllAuthenticator", "authenticator: PasswordAuthenticator"));

        final String enabledEncryptionBlock = "client_encryption_options:\n"
                + "  enabled: true\n"
                + "  optional: true\n"
                + "  keystore: " + KEYSTORE_CONTAINER_PATH + "\n"
                + "  keystore_password: " + serverKeyStore.password() + "\n"
                + "  require_client_auth: false\n"
                + "  store_type: " + STORE_TYPE + "\n";
        final UnaryOperator<String> enableEncryption = requirePatched("client_encryption_options",
                config -> config.replace(DISABLED_ENCRYPTION_BLOCK, enabledEncryptionBlock));

        return CassandraConfigOverrides.writePatchedConfig(config -> enableEncryption.apply(enableAuth.apply(config)));
    }

    private static UnaryOperator<String> requirePatched(final String what, final UnaryOperator<String> patch) {
        return config -> {
            final String patched = patch.apply(config);
            if (patched.equals(config)) {
                throw new IllegalStateException("The " + what + " patch matched nothing in the base Cassandra config");
            }
            return patched;
        };
    }
}
