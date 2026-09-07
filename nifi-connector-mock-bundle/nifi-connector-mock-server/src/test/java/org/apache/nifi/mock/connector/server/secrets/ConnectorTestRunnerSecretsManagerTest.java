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

package org.apache.nifi.mock.connector.server.secrets;

import org.apache.nifi.components.connector.Secret;
import org.apache.nifi.components.connector.SecretReference;
import org.apache.nifi.mock.connector.server.StandardConnectorMockServer;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectorTestRunnerSecretsManagerTest {
    private static final String SECRET_NAME = "password";
    private static final String SECRET_VALUE = "secret-value";
    private static final String FULLY_QUALIFIED_SECRET_NAME = ConnectorTestRunnerSecretProvider.SECRET_PROVIDER_NAME + "."
        + ConnectorTestRunnerSecretProvider.GROUP_NAME + "." + SECRET_NAME;

    @Test
    void testGetSecretUsingDiscoveredFullyQualifiedName() {
        final ConnectorTestRunnerSecretsManager secretsManager = new ConnectorTestRunnerSecretsManager();
        secretsManager.addSecret(SECRET_NAME, SECRET_VALUE);

        final Secret discoveredSecret = secretsManager.getAllSecrets().getFirst();
        assertEquals(FULLY_QUALIFIED_SECRET_NAME, discoveredSecret.getFullyQualifiedName());

        final SecretReference secretReference = new SecretReference(
            discoveredSecret.getProviderId(),
            discoveredSecret.getProviderName(),
            discoveredSecret.getName(),
            discoveredSecret.getFullyQualifiedName()
        );

        final Optional<Secret> resolvedSecret = secretsManager.getSecret(secretReference);

        assertTrue(resolvedSecret.isPresent(), "Discovered secret should be resolved using its fully qualified name");
        final Secret secret = resolvedSecret.orElseThrow();
        assertEquals(SECRET_NAME, secret.getName());
        assertEquals(FULLY_QUALIFIED_SECRET_NAME, secret.getFullyQualifiedName());
        assertEquals(SECRET_VALUE, secret.getValue());
    }

    @Test
    void testCreateSecretReferenceUsesFullyQualifiedName() {
        final ConnectorTestRunnerSecretsManager secretsManager = new ConnectorTestRunnerSecretsManager();
        secretsManager.addSecret(SECRET_NAME, SECRET_VALUE);
        final StandardConnectorMockServer server = new StandardConnectorMockServer();

        final SecretReference secretReference = server.createSecretReference(SECRET_NAME);

        assertEquals(FULLY_QUALIFIED_SECRET_NAME, secretReference.getFullyQualifiedName());
        assertEquals(SECRET_VALUE, secretsManager.getSecret(secretReference).orElseThrow().getValue());
    }

    @Test
    void testGetSecretWithDotInName() {
        final String secretName = "database.password";
        final ConnectorTestRunnerSecretsManager secretsManager = new ConnectorTestRunnerSecretsManager();
        secretsManager.addSecret(secretName, SECRET_VALUE);

        final Secret discoveredSecret = secretsManager.getAllSecrets().getFirst();
        final SecretReference secretReference = new SecretReference(
            discoveredSecret.getProviderId(),
            discoveredSecret.getProviderName(),
            discoveredSecret.getName(),
            discoveredSecret.getFullyQualifiedName()
        );

        assertEquals("TestRunnerSecretsManager.Default.database.password", discoveredSecret.getFullyQualifiedName());
        assertEquals(SECRET_VALUE, secretsManager.getSecret(secretReference).orElseThrow().getValue());
    }
}
