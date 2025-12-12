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
import org.apache.nifi.components.connector.secrets.SecretProvider;
import org.apache.nifi.components.connector.secrets.SecretsManager;
import org.apache.nifi.components.connector.secrets.SecretsManagerInitializationContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ConnectorTestRunnerSecretsManager implements SecretsManager {
    private final ConnectorTestRunnerSecretProvider secretProvider = new ConnectorTestRunnerSecretProvider();

    @Override
    public void initialize(final SecretsManagerInitializationContext initializationContext) {
    }

    public void addSecret(final String name, final String value) {
        secretProvider.addSecret(name, value);
    }

    @Override
    public List<Secret> getAllSecrets() {
        return secretProvider.getAllSecrets();
    }

    @Override
    public Set<SecretProvider> getSecretProviders() {
        return Set.of(secretProvider);
    }

    @Override
    public Optional<Secret> getSecret(final SecretReference secretReference) {
        // Check that appropriate provider given
        final SecretProvider provider = getProvider(secretReference);
        if (provider == null) {
            return Optional.empty();
        }

        final List<Secret> secrets = provider.getSecrets(List.of(secretReference.getSecretName()));
        return secrets.isEmpty() ? Optional.empty() : Optional.of(secrets.getFirst());
    }

    private SecretProvider getProvider(final SecretReference secretReference) {
        final String providerId = secretReference.getProviderId();
        if (providerId != null) {
            for (final SecretProvider provider : getSecretProviders()) {
                if (provider.getProviderId().equals(providerId)) {
                    return provider;
                }
            }

            return null;
        }

        final String providerName = secretReference.getProviderName();
        if (providerName != null) {
            for (final SecretProvider provider : getSecretProviders()) {
                if (provider.getProviderName().equals(providerName)) {
                    return provider;
                }
            }
        }

        return null;
    }

    @Override
    public Map<SecretReference, Secret> getSecrets(final Set<SecretReference> secretReferences) {
        final Map<SecretReference, Secret> secrets = new HashMap<>();
        for (final SecretReference reference : secretReferences) {
            final Secret secret = getSecret(reference).orElse(null);
            secrets.put(reference, secret);
        }

        return secrets;
    }
}
