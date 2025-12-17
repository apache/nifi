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

package org.apache.nifi.components.connector.secrets;

import org.apache.nifi.components.connector.Secret;
import org.apache.nifi.components.connector.SecretReference;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.flow.FlowManager;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ParameterProviderSecretsManager implements SecretsManager {
    private FlowManager flowManager;

    @Override
    public void initialize(final SecretsManagerInitializationContext initializationContext) {
        this.flowManager = initializationContext.getFlowManager();
    }

    @Override
    public List<Secret> getAllSecrets() {
        final List<Secret> secrets = new ArrayList<>();
        for (final SecretProvider provider : getSecretProviders()) {
            secrets.addAll(provider.getAllSecrets());
        }

        // Sort secrets by Provider Name, then Group Name, then Secret Name
        secrets.sort(Comparator.comparing(Secret::getProviderName)
            .thenComparing(Secret::getGroupName)
            .thenComparing(Secret::getName));

        return secrets;
    }

    @Override
    public Set<SecretProvider> getSecretProviders() {
        final Set<SecretProvider> providers = new HashSet<>();
        for (final ParameterProviderNode parameterProviderNode : flowManager.getAllParameterProviders()) {
            providers.add(new ParameterProviderSecretProvider(parameterProviderNode));
        }

        return providers;
    }

    @Override
    public Optional<Secret> getSecret(final SecretReference secretReference) {
        final SecretProvider provider = findProvider(secretReference);
        if (provider == null) {
            return Optional.empty();
        }

        final List<Secret> secrets = provider.getSecrets(List.of(secretReference.getFullyQualifiedName()));
        if (secrets.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(secrets.getFirst());
    }

    @Override
    public Map<SecretReference, Secret> getSecrets(final Set<SecretReference> secretReferences) {
        // Partition secret references by Provider
        final Map<SecretProvider, Set<SecretReference>> referencesByProvider = new HashMap<>();
        for (final SecretReference secretReference : secretReferences) {
            final SecretProvider provider = findProvider(secretReference);
            referencesByProvider.computeIfAbsent(provider, k -> new HashSet<>()).add(secretReference);
        }

        final Map<SecretReference, Secret> secrets = new HashMap<>();
        for (final Map.Entry<SecretProvider, Set<SecretReference>> entry : referencesByProvider.entrySet()) {
            final SecretProvider provider = entry.getKey();
            final Set<SecretReference> references = entry.getValue();

            // If no provider found, be sure to map to a null Secret rather than skipping
            if (provider == null) {
                for (final SecretReference secretReference : references) {
                    secrets.put(secretReference, null);
                }

                continue;
            }

            final List<String> secretNames = new ArrayList<>();
            references.forEach(ref -> secretNames.add(ref.getFullyQualifiedName()));
            final List<Secret> retrievedSecrets = provider.getSecrets(secretNames);
            final Map<String, Secret> secretsByName = retrievedSecrets.stream()
                .collect(Collectors.toMap(Secret::getFullyQualifiedName, Function.identity()));

            for (final SecretReference secretReference : references) {
                final Secret secret = secretsByName.get(secretReference.getFullyQualifiedName());
                secrets.put(secretReference, secret);
            }
        }

        return secrets;
    }

    private SecretProvider findProvider(final SecretReference secretReference) {
        final Set<SecretProvider> providers = getSecretProviders();

        // Search first by Provider ID, if it's provided.
        final String providerId = secretReference.getProviderId();
        if (providerId != null) {
            for (final SecretProvider provider : providers) {
                if (providerId.equals(provider.getProviderId())) {
                    return provider;
                }
            }

            // If ID is provided but doesn't match, do not consider name.
            return null;
        }

        // No Provider found by ID, search by Provider Name
        final String providerName = secretReference.getProviderName();
        if (providerName != null) {
            for (final SecretProvider provider : providers) {
                if (providerName.equals(provider.getProviderName())) {
                    return provider;
                }
            }
        }

        // No Provider found
        return null;
    }
}
