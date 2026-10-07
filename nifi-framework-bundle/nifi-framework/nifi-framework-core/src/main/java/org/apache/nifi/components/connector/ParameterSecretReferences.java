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

package org.apache.nifi.components.connector;

import org.apache.nifi.asset.Asset;
import org.apache.nifi.components.connector.components.ParameterValue;
import org.apache.nifi.components.connector.secrets.SecretsManager;
import org.apache.nifi.flow.VersionedConnector;
import org.apache.nifi.flow.VersionedConnectorValueReference;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.security.encryption.PropertyEncryptionEncoder;
import org.apache.nifi.security.encryption.PropertyEncryptionException;
import org.apache.nifi.security.encryption.PropertyEncryptionProvider;
import org.apache.nifi.security.encryption.SensitivePropertyCodec;
import org.apache.nifi.security.encryption.SensitivePropertyContext;
import org.apache.nifi.security.encryption.SensitivePropertyContextFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Matches and resolves relationships between Connector-managed Parameters and Secret References. Sensitive parameter
 * values are persisted encrypted with the sensitive properties key, as in any other Parameter Context. A sensitive
 * parameter whose value is exactly one configured secret is also bound to that Secret Reference, and on restore takes the
 * current value of the secret from the {@link SecretsManager} instead of its persisted value.
 */
final class ParameterSecretReferences {

    private ParameterSecretReferences() {
    }

    /**
     * Converts a persisted Connector's managed parameter secret references into runtime Secret References.
     * Entries that are null or not a secret reference are omitted.
     */
    static Map<String, SecretReference> convertManagedParameterSecretReferences(final VersionedConnector versionedConnector) {
        final Map<String, VersionedConnectorValueReference> versionedReferences = versionedConnector.getManagedParameterSecretReferences();
        if (versionedReferences == null || versionedReferences.isEmpty()) {
            return Map.of();
        }

        final Map<String, SecretReference> references = new HashMap<>();
        for (final Map.Entry<String, VersionedConnectorValueReference> entry : versionedReferences.entrySet()) {
            final VersionedConnectorValueReference versionedReference = entry.getValue();
            if (versionedReference == null || !ConnectorValueType.SECRET_REFERENCE.name().equals(versionedReference.getValueType())) {
                continue;
            }

            references.put(entry.getKey(), new SecretReference(versionedReference.getProviderId(), versionedReference.getProviderName(),
                versionedReference.getSecretName(), versionedReference.getFullyQualifiedSecretName()));
        }
        return references;
    }

    /**
     * Returns the Secret Reference for each sensitive parameter whose value equals exactly one of the supplied secrets.
     * A parameter whose value matches more than one reference is omitted.
     */
    static Map<String, SecretReference> matchSensitiveParametersToSecretReferences(final ParameterContext parameterContext, final Map<SecretReference, Secret> secrets) {
        final Map<String, SecretReference> references = new HashMap<>();
        if (parameterContext == null || secrets == null || secrets.isEmpty()) {
            return references;
        }

        for (final Parameter parameter : parameterContext.getParameters().values()) {
            final String value = parameter.getValue();
            final List<Asset> assets = parameter.getReferencedAssets();
            if (!parameter.getDescriptor().isSensitive() || value == null || value.isEmpty() || assets != null && !assets.isEmpty()) {
                continue;
            }

            SecretReference matchingReference = null;
            boolean ambiguous = false;
            for (final Map.Entry<SecretReference, Secret> entry : secrets.entrySet()) {
                final Secret secret = entry.getValue();
                if (secret == null || !value.equals(secret.getValue())) {
                    continue;
                }

                if (matchingReference == null) {
                    matchingReference = entry.getKey();
                } else if (!matchingReference.equals(entry.getKey())) {
                    ambiguous = true;
                    break;
                }
            }

            if (matchingReference != null && !ambiguous) {
                references.put(parameter.getDescriptor().getName(), matchingReference);
            }
        }

        return references;
    }

    /**
     * Resolves the values of a persisted managed Parameter Context without modifying it. A sensitive parameter bound to a
     * Secret Reference takes the current value of that secret, or is left null and reported when the secret cannot be
     * resolved. Any other sensitive parameter takes its persisted value decrypted with the supplied provider, or is left null
     * and reported when the value cannot be decrypted.
     */
    static Resolution resolve(final VersionedParameterContext parameterContext, final Map<String, SecretReference> parameterSecretReferences,
            final SecretsManager secretsManager, final PropertyEncryptionProvider propertyEncryptionProvider) {
        if (parameterContext == null) {
            return new Resolution(null, List.of(), Map.of(), Set.of());
        }

        final VersionedParameterContext resolved = copyContext(parameterContext);
        final Map<String, SecretReference> references = parameterSecretReferences == null ? Map.of() : parameterSecretReferences;
        final Set<SecretReference> distinctReferences = new HashSet<>(references.values());
        final Map<SecretReference, Secret> secrets = distinctReferences.isEmpty()
            ? Map.of()
            : secretsManager.getSecrets(distinctReferences, false);
        final Map<String, SecretReference> unresolved = new HashMap<>();
        final Set<String> undecryptable = new HashSet<>();
        final List<ParameterValue> resolvedValues = new ArrayList<>();

        if (resolved.getParameters() != null) {
            for (final VersionedParameter parameter : resolved.getParameters()) {
                if (!parameter.isSensitive()) {
                    continue;
                }

                final SecretReference reference = references.get(parameter.getName());
                if (reference == null) {
                    final String value = parameter.getValue();
                    if (PropertyEncryptionEncoder.isEncrypted(value)) {
                        final SensitivePropertyContext context = SensitivePropertyContextFactory.forParameter(resolved.getName(), parameter.getName());
                        try {
                            parameter.setValue(SensitivePropertyCodec.decrypt(propertyEncryptionProvider, PropertyEncryptionEncoder.getDecoded(value), context));
                        } catch (final PropertyEncryptionException e) {
                            parameter.setValue(null);
                            undecryptable.add(parameter.getName());
                        }
                    }

                    continue;
                }

                final Secret secret = secrets.get(reference);
                if (secret == null) {
                    parameter.setValue(null);
                    unresolved.put(parameter.getName(), reference);
                    continue;
                }

                parameter.setValue(secret.getValue());
                resolvedValues.add(new ParameterValue.Builder()
                    .name(parameter.getName())
                    .value(secret.getValue())
                    .sensitive(true)
                    .build());
            }
        }

        return new Resolution(resolved, resolvedValues, unresolved, undecryptable);
    }

    private static VersionedParameterContext copyContext(final VersionedParameterContext source) {
        final VersionedParameterContext copy = new VersionedParameterContext();
        copy.setIdentifier(source.getIdentifier());
        copy.setInstanceIdentifier(source.getInstanceIdentifier());
        copy.setGroupIdentifier(source.getGroupIdentifier());
        copy.setName(source.getName());
        copy.setComments(source.getComments());
        copy.setPosition(source.getPosition());
        copy.setDescription(source.getDescription());
        copy.setInheritedParameterContexts(source.getInheritedParameterContexts() == null ? null : new ArrayList<>(source.getInheritedParameterContexts()));
        copy.setParameterProvider(source.getParameterProvider());
        copy.setParameterGroupName(source.getParameterGroupName());
        copy.setSynchronized(source.isSynchronized());

        if (source.getParameters() != null) {
            final Set<VersionedParameter> parameters = new HashSet<>();
            for (final VersionedParameter sourceParameter : source.getParameters()) {
                final VersionedParameter parameter = new VersionedParameter();
                parameter.setName(sourceParameter.getName());
                parameter.setDescription(sourceParameter.getDescription());
                parameter.setSensitive(sourceParameter.isSensitive());
                parameter.setProvided(sourceParameter.isProvided());
                parameter.setValue(sourceParameter.getValue());
                parameter.setReferencedAssets(sourceParameter.getReferencedAssets() == null ? null : new ArrayList<>(sourceParameter.getReferencedAssets()));
                parameters.add(parameter);
            }
            copy.setParameters(parameters);
        }

        return copy;
    }

    /**
     * @param parameterContext the resolved Parameter Context holding plaintext sensitive values
     * @param parameterValues the values taken from Secret References
     * @param unresolvedReferences Secret References that could not be resolved, keyed by parameter name
     * @param undecryptableParameters names of parameters whose persisted value could not be decrypted
     */
    record Resolution(
            VersionedParameterContext parameterContext,
            List<ParameterValue> parameterValues,
            Map<String, SecretReference> unresolvedReferences,
            Set<String> undecryptableParameters) {
    }
}
