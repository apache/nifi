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

import org.apache.nifi.asset.AssetManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StandardConnectorConfigurationContext implements MutableConnectorConfigurationContext, Cloneable {
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();
    private final AssetManager assetManager;
    private final SecretsManager secretsManager;

    final Map<String, List<PropertyGroupConfiguration>> propertyGroupConfigurations = new HashMap<>();
    final Map<String, List<PropertyGroupConfiguration>> resolvedPropertyGroupConfigurations = new HashMap<>();

    public StandardConnectorConfigurationContext(final AssetManager assetManager, final SecretsManager secretsManager) {
        this.assetManager = assetManager;
        this.secretsManager = secretsManager;
    }

    @Override
    public ConnectorPropertyValue getProperty(final String stepName, final String propertyName) {
        return getProperty(stepName, propertyName, null);
    }

    private ConnectorPropertyValue getProperty(final String stepName, final String propertyName, final String defaultValue) {
        readLock.lock();
        try {
            final List<PropertyGroupConfiguration> groupConfigs = resolvedPropertyGroupConfigurations.get(stepName);
            if (groupConfigs == null) {
                return new StandardConnectorPropertyValue(defaultValue);
            }

            for (final PropertyGroupConfiguration groupConfig : groupConfigs) {
                final ConnectorValueReference valueReference = groupConfig.propertyValues().get(propertyName);
                if (valueReference != null) {
                    // The resolvedPropertyGroupConfigurations contains only StringLiteralValue references.
                    return new StandardConnectorPropertyValue(((StringLiteralValue) valueReference).getValue());
                }
            }

            // Property not found in any group
            return new StandardConnectorPropertyValue(defaultValue);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public ConnectorPropertyValue getProperty(final ConfigurationStep configurationStep, final ConnectorPropertyDescriptor connectorPropertyDescriptor) {
        return getProperty(configurationStep.getName(), connectorPropertyDescriptor.getName(), connectorPropertyDescriptor.getDefaultValue());
    }

    @Override
    public StandardConnectorConfigurationContext createWithOverrides(final String stepName, final Map<String, String> propertyOverrides) {
        final StandardConnectorConfigurationContext created = new StandardConnectorConfigurationContext(assetManager, secretsManager);
        readLock.lock();
        try {
            for (final Map.Entry<String, List<PropertyGroupConfiguration>> stepEntry : propertyGroupConfigurations.entrySet()) {
                final String existingStepName = stepEntry.getKey();
                final Map<String, ConnectorValueReference> existingProperties = getAllPropertiesFromGroups(stepEntry.getValue());

                if (!existingStepName.equals(stepName)) {
                    created.setProperties(existingStepName, existingProperties);
                    continue;
                }

                final Map<String, ConnectorValueReference> mergedProperties = new HashMap<>(existingProperties);
                for (final Map.Entry<String, String> override : propertyOverrides.entrySet()) {
                    final String propertyValue = override.getValue();
                    mergedProperties.put(override.getKey(), propertyValue == null ? null : new StringLiteralValue(propertyValue));
                }
                created.setProperties(stepName, mergedProperties);
            }

            return created;
        } finally {
            readLock.unlock();
        }
    }

    private Map<String, ConnectorValueReference> getAllPropertiesFromGroups(final List<PropertyGroupConfiguration> groupConfigs) {
        final Map<String, ConnectorValueReference> allProperties = new HashMap<>();
        if (groupConfigs != null) {
            for (final PropertyGroupConfiguration groupConfig : groupConfigs) {
                allProperties.putAll(groupConfig.propertyValues());
            }
        }
        return allProperties;
    }


    @Override
    public ConfigurationUpdateResult setProperties(final String stepName, final Map<String, ConnectorValueReference> propertyValues) {
        writeLock.lock();
        try {
            final Map<String, ConnectorValueReference> existingProperties = getAllProperties(stepName);
            if (Objects.equals(existingProperties, propertyValues)) {
                return ConfigurationUpdateResult.NO_CHANGES;
            }

            final Map<String, ConnectorValueReference> mergedProperties = new HashMap<>(existingProperties);
            mergedProperties.putAll(propertyValues);

            final Map<String, ConnectorValueReference> resolvedProperties = resolvePropertyValues(mergedProperties);

            final PropertyGroupConfiguration groupConfig = new PropertyGroupConfiguration("", mergedProperties);
            final PropertyGroupConfiguration resolvedGroupConfig = new PropertyGroupConfiguration("", resolvedProperties);
            this.propertyGroupConfigurations.put(stepName, List.of(groupConfig));
            this.resolvedPropertyGroupConfigurations.put(stepName, List.of(resolvedGroupConfig));

            return ConfigurationUpdateResult.CHANGES_MADE;
        } finally {
            writeLock.unlock();
        }
    }

    private Map<String, ConnectorValueReference> getAllProperties(final String stepName) {
        final Map<String, ConnectorValueReference> allProperties = new HashMap<>();
        final List<PropertyGroupConfiguration> groupConfigs = this.propertyGroupConfigurations.get(stepName);
        if (groupConfigs != null) {
            for (final PropertyGroupConfiguration groupConfig : groupConfigs) {
                allProperties.putAll(groupConfig.propertyValues());
            }
        }
        return allProperties;
    }

    private Map<String, ConnectorValueReference> resolvePropertyValues(final Map<String, ConnectorValueReference> propertyValues) {
        final Map<String, ConnectorValueReference> resolvedProperties = new HashMap<>();
        for (final Map.Entry<String, ConnectorValueReference> entry : propertyValues.entrySet()) {
            final ConnectorValueReference resolved = resolve(entry.getValue());
            resolvedProperties.put(entry.getKey(), resolved);
        }
        return resolvedProperties;
    }

    private ConnectorValueReference resolve(final ConnectorValueReference reference) {
        if (reference == null) {
            return null;
        }

        try {
            return switch (reference) {
                case StringLiteralValue stringLiteral -> stringLiteral;
                case AssetReference assetReference -> assetManager.getAsset(assetReference.getAssetIdentifier())
                    .map(asset -> asset.getFile().getAbsolutePath())
                    .map(StringLiteralValue::new)
                    .orElse(null);
                case SecretReference secretReference -> new StringLiteralValue(getSecretValue(secretReference));
            };
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Unable to obtain Secrets from Secret Manager", ioe);
        }
    }

    private String getSecretValue(final SecretReference secretReference) throws IOException {
        final SecretProvider provider = getSecretProvider(secretReference);
        if (provider == null) {
            return null;
        }

        final List<Secret> secrets = provider.getSecrets(List.of(secretReference.getSecretName()));
        return secrets.isEmpty() ? null : secrets.getFirst().getValue();
    }

    private SecretProvider getSecretProvider(final SecretReference secretReference) {
        final Set<SecretProvider> providers = secretsManager.getSecretProviders();
        for (final SecretProvider provider : providers) {
            if (Objects.equals(provider.getProviderId(), secretReference.getProviderId())) {
                return provider;
            }
        }

        for (final SecretProvider provider : providers) {
            if (Objects.equals(provider.getProviderName(), secretReference.getProviderName())) {
                return provider;
            }
        }

        return null;
    }

    @Override
    public ConfigurationUpdateResult replaceProperties(final String stepName, final Map<String, ConnectorValueReference> propertyValues) {
        writeLock.lock();
        try {
            final Map<String, ConnectorValueReference> existingProperties = getAllProperties(stepName);
            if (Objects.equals(existingProperties, propertyValues)) {
                return ConfigurationUpdateResult.NO_CHANGES;
            }

            final Map<String, ConnectorValueReference> resolvedProperties = resolvePropertyValues(propertyValues);

            final PropertyGroupConfiguration groupConfig = new PropertyGroupConfiguration("", new HashMap<>(propertyValues));
            final PropertyGroupConfiguration resolvedGroupConfig = new PropertyGroupConfiguration("", resolvedProperties);
            this.propertyGroupConfigurations.put(stepName, List.of(groupConfig));
            this.resolvedPropertyGroupConfigurations.put(stepName, List.of(resolvedGroupConfig));

            return ConfigurationUpdateResult.CHANGES_MADE;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public ConnectorConfiguration toConnectorConfiguration() {
        readLock.lock();
        try {
            final List<ConfigurationStepConfiguration> stepConfigs = new ArrayList<>();
            for (final Map.Entry<String, List<PropertyGroupConfiguration>> entry : propertyGroupConfigurations.entrySet()) {
                final String stepName = entry.getKey();
                final List<PropertyGroupConfiguration> groupConfigurations = entry.getValue();

                stepConfigs.add(new ConfigurationStepConfiguration(stepName, groupConfigurations));
            }

            return new ConnectorConfiguration(stepConfigs);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public MutableConnectorConfigurationContext clone() {
        readLock.lock();
        try {
            final StandardConnectorConfigurationContext cloned = new StandardConnectorConfigurationContext(assetManager, secretsManager);
            for (final Map.Entry<String, List<PropertyGroupConfiguration>> entry : this.propertyGroupConfigurations.entrySet()) {
                cloned.setProperties(entry.getKey(), getAllPropertiesFromGroups(entry.getValue()));
            }
            return cloned;
        } finally {
            readLock.unlock();
        }
    }

}
