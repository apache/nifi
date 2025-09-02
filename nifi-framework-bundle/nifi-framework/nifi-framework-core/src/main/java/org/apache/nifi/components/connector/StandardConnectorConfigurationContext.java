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
    public ConnectorPropertyValue getProperty(final String stepName, final String groupName, final String propertyName) {
        return getProperty(stepName, groupName, propertyName, null);
    }

    private ConnectorPropertyValue getProperty(final String stepName, final String groupName, final String propertyName, final String defaultValue) {
        readLock.lock();
        try {
            final List<PropertyGroupConfiguration> groupConfigs = resolvedPropertyGroupConfigurations.get(stepName);
            if (groupConfigs == null) {
                return new StandardConnectorPropertyValue(defaultValue);
            }

            for (final PropertyGroupConfiguration groupConfig : groupConfigs) {
                if (!Objects.equals(groupConfig.groupName(), groupName)) {
                    continue;
                }

                final ConnectorValueReference valueReference = groupConfig.propertyValues().get(propertyName);
                if (valueReference == null) {
                    return new StandardConnectorPropertyValue(defaultValue);
                }

                // The resolvedPropertyGroupConfigurations contains only StringLiteralValue references.
                return new StandardConnectorPropertyValue(((StringLiteralValue) valueReference).getValue());
            }

            // Property Group not found
            return new StandardConnectorPropertyValue(defaultValue);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public ConnectorPropertyValue getProperty(final ConfigurationStep configurationStep, final ConnectorPropertyGroup group, final ConnectorPropertyDescriptor connectorPropertyDescriptor) {
        return getProperty(configurationStep.getName(), group.getName(), connectorPropertyDescriptor.getName(), connectorPropertyDescriptor.getDefaultValue());
    }

    @Override
    public StandardConnectorConfigurationContext createWithOverrides(final String stepName, final List<PropertyGroupConfiguration> propertyOverrides) {
        final StandardConnectorConfigurationContext created = new StandardConnectorConfigurationContext(assetManager, secretsManager);
        readLock.lock();
        try {
            for (final Map.Entry<String, List<PropertyGroupConfiguration>> stepEntry : propertyGroupConfigurations.entrySet()) {
                final String existingStepName = stepEntry.getKey();
                final List<PropertyGroupConfiguration> existingGroupConfigs = stepEntry.getValue();

                // If this is not the step to override, just copy the existing configs.
                if (!existingStepName.equals(stepName)) {
                    created.setProperties(existingStepName, existingGroupConfigs);
                    continue;
                }

                final List<PropertyGroupConfiguration> createdGroupConfigs = new ArrayList<>();

                // Merge properties for this step.
                final Map<String, PropertyGroupConfiguration> existingGroupConfigMap = new HashMap<>();
                for (final PropertyGroupConfiguration existingGroupConfig : existingGroupConfigs) {
                    existingGroupConfigMap.put(existingGroupConfig.groupName(), existingGroupConfig);
                }

                for (final PropertyGroupConfiguration override : propertyOverrides) {
                    final Map<String, ConnectorValueReference> mergedProperties = new HashMap<>();

                    final PropertyGroupConfiguration existing = existingGroupConfigMap.get(override.groupName());
                    if (existing != null) {
                        mergedProperties.putAll(existing.propertyValues());
                    }
                    mergedProperties.putAll(override.propertyValues());

                    createdGroupConfigs.add(new PropertyGroupConfiguration(override.groupName(), mergedProperties));
                }

                created.setProperties(stepName, createdGroupConfigs);
            }

            return created;
        } finally {
            readLock.unlock();
        }
    }


    @Override
    public ConfigurationUpdateResult setProperties(final String stepName, final List<PropertyGroupConfiguration> propertyGroupConfigurations) {
        writeLock.lock();
        try {
            final List<PropertyGroupConfiguration> existingGroupConfigs = this.propertyGroupConfigurations.get(stepName);
            if (Objects.equals(existingGroupConfigs, propertyGroupConfigurations)) {
                return ConfigurationUpdateResult.NO_CHANGES;
            }

            final List<PropertyGroupConfiguration> resolvedConfigs = propertyGroupConfigurations.stream()
                .map(this::resolvePropertyGroupConfiguration)
                .toList();

            // Merge the new configurations with existing ones.
            this.propertyGroupConfigurations.put(stepName, merge(existingGroupConfigs, propertyGroupConfigurations));
            this.resolvedPropertyGroupConfigurations.compute(stepName, (key, existingResolvedConfigs) -> merge(existingResolvedConfigs, resolvedConfigs));

            return ConfigurationUpdateResult.CHANGES_MADE;
        } finally {
            writeLock.unlock();
        }
    }

    private PropertyGroupConfiguration resolvePropertyGroupConfiguration(final PropertyGroupConfiguration groupConfig) {
        final Map<String, ConnectorValueReference> resolvedProperties = new HashMap<>();

        for (final Map.Entry<String, ConnectorValueReference> entry : groupConfig.propertyValues().entrySet()) {
            final String propertyName = entry.getKey();
            final ConnectorValueReference reference = entry.getValue();
            final ConnectorValueReference resolved = resolve(reference);
            resolvedProperties.put(propertyName, resolved);
        }

        return new PropertyGroupConfiguration(groupConfig.groupName(), resolvedProperties);
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
    public ConfigurationUpdateResult replaceProperties(final String stepName, final List<PropertyGroupConfiguration> propertyGroupConfigurations) {
        writeLock.lock();
        try {
            final List<PropertyGroupConfiguration> existingGroupConfigs = this.propertyGroupConfigurations.get(stepName);
            if (Objects.equals(existingGroupConfigs, propertyGroupConfigurations)) {
                return ConfigurationUpdateResult.NO_CHANGES;
            }

            final List<PropertyGroupConfiguration> resolvedConfigs = propertyGroupConfigurations.stream()
                .map(this::resolvePropertyGroupConfiguration)
                .toList();

            this.propertyGroupConfigurations.put(stepName, copyPropertyGroupConfigurations(propertyGroupConfigurations));
            this.resolvedPropertyGroupConfigurations.put(stepName, copyPropertyGroupConfigurations(resolvedConfigs));

            return ConfigurationUpdateResult.CHANGES_MADE;
        } finally {
            writeLock.unlock();
        }
    }

    private List<PropertyGroupConfiguration> copyPropertyGroupConfigurations(final List<PropertyGroupConfiguration> groupConfigs) {
        final List<PropertyGroupConfiguration> copiedConfigs = new ArrayList<>();

        for (final PropertyGroupConfiguration groupConfig : groupConfigs) {
            final Map<String, ConnectorValueReference> copiedProperties = new HashMap<>(groupConfig.propertyValues());
            copiedConfigs.add(new PropertyGroupConfiguration(groupConfig.groupName(), copiedProperties));
        }

        return copiedConfigs;
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

    private List<PropertyGroupConfiguration> merge(final List<PropertyGroupConfiguration> existingGroupConfigs, final List<PropertyGroupConfiguration> newGroupConfigs) {
        if (existingGroupConfigs == null || existingGroupConfigs.isEmpty()) {
            return new ArrayList<>(newGroupConfigs);
        }

        if (newGroupConfigs == null || newGroupConfigs.isEmpty()) {
            return existingGroupConfigs;
        }

        final Map<String, PropertyGroupConfiguration> mergedMap = new HashMap<>();
        for (final PropertyGroupConfiguration groupConfig : existingGroupConfigs) {
            mergedMap.put(groupConfig.groupName(), groupConfig);
        }

        for (final PropertyGroupConfiguration groupConfig : newGroupConfigs) {
            final PropertyGroupConfiguration existingConfiguration = mergedMap.get(groupConfig.groupName());
            final PropertyGroupConfiguration mergedGroupConfig = merge(existingConfiguration, groupConfig);
            mergedMap.put(groupConfig.groupName(), mergedGroupConfig);
        }

        return List.copyOf(mergedMap.values());
    }

    private PropertyGroupConfiguration merge(final PropertyGroupConfiguration existingConfiguration, final PropertyGroupConfiguration newConfiguration) {
        if (Objects.equals(existingConfiguration, newConfiguration)) {
            return existingConfiguration;
        }
        if (existingConfiguration == null) {
            return newConfiguration;
        }
        if (newConfiguration == null) {
            return existingConfiguration;
        }

        final Map<String, ConnectorValueReference> mergedProperties = new HashMap<>(existingConfiguration.propertyValues());
        mergedProperties.putAll(newConfiguration.propertyValues());
        return new PropertyGroupConfiguration(existingConfiguration.groupName(), mergedProperties);
    }

    @Override
    public MutableConnectorConfigurationContext clone() {
        readLock.lock();
        try {
            final StandardConnectorConfigurationContext cloned = new StandardConnectorConfigurationContext(assetManager, secretsManager);
            for (final Map.Entry<String, List<PropertyGroupConfiguration>> entry : this.propertyGroupConfigurations.entrySet()) {
                final String stepName = entry.getKey();
                final List<PropertyGroupConfiguration> clonedGroupConfigs = copyPropertyGroupConfigurations(entry.getValue());
                cloned.propertyGroupConfigurations.put(stepName, clonedGroupConfigs);
            }

            for (final Map.Entry<String, List<PropertyGroupConfiguration>> entry : this.resolvedPropertyGroupConfigurations.entrySet()) {
                final String stepName = entry.getKey();
                final List<PropertyGroupConfiguration> clonedGroupConfigs = copyPropertyGroupConfigurations(entry.getValue());
                cloned.resolvedPropertyGroupConfigurations.put(stepName, clonedGroupConfigs);
            }

            return cloned;
        } finally {
            readLock.unlock();
        }
    }

}
