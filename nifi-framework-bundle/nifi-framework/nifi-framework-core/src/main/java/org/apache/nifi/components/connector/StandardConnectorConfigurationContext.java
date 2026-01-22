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
import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.components.connector.secrets.SecretProvider;
import org.apache.nifi.components.connector.secrets.SecretsManager;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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

    final Map<String, StepConfiguration> propertyConfigurations = new HashMap<>();
    final Map<String, StepConfiguration> resolvedPropertyConfigurations = new HashMap<>();

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
            final StepConfiguration resolvedConfig = resolvedPropertyConfigurations.get(stepName);
            if (resolvedConfig == null) {
                return new StandardConnectorPropertyValue(defaultValue);
            }

            final ConnectorValueReference valueReference = resolvedConfig.getPropertyValue(propertyName);
            if (valueReference != null) {
                // The resolvedPropertyConfigurations contains only StringLiteralValue references.
                return new StandardConnectorPropertyValue(((StringLiteralValue) valueReference).getValue());
            }

            // Property not found
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
    public Set<String> getPropertyNames(final String configurationStepName) {
        readLock.lock();
        try {
            final StepConfiguration config = propertyConfigurations.get(configurationStepName);
            if (config == null) {
                return Collections.emptySet();
            }

            return new HashSet<>(config.getPropertyValues().keySet());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Set<String> getPropertyNames(final ConfigurationStep configurationStep) {
        return getPropertyNames(configurationStep.getName());
    }

    @Override
    public StepConfigurationContext scopedToStep(final String stepName) {
        return new StandardStepConfigurationContext(stepName, this);
    }

    @Override
    public StepConfigurationContext scopedToStep(final ConfigurationStep configurationStep) {
        return scopedToStep(configurationStep.getName());
    }

    @Override
    public StandardConnectorConfigurationContext createWithOverrides(final String stepName, final Map<String, String> propertyOverrides) {
        final StandardConnectorConfigurationContext created = new StandardConnectorConfigurationContext(assetManager, secretsManager);
        readLock.lock();
        try {
            for (final Map.Entry<String, StepConfiguration> stepEntry : propertyConfigurations.entrySet()) {
                final String existingStepName = stepEntry.getKey();
                final StepConfiguration existingConfig = stepEntry.getValue();

                if (!existingStepName.equals(stepName)) {
                    created.setProperties(existingStepName, new StepConfiguration(new HashMap<>(existingConfig.getPropertyValues())));
                    continue;
                }

                final Map<String, ConnectorValueReference> mergedProperties = new HashMap<>(existingConfig.getPropertyValues());
                for (final Map.Entry<String, String> override : propertyOverrides.entrySet()) {
                    final String propertyValue = override.getValue();
                    if (propertyValue == null) {
                        mergedProperties.remove(override.getKey());
                        continue;
                    }

                    mergedProperties.put(override.getKey(), new StringLiteralValue(propertyValue));
                }
                created.setProperties(stepName, new StepConfiguration(mergedProperties));
            }

            return created;
        } finally {
            readLock.unlock();
        }
    }


    @Override
    public ConfigurationUpdateResult setProperties(final String stepName, final StepConfiguration configuration) {
        writeLock.lock();
        try {
            final StepConfiguration existingConfig = propertyConfigurations.get(stepName);
            final Map<String, ConnectorValueReference> existingProperties = existingConfig != null ? existingConfig.getPropertyValues() : new HashMap<>();
            final Map<String, ConnectorValueReference> mergedProperties = new HashMap<>(existingProperties);
            mergedProperties.putAll(configuration.getPropertyValues());

            final StepConfiguration resolvedConfig = resolvePropertyValues(mergedProperties);

            final StepConfiguration updatedStepConfig = new StepConfiguration(new HashMap<>(mergedProperties));
            final StepConfiguration existingStepConfig = this.propertyConfigurations.put(stepName, updatedStepConfig);
            final StepConfiguration existingResolvedStepConfig = this.resolvedPropertyConfigurations.put(stepName, resolvedConfig);

            if (Objects.equals(existingStepConfig, updatedStepConfig) && Objects.equals(existingResolvedStepConfig, resolvedConfig)) {
                return ConfigurationUpdateResult.NO_CHANGES;
            }

            return ConfigurationUpdateResult.CHANGES_MADE;
        } finally {
            writeLock.unlock();
        }
    }

    private StepConfiguration resolvePropertyValues(final Map<String, ConnectorValueReference> propertyValues) {
        final Map<String, ConnectorValueReference> resolvedProperties = new HashMap<>();
        for (final Map.Entry<String, ConnectorValueReference> entry : propertyValues.entrySet()) {
            final ConnectorValueReference resolved = resolve(entry.getValue());
            resolvedProperties.put(entry.getKey(), resolved);
        }
        return new StepConfiguration(resolvedProperties);
    }

    private ConnectorValueReference resolve(final ConnectorValueReference reference) {
        if (reference == null) {
            return null;
        }

        try {
            return switch (reference) {
                case StringLiteralValue stringLiteral -> stringLiteral;
                case AssetReference assetReference -> resolveAssetReferences(assetReference);
                case SecretReference secretReference -> new StringLiteralValue(getSecretValue(secretReference));
            };
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Unable to obtain Secrets from Secret Manager", ioe);
        }
    }

    private StringLiteralValue resolveAssetReferences(final AssetReference assetReference) {
        final Set<String> resolvedAssetValues = new HashSet<>();
        for (final String assetId : assetReference.getAssetIdentifiers()) {
            assetManager.getAsset(assetId)
                .map(Asset::getFile)
                .map(File::getAbsolutePath)
                .ifPresent(resolvedAssetValues::add);
        }

        return new StringLiteralValue(String.join(",", resolvedAssetValues));
    }

    private String getSecretValue(final SecretReference secretReference) throws IOException {
        final SecretProvider provider = getSecretProvider(secretReference);
        if (provider == null) {
            return null;
        }

        final List<Secret> secrets = provider.getSecrets(List.of(secretReference.getFullyQualifiedName()));
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

        // Try to find by Fully Qualified Name prefix
        final String fqn = secretReference.getFullyQualifiedName();
        if (fqn != null) {
            for (final SecretProvider provider : providers) {
                if (fqn.startsWith(provider.getProviderName() + ".")) {
                    return provider;
                }
            }
        }

        return null;
    }

    @Override
    public ConfigurationUpdateResult replaceProperties(final String stepName, final StepConfiguration configuration) {
        writeLock.lock();
        try {
            final StepConfiguration resolvedConfig = resolvePropertyValues(configuration.getPropertyValues());

            final StepConfiguration updatedStepConfig = new StepConfiguration(new HashMap<>(configuration.getPropertyValues()));
            final StepConfiguration existingStepConfig = this.propertyConfigurations.put(stepName, updatedStepConfig);
            final StepConfiguration existingResolvedStepConfig = this.resolvedPropertyConfigurations.put(stepName, resolvedConfig);

            if (Objects.equals(existingStepConfig, updatedStepConfig) && Objects.equals(existingResolvedStepConfig, resolvedConfig)) {
                return ConfigurationUpdateResult.NO_CHANGES;
            }

            return ConfigurationUpdateResult.CHANGES_MADE;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public ConnectorConfiguration toConnectorConfiguration() {
        readLock.lock();
        try {
            final Set<NamedStepConfiguration> stepConfigs = new HashSet<>();
            for (final Map.Entry<String, StepConfiguration> entry : propertyConfigurations.entrySet()) {
                final String stepName = entry.getKey();
                final StepConfiguration config = entry.getValue();
                final StepConfiguration configCopy = new StepConfiguration(new HashMap<>(config.getPropertyValues()));

                stepConfigs.add(new NamedStepConfiguration(stepName, configCopy));
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
            for (final Map.Entry<String, StepConfiguration> entry : this.propertyConfigurations.entrySet()) {
                cloned.setProperties(entry.getKey(), new StepConfiguration(new HashMap<>(entry.getValue().getPropertyValues())));
            }
            return cloned;
        } finally {
            readLock.unlock();
        }
    }

}
