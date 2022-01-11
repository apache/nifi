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
package org.apache.nifi.properties.configuration;

import org.apache.nifi.properties.BootstrapProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * Shared Client Provider for reading Client Properties from file referenced in configured Bootstrap Property Key
 *
 * @param <T> Client Type
 */
public abstract class BootstrapPropertiesClientProvider<T> implements ClientProvider<T> {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final BootstrapProperties.BootstrapPropertyKey bootstrapPropertyKey;

    public BootstrapPropertiesClientProvider(final BootstrapProperties.BootstrapPropertyKey bootstrapPropertyKey) {
        this.bootstrapPropertyKey = Objects.requireNonNull(bootstrapPropertyKey, "Bootstrap Property Key required");
    }

    /**
     * Get Client using Client Properties
     *
     * @param clientProperties Client Properties can be null
     * @return Configured Client or empty when Client Properties object is null
     */
    @Override
    public Optional<T> getClient(final Properties clientProperties) {
        return isMissingProperties(clientProperties) ? Optional.empty() : Optional.of(getConfiguredClient(clientProperties));
    }

    /**
     * Get Client Properties from file referenced in Bootstrap Properties
     *
     * @param bootstrapProperties Bootstrap Properties
     * @return Client Properties or empty when not configured
     */
    @Override
    public Optional<Properties> getClientProperties(final BootstrapProperties bootstrapProperties) {
        Objects.requireNonNull(bootstrapProperties, "Bootstrap Properties required");
        final String clientBootstrapPropertiesPath = bootstrapProperties.getProperty(bootstrapPropertyKey).orElse(null);
        if (clientBootstrapPropertiesPath == null || clientBootstrapPropertiesPath.isEmpty()) {
            logger.debug("Client Properties [{}] not configured", bootstrapPropertyKey);
            return Optional.empty();
        } else {
            final Path propertiesPath = Paths.get(clientBootstrapPropertiesPath);
            if (Files.exists(propertiesPath)) {
                try {
                    final Properties clientProperties = new Properties();
                    try (final InputStream inputStream = Files.newInputStream(propertiesPath)) {
                        clientProperties.load(inputStream);
                    }
                    return Optional.of(clientProperties);
                } catch (final IOException e) {
                    final String message = String.format("Loading Client Properties Failed [%s]", propertiesPath);
                    throw new UncheckedIOException(message, e);
                }
            } else {
                logger.debug("Client Properties [{}] Path [{}] not found", bootstrapPropertyKey, propertiesPath);
                return Optional.empty();
            }
        }
    }

    /**
     * Get Configured Client using Client Properties
     *
     * @param clientProperties Client Properties
     * @return Configured Client
     */
    protected abstract T getConfiguredClient(final Properties clientProperties);

    /**
     * Get Property Names required for initializing client in order to perform initial validation
     *
     * @return Set of required client property names
     */
    protected abstract Set<String> getRequiredPropertyNames();

    private boolean isMissingProperties(final Properties clientProperties) {
        return clientProperties == null || getRequiredPropertyNames().stream().anyMatch(propertyName -> {
            final String property = clientProperties.getProperty(propertyName);
            return property == null || property.isEmpty();
        });
    }
}
