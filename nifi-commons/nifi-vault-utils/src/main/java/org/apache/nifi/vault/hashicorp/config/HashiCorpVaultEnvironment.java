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
package org.apache.nifi.vault.hashicorp.config;

import org.apache.nifi.vault.hashicorp.HashiCorpVaultConfigurationException;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/**
 * A custom Spring Environment that uses configured POJO properties to provide the expected property values
 * for Spring Vault.
 */
public class HashiCorpVaultEnvironment implements Environment {
    public static final String VAULT_URI = "vault.uri";
    public static final String VAULT_SSL_KEYSTORE = "vault.ssl.key-store";
    public static final String VAULT_SSL_KEYSTORE_PASSWORD = "vault.ssl.key-store-password";
    public static final String VAULT_SSL_KEYSTORE_TYPE = "vault.ssl.key-store-type";
    public static final String VAULT_SSL_TRUSTSTORE = "vault.ssl.trust-store";
    public static final String VAULT_SSL_TRUSTSTORE_PASSWORD = "vault.ssl.trust-store-password";
    public static final String VAULT_SSL_TRUSTSTORE_TYPE = "vault.ssl.trust-store-type";
    public static final String VAULT_SSL_ENABLED_PROTOCOLS = "vault.ssl.enabled-protocols";
    public static final String VAULT_SSL_ENABLED_CIPHER_SUITES = "vault.ssl.enabled-cipher-suites";

    private final Properties authProperties;
    private final HashiCorpVaultProperties vaultProperties;

    public HashiCorpVaultEnvironment(final HashiCorpVaultProperties vaultProperties) throws HashiCorpVaultConfigurationException {
        Objects.requireNonNull(vaultProperties, "Vault Properties are required");
        this.vaultProperties = vaultProperties;

        this.authProperties = this.getAuthProperties(vaultProperties)
                .orElseThrow(() -> new HashiCorpVaultConfigurationException("Vault auth properties file is required"));
    }

    private Optional<Properties> getAuthProperties(final HashiCorpVaultProperties properties) throws HashiCorpVaultConfigurationException {
        if (properties.getAuthPropertiesFilename() == null) {
            return Optional.empty();
        }

        final File authPropertiesFile = Paths.get(properties.getAuthPropertiesFilename()).toFile();
        if (!authPropertiesFile.exists()) {
            return Optional.empty();
        }

        final Properties authProperties = new Properties();
        try (final FileReader reader = new FileReader(authPropertiesFile)) {
            authProperties.load(reader);
        } catch (final IOException e) {
            final String message = String.format("Failed to read Vault auth properties [%s]", authPropertiesFile);
            throw new HashiCorpVaultConfigurationException(message, e);
        }
        return Optional.of(authProperties);
    }

    @Override
    public boolean containsProperty(final String propertyName) {
        return this.getProperty(propertyName) != null;
    }

    /**
     * This maps the relevant Spring Vault properties to the configured properties from VaultProperties.
     * @param key The Spring Vault property name (e.g., vault.uri)
     * @return The configured property value
     */
    @Override
    public String getProperty(final String key) {
        for (Method method : HashiCorpVaultProperties.class.getDeclaredMethods()) {
            if (method.isAnnotationPresent(HashiCorpVaultProperty.class)
                    && method.getAnnotation(HashiCorpVaultProperty.class).key().equals(key)) {
                try {
                    return (String) method.invoke(vaultProperties);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new IllegalStateException(String.format("Error retrieving VaultProperty [%s]", key), e);
                }
            }
        }

        return authProperties.getProperty(key);
    }

    @Override
    public String getProperty(final String key, final String defaultValue) {
        return this.getProperty(key, String.class, defaultValue);
    }

    @Override
    public <T> T getProperty(final String key, final Class<T> targetType) {
        return this.getProperty(key, targetType, null);
    }

    @Override
    public <T> T getProperty(final String key, final Class<T> targetType, final T defaultValue) {
        final String value = this.getProperty(key);
        if (targetType.isAssignableFrom(URI.class)) {
            return value == null ? defaultValue : (T) URI.create(value);
        } else {
            return (T) (value == null ? defaultValue : value);
        }
    }

    // The rest of these are not actually called from EnvironmentVaultConfiguration
    @Override
    public String getRequiredProperty(String s) throws IllegalStateException {
        throw new UnsupportedOperationException("Method not supported");
    }

    @Override
    public <T> T getRequiredProperty(String s, Class<T> aClass) throws IllegalStateException {
        throw new UnsupportedOperationException("Method not supported");
    }

    @Override
    public String resolvePlaceholders(String s) {
        throw new UnsupportedOperationException("Method not supported");
    }

    @Override
    public String resolveRequiredPlaceholders(String s) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Method not supported");
    }

    @Override
    public String[] getActiveProfiles() {
        throw new UnsupportedOperationException("Method not supported");
    }

    @Override
    public String[] getDefaultProfiles() {
        throw new UnsupportedOperationException("Method not supported");
    }

    @Override
    public boolean acceptsProfiles(String... strings) {
        throw new UnsupportedOperationException("Method not supported");
    }

    @Override
    public boolean acceptsProfiles(Profiles profiles) {
        throw new UnsupportedOperationException("Method not supported");
    }
}
