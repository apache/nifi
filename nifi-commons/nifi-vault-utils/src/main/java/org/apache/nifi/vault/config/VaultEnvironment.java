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
package org.apache.nifi.vault.config;

import org.apache.nifi.vault.VaultConfigurationException;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/**
 * A custom Spring Environment that uses configured POJO properties to provide the expected property values
 * for Spring Vault.
 */
public class VaultEnvironment implements Environment {
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
    private final VaultProperties vaultProperties;

    public VaultEnvironment(final VaultProperties vaultProperties) throws VaultConfigurationException {
        Objects.requireNonNull(vaultProperties, "Vault Properties are required");
        this.vaultProperties = vaultProperties;

        Optional<Properties> authProperties = this.getAuthProperties(vaultProperties);
        if (!authProperties.isPresent()) {
            throw new VaultConfigurationException("Vault auth properties file is required");
        } else {
            this.authProperties = authProperties.get();
        }
    }

    private Optional<Properties> getAuthProperties(VaultProperties properties) throws VaultConfigurationException {
        if (properties.getAuthPropertiesFilename() == null) {
            return Optional.empty();
        }

        File authPropertiesFile = Paths.get(properties.getAuthPropertiesFilename()).toFile();
        if (!authPropertiesFile.exists()) {
            return Optional.empty();
        }

        Properties authProperties = new Properties();
        try (final FileReader reader = new FileReader(authPropertiesFile)) {
            authProperties.load(reader);
        } catch (final IOException e) {
            final String message = String.format("Failed to read Vault auth properties [%s]", authPropertiesFile);
            throw new VaultConfigurationException(message, e);
        }
        return Optional.of(authProperties);
    }

    @Override
    public boolean containsProperty(String propertyName) {
        return this.getProperty(propertyName) != null;
    }

    /**
     * This maps the relevant Spring Vault properties to the configured properties from VaultProperties.
     * @param key The Spring Vault property name (e.g., vault.uri)
     * @return The configured property value
     */
    @Override
    public String getProperty(String key) {
        if (VAULT_URI.equals(key)) {
            return vaultProperties.getUri();
        } else if (VAULT_SSL_KEYSTORE.equals(key)) {
            return vaultProperties.getKeystore();
        } else if (VAULT_SSL_KEYSTORE_PASSWORD.equals(key)) {
            return vaultProperties.getKeystorePassword();
        } else if (VAULT_SSL_KEYSTORE_TYPE.equals(key)) {
            return vaultProperties.getKeystoreType();
        } else if (VAULT_SSL_TRUSTSTORE.equals(key)) {
            return vaultProperties.getTruststore();
        } else if (VAULT_SSL_TRUSTSTORE_PASSWORD.equals(key)) {
            return vaultProperties.getTruststorePassword();
        } else if (VAULT_SSL_TRUSTSTORE_TYPE.equals(key)) {
            return vaultProperties.getTruststoreType();
        } else if (VAULT_SSL_ENABLED_CIPHER_SUITES.equals(key)) {
            return vaultProperties.getEnabledTlsCipherSuites();
        } else if (VAULT_SSL_ENABLED_PROTOCOLS.equals(key)) {
            return vaultProperties.getEnabledTlsProtocols();
        } else {
            return authProperties.getProperty(key);
        }
    }

    @Override
    public String getProperty(String key, String defaultValue) {
        return this.getProperty(key, String.class, defaultValue);
    }

    @Override
    public <T> T getProperty(String key, Class<T> targetType) {
        return this.getProperty(key, targetType, null);
    }

    @Override
    public <T> T getProperty(String key, Class<T> targetType, T defaultValue) {
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
        return null;
    }

    @Override
    public <T> T getRequiredProperty(String s, Class<T> aClass) throws IllegalStateException {
        return null;
    }

    @Override
    public String resolvePlaceholders(String s) {
        return null;
    }

    @Override
    public String resolveRequiredPlaceholders(String s) throws IllegalArgumentException {
        return null;
    }

    @Override
    public String[] getActiveProfiles() {
        return new String[0];
    }

    @Override
    public String[] getDefaultProfiles() {
        return new String[0];
    }

    @Override
    public boolean acceptsProfiles(String... strings) {
        return false;
    }

    @Override
    public boolean acceptsProfiles(Profiles profiles) {
        return false;
    }
}
