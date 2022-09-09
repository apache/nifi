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

import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.vault.hashicorp.HashiCorpVaultConfigurationException;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.support.ResourcePropertySource;
import org.springframework.vault.client.RestTemplateFactory;
import org.springframework.vault.config.EnvironmentVaultConfiguration;
import org.springframework.vault.core.VaultKeyValueOperationsSupport.KeyValueBackend;
import org.springframework.vault.support.ClientOptions;
import org.springframework.vault.support.SslConfiguration;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * A Vault configuration that uses the NiFiVaultEnvironment.
 */
public class HashiCorpVaultConfiguration extends EnvironmentVaultConfiguration {
    private static final int KV_V1 = 1;
    private static final int KV_V2 = 2;

    public enum VaultConfigurationKey {
        AUTHENTICATION_PROPERTIES_FILE("vault.authentication.properties.file"),
        READ_TIMEOUT("vault.read.timeout"),
        CONNECTION_TIMEOUT("vault.connection.timeout"),
        KV_VERSION("vault.kv.version"),
        URI("vault.uri");

        private final String key;

        VaultConfigurationKey(final String key) {
            this.key = key;
        }

        /**
         * Returns the property key.
         * @return The property key
         */
        public String getKey() {
            return key;
        }
    }

    private static final String HTTPS = "https";

    private final SslConfiguration sslConfiguration;
    private final ClientOptions clientOptions;
    private final KeyValueBackend keyValueBackend;

    /**
     * Creates a HashiCorpVaultConfiguration from property sources, in increasing precedence.
     * @param propertySources A series of Spring PropertySource objects (the last in the list take precedence over
     *                        sources earlier in the list)
     * @throws HashiCorpVaultConfigurationException If the authentication properties file could not be read
     */
    public HashiCorpVaultConfiguration(final PropertySource<?>... propertySources) {
        final ConfigurableEnvironment env = new StandardEnvironment();
        for (final PropertySource<?> propertySource : propertySources) {
            env.getPropertySources().addFirst(propertySource);
        }

        if (env.containsProperty(VaultConfigurationKey.AUTHENTICATION_PROPERTIES_FILE.key)) {
            final String authPropertiesFilename = env.getProperty(VaultConfigurationKey.AUTHENTICATION_PROPERTIES_FILE.key);
            try {
                final PropertySource<?> authPropertiesSource = createPropertiesFileSource(authPropertiesFilename);
                env.getPropertySources().addFirst(authPropertiesSource);
            } catch (IOException e) {
                throw new HashiCorpVaultConfigurationException("Could not load HashiCorp Vault authentication properties " + authPropertiesFilename, e);
            }
        }

        KeyValueBackend keyValueBackend = KeyValueBackend.KV_1;
        if (env.containsProperty(VaultConfigurationKey.KV_VERSION.key)) {
            final String kvVersion = env.getProperty(VaultConfigurationKey.KV_VERSION.key);
            try {
                int kvVersionNumber = Integer.parseInt(kvVersion);
                if (kvVersionNumber == KV_V2) {
                    keyValueBackend = KeyValueBackend.KV_2;
                } else if (kvVersionNumber != KV_V1) {
                    throw new IllegalArgumentException("K/V v" + kvVersion + " is not recognized");
                }
            } catch (final IllegalArgumentException e) {
                throw new HashiCorpVaultConfigurationException("Unrecognized " + VaultConfigurationKey.KV_VERSION.key + ": " + kvVersion, e);
            }
        }
        this.keyValueBackend = keyValueBackend;
        validateProperties(env);

        this.setApplicationContext(new HashiCorpVaultApplicationContext(env));

        sslConfiguration = env.getProperty(VaultConfigurationKey.URI.key).contains(HTTPS)
                ? super.sslConfiguration() : SslConfiguration.unconfigured();

        clientOptions = getClientOptions();
    }

    private void validateProperties(final ConfigurableEnvironment environment) {
        try {
            final String vaultUri = Objects.requireNonNull(environment.getProperty(VaultConfigurationKey.URI.key),
                    "Missing required property " + VaultConfigurationKey.URI.key);
            if (vaultUri.startsWith(HTTPS)) {
                requireSslProperty("vault.ssl.key-store", environment);
                requireSslProperty("vault.ssl.key-store-password", environment);
                requireSslProperty("vault.ssl.key-store-type", environment);
                requireSslProperty("vault.ssl.trust-store", environment);
                requireSslProperty("vault.ssl.trust-store-password", environment);
                requireSslProperty("vault.ssl.trust-store-type", environment);
            }
        } catch (final NullPointerException e) {
            // Rethrow as IllegalArgumentException
            throw new IllegalArgumentException(e.getMessage(), e);
        }

    }

    private void requireSslProperty(final String propertyName, final ConfigurableEnvironment environment) {
        Objects.requireNonNull(environment.getProperty(propertyName), propertyName + " is required with an https URI");
    }


    public KeyValueBackend getKeyValueBackend() {
        return keyValueBackend;
    }

    /**
     * A convenience method to create a PropertySource from a file on disk.
     * @param filename The properties filename.
     * @return A PropertySource containing the properties in the given file
     * @throws IOException If the file could not be read
     */
    public static PropertySource<?> createPropertiesFileSource(final String filename) throws IOException {
        return new ResourcePropertySource(new FileSystemResource(Paths.get(filename)));
    }

    @Override
    public ClientOptions clientOptions() {
        return clientOptions;
    }

    @Override
    protected RestTemplateFactory getRestTemplateFactory() {
        return this.restTemplateFactory(clientHttpRequestFactoryWrapper());
    }

    @Override
    public SslConfiguration sslConfiguration() {
        return sslConfiguration;
    }

    private ClientOptions getClientOptions() {
        final ClientOptions clientOptions = new ClientOptions();
        Duration readTimeoutDuration = clientOptions.getReadTimeout();
        Duration connectionTimeoutDuration = clientOptions.getConnectionTimeout();
        final String configuredReadTimeout = getEnvironment().getProperty(VaultConfigurationKey.READ_TIMEOUT.key);
        if (configuredReadTimeout != null) {
            readTimeoutDuration = getDuration(configuredReadTimeout);
        }
        final String configuredConnectionTimeout = getEnvironment().getProperty(VaultConfigurationKey.CONNECTION_TIMEOUT.key);
        if (configuredConnectionTimeout != null) {
            connectionTimeoutDuration = getDuration(configuredConnectionTimeout);
        }
        return new ClientOptions(connectionTimeoutDuration, readTimeoutDuration);
    }

    private static Duration getDuration(final String formattedDuration) {
        final double duration = FormatUtils.getPreciseTimeDuration(formattedDuration, TimeUnit.MILLISECONDS);
        return Duration.ofMillis(Double.valueOf(duration).longValue());
    }
}
