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
import org.springframework.vault.support.ClientOptions;
import org.springframework.vault.support.SslConfiguration;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * A Vault configuration that uses the NiFiVaultEnvironment.
 */
public class HashiCorpVaultConfiguration extends EnvironmentVaultConfiguration {
    public enum VaultConfigurationKey {
        AUTHENTICATION_PROPERTIES_FILE("vault.authentication.properties.file"),
        READ_TIMEOUT("vault.read.timeout"),
        CONNECTION_TIMEOUT("vault.connection.timeout"),
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

    /**
     * Creates a HashiCorpVaultConfiguration from property sources
     * @param propertySources A series of Spring PropertySource objects
     * @throws HashiCorpVaultConfigurationException If the authentication properties file could not be read
     */
    public HashiCorpVaultConfiguration(final PropertySource<?>... propertySources) {
        final ConfigurableEnvironment env = new StandardEnvironment();
        for(final PropertySource<?> propertySource : propertySources) {
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

        this.setApplicationContext(new HashiCorpVaultApplicationContext(env));

        sslConfiguration = env.getProperty(VaultConfigurationKey.URI.key).contains(HTTPS)
                ? super.sslConfiguration() : SslConfiguration.unconfigured();

        clientOptions = getClientOptions();
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

    private static Duration getDuration(String formattedDuration) {
        final double duration = FormatUtils.getPreciseTimeDuration(formattedDuration, TimeUnit.MILLISECONDS);
        return Duration.ofMillis(Double.valueOf(duration).longValue());
    }
}
