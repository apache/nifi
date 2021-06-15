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
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * A Vault configuration that uses the NiFiVaultEnvironment.
 */
public class HashiCorpVaultConfiguration extends EnvironmentVaultConfiguration {
    private static final String HTTPS = "https";

    private final SslConfiguration sslConfiguration;
    private final ClientOptions clientOptions;

    public HashiCorpVaultConfiguration(final HashiCorpVaultProperties vaultProperties) throws HashiCorpVaultConfigurationException {
        final ConfigurableEnvironment env = new StandardEnvironment();

        try {
            env.getPropertySources().addFirst(new ResourcePropertySource(new FileSystemResource(Paths.get(vaultProperties.getAuthPropertiesFilename()))));
        } catch (IOException e) {
            throw new HashiCorpVaultConfigurationException("Could not load auth properties", e);
        }
        env.getPropertySources().addFirst(new HashiCorpVaultPropertySource(vaultProperties));

        this.setApplicationContext(new HashiCorpVaultApplicationContext(env));

        sslConfiguration = vaultProperties.getUri().contains(HTTPS)
                ? super.sslConfiguration() : SslConfiguration.unconfigured();

        clientOptions = getClientOptions(vaultProperties);
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

    private static ClientOptions getClientOptions(final HashiCorpVaultProperties vaultProperties) {
        final ClientOptions clientOptions = new ClientOptions();
        Duration readTimeoutDuration = clientOptions.getReadTimeout();
        Duration connectionTimeoutDuration = clientOptions.getConnectionTimeout();
        final Optional<String> configuredReadTimeout = vaultProperties.getReadTimeout();
        if (configuredReadTimeout.isPresent()) {
            readTimeoutDuration = getDuration(configuredReadTimeout.get());
        }
        final Optional<String> configuredConnectionTimeout = vaultProperties.getConnectionTimeout();
        if (configuredConnectionTimeout.isPresent()) {
            connectionTimeoutDuration = getDuration(configuredConnectionTimeout.get());
        }
        return new ClientOptions(connectionTimeoutDuration, readTimeoutDuration);
    }

    private static Duration getDuration(String formattedDuration) {
        final double duration = FormatUtils.getPreciseTimeDuration(formattedDuration, TimeUnit.MILLISECONDS);
        return Duration.ofMillis(Double.valueOf(duration).longValue());
    }
}
