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
package org.apache.nifi.vault.hashicorp;

import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultConfiguration;
import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultProperties;
import org.springframework.vault.authentication.SimpleSessionManager;
import org.springframework.vault.client.ClientHttpRequestFactoryFactory;
import org.springframework.vault.core.VaultTemplate;
import org.springframework.vault.core.VaultTransitOperations;
import org.springframework.vault.support.Ciphertext;
import org.springframework.vault.support.ClientOptions;
import org.springframework.vault.support.Plaintext;
import org.springframework.vault.support.SslConfiguration;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Implements the VaultCommunicationService using Spring Vault
 */
public class StandardHashiCorpVaultCommunicationService implements HashiCorpVaultCommunicationService {
    private static final String HTTPS = "https";

    private final HashiCorpVaultConfiguration vaultConfiguration;
    private final VaultTemplate vaultTemplate;
    private final VaultTransitOperations transitOperations;

    /**
     * Creates a VaultCommunicationService that uses Spring Vault.
     * @param vaultProperties Properties to configure the service
     * @throws HashiCorpVaultConfigurationException If the configuration was invalid
     */
    public StandardHashiCorpVaultCommunicationService(final HashiCorpVaultProperties vaultProperties) throws HashiCorpVaultConfigurationException {
        this.vaultConfiguration = new HashiCorpVaultConfiguration(vaultProperties);

        final SslConfiguration sslConfiguration = vaultProperties.getUri().contains(HTTPS)
                ? vaultConfiguration.sslConfiguration() : SslConfiguration.unconfigured();

        final ClientOptions clientOptions = getClientOptions(vaultProperties);

        vaultTemplate = new VaultTemplate(vaultConfiguration.vaultEndpoint(),
                ClientHttpRequestFactoryFactory.create(clientOptions, sslConfiguration),
                new SimpleSessionManager(vaultConfiguration.clientAuthentication()));

        transitOperations = vaultTemplate.opsForTransit();
    }

    private static ClientOptions getClientOptions(HashiCorpVaultProperties vaultProperties) {
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

    @Override
    public String encrypt(final String transitKey, final byte[] plainText) {
        return transitOperations.encrypt(transitKey, Plaintext.of(plainText)).getCiphertext();
    }

    @Override
    public byte[] decrypt(final String transitKey, final String cipherText) {
        return transitOperations.decrypt(transitKey, Ciphertext.of(cipherText)).getPlaintext();
    }
}
