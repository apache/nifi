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

import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultConfiguration;
import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultProperties;
import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.vault.authentication.SimpleSessionManager;
import org.springframework.vault.client.ClientHttpRequestFactoryFactory;
import org.springframework.vault.core.VaultTemplate;
import org.springframework.vault.core.VaultTransitOperations;
import org.springframework.vault.support.Ciphertext;
import org.springframework.vault.support.Plaintext;

/**
 * Implements the VaultCommunicationService using Spring Vault
 */
public class StandardHashiCorpVaultCommunicationService implements HashiCorpVaultCommunicationService {

    private final HashiCorpVaultConfiguration vaultConfiguration;
    private final VaultTemplate vaultTemplate;
    private final VaultTransitOperations transitOperations;

    /**
     * Creates a VaultCommunicationService that uses Spring Vault.
     * @param propertySources Property sources to configure the service
     * @throws HashiCorpVaultConfigurationException If the configuration was invalid
     */
    public StandardHashiCorpVaultCommunicationService(final PropertySource<?>... propertySources) throws HashiCorpVaultConfigurationException {
        vaultConfiguration = new HashiCorpVaultConfiguration(propertySources);

        vaultTemplate = new VaultTemplate(vaultConfiguration.vaultEndpoint(),
                ClientHttpRequestFactoryFactory.create(vaultConfiguration.clientOptions(), vaultConfiguration.sslConfiguration()),
                new SimpleSessionManager(vaultConfiguration.clientAuthentication()));

        transitOperations = vaultTemplate.opsForTransit();
    }

    /**
     * Creates a VaultCommunicationService that uses Spring Vault.
     * @param vaultProperties Properties to configure the service
     * @throws HashiCorpVaultConfigurationException If the configuration was invalid
     */
    public StandardHashiCorpVaultCommunicationService(final HashiCorpVaultProperties vaultProperties) throws HashiCorpVaultConfigurationException {
        this(new HashiCorpVaultPropertySource(vaultProperties));
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
