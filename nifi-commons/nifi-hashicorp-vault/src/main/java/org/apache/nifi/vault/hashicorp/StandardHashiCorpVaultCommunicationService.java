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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultConfiguration;
import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultProperties;
import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.vault.authentication.SimpleSessionManager;
import org.springframework.vault.client.ClientHttpRequestFactoryFactory;
import org.springframework.vault.core.VaultKeyValueOperations;
import org.springframework.vault.core.VaultKeyValueOperationsSupport.KeyValueBackend;
import org.springframework.vault.core.VaultTemplate;
import org.springframework.vault.core.VaultTransitOperations;
import org.springframework.vault.support.Ciphertext;
import org.springframework.vault.support.Plaintext;
import org.springframework.vault.support.VaultResponseSupport;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Implements the VaultCommunicationService using Spring Vault
 */
public class StandardHashiCorpVaultCommunicationService implements HashiCorpVaultCommunicationService {
    private final VaultTemplate vaultTemplate;
    private final VaultTransitOperations transitOperations;
    private final Map<String, VaultKeyValueOperations> keyValueOperationsMap;
    private final KeyValueBackend keyValueBackend;

    /**
     * Creates a VaultCommunicationService that uses Spring Vault.
     * @param propertySources Property sources to configure the service
     * @throws HashiCorpVaultConfigurationException If the configuration was invalid
     */
    public StandardHashiCorpVaultCommunicationService(final PropertySource<?>... propertySources) throws HashiCorpVaultConfigurationException {
        final HashiCorpVaultConfiguration vaultConfiguration = new HashiCorpVaultConfiguration(propertySources);

        vaultTemplate = new VaultTemplate(vaultConfiguration.vaultEndpoint(),
                ClientHttpRequestFactoryFactory.create(vaultConfiguration.clientOptions(), vaultConfiguration.sslConfiguration()),
                new SimpleSessionManager(vaultConfiguration.clientAuthentication()));

        transitOperations = vaultTemplate.opsForTransit();
        keyValueBackend = vaultConfiguration.getKeyValueBackend();
        keyValueOperationsMap = new HashMap<>();
    }

    @Override
    public String getServerVersion() {
        return vaultTemplate.opsForSys().health().getVersion();
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
    public String encrypt(final String transitPath, final byte[] plainText) {
        return transitOperations.encrypt(transitPath, Plaintext.of(plainText)).getCiphertext();
    }

    @Override
    public byte[] decrypt(final String transitPath, final String cipherText) {
        return transitOperations.decrypt(transitPath, Ciphertext.of(cipherText)).getPlaintext();
    }

    /**
     * Writes the value to the "value" secretKey of the secret with the path [keyValuePath]/[secretKey].
     * @param keyValuePath The Vault path to use for the configured Key/Value v1 Secrets Engine
     * @param secretKey The secret secretKey
     * @param value The secret value
     */
    @Override
    public void writeKeyValueSecret(final String keyValuePath, final String secretKey, final String value) {
        Objects.requireNonNull(keyValuePath, "Vault K/V path must be specified");
        Objects.requireNonNull(secretKey, "Secret secretKey must be specified");
        Objects.requireNonNull(value, "Secret value must be specified");
        final VaultKeyValueOperations keyValueOperations = keyValueOperationsMap
                .computeIfAbsent(keyValuePath, path -> vaultTemplate.opsForKeyValue(path, keyValueBackend));
        keyValueOperations.put(secretKey, new SecretData(value));
    }

    /**
     * Returns the value of the "value" secretKey from the secret at the path [keyValuePath]/[secretKey].
     * @param keyValuePath The Vault path to use for the configured Key/Value v1 Secrets Engine
     * @param secretKey The secret secretKey
     * @return The value of the secret
     */
    @Override
    public Optional<String> readKeyValueSecret(final String keyValuePath, final String secretKey) {
        Objects.requireNonNull(keyValuePath, "Vault K/V path must be specified");
        Objects.requireNonNull(secretKey, "Secret secretKey must be specified");
        final VaultKeyValueOperations keyValueOperations = keyValueOperationsMap
                .computeIfAbsent(keyValuePath, path -> vaultTemplate.opsForKeyValue(path, keyValueBackend));
        final VaultResponseSupport<SecretData> response = keyValueOperations.get(secretKey, SecretData.class);
        return response == null ? Optional.empty() : Optional.ofNullable(response.getRequiredData().getValue());
    }

    @Override
    public void writeKeyValueSecretMap(final String keyValuePath, final String secretKey, final Map<String, String> keyValues) {
        Objects.requireNonNull(keyValuePath, "Vault K/V path must be specified");
        Objects.requireNonNull(secretKey, "Secret secretKey must be specified");
        Objects.requireNonNull(keyValues, "Key/values map must be specified");
        if (keyValues.isEmpty()) {
            return;
        }
        final VaultKeyValueOperations keyValueOperations = keyValueOperationsMap
                .computeIfAbsent(keyValuePath, path -> vaultTemplate.opsForKeyValue(path, keyValueBackend));
        keyValueOperations.put(secretKey, keyValues);
    }

    @Override
    public Map<String, String> readKeyValueSecretMap(final String keyValuePath, final String key) {
        return readKeyValueSecretMap(keyValuePath, key, keyValueBackend.name());
    }

    @Override
    public Map<String, String> readKeyValueSecretMap(final String keyValuePath, final String key, final String version) {
        final VaultResponseSupport<Map> response = vaultTemplate.opsForKeyValue(keyValuePath, KeyValueBackend.valueOf(version)).get(key, Map.class);
        return response == null ? Collections.emptyMap() : (Map<String, String>) response.getRequiredData();
    }

    @Override
    public List<String> listKeyValueSecrets(final String keyValuePath) {
        return listKeyValueSecrets(keyValuePath, KeyValueBackend.KV_1.name());
    }

    @Override
    public List<String> listKeyValueSecrets(final String keyValuePath, final String version) {
        return vaultTemplate.opsForKeyValue(keyValuePath, KeyValueBackend.valueOf(version)).list("/");
    }

    private static class SecretData {
        private final String value;

        @JsonCreator
        public SecretData(@JsonProperty("value") final String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
