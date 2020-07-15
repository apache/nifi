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
package org.apache.nifi.security.kms;

import java.security.KeyManagementException;
import java.util.Map;
import java.util.stream.Collectors;
import javax.crypto.SecretKey;
import org.apache.nifi.security.repository.config.RepositoryEncryptionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class to build {@link KeyProvider} instances. Currently supports {@link StaticKeyProvider} and {@link FileBasedKeyProvider}.
 */
public class KeyProviderFactory {
    private static final Logger logger = LoggerFactory.getLogger(KeyProviderFactory.class);

    /**
     * Returns a key provider instantiated from the configuration values in a {@link RepositoryEncryptionConfiguration} object.
     *
     * @param rec     the data container for config values (usually extracted from {@link org.apache.nifi.util.NiFiProperties})
     * @param rootKey the root key used to decrypt wrapped keys
     * @return the configured key provider
     * @throws KeyManagementException if the key provider cannot be instantiated
     */
    public static KeyProvider buildKeyProvider(RepositoryEncryptionConfiguration rec, SecretKey rootKey) throws KeyManagementException {
        if (rec == null) {
            throw new KeyManagementException("The repository encryption configuration values are required to build a key provider");
        }
        return buildKeyProvider(rec.getKeyProviderImplementation(), rec.getKeyProviderLocation(), rec.getEncryptionKeyId(), rec.getEncryptionKeys(), rootKey);
    }

    /**
     * Returns a key provider instantiated from the configuration values in a {@link RepositoryEncryptionConfiguration} object.
     *
     * @param implementationClassName the key provider class name
     * @param keyProviderLocation     the filepath/URL of the stored keys
     * @param keyId                   the active key id
     * @param encryptionKeys          the available encryption keys
     * @param rootKey                 the root key used to decrypt wrapped keys
     * @return the configured key provider
     * @throws KeyManagementException if the key provider cannot be instantiated
     */
    public static KeyProvider buildKeyProvider(String implementationClassName, String keyProviderLocation, String keyId, Map<String, String> encryptionKeys,
                                               SecretKey rootKey) throws KeyManagementException {
        KeyProvider keyProvider;

        implementationClassName = CryptoUtils.handleLegacyPackages(implementationClassName);

        if (StaticKeyProvider.class.getName().equals(implementationClassName)) {
            // Get all the keys (map) from config
            if (CryptoUtils.isValidKeyProvider(implementationClassName, keyProviderLocation, keyId, encryptionKeys)) {
                Map<String, SecretKey> formedKeys = encryptionKeys.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> {
                                    try {
                                        return CryptoUtils.formKeyFromHex(e.getValue());
                                    } catch (KeyManagementException e1) {
                                        // This should never happen because the hex has already been validated
                                        logger.error("Encountered an error: ", e1);
                                        return null;
                                    }
                                }));
                keyProvider = new StaticKeyProvider(formedKeys);
            } else {
                final String msg = "The StaticKeyProvider definition is not valid";
                logger.error(msg);
                throw new KeyManagementException(msg);
            }
        } else if (FileBasedKeyProvider.class.getName().equals(implementationClassName)) {
            keyProvider = new FileBasedKeyProvider(keyProviderLocation, rootKey);
            if (!keyProvider.keyExists(keyId)) {
                throw new KeyManagementException("The specified key ID " + keyId + " is not in the key definition file");
            }
        } else {
            throw new KeyManagementException("Invalid key provider implementation provided: " + implementationClassName);
        }

        return keyProvider;
    }

    /**
     * Returns true if this {@link KeyProvider} implementation requires the presence of the {@code root key} in order to decrypt the available data encryption keys.
     *
     * @param implementationClassName the key provider implementation class
     * @return true if this implementation requires the root key to operate
     * @throws KeyManagementException if the provided class name is not a valid key provider implementation
     */
    public static boolean requiresRootKey(String implementationClassName) throws KeyManagementException {
        implementationClassName = CryptoUtils.handleLegacyPackages(implementationClassName);
        return FileBasedKeyProvider.class.getName().equals(implementationClassName);
    }
}
