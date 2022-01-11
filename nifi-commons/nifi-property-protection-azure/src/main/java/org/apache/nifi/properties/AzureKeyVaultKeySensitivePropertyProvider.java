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
package org.apache.nifi.properties;

import com.azure.security.keyvault.keys.models.KeyVaultKey;
import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.azure.security.keyvault.keys.cryptography.models.DecryptResult;
import com.azure.security.keyvault.keys.cryptography.models.EncryptResult;
import com.azure.security.keyvault.keys.cryptography.models.EncryptionAlgorithm;
import com.azure.security.keyvault.keys.models.KeyOperation;
import com.azure.security.keyvault.keys.models.KeyProperties;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Microsoft Azure Key Vault Key Sensitive Property Provider using Cryptography Client for encryption operations
 */
public class AzureKeyVaultKeySensitivePropertyProvider extends ClientBasedEncodedSensitivePropertyProvider<CryptographyClient> {
    protected static final String ENCRYPTION_ALGORITHM_PROPERTY = "azure.keyvault.encryption.algorithm";

    protected static final List<KeyOperation> REQUIRED_OPERATIONS = Arrays.asList(KeyOperation.DECRYPT, KeyOperation.ENCRYPT);

    private static final String IDENTIFIER_KEY = "azure/keyvault/key";

    private EncryptionAlgorithm encryptionAlgorithm;

    AzureKeyVaultKeySensitivePropertyProvider(final CryptographyClient cryptographyClient, final Properties properties) {
        super(cryptographyClient, properties);
    }

    @Override
    public String getIdentifierKey() {
        return IDENTIFIER_KEY;
    }

    /**
     * Validate Client and Key Operations with Encryption Algorithm when configured
     *
     * @param cryptographyClient Cryptography Client
     */
    @Override
    protected void validate(final CryptographyClient cryptographyClient) {
        if (cryptographyClient == null) {
            logger.debug("Azure Cryptography Client not configured");
        } else {
            try {
                final KeyVaultKey keyVaultKey = cryptographyClient.getKey();
                final String id = keyVaultKey.getId();
                final KeyProperties keyProperties = keyVaultKey.getProperties();
                if (keyProperties.isEnabled()) {
                    final List<KeyOperation> keyOperations = keyVaultKey.getKeyOperations();
                    if (keyOperations.containsAll(REQUIRED_OPERATIONS)) {
                        logger.info("Azure Key Vault Key [{}] Validated", id);
                    } else {
                        throw new SensitivePropertyProtectionException(String.format("Azure Key Vault Key [%s] Missing Operations %s", id, REQUIRED_OPERATIONS));
                    }
                } else {
                    throw new SensitivePropertyProtectionException(String.format("Azure Key Vault Key [%s] Disabled", id));
                }
            } catch (final RuntimeException e) {
                throw new SensitivePropertyProtectionException("Azure Key Vault Key Validation Failed", e);
            }
            final String algorithm = getProperties().getProperty(ENCRYPTION_ALGORITHM_PROPERTY);
            if (algorithm == null || algorithm.isEmpty()) {
                throw new SensitivePropertyProtectionException("Azure Key Vault Key Algorithm not configured");
            }
            encryptionAlgorithm = EncryptionAlgorithm.fromString(algorithm);
        }
    }

    /**
     * Get encrypted bytes
     *
     * @param bytes Unprotected bytes
     * @return Encrypted bytes
     */
    @Override
    protected byte[] getEncrypted(final byte[] bytes) {
        final EncryptResult encryptResult = getClient().encrypt(encryptionAlgorithm, bytes);
        return encryptResult.getCipherText();
    }

    /**
     * Get decrypted bytes
     *
     * @param bytes Encrypted bytes
     * @return Decrypted bytes
     */
    @Override
    protected byte[] getDecrypted(final byte[] bytes) {
        final DecryptResult decryptResult = getClient().decrypt(encryptionAlgorithm, bytes);
        return decryptResult.getPlainText();
    }
}
