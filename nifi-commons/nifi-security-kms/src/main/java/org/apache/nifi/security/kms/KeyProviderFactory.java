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

import org.apache.commons.codec.DecoderException;
import org.apache.nifi.security.kms.configuration.FileBasedKeyProviderConfiguration;
import org.apache.nifi.security.kms.configuration.KeyProviderConfiguration;
import org.apache.nifi.security.kms.configuration.KeyStoreKeyProviderConfiguration;
import org.apache.nifi.security.kms.configuration.StaticKeyProviderConfiguration;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.security.kms.reader.KeyReaderException;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;

/**
 * Key Provider Factory
 */
public class KeyProviderFactory {
    private static final String SECRET_KEY_ALGORITHM = "AES";

    /**
     * Get Key Provider based on Configuration
     *
     * @param configuration Key Provider Configuration
     * @return Key Provider
     */
    public static KeyProvider getKeyProvider(final KeyProviderConfiguration<?> configuration) {
        KeyProvider keyProvider;

        if (configuration instanceof StaticKeyProviderConfiguration) {
            final StaticKeyProviderConfiguration providerConfiguration = (StaticKeyProviderConfiguration) configuration;
            final Map<String, SecretKey> secretKeys;
            try {
                secretKeys = getSecretKeys(providerConfiguration.getKeys());
                keyProvider = new StaticKeyProvider(secretKeys);
            } catch (final DecoderException e) {
                throw new KeyReaderException("Decoding Hexadecimal Secret Keys failed", e);
            }
        } else if (configuration instanceof FileBasedKeyProviderConfiguration) {
            final FileBasedKeyProviderConfiguration providerConfiguration = (FileBasedKeyProviderConfiguration) configuration;
            final Path keyProviderPath = Paths.get(providerConfiguration.getLocation());
            keyProvider = new FileBasedKeyProvider(keyProviderPath, providerConfiguration.getRootKey());
        } else if (configuration instanceof KeyStoreKeyProviderConfiguration) {
            final KeyStoreKeyProviderConfiguration providerConfiguration = (KeyStoreKeyProviderConfiguration) configuration;
            final KeyStore keyStore = providerConfiguration.getKeyStore();
            keyProvider = new KeyStoreKeyProvider(keyStore, providerConfiguration.getKeyPassword());
        } else {
            throw new UnsupportedOperationException(String.format("Key Provider [%s] not supported", configuration.getKeyProviderClass().getName()));
        }

        return keyProvider;
    }

    private static Map<String, SecretKey> getSecretKeys(final Map<String, String> keys) throws DecoderException {
        final Map<String, SecretKey> secretKeys = new HashMap<>();

        for (final Map.Entry<String, String> keyEntry : keys.entrySet()) {
            final byte[] encodedSecretKey = Hex.decodeHex(keyEntry.getValue());
            final SecretKey secretKey = new SecretKeySpec(encodedSecretKey, SECRET_KEY_ALGORITHM);
            secretKeys.put(keyEntry.getKey(), secretKey);
        }

        return secretKeys;
    }
}
