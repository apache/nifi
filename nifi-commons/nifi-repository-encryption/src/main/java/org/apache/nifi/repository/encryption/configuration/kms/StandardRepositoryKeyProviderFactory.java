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
package org.apache.nifi.repository.encryption.configuration.kms;

import org.apache.nifi.repository.encryption.configuration.EncryptedRepositoryType;
import org.apache.nifi.security.kms.KeyProvider;
import org.apache.nifi.security.kms.KeyProviderFactory;
import org.apache.nifi.security.kms.configuration.KeyProviderConfiguration;
import org.apache.nifi.security.kms.configuration.KeyStoreKeyProviderConfiguration;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;

import java.security.KeyStore;
import java.util.Objects;

import static org.apache.nifi.util.NiFiProperties.REPOSITORY_ENCRYPTION_KEY_PROVIDER;
import static org.apache.nifi.util.NiFiProperties.REPOSITORY_ENCRYPTION_KEY_PROVIDER_KEYSTORE_LOCATION;
import static org.apache.nifi.util.NiFiProperties.REPOSITORY_ENCRYPTION_KEY_PROVIDER_KEYSTORE_PASSWORD;

/**
 * Standard implementation of Repository Key Provider Factory supporting shared and fallback properties
 */
public class StandardRepositoryKeyProviderFactory implements RepositoryKeyProviderFactory {
    /**
     * Get Key Provider for specified Encrypted Repository Type using shared and fallback NiFi Properties
     *
     * @param encryptedRepositoryType Encrypted Repository Type
     * @param niFiProperties NiFi Properties
     * @return Key Provider configured using applicable properties
     */
    @Override
    public KeyProvider getKeyProvider(final EncryptedRepositoryType encryptedRepositoryType, final NiFiProperties niFiProperties) {
        Objects.requireNonNull(encryptedRepositoryType, "Encrypted Repository Type required");
        Objects.requireNonNull(niFiProperties, "NiFi Properties required");
        final EncryptionKeyProvider encryptionKeyProvider = getEncryptionKeyProvider(encryptedRepositoryType, niFiProperties);
        final KeyProviderConfiguration<?> keyProviderConfiguration = getKeyProviderConfiguration(encryptionKeyProvider, niFiProperties);
        return KeyProviderFactory.getKeyProvider(keyProviderConfiguration);
    }

    private EncryptionKeyProvider getEncryptionKeyProvider(final EncryptedRepositoryType encryptedRepositoryType, final NiFiProperties niFiProperties) {
        final String sharedKeyProvider = niFiProperties.getProperty(REPOSITORY_ENCRYPTION_KEY_PROVIDER);

        if (StringUtils.isBlank(sharedKeyProvider)) {
            final String message = String.format("Key Provider [%s] not configured for Repository Type [%s] ", sharedKeyProvider, encryptedRepositoryType);
            throw new EncryptedConfigurationException(message);
        }

        try {
            return EncryptionKeyProvider.valueOf(sharedKeyProvider);
        } catch (final IllegalArgumentException e) {
            final String message = String.format("Key Provider [%s] not supported for Repository Type [%s] ", sharedKeyProvider, encryptedRepositoryType);
            throw new EncryptedConfigurationException(message);
        }
    }

    private KeyProviderConfiguration<?> getKeyProviderConfiguration(final EncryptionKeyProvider encryptionKeyProvider,
                                                                    final NiFiProperties niFiProperties) {
        if (EncryptionKeyProvider.KEYSTORE == encryptionKeyProvider) {
            final String providerPassword = niFiProperties.getProperty(REPOSITORY_ENCRYPTION_KEY_PROVIDER_KEYSTORE_PASSWORD);
            if (StringUtils.isBlank(providerPassword)) {
                throw new EncryptedConfigurationException("Key Provider Password not configured");
            }
            final char[] keyStorePassword = providerPassword.toCharArray();
            final String location = niFiProperties.getProperty(REPOSITORY_ENCRYPTION_KEY_PROVIDER_KEYSTORE_LOCATION);
            final KeystoreType keystoreType = KeyStoreUtils.getKeystoreTypeFromExtension(location);
            try {
                final KeyStore keyStore = KeyStoreUtils.loadSecretKeyStore(location, keyStorePassword, keystoreType.getType());
                return new KeyStoreKeyProviderConfiguration(keyStore, keyStorePassword);
            } catch (final TlsException e) {
                throw new EncryptedConfigurationException("Key Store Provider loading failed", e);
            }
        } else {
            throw new UnsupportedOperationException(String.format("Key Provider [%s] not supported", encryptionKeyProvider));
        }
    }
}
