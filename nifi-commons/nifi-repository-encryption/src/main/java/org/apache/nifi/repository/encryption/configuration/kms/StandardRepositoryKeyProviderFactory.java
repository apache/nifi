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
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.util.Map;
import java.util.Objects;

import static org.apache.nifi.util.NiFiProperties.REPOSITORY_ENCRYPTION_KEY_PROVIDER;
import static org.apache.nifi.util.NiFiProperties.REPOSITORY_ENCRYPTION_KEY_PROVIDER_KEYSTORE_LOCATION;
import static org.apache.nifi.util.NiFiProperties.REPOSITORY_ENCRYPTION_KEY_PROVIDER_KEYSTORE_PASSWORD;

/**
 * Standard implementation of Repository Key Provider Factory supporting shared and fallback properties
 */
public class StandardRepositoryKeyProviderFactory implements RepositoryKeyProviderFactory {
    private static final Map<KeystoreType, String> KEY_STORE_EXTENSIONS = Map.of(
            KeystoreType.BCFKS, ".bcfks",
            KeystoreType.JKS, ".jks",
            KeystoreType.PKCS12, ".p12"
    );

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
            final KeystoreType keystoreType = getKeystoreTypeFromExtension(location);
            try {
                final KeyStore keyStore = loadSecretKeyStore(location, keyStorePassword, keystoreType);
                return new KeyStoreKeyProviderConfiguration(keyStore, keyStorePassword);
            } catch (final GeneralSecurityException e) {
                throw new EncryptedConfigurationException("Key Store Provider loading failed", e);
            }
        } else {
            throw new UnsupportedOperationException(String.format("Key Provider [%s] not supported", encryptionKeyProvider));
        }
    }

    private KeystoreType getKeystoreTypeFromExtension(final String keystorePath) {
        KeystoreType keystoreType = KeystoreType.PKCS12;

        for (final Map.Entry<KeystoreType, String> keystoreTypeEntry : KEY_STORE_EXTENSIONS.entrySet()) {
            final String extension = keystoreTypeEntry.getValue().toLowerCase();
            if (keystorePath.endsWith(extension)) {
                keystoreType = keystoreTypeEntry.getKey();
                break;
            }
        }

        return keystoreType;
    }

    private KeyStore loadSecretKeyStore(final String location, final char[] keyStorePassword, final KeystoreType keystoreType) throws GeneralSecurityException {
        final KeyStore keyStore = getSecretKeyStore(keystoreType);
        try (InputStream inputStream = new FileInputStream(location)) {
            keyStore.load(inputStream, keyStorePassword);
        } catch (final IOException e) {
            throw new GeneralSecurityException("KeyStore loading failed [%s]".formatted(location), e);
        }
        return keyStore;
    }

    public static KeyStore getSecretKeyStore(final KeystoreType keystoreType) throws KeyStoreException {
        if (KeystoreType.BCFKS == keystoreType) {
            return KeyStore.getInstance(keystoreType.getType(), new BouncyCastleProvider());
        } else if (KeystoreType.PKCS12 == keystoreType) {
            return KeyStore.getInstance(keystoreType.getType());
        } else {
            throw new KeyStoreException(String.format("Keystore Type [%s] does not support Secret Keys", keystoreType.getType()));
        }
    }
}
