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
import org.apache.nifi.repository.encryption.configuration.EncryptionKeyProvider;
import org.apache.nifi.security.kms.FileBasedKeyProvider;
import org.apache.nifi.security.kms.KeyProvider;
import org.apache.nifi.security.kms.KeyProviderFactory;
import org.apache.nifi.security.kms.KeyStoreKeyProvider;
import org.apache.nifi.security.kms.StaticKeyProvider;
import org.apache.nifi.security.kms.configuration.FileBasedKeyProviderConfiguration;
import org.apache.nifi.security.kms.configuration.KeyProviderConfiguration;
import org.apache.nifi.security.kms.configuration.KeyStoreKeyProviderConfiguration;
import org.apache.nifi.security.kms.configuration.StaticKeyProviderConfiguration;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.util.NiFiBootstrapUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.bouncycastle.util.encoders.DecoderException;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.nifi.util.NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_PASSWORD;
import static org.apache.nifi.util.NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_PASSWORD;
import static org.apache.nifi.util.NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_PASSWORD;
import static org.apache.nifi.util.NiFiProperties.REPOSITORY_ENCRYPTION_KEY_PROVIDER;
import static org.apache.nifi.util.NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_LOCATION;
import static org.apache.nifi.util.NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS;
import static org.apache.nifi.util.NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_LOCATION;
import static org.apache.nifi.util.NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS;
import static org.apache.nifi.util.NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_LOCATION;
import static org.apache.nifi.util.NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS;
import static org.apache.nifi.util.NiFiProperties.REPOSITORY_ENCRYPTION_KEY_PROVIDER_KEYSTORE_LOCATION;
import static org.apache.nifi.util.NiFiProperties.REPOSITORY_ENCRYPTION_KEY_PROVIDER_KEYSTORE_PASSWORD;

/**
 * Standard implementation of Repository Key Provider Factory supporting shared and fallback properties
 */
public class StandardRepositoryKeyProviderFactory implements RepositoryKeyProviderFactory {
    private static final Logger logger = LoggerFactory.getLogger(StandardRepositoryKeyProviderFactory.class);

    private static final String ROOT_KEY_ALGORITHM = "AES";

    private static final Map<EncryptedRepositoryType, String> REPOSITORY_KEY_PROVIDER_CLASS_PROPERTIES = new HashMap<>();

    private static final Map<EncryptedRepositoryType, String> REPOSITORY_KEY_PROVIDER_LOCATION_PROPERTIES = new HashMap<>();

    private static final Map<EncryptedRepositoryType, String> REPOSITORY_KEY_PROVIDER_PASSWORD_PROPERTIES = new HashMap<>();

    static {
        REPOSITORY_KEY_PROVIDER_CLASS_PROPERTIES.put(EncryptedRepositoryType.CONTENT, CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS);
        REPOSITORY_KEY_PROVIDER_CLASS_PROPERTIES.put(EncryptedRepositoryType.FLOW_FILE, FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS);
        REPOSITORY_KEY_PROVIDER_CLASS_PROPERTIES.put(EncryptedRepositoryType.PROVENANCE, PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS);

        REPOSITORY_KEY_PROVIDER_LOCATION_PROPERTIES.put(EncryptedRepositoryType.CONTENT, CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_LOCATION);
        REPOSITORY_KEY_PROVIDER_LOCATION_PROPERTIES.put(EncryptedRepositoryType.FLOW_FILE, FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_LOCATION);
        REPOSITORY_KEY_PROVIDER_LOCATION_PROPERTIES.put(EncryptedRepositoryType.PROVENANCE, PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_LOCATION);

        REPOSITORY_KEY_PROVIDER_PASSWORD_PROPERTIES.put(EncryptedRepositoryType.CONTENT, CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_PASSWORD);
        REPOSITORY_KEY_PROVIDER_PASSWORD_PROPERTIES.put(EncryptedRepositoryType.FLOW_FILE, FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_PASSWORD);
        REPOSITORY_KEY_PROVIDER_PASSWORD_PROPERTIES.put(EncryptedRepositoryType.PROVENANCE, PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_PASSWORD);
    }

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
        final KeyProviderConfiguration<?> keyProviderConfiguration = getKeyProviderConfiguration(encryptedRepositoryType, encryptionKeyProvider, niFiProperties);
        return KeyProviderFactory.getKeyProvider(keyProviderConfiguration);
    }

    private EncryptionKeyProvider getEncryptionKeyProvider(final EncryptedRepositoryType encryptedRepositoryType, final NiFiProperties niFiProperties) {
        EncryptionKeyProvider encryptionKeyProvider = null;
        final String sharedKeyProvider = niFiProperties.getProperty(REPOSITORY_ENCRYPTION_KEY_PROVIDER);

        if (StringUtils.isBlank(sharedKeyProvider)) {
            logger.debug("Key Provider Property [{}] not configured", REPOSITORY_ENCRYPTION_KEY_PROVIDER);

            final String classProperty = REPOSITORY_KEY_PROVIDER_CLASS_PROPERTIES.get(encryptedRepositoryType);
            final String implementationClass = niFiProperties.getProperty(classProperty);
            if (StringUtils.isBlank(implementationClass)) {
                final String message = String.format("Key Provider Property [%s] not configured", classProperty);
                throw new EncryptedConfigurationException(message);
            } else {
                encryptionKeyProvider = getEncryptionKeyProvider(implementationClass);
            }
        } else {
            for (final EncryptionKeyProvider currentEncryptKeyProvider : EncryptionKeyProvider.values()) {
                if (currentEncryptKeyProvider.toString().equals(sharedKeyProvider)) {
                    encryptionKeyProvider = currentEncryptKeyProvider;
                }
            }
        }

        if (encryptionKeyProvider == null) {
            final String message = String.format("Key Provider [%s] not found for Repository Type [%s] ", sharedKeyProvider, encryptedRepositoryType);
            throw new EncryptedConfigurationException(message);
        }

        return encryptionKeyProvider;
    }

    private EncryptionKeyProvider getEncryptionKeyProvider(final String implementationClass) {
        EncryptionKeyProvider encryptionKeyProvider;

        if (implementationClass.endsWith(FileBasedKeyProvider.class.getSimpleName())) {
            encryptionKeyProvider = EncryptionKeyProvider.FILE_PROPERTIES;
        } else if (implementationClass.endsWith(KeyStoreKeyProvider.class.getSimpleName())) {
            encryptionKeyProvider = EncryptionKeyProvider.KEYSTORE;
        } else if (implementationClass.endsWith(StaticKeyProvider.class.getSimpleName())) {
            encryptionKeyProvider = EncryptionKeyProvider.NIFI_PROPERTIES;
        } else {
            final String message = String.format("Key Provider Class [%s] not supported", implementationClass);
            throw new IllegalArgumentException(message);
        }

        return encryptionKeyProvider;
    }

    private Map<String, String> getEncryptionKeys(final EncryptedRepositoryType encryptedRepositoryType, final NiFiProperties niFiProperties) {
        switch (encryptedRepositoryType) {
            case CONTENT:
                return niFiProperties.getContentRepositoryEncryptionKeys();
            case FLOW_FILE:
                return niFiProperties.getFlowFileRepoEncryptionKeys();
            case PROVENANCE:
                return niFiProperties.getProvenanceRepoEncryptionKeys();
            default:
                throw new IllegalArgumentException(String.format("Repository Type [%s] not supported", encryptedRepositoryType));
        }
    }

    private KeyProviderConfiguration<?> getKeyProviderConfiguration(final EncryptedRepositoryType encryptedRepositoryType,
                                                                    final EncryptionKeyProvider encryptionKeyProvider,
                                                                    final NiFiProperties niFiProperties) {
        if (EncryptionKeyProvider.NIFI_PROPERTIES == encryptionKeyProvider) {
            final Map<String, String> encryptionKeys = getEncryptionKeys(encryptedRepositoryType, niFiProperties);
            return new StaticKeyProviderConfiguration(encryptionKeys);
        } else if (EncryptionKeyProvider.FILE_PROPERTIES == encryptionKeyProvider) {
            final SecretKey rootKey = getRootKey();
            final String location = getProviderLocation(encryptedRepositoryType, niFiProperties);
            return new FileBasedKeyProviderConfiguration(location, rootKey);
        } else if (EncryptionKeyProvider.KEYSTORE == encryptionKeyProvider) {
            final String providerPassword = getProviderPassword(encryptedRepositoryType, niFiProperties);
            if (StringUtils.isBlank(providerPassword)) {
                throw new EncryptedConfigurationException("Key Provider Password not configured");
            }
            final char[] keyStorePassword = providerPassword.toCharArray();
            final String providerLocation = getProviderLocation(encryptedRepositoryType, niFiProperties);
            final String location = niFiProperties.getProperty(REPOSITORY_ENCRYPTION_KEY_PROVIDER_KEYSTORE_LOCATION, providerLocation);
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

    private String getProviderLocation(final EncryptedRepositoryType encryptedRepositoryType, final NiFiProperties niFiProperties) {
        final String locationProperty = REPOSITORY_KEY_PROVIDER_LOCATION_PROPERTIES.get(encryptedRepositoryType);
        return niFiProperties.getProperty(locationProperty);
    }

    private String getProviderPassword(final EncryptedRepositoryType encryptedRepositoryType, final NiFiProperties niFiProperties) {
        final String providerPassword = niFiProperties.getProperty(REPOSITORY_ENCRYPTION_KEY_PROVIDER_KEYSTORE_PASSWORD);
        final String passwordProperty = REPOSITORY_KEY_PROVIDER_PASSWORD_PROPERTIES.get(encryptedRepositoryType);
        return niFiProperties.getProperty(passwordProperty, providerPassword);
    }

    private static SecretKey getRootKey() {
        try {
            String rootKeyHex = NiFiBootstrapUtils.extractKeyFromBootstrapFile();
            return new SecretKeySpec(Hex.decode(rootKeyHex), ROOT_KEY_ALGORITHM);
        } catch (final IOException | DecoderException e) {
            throw new EncryptedConfigurationException("Read Root Key from Bootstrap Failed", e);
        }
    }
}
