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
package org.apache.nifi.security.repository;

import static org.apache.nifi.security.kms.CryptoUtils.isValidKeyProvider;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import org.apache.nifi.security.kms.CryptoUtils;
import org.apache.nifi.security.kms.EncryptionException;
import org.apache.nifi.security.kms.FileBasedKeyProvider;
import org.apache.nifi.security.kms.KeyProvider;
import org.apache.nifi.security.kms.KeyProviderFactory;
import org.apache.nifi.security.kms.KeyStoreKeyProvider;
import org.apache.nifi.security.kms.StaticKeyProvider;
import org.apache.nifi.security.kms.configuration.FileBasedKeyProviderConfiguration;
import org.apache.nifi.security.kms.configuration.KeyProviderConfiguration;
import org.apache.nifi.security.kms.configuration.KeyStoreKeyProviderConfiguration;
import org.apache.nifi.security.kms.configuration.StaticKeyProviderConfiguration;
import org.apache.nifi.security.repository.config.RepositoryEncryptionConfiguration;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.security.util.crypto.AESKeyedCipherProvider;
import org.apache.nifi.stream.io.NonCloseableInputStream;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepositoryEncryptorUtils {
    private static final Logger logger = LoggerFactory.getLogger(RepositoryEncryptorUtils.class);

    private static final int CONTENT_HEADER_SIZE = 2;
    private static final int IV_LENGTH = 16;
    private static final int MIN_METADATA_LENGTH = IV_LENGTH + 3 + 3; // 3 delimiters and 3 non-zero elements
    private static final String EWAPR_CLASS_NAME = "org.apache.nifi.provenance.EncryptedWriteAheadProvenanceRepository";

    // TODO: Add Javadoc

    public static byte[] serializeEncryptionMetadata(RepositoryObjectEncryptionMetadata metadata) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream outputStream = new ObjectOutputStream(baos);
        outputStream.writeObject(metadata);
        outputStream.close();
        return baos.toByteArray();
    }

    public static Cipher initCipher(AESKeyedCipherProvider aesKeyedCipherProvider, EncryptionMethod method, int mode, SecretKey key, byte[] ivBytes) throws EncryptionException {
        try {
            if (method == null || key == null || ivBytes == null) {
                throw new IllegalArgumentException("Missing critical information");
            }
            return aesKeyedCipherProvider.getCipher(method, key, ivBytes, mode == Cipher.ENCRYPT_MODE);
        } catch (Exception e) {
            logger.error("Encountered an exception initializing the cipher", e);
            throw new EncryptionException(e);
        }
    }

    public static RepositoryObjectEncryptionMetadata extractEncryptionMetadata(byte[] encryptedRecord) throws EncryptionException, IOException, ClassNotFoundException {
        // TODO: Inject parser for min metadata length
        if (encryptedRecord == null || encryptedRecord.length < MIN_METADATA_LENGTH) {
            throw new EncryptionException("The encrypted record is too short to contain the metadata");
        }

        // TODO: Inject parser for SENTINEL vs non-SENTINEL
        // Skip the first byte (SENTINEL) and don't need to copy all the serialized record
        ByteArrayInputStream bais = new ByteArrayInputStream(encryptedRecord);
        // bais.read();
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (RepositoryObjectEncryptionMetadata) ois.readObject();
        }
    }

    public static RepositoryObjectEncryptionMetadata extractEncryptionMetadata(InputStream encryptedRecord) throws EncryptionException, IOException, ClassNotFoundException {
        // TODO: Inject parser for min metadata length
        if (encryptedRecord == null) {
            throw new EncryptionException("The encrypted record is too short to contain the metadata");
        }

        // TODO: Inject parser for SENTINEL vs non-SENTINEL
        // Skip the first two bytes (EM_START_SENTINEL) and don't need to copy all the serialized record
        // TODO: May need to seek for EM_START_SENTINEL segment first
        encryptedRecord.read(new byte[CONTENT_HEADER_SIZE]);
        try (ObjectInputStream ois = new ObjectInputStream(new NonCloseableInputStream(encryptedRecord))) {
            return (RepositoryObjectEncryptionMetadata) ois.readObject();
        }
    }

    public static byte[] extractCipherBytes(byte[] encryptedRecord, RepositoryObjectEncryptionMetadata metadata) {
        // If the length is known, there is no header, start from total length - cipher length
        // If the length is unknown (streaming/content), calculate the metadata length + header length and start from there
        int cipherBytesStart = metadata.cipherByteLength > 0 ? encryptedRecord.length - metadata.cipherByteLength : metadata.length() + CONTENT_HEADER_SIZE;
        return Arrays.copyOfRange(encryptedRecord, cipherBytesStart, encryptedRecord.length);
    }

    /**
     * Returns {@code true} if the specified repository is correctly configured for an
     * encrypted implementation. Requires the repository implementation to support encryption
     * and at least one valid key to be configured.
     *
     * @param niFiProperties the {@link NiFiProperties} instance to validate
     * @param repositoryType the specific repository configuration to check
     * @return true if encryption is successfully configured for the specified repository
     */
    public static boolean isRepositoryEncryptionConfigured(NiFiProperties niFiProperties, RepositoryType repositoryType) {
        switch (repositoryType) {
            case CONTENT:
                return isContentRepositoryEncryptionConfigured(niFiProperties);
            case PROVENANCE:
                return isProvenanceRepositoryEncryptionConfigured(niFiProperties);
            case FLOWFILE:
                return isFlowFileRepositoryEncryptionConfigured(niFiProperties);
            default:
                logger.warn("Repository encryption configuration validation attempted for {}, an invalid repository type", repositoryType);
                return false;
        }
    }

    /**
     * Returns {@code true} if the provenance repository is correctly configured for an
     * encrypted implementation. Requires the repository implementation to support encryption
     * and at least one valid key to be configured.
     *
     * @param niFiProperties the {@link NiFiProperties} instance to validate
     * @return true if encryption is successfully configured for the provenance repository
     */
    static boolean isProvenanceRepositoryEncryptionConfigured(NiFiProperties niFiProperties) {
        final String implementationClassName = niFiProperties.getProperty(NiFiProperties.PROVENANCE_REPO_IMPLEMENTATION_CLASS);
        // Referencing EWAPR.class.getName() would require a dependency on the module
        boolean encryptedRepo = EWAPR_CLASS_NAME.equals(implementationClassName);
        if (encryptedRepo) {
            return isValidKeyProvider(
                    niFiProperties.getProperty(NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS),
                    niFiProperties.getProperty(NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_LOCATION),
                    niFiProperties.getProvenanceRepoEncryptionKeyId(),
                    niFiProperties.getProvenanceRepoEncryptionKeys());
        } else {
            return false;
        }
    }

    /**
     * Returns {@code true} if the content repository is correctly configured for an encrypted
     * implementation. Requires the repository implementation to support encryption and at least
     * one valid key to be configured.
     *
     * @param niFiProperties the {@link NiFiProperties} instance to validate
     * @return true if encryption is successfully configured for the content repository
     */
    static boolean isContentRepositoryEncryptionConfigured(NiFiProperties niFiProperties) {
        final String implementationClassName = niFiProperties.getProperty(NiFiProperties.CONTENT_REPOSITORY_IMPLEMENTATION);
        // Referencing EFSR.class.getName() would require a dependency on the module
        boolean encryptedRepo = CryptoUtils.ENCRYPTED_FSR_CLASS_NAME.equals(implementationClassName);
        if (encryptedRepo) {
            return isValidKeyProvider(
                    niFiProperties.getProperty(NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS),
                    niFiProperties.getProperty(NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_LOCATION),
                    niFiProperties.getContentRepositoryEncryptionKeyId(),
                    niFiProperties.getContentRepositoryEncryptionKeys());
        } else {
            return false;
        }
    }

    /**
     * Returns {@code true} if the flowfile repository is correctly configured for an encrypted
     * implementation. Requires the repository implementation to support encryption and at least
     * one valid key to be configured.
     *
     * @param niFiProperties the {@link NiFiProperties} instance to validate
     * @return true if encryption is successfully configured for the flowfile repository
     */
    static boolean isFlowFileRepositoryEncryptionConfigured(NiFiProperties niFiProperties) {
        final String implementationClassName = niFiProperties.getProperty(NiFiProperties.FLOWFILE_REPOSITORY_IMPLEMENTATION);
        boolean encryptedRepo = CryptoUtils.EWAFFR_CLASS_NAME.equals(implementationClassName);
        if (encryptedRepo) {
            return isValidKeyProvider(
                    niFiProperties.getProperty(NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS),
                    niFiProperties.getProperty(NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_LOCATION),
                    niFiProperties.getFlowFileRepoEncryptionKeyId(),
                    niFiProperties.getFlowFileRepoEncryptionKeys());
        } else {
            return false;
        }
    }

    /**
     * Utility method which returns the {@link KeyProvider} implementation class name for a given repository type.
     *
     * @param repositoryType the {@link RepositoryType} indicator
     * @return the FQCN of the implementation or {@code "no_such_key_provider_defined"} for unsupported repository types
     */
    static String determineKeyProviderImplementationClassName(RepositoryType repositoryType) {
        // TODO: Change to build string directly using repository type packagePath property or universal in NIFI-6617
        if (repositoryType == null) {
            logger.warn("Could not determine key provider implementation class name for null repository");
            return "no_such_key_provider_defined";
        }
        switch (repositoryType) {
            case FLOWFILE:
                return NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS;
            case CONTENT:
                return NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS;
            case PROVENANCE:
                return NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS;
            default:
                logger.warn("Could not determine key provider implementation class name for " + repositoryType.getName());
                return "no_such_key_provider_defined";
        }
    }

    /**
     * Returns a configured {@link KeyProvider} instance for the specified repository type given the configuration values in {@code nifi.properties}.
     *
     * @param niFiProperties the {@link NiFiProperties} object
     * @param repositoryType the {@link RepositoryType} indicator
     * @return the configured KeyProvider
     * @throws IOException if there is a problem reading the properties or they are not valid & complete
     */
    public static KeyProvider validateAndBuildRepositoryKeyProvider(NiFiProperties niFiProperties, RepositoryType repositoryType) throws IOException {
        if (isRepositoryEncryptionConfigured(niFiProperties, repositoryType)) {
            try {
                final RepositoryEncryptionConfiguration configuration = RepositoryEncryptionConfiguration.fromNiFiProperties(niFiProperties, repositoryType);
                return getKeyProvider(configuration);
            } catch (final KeyManagementException e) {
                final String msg = "Encountered an error building the key provider";
                logger.error(msg, e);
                throw new IOException(msg, e);
            }
        } else {
            throw new IOException("The provided configuration does not support an encrypted " + repositoryType.getName());
        }
    }

    /**
     * Returns a configured {@link KeyProvider} instance for the specified repository type given the configuration values.
     *
     * @param repositoryEncryptionConfiguration the {@link RepositoryEncryptionConfiguration} object
     * @return the configured KeyProvider
     * @throws IOException if there is a problem reading the properties or they are not valid & complete
     */
    public static KeyProvider validateAndBuildRepositoryKeyProvider(final RepositoryEncryptionConfiguration repositoryEncryptionConfiguration) throws IOException {
        try {
            return getKeyProvider(repositoryEncryptionConfiguration);
        } catch (final KeyManagementException e) {
            final String msg = "Encountered an error building the key provider";
            logger.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    private static KeyProvider getKeyProvider(final RepositoryEncryptionConfiguration repositoryEncryptionConfiguration) throws KeyManagementException {
        final KeyProviderConfiguration<?> keyProviderConfiguration = getKeyProviderConfiguration(repositoryEncryptionConfiguration);
        final KeyProvider keyProvider = KeyProviderFactory.getKeyProvider(keyProviderConfiguration);

        final String keyId = repositoryEncryptionConfiguration.getEncryptionKeyId();
        if (keyProvider.keyExists(keyId)) {
            return keyProvider;
        } else {
            throw new KeyManagementException(String.format("Key Identifier [%s] not found in Key Provider", keyId));
        }
    }

    private static KeyProviderConfiguration<?> getKeyProviderConfiguration(final RepositoryEncryptionConfiguration repositoryEncryptionConfiguration) throws KeyManagementException {
        final String keyProviderImplementation = repositoryEncryptionConfiguration.getKeyProviderImplementation();
        if (keyProviderImplementation.endsWith(StaticKeyProvider.class.getSimpleName())) {
            return new StaticKeyProviderConfiguration(repositoryEncryptionConfiguration.getEncryptionKeys());
        } else if (keyProviderImplementation.endsWith(FileBasedKeyProvider.class.getSimpleName())) {
            final SecretKey rootKey = CryptoUtils.getRootKey();
            return new FileBasedKeyProviderConfiguration(repositoryEncryptionConfiguration.getKeyProviderLocation(), rootKey);
        } else if (keyProviderImplementation.endsWith(KeyStoreKeyProvider.class.getSimpleName())) {
            final String keyProviderPassword = repositoryEncryptionConfiguration.getKeyProviderPassword();
            if (StringUtils.isBlank(keyProviderPassword)) {
                throw new KeyManagementException("Key Provider Password not configured");
            }
            final String location = repositoryEncryptionConfiguration.getKeyProviderLocation();
            final char[] keyStorePassword = repositoryEncryptionConfiguration.getKeyProviderPassword().toCharArray();
            final String keyStoreType = repositoryEncryptionConfiguration.getKeyStoreType();
            try {
                final KeyStore keyStore = KeyStoreUtils.loadSecretKeyStore(location, keyStorePassword, keyStoreType);
                return new KeyStoreKeyProviderConfiguration(keyStore, keyStorePassword);
            } catch (final TlsException e) {
                throw new KeyManagementException("Key Store Provider loading failed", e);
            }
        } else {
            throw new UnsupportedOperationException(String.format("Key Provider Implementation [%s] not supported", keyProviderImplementation));
        }
    }
}
