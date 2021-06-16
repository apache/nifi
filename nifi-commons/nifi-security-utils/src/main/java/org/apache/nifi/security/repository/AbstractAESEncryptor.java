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

import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.Security;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.security.kms.EncryptionException;
import org.apache.nifi.security.kms.KeyProvider;
import org.apache.nifi.security.util.crypto.AESKeyedCipherProvider;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractAESEncryptor implements RepositoryObjectEncryptor {
    private static final Logger logger = LoggerFactory.getLogger(AbstractAESEncryptor.class);
    private static final byte[] EM_START_SENTINEL = new byte[]{0x00, 0x00};
    private static final byte[] EM_END_SENTINEL = new byte[]{(byte) 0xFF, (byte) 0xFF};
    private static String ALGORITHM = "AES/CTR/NoPadding";
    protected static final int IV_LENGTH = 16;
    protected static final byte[] EMPTY_IV = new byte[IV_LENGTH];
    // private static final String VERSION = "v1";
    // private static final List<String> SUPPORTED_VERSIONS = Arrays.asList(VERSION);

    protected KeyProvider keyProvider;

    protected AESKeyedCipherProvider aesKeyedCipherProvider = new AESKeyedCipherProvider();

    /**
     * Initializes the encryptor with a {@link KeyProvider}.
     *
     * @param keyProvider the key provider which will be responsible for accessing keys
     * @throws KeyManagementException if there is an issue configuring the key provider
     */
    @Override
    public void initialize(KeyProvider keyProvider) throws KeyManagementException {
        this.keyProvider = keyProvider;

        if (this.aesKeyedCipherProvider == null) {
            this.aesKeyedCipherProvider = new AESKeyedCipherProvider();
        }

        if (Security.getProvider("BC") == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    /**
     * Available for dependency injection to override the default {@link AESKeyedCipherProvider} if necessary.
     *
     * @param cipherProvider the AES cipher provider to use
     */
    void setCipherProvider(AESKeyedCipherProvider cipherProvider) {
        this.aesKeyedCipherProvider = cipherProvider;
    }

    /**
     * Utility method which extracts the {@link RepositoryObjectEncryptionMetadata} object from the {@code byte[]} or
     * {@link InputStream} provided and verifies common validation across both streaming and block decryption. Returns
     * the extracted metadata object.
     *
     * @param ciphertextSource the encrypted source -- can be {@code byte[]} or {@code InputStream}
     * @param identifier the unique identifier for this source
     * @param descriptor the generic name for this source type for logging/error messages
     * @param supportedVersions the list of supported versions for the particular encryptor calling this method (see
     * {@link org.apache.nifi.security.repository.stream.aes.RepositoryObjectAESCTREncryptor} and
     * {@link org.apache.nifi.security.repository.block.aes.RepositoryObjectAESGCMEncryptor} for
     * {@code SUPPORTED_VERSIONS})
     * @return the extracted {@link RepositoryObjectEncryptionMetadata} object
     * @throws EncryptionException if there is an exception parsing or validating the source
     */
    public static RepositoryObjectEncryptionMetadata prepareObjectForDecryption(Object ciphertextSource,
                                                                                String identifier, String descriptor, List<String> supportedVersions) throws EncryptionException {
        if (ciphertextSource == null) {
            throw new EncryptionException("The encrypted " + descriptor + " cannot be missing");
        }

        RepositoryObjectEncryptionMetadata metadata;
        try {
            if (ciphertextSource instanceof InputStream) {
                logger.debug("Detected encrypted input stream for {} with ID {}", descriptor, identifier);
                InputStream ciphertextStream = (InputStream) ciphertextSource;
                metadata = RepositoryEncryptorUtils.extractEncryptionMetadata(ciphertextStream);
            } else if (ciphertextSource instanceof byte[]) {
                logger.debug("Detected byte[] for {} with ID {}", descriptor, identifier);
                byte[] ciphertextBytes = (byte[]) ciphertextSource;
                metadata = RepositoryEncryptorUtils.extractEncryptionMetadata(ciphertextBytes);
            } else {
                String errorMsg = "The " + descriptor + " with ID " + identifier + " was detected as " + ciphertextSource.getClass().getSimpleName() + "; this is not a supported source of ciphertext";
                logger.error(errorMsg);
                throw new EncryptionException(errorMsg);
            }
        } catch (IOException | ClassNotFoundException e) {
            final String msg = "Encountered an error reading the encryption metadata: ";
            logger.error(msg, e);
            throw new EncryptionException(msg, e);
        }

        if (!supportedVersions.contains(metadata.version)) {
            throw new EncryptionException("The " + descriptor + " with ID " + identifier
                    + " was encrypted with version " + metadata.version
                    + " which is not in the list of supported versions " + StringUtils.join(supportedVersions, ","));
        }

        return metadata;
    }
}
