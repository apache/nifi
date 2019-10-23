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
package org.apache.nifi.security.repository.block.aes;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import org.apache.nifi.security.kms.CryptoUtils;
import org.apache.nifi.security.kms.EncryptionException;
import org.apache.nifi.security.repository.AbstractAESEncryptor;
import org.apache.nifi.security.repository.RepositoryEncryptorUtils;
import org.apache.nifi.security.repository.RepositoryObjectEncryptionMetadata;
import org.apache.nifi.security.repository.block.BlockEncryptionMetadata;
import org.apache.nifi.security.repository.block.RepositoryObjectBlockEncryptor;
import org.apache.nifi.security.util.EncryptionMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implementation of the {@link RepositoryObjectBlockEncryptor} handles block data by accepting
 * {@code byte[]} parameters and returning {@code byte[]} which contain the encrypted/decrypted content. This class
 * should be used when a repository needs to persist and retrieve block data with the length known a priori (i.e.
 * provenance records or flowfile attribute maps). For repositories handling streams of data with unknown or large
 * lengths (i.e. content claims), use the
 * {@link org.apache.nifi.security.repository.stream.aes.RepositoryObjectAESCTREncryptor} which does not provide
 * authenticated encryption but performs much better with large data.
 */
public class RepositoryObjectAESGCMEncryptor extends AbstractAESEncryptor implements RepositoryObjectBlockEncryptor {
    private static final Logger logger = LoggerFactory.getLogger(RepositoryObjectAESGCMEncryptor.class);
    private static final String ALGORITHM = "AES/GCM/NoPadding";

    // TODO: Increment the version for new implementations?
    private static final String VERSION = "v1";
    private static final List<String> SUPPORTED_VERSIONS = Arrays.asList(VERSION);
    private static final int MIN_METADATA_LENGTH = IV_LENGTH + 3 + 3; // 3 delimiters and 3 non-zero elements
    private static final int METADATA_DEFAULT_LENGTH = (20 + ALGORITHM.length() + IV_LENGTH + VERSION.length()) * 2; // Default to twice the expected length
    private static final byte[] SENTINEL = new byte[]{0x01};

    // TODO: Dependency injection for record parser (provenance SENTINEL vs. flowfile)?

    /**
     * Encrypts the serialized byte[].
     *
     * @param plainRecord the plain record, serialized to a byte[]
     * @param recordId    an identifier for this record (eventId, generated, etc.)
     * @param keyId       the ID of the key to use
     * @return the encrypted record
     * @throws EncryptionException if there is an issue encrypting this record
     */
    @Override
    public byte[] encrypt(byte[] plainRecord, String recordId, String keyId) throws EncryptionException {
        if (plainRecord == null || CryptoUtils.isEmpty(keyId)) {
            throw new EncryptionException("The repository object and key ID cannot be missing");
        }

        if (keyProvider == null || !keyProvider.keyExists(keyId)) {
            throw new EncryptionException("The requested key ID is not available");
        } else {
            byte[] ivBytes = new byte[IV_LENGTH];
            new SecureRandom().nextBytes(ivBytes);
            try {
                // TODO: Add object type to description
                logger.debug("Encrypting repository object " + recordId + " with key ID " + keyId);
                Cipher cipher = RepositoryEncryptorUtils.initCipher(aesKeyedCipherProvider, EncryptionMethod.AES_GCM, Cipher.ENCRYPT_MODE, keyProvider.getKey(keyId), ivBytes);
                ivBytes = cipher.getIV();

                // Perform the actual encryption
                byte[] cipherBytes = cipher.doFinal(plainRecord);

                // Serialize and concat encryption details fields (keyId, algo, IV, version, CB length) outside of encryption
                RepositoryObjectEncryptionMetadata metadata = new BlockEncryptionMetadata(keyId, ALGORITHM, ivBytes, VERSION, cipherBytes.length);
                byte[] serializedEncryptionMetadata = RepositoryEncryptorUtils.serializeEncryptionMetadata(metadata);
                logger.debug("Generated encryption metadata ({} bytes) for repository object {}", serializedEncryptionMetadata.length, recordId);

                // Add the sentinel byte of 0x01
                // TODO: Remove (required for prov repo but not FF repo)
                logger.debug("Encrypted repository object " + recordId + " with key ID " + keyId);
                // return CryptoUtils.concatByteArrays(SENTINEL, serializedEncryptionMetadata, cipherBytes);
                return CryptoUtils.concatByteArrays(serializedEncryptionMetadata, cipherBytes);
            } catch (EncryptionException | BadPaddingException | IllegalBlockSizeException | IOException | KeyManagementException e) {
                final String msg = "Encountered an exception encrypting repository object " + recordId;
                logger.error(msg, e);
                throw new EncryptionException(msg, e);
            }
        }
    }

    /**
     * Decrypts the provided byte[] (an encrypted record with accompanying metadata).
     *
     * @param encryptedRecord the encrypted record in byte[] form
     * @param recordId        an identifier for this record (eventId, generated, etc.)
     * @return the decrypted record
     * @throws EncryptionException if there is an issue decrypting this record
     */
    @Override
    public byte[] decrypt(byte[] encryptedRecord, String recordId) throws EncryptionException {
        RepositoryObjectEncryptionMetadata metadata = prepareObjectForDecryption(encryptedRecord, recordId, "repository object", SUPPORTED_VERSIONS);

        // TODO: Actually use the version to determine schema, etc.

        if (keyProvider == null || !keyProvider.keyExists(metadata.keyId) || CryptoUtils.isEmpty(metadata.keyId)) {
            throw new EncryptionException("The requested key ID " + metadata.keyId + " is not available");
        } else {
            try {
                logger.debug("Decrypting repository object " + recordId + " with key ID " + metadata.keyId);
                EncryptionMethod method = EncryptionMethod.forAlgorithm(metadata.algorithm);
                Cipher cipher = RepositoryEncryptorUtils.initCipher(aesKeyedCipherProvider, method, Cipher.DECRYPT_MODE, keyProvider.getKey(metadata.keyId), metadata.ivBytes);

                // Strip the metadata away to get just the cipher bytes
                byte[] cipherBytes = extractCipherBytes(encryptedRecord, metadata);

                // Perform the actual decryption
                byte[] plainBytes = cipher.doFinal(cipherBytes);

                logger.debug("Decrypted repository object " + recordId + " with key ID " + metadata.keyId);
                return plainBytes;
            } catch (EncryptionException | BadPaddingException | IllegalBlockSizeException | KeyManagementException e) {
                final String msg = "Encountered an exception decrypting repository object " + recordId;
                logger.error(msg, e);
                throw new EncryptionException(msg, e);
            }
        }
    }

    /**
     * Returns a valid key identifier for this encryptor (valid for encryption and decryption) or throws an exception if none are available.
     *
     * @return the key ID
     * @throws KeyManagementException if no available key IDs are valid for both operations
     */
    @Override
    public String getNextKeyId() throws KeyManagementException {
        if (keyProvider != null) {
            List<String> availableKeyIds = keyProvider.getAvailableKeyIds();
            if (!availableKeyIds.isEmpty()) {
                return availableKeyIds.get(0);
            }
        }
        throw new KeyManagementException("No available key IDs");
    }


    private byte[] extractCipherBytes(byte[] encryptedRecord, RepositoryObjectEncryptionMetadata metadata) {
        return Arrays.copyOfRange(encryptedRecord, encryptedRecord.length - metadata.cipherByteLength, encryptedRecord.length);
    }

    @Override
    public String toString() {
        return "Repository Object Block Encryptor using AES G/CM with Key Provider: " + keyProvider.toString();
    }
}
