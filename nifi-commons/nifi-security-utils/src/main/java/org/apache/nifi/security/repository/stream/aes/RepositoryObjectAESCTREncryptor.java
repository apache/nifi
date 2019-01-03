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
package org.apache.nifi.security.repository.stream.aes;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.KeyManagementException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import org.apache.nifi.security.kms.CryptoUtils;
import org.apache.nifi.security.kms.EncryptionException;
import org.apache.nifi.security.repository.AbstractAESEncryptor;
import org.apache.nifi.security.repository.RepositoryEncryptorUtils;
import org.apache.nifi.security.repository.RepositoryObjectEncryptionMetadata;
import org.apache.nifi.security.repository.StreamingEncryptionMetadata;
import org.apache.nifi.security.repository.stream.RepositoryObjectStreamEncryptor;
import org.apache.nifi.security.util.EncryptionMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implementation of the {@link RepositoryObjectStreamEncryptor} handles streaming data by accepting
 * {@link OutputStream} and {@link InputStream} parameters and returning custom implementations which wrap the normal
 * behavior with encryption/decryption logic transparently. This class should be used when a repository needs to persist
 * and retrieve streaming data (i.e. content claims). For repositories handling limited blocks of data with the length
 * known a priori (i.e. provenance records or flowfile attribute maps), use the
 * {@link org.apache.nifi.security.repository.block.aes.RepositoryObjectAESGCMEncryptor} which will provide
 * authenticated encryption.
 */
public class RepositoryObjectAESCTREncryptor extends AbstractAESEncryptor implements RepositoryObjectStreamEncryptor {
    private static final Logger logger = LoggerFactory.getLogger(RepositoryObjectAESCTREncryptor.class);
    private static final byte[] EM_START_SENTINEL = new byte[]{0x00, 0x00};
    private static String ALGORITHM = "AES/CTR/NoPadding";
    private static final String VERSION = "v1";
    private static final List<String> SUPPORTED_VERSIONS = Arrays.asList(VERSION);

    /**
     * Returns an {@link OutputStream} which encrypts the content of the provided OutputStream. This method works on
     * streams to allow for streaming data rather than blocks of bytes of a known length. It is recommended to use this
     * for data like flowfile content claims, rather than provenance records or flowfile attribute maps.
     *
     * @param plainStream the plain OutputStream which is being written to
     * @param streamId    an identifier for this stream (eventId, generated, etc.)
     * @param keyId       the ID of the key to use
     * @return a stream which will encrypt the data and include the {@link RepositoryObjectEncryptionMetadata}
     * @throws EncryptionException if there is an issue encrypting this streaming repository object
     */
    @Override
    public OutputStream encrypt(OutputStream plainStream, String streamId, String keyId) throws EncryptionException {
        if (plainStream == null || CryptoUtils.isEmpty(keyId)) {
            throw new EncryptionException("The streaming repository object and key ID cannot be missing");
        }

        if (keyProvider == null || !keyProvider.keyExists(keyId)) {
            throw new EncryptionException("The requested key ID is not available");
        } else {
            byte[] ivBytes = new byte[IV_LENGTH];
            new SecureRandom().nextBytes(ivBytes);
            try {
                logger.debug("Encrypting streaming repository object " + streamId + " with key ID " + keyId);
                Cipher cipher = RepositoryEncryptorUtils.initCipher(aesKeyedCipherProvider, EncryptionMethod.forAlgorithm(ALGORITHM), Cipher.ENCRYPT_MODE, keyProvider.getKey(keyId), ivBytes);
                ivBytes = cipher.getIV();

                // Prepare the output stream for the actual encryption
                CipherOutputStream cipherOutputStream = new CipherOutputStream(plainStream, cipher);

                // Serialize and concat encryption details fields (keyId, algo, IV, version, CB length) outside of encryption
                RepositoryObjectEncryptionMetadata metadata = new StreamingEncryptionMetadata(keyId, ALGORITHM, ivBytes, VERSION);
                byte[] serializedEncryptionMetadata = RepositoryEncryptorUtils.serializeEncryptionMetadata(metadata);

                // Write the SENTINEL bytes and the encryption metadata to the raw output stream
                plainStream.write(EM_START_SENTINEL);
                plainStream.write(serializedEncryptionMetadata);
                plainStream.flush();

                logger.debug("Encrypted streaming repository object " + streamId + " with key ID " + keyId);
                return cipherOutputStream;
            } catch (EncryptionException | IOException | KeyManagementException e) {
                final String msg = "Encountered an exception encrypting streaming repository object " + streamId;
                logger.error(msg, e);
                throw new EncryptionException(msg, e);
            }
        }
    }

    /**
     * Returns an {@link InputStream} which decrypts the content of the provided InputStream. The provided InputStream
     * must contain a valid {@link RepositoryObjectEncryptionMetadata} object at the start of the stream. This method
     * works on streams to allow for streaming data rather than blocks of bytes of a known length. It is recommended to
     * use this for data like flowfile content claims, rather than provenance records or flowfile attribute maps.
     *
     * @param encryptedInputStream the encrypted InputStream (starting with the plaintext ROEM) which is being read from
     * @param streamId             an identifier for this stream (eventId, generated, etc.)
     * @return a stream which will decrypt the data based on the data in the {@link RepositoryObjectEncryptionMetadata}
     * @throws EncryptionException if there is an issue decrypting this streaming repository object
     */
    @Override
    public InputStream decrypt(InputStream encryptedInputStream, String streamId) throws EncryptionException {
        RepositoryObjectEncryptionMetadata metadata = prepareObjectForDecryption(encryptedInputStream, streamId, "streaming repository object", SUPPORTED_VERSIONS);

        if (keyProvider == null || !keyProvider.keyExists(metadata.keyId) || CryptoUtils.isEmpty(metadata.keyId)) {
            throw new EncryptionException("The requested key ID " + metadata.keyId + " is not available");
        } else {
            try {
                logger.debug("Decrypting streaming repository object with ID " + streamId + " with key ID " + metadata.keyId);
                EncryptionMethod method = EncryptionMethod.forAlgorithm(metadata.algorithm);
                Cipher cipher = RepositoryEncryptorUtils.initCipher(aesKeyedCipherProvider, method, Cipher.DECRYPT_MODE, keyProvider.getKey(metadata.keyId), metadata.ivBytes);

                // Return a new CipherInputStream wrapping the encrypted stream at the present location
                CipherInputStream cipherInputStream = new CipherInputStream(encryptedInputStream, cipher);

                logger.debug("Decrypted streaming repository object with ID " + streamId + " with key ID " + metadata.keyId);
                return cipherInputStream;
            } catch (EncryptionException | KeyManagementException e) {
                final String msg = "Encountered an exception decrypting streaming repository object with ID " + streamId;
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
        return null;
    }
}
