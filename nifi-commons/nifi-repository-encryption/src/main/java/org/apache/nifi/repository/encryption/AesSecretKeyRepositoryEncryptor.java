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
package org.apache.nifi.repository.encryption;

import org.apache.nifi.repository.encryption.configuration.EncryptionMetadataHeader;
import org.apache.nifi.repository.encryption.configuration.EncryptionProtocol;
import org.apache.nifi.repository.encryption.configuration.RepositoryEncryptionMethod;
import org.apache.nifi.repository.encryption.metadata.RecordMetadata;
import org.apache.nifi.repository.encryption.metadata.RecordMetadataSerializer;
import org.apache.nifi.repository.encryption.metadata.serialization.RecordMetadataObjectInputStream;
import org.apache.nifi.repository.encryption.metadata.serialization.StandardRecordMetadataSerializer;
import org.apache.nifi.security.kms.KeyProvider;
import org.apache.nifi.stream.io.NonCloseableInputStream;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Objects;

/**
 * AES Secret Key abstract implementation of Repository Encryptor with shared methods for key retrieval and cipher handling
 *
 * @param <I> Input Record Type for Encryption
 * @param <O> Output Record Type for Decryption
 */
public abstract class AesSecretKeyRepositoryEncryptor<I, O> implements RepositoryEncryptor<I, O> {
    private static final int INITIALIZATION_VECTOR_LENGTH = 16;

    private static final int TAG_LENGTH = 128;

    private static final int END_OF_STREAM = -1;

    private static final int STREAM_LENGTH = -1;

    private static final RecordMetadataSerializer RECORD_METADATA_SERIALIZER = new StandardRecordMetadataSerializer(EncryptionProtocol.VERSION_1);

    private final SecureRandom secureRandom;

    private final KeyProvider keyProvider;

    private final EncryptionMetadataHeader encryptionMetadataHeader;

    private final RepositoryEncryptionMethod repositoryEncryptionMethod;

    AesSecretKeyRepositoryEncryptor(final RepositoryEncryptionMethod repositoryEncryptionMethod,
                                    final KeyProvider keyProvider,
                                    final EncryptionMetadataHeader encryptionMetadataHeader) {
        this.repositoryEncryptionMethod = Objects.requireNonNull(repositoryEncryptionMethod, "Encryption Method required");
        this.keyProvider = Objects.requireNonNull(keyProvider, "Key Provider required");
        this.encryptionMetadataHeader = Objects.requireNonNull(encryptionMetadataHeader, "Encryption Metadata Header required");
        this.secureRandom = new SecureRandom();
    }

    /**
     * Encrypt record creates an encryption cipher for delegation to implementations
     *
     * @param record Record to be encrypted
     * @param recordId Record Identifier
     * @param keyId Key Identifier of key to be used for encryption
     * @return Encrypted record
     */
    @Override
    public I encrypt(final I record, final String recordId, final String keyId) {
        Objects.requireNonNull(record, "Record required");
        Objects.requireNonNull(recordId, "Record ID required");
        Objects.requireNonNull(keyId, "Key ID required");

        final Cipher cipher = getEncryptionCipher(keyId);
        return encrypt(record, recordId, keyId, cipher);
    }

    /**
     * Encrypt record using configured Cipher
     *
     * @param record Record to be encrypted
     * @param recordId Record Identifier
     * @param keyId Key Identifier of key to be used for encryption
     * @param cipher Cipher to be used for encryption
     * @return Encrypted record
     */
    protected abstract I encrypt(I record, String recordId, String keyId, Cipher cipher);

    /**
     * Get initialized Cipher for encryption using random initialization vector
     *
     * @param keyId Key Identifier for encryption
     * @return Initialized Cipher for encryption
     */
    protected Cipher getEncryptionCipher(final String keyId) {
        final byte[] initializationVector = getInitializationVector();
        return getCipher(Cipher.ENCRYPT_MODE, keyId, initializationVector);
    }

    /**
     * Get initialized Cipher for decryption using provided initialization vector
     *
     * @param keyId Key Identifier for decryption
     * @param initializationVector Initialization Vector for Cipher
     * @return Initialized Cipher for decryption
     */
    protected Cipher getDecryptionCipher(final String keyId, final byte[] initializationVector) {
        return getCipher(Cipher.DECRYPT_MODE, keyId, initializationVector);
    }

    /**
     * Get serialized Encryption Metadata using specified parameters with known encrypted length
     *
     * @param keyId Encryption Key Identifier
     * @param initializationVector Initialization Vector for Cipher
     * @param cipherLength Length of encrypted contents
     * @return Serialized Repository Encryption Metadata
     */
    protected byte[] getMetadata(final String keyId, final byte[] initializationVector, final int cipherLength) {
        return writeMetadata(keyId, initializationVector, cipherLength);
    }

    /**
     * Get serialized Encryption Metadata for streaming implementations with unknown encrypted length
     *
     * @param keyId Encryption Key Identifier
     * @param initializationVector Initialization Vector for Cipher
     * @return Serialized Repository Encryption Metadata
     */
    protected byte[] getMetadata(final String keyId, final byte[] initializationVector) {
        return writeMetadata(keyId, initializationVector, STREAM_LENGTH);
    }

    /**
     * Read Encryption Metadata and header from InputStream using ObjectInputStream and avoid closing provided InputStream
     *
     * @param inputStream Input Stream containing metadata to be read
     * @return Record Metadata
     */
    protected RecordMetadata readMetadata(final InputStream inputStream) {
        final int headerLength = encryptionMetadataHeader.getLength();
        try {
            final int headerBytesRead = inputStream.read(new byte[headerLength]);
            if (END_OF_STREAM == headerBytesRead) {
                throw new RepositoryEncryptionException("End of InputStream while reading metadata header");
            }
        } catch (final IOException e) {
            throw new RepositoryEncryptionException(String.format("Read Metadata Header bytes [%d] failed", headerLength), e);
        }

        try (final RecordMetadataObjectInputStream objectInputStream = new RecordMetadataObjectInputStream(new NonCloseableInputStream(inputStream))) {
            return objectInputStream.getRecordMetadata();
        } catch (final IOException e) {
            throw new RepositoryEncryptionException("Read Encryption Metadata Failed", e);
        }
    }

    /**
     * Write Encryption Metadata with Header as a byte array using ObjectOutputStream
     *
     * @param keyId Encryption Key Identifier
     * @param initializationVector Initialization Vector for Cipher
     * @param cipherLength Length of encrypted contents
     * @return Byte array containing serialized metadata
     */
    private byte[] writeMetadata(final String keyId, final byte[] initializationVector, final int cipherLength) {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            outputStream.write(encryptionMetadataHeader.getHeader());
            final byte[] metadata = RECORD_METADATA_SERIALIZER.writeMetadata(keyId, initializationVector, cipherLength, repositoryEncryptionMethod);
            outputStream.write(metadata);
        } catch (final IOException e) {
            throw new RepositoryEncryptionException("Write Encryption Metadata Failed", e);
        }

        return outputStream.toByteArray();
    }

    private byte[] getInitializationVector() {
        final byte[] initializationVector = new byte[INITIALIZATION_VECTOR_LENGTH];
        secureRandom.nextBytes(initializationVector);
        return initializationVector;
    }

    private Cipher getCipher(final int mode, final String keyId, final byte[] initializationVector) {
        final SecretKey secretKey = getSecretKey(keyId);
        final String algorithm = repositoryEncryptionMethod.getAlgorithm();
        try {
            final Cipher cipher = Cipher.getInstance(algorithm);
            final AlgorithmParameterSpec algorithmParameterSpec = getAlgorithmParametersSpec(initializationVector);
            cipher.init(mode, secretKey, algorithmParameterSpec, secureRandom);
            return cipher;
        } catch (final NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | InvalidAlgorithmParameterException e) {
            final String message = String.format("Cipher [%s] Mode [%d] Key ID [%s] configuration failed", algorithm, mode, keyId);
            throw new RepositoryEncryptionException(message, e);
        }
    }

    private AlgorithmParameterSpec getAlgorithmParametersSpec(final byte[] initializationVector) {
        if (RepositoryEncryptionMethod.AES_GCM == repositoryEncryptionMethod) {
            return new GCMParameterSpec(TAG_LENGTH, initializationVector);
        } else {
            return new IvParameterSpec(initializationVector);
        }
    }

    private SecretKey getSecretKey(final String keyId) {
        if (keyProvider.keyExists(keyId)) {
            try {
                return keyProvider.getKey(keyId);
            } catch (final KeyManagementException e) {
                throw new RepositoryEncryptionException(String.format("Key ID [%s] retrieval failed", keyId), e);
            }
        } else {
            throw new RepositoryEncryptionException(String.format("Key ID [%s] not found", keyId));
        }
    }
}
