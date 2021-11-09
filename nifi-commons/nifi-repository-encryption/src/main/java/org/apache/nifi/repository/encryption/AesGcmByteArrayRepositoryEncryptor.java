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
import org.apache.nifi.repository.encryption.configuration.RepositoryEncryptionMethod;
import org.apache.nifi.repository.encryption.metadata.RecordMetadata;
import org.apache.nifi.security.kms.KeyProvider;
import org.bouncycastle.util.Arrays;

import javax.crypto.Cipher;
import java.io.ByteArrayInputStream;
import java.security.GeneralSecurityException;

/**
 * Repository Encryptor implementation using AES-GCM for encrypting and decrypting byte arrays
 */
public class AesGcmByteArrayRepositoryEncryptor extends AesSecretKeyRepositoryEncryptor<byte[], byte[]> {
    /**
     * AES-GCM Byte Array Repository Encryptor with required Key Provider
     *
     * @param keyProvider Key Provider
     * @param encryptionMetadataHeader Encryption Metadata Header
     */
    public AesGcmByteArrayRepositoryEncryptor(final KeyProvider keyProvider, final EncryptionMetadataHeader encryptionMetadataHeader) {
        super(RepositoryEncryptionMethod.AES_GCM, keyProvider, encryptionMetadataHeader);
    }

    /**
     * Decrypt byte array record using metadata read from start of record
     *
     * @param record Record to be decrypted
     * @param recordId Record Identifier
     * @return Decrypted byte array record
     */
    @Override
    public byte[] decrypt(final byte[] record, final String recordId) {
        try {
            final RecordMetadata metadata = readMetadata(new ByteArrayInputStream(record));
            final Cipher cipher = getDecryptionCipher(metadata.getKeyId(), metadata.getInitializationVector());
            final int cipherLength = metadata.getLength();
            final int startIndex = record.length - cipherLength;
            return cipher.doFinal(record, startIndex, cipherLength);
        } catch (final GeneralSecurityException e) {
            throw new RepositoryEncryptionException(String.format("Decryption Failed for Record ID [%s]", recordId), e);
        }
    }

    /**
     * Encrypt byte array record using provided Cipher writes serialized metadata and encrypted byte array
     *
     * @param record Record byte array to be encrypted
     * @param recordId Record Identifier
     * @param keyId Key Identifier used for encryption
     * @param cipher Cipher used for encryption
     * @return Byte array prefixed with serialized metadata followed by encrypted byte array
     */
    @Override
    protected byte[] encrypt(final byte[] record, final String recordId, final String keyId, final Cipher cipher) {
        try {
            final byte[] encryptedRecord = cipher.doFinal(record);
            final byte[] serializedMetadata = getMetadata(keyId, cipher.getIV(), encryptedRecord.length);
            return Arrays.concatenate(serializedMetadata, encryptedRecord);
        } catch (final GeneralSecurityException e) {
            throw new RepositoryEncryptionException(String.format("Encryption Failed for Record ID [%s]", recordId), e);
        }
    }
}
