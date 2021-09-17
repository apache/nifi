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

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Repository Encryptor implementation using AES-CTR for encrypting and decrypting streams
 */
public class AesCtrStreamRepositoryEncryptor extends AesSecretKeyRepositoryEncryptor<OutputStream, InputStream> {
    /**
     * AES-CTR Stream Repository Encryptor with required Key Provider
     *
     * @param keyProvider Key Provider
     */
    public AesCtrStreamRepositoryEncryptor(final KeyProvider keyProvider, final EncryptionMetadataHeader encryptionMetadataHeader) {
        super(RepositoryEncryptionMethod.AES_CTR, keyProvider, encryptionMetadataHeader);
    }

    /**
     * Decrypt Input Stream using metadata read from start of stream
     *
     * @param record Input Stream to be decrypted
     * @param recordId Record Identifier
     * @return Decrypted Cipher Input Stream
     */
    @Override
    public InputStream decrypt(final InputStream record, final String recordId) {
        final RecordMetadata metadata = readMetadata(record);
        final Cipher cipher = getDecryptionCipher(metadata.getKeyId(), metadata.getInitializationVector());
        return new CipherInputStream(record, cipher);
    }

    /**
     * Encrypt Output Stream using Cipher writing serialized metadata prior to returning
     *
     * @param record Output Stream to be wrapper for encryption
     * @param recordId Record Identifier
     * @param keyId Key Identifier used for encryption
     * @param cipher Cipher used for encryption
     * @return Cipher Output Stream
     */
    @Override
    protected OutputStream encrypt(final OutputStream record, final String recordId, final String keyId, final Cipher cipher) {
        try {
            final byte[] serializedMetadata = getMetadata(keyId, cipher.getIV());
            record.write(serializedMetadata);
            record.flush();
            return new CipherOutputStream(record, cipher);
        } catch (final IOException e) {
            throw new RepositoryEncryptionException(String.format("Encryption Failed for Record ID [%s]", recordId), e);
        }
    }
}
