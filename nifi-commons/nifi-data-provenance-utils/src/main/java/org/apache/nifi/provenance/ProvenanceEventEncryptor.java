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
package org.apache.nifi.provenance;

import java.security.KeyManagementException;
import org.apache.nifi.security.kms.KeyProvider;

public interface ProvenanceEventEncryptor {

    /**
     * Initializes the encryptor with a {@link KeyProvider}.
     *
     * @param keyProvider the key provider which will be responsible for accessing keys
     * @throws KeyManagementException if there is an issue configuring the key provider
     */
    void initialize(KeyProvider keyProvider) throws KeyManagementException;

    /**
     * Encrypts the provided {@link ProvenanceEventRecord}, serialized to a byte[] by the RecordWriter.
     *
     * @param plainRecord the plain record, serialized to a byte[]
     * @param recordId    an identifier for this record (eventId, generated, etc.)
     * @param keyId       the ID of the key to use
     * @return the encrypted record
     * @throws EncryptionException if there is an issue encrypting this record
     */
    byte[] encrypt(byte[] plainRecord, String recordId, String keyId) throws EncryptionException;

    /**
     * Decrypts the provided byte[] (an encrypted record with accompanying metadata).
     *
     * @param encryptedRecord the encrypted record in byte[] form
     * @param recordId        an identifier for this record (eventId, generated, etc.)
     * @return the decrypted record
     * @throws EncryptionException if there is an issue decrypting this record
     */
    byte[] decrypt(byte[] encryptedRecord, String recordId) throws EncryptionException;

    /**
     * Returns a valid key identifier for this encryptor (valid for encryption and decryption) or throws an exception if none are available.
     *
     * @return the key ID
     * @throws KeyManagementException if no available key IDs are valid for both operations
     */
    String getNextKeyId() throws KeyManagementException;
}
