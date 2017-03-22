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

public interface ProvenanceEventEncryptor {

    /**
     * Initializes the encryptor with a {@link KeyProvider}.
     *
     * @param keyProvider the key provider which will be responsible for accessing keys
     * @throws KeyManagementException if there is an issue configuring the key provider
     */
    void initialize(KeyProvider keyProvider) throws KeyManagementException;

    /**
     * Encrypts the provided {@see ProvenanceEventRecord}.
     *
     * @param plainRecord the plain record
     * @param keyId       the ID of the key to use
     * @return the encrypted record
     * @throws EncryptionException if there is an issue encrypting this record
     */
    EncryptedProvenanceEventRecord encrypt(ProvenanceEventRecord plainRecord, String keyId) throws EncryptionException;

    /**
     * Decrypts the provided {@see ProvenanceEventRecord}.
     *
     * @param encryptedRecord the encrypted record
     * @return the decrypted record
     * @throws EncryptionException if there is an issue decrypting this record
     */
    ProvenanceEventRecord decrypt(EncryptedProvenanceEventRecord encryptedRecord) throws EncryptionException;
}
