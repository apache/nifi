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

public interface ProvenanceEventEncryptor {

    /**
     * Encrypts the provided {@see ProvenanceEventRecord}.
     *
     * @param plainRecord the plain record
     * @return the encrypted record
     * @throws EncryptionException if there is an issue encrypting this record
     */
    ProvenanceEventRecord encrypt(ProvenanceEventRecord plainRecord) throws EncryptionException;

    /**
     * Decrypts the provided {@see ProvenanceEventRecord}.
     *
     * @param encryptedRecord the encrypted record
     * @return the decrypted record
     * @throws EncryptionException if there is an issue decrypting this record
     */
    ProvenanceEventRecord decrypt(ProvenanceEventRecord encryptedRecord) throws EncryptionException;
}
