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

/**
 * Repository Encryptor abstracts encrypting and decrypting records
 *
 * @param <I> Input Record Type for Encryption
 * @param <O> Output Record Type for Decryption
 */
public interface RepositoryEncryptor<I, O> {
    /**
     * Encrypt identified record using specified key
     *
     * @param record Record to be encrypted
     * @param recordId Record Identifier
     * @param keyId Key Identifier of key to be used for encryption
     * @return Encrypted Record
     */
    I encrypt(I record, String recordId, String keyId);

    /**
     * Decrypt identified record
     *
     * @param record Record to be decrypted
     * @param recordId Record Identifier
     * @return Decrypted Record
     */
    O decrypt(O record, String recordId);
}
