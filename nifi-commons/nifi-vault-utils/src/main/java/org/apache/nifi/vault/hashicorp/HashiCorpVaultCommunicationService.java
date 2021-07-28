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
package org.apache.nifi.vault.hashicorp;

import java.util.Optional;

/**
 * A service to handle all communication with an instance of HashiCorp Vault.
 * @see <a href="https://www.vaultproject.io/">https://www.vaultproject.io/</a>
 */
public interface HashiCorpVaultCommunicationService {

    /**
     * Encrypts the given plaintext using Vault's Transit Secrets Engine.
     *
     * @see <a href="https://www.vaultproject.io/api-docs/secret/transit">https://www.vaultproject.io/api-docs/secret/transit</a>
     * @param transitPath The Vault path to use for the configured Transit Secrets Engine
     * @param plainText The plaintext to encrypt
     * @return The cipher text
     */
    String encrypt(String transitPath, byte[] plainText);

    /**
     * Decrypts the given cipher text using Vault's Transit Secrets Engine.
     *
     * @see <a href="https://www.vaultproject.io/api-docs/secret/transit">https://www.vaultproject.io/api-docs/secret/transit</a>
     * @param transitPath The Vault path to use for the configured Transit Secrets Engine
     * @param cipherText The cipher text to decrypt
     * @return The decrypted plaintext
     */
    byte[] decrypt(String transitPath, String cipherText);

    /**
     * Writes a secret using Vault's unversioned Key/Value Secrets Engine.
     *
     * @see <a href="https://www.vaultproject.io/api-docs/secret/kv/kv-v1">https://www.vaultproject.io/api-docs/secret/kv/kv-v1</a>
     * @param keyValuePath The Vault path to use for the configured Key/Value v1 Secrets Engine
     * @param key The secret key
     * @param value The secret value
     */
    void writeKeyValueSecret(String keyValuePath, String key, String value);

    /**
     * Reads a secret from Vault's unversioned Key/Value Secrets Engine.
     *
     * @see <a href="https://www.vaultproject.io/api-docs/secret/kv/kv-v1">https://www.vaultproject.io/api-docs/secret/kv/kv-v1</a>
     * @param keyValuePath The Vault path to use for the configured Key/Value v1 Secrets Engine
     * @param key The secret key
     * @return The secret value, or empty if not found
     */
    Optional<String> readKeyValueSecret(String keyValuePath, String key);
}
