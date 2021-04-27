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

/**
 * A service to handle all communication with an instance of HashiCorp Vault.
 * @see <a href="https://www.vaultproject.io/">https://www.vaultproject.io/</a>
 */
public interface HashiCorpVaultCommunicationService {

    /**
     * Encrypts the given plaintext using Vault's Transit Secrets Engine.
     *
     * @see <a href="https://www.vaultproject.io/api-docs/secret/transit">https://www.vaultproject.io/api-docs/secret/transit</a>
     * @param transitKey A named encryption key used in the Transit Secrets Engine.  The key is expected to have
     *                   already been configured in the Vault instance.
     * @param plainText The plaintext to encrypt
     * @return The cipher text
     */
    String encrypt(String transitKey, byte[] plainText);

    /**
     * Decrypts the given cipher text using Vault's Transit Secrets Engine.
     *
     * @see <a href="https://www.vaultproject.io/api-docs/secret/transit">https://www.vaultproject.io/api-docs/secret/transit</a>
     * @param transitKey A named encryption key used in the Transit Secrets Engine.  The key is expected to have
     *                   already been configured in the Vault instance.
     * @param cipherText The cipher text to decrypt
     * @return The decrypted plaintext
     */
    byte[] decrypt(String transitKey, String cipherText);
}
