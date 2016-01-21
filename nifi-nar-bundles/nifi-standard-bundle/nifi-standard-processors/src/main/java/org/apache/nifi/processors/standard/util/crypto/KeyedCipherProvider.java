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
package org.apache.nifi.processors.standard.util.crypto;

import org.apache.nifi.security.util.EncryptionMethod;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;

public interface KeyedCipherProvider {
    /**
     * Returns an initialized cipher for the specified algorithm. The IV is provided externally to allow for non-deterministic IVs, as IVs
     * deterministically derived from the password are a potential vulnerability and compromise semantic security. See
     * <a href="http://crypto.stackexchange.com/a/3970/12569">Ilmari Karonen's answer on Crypto Stack Exchange</a>
     *
     * @param encryptionMethod the {@link EncryptionMethod}
     * @param key              the key
     * @param iv               the IV or nonce
     * @param encryptMode      true for encrypt, false for decrypt
     * @return the initialized cipher
     * @throws Exception if there is a problem initializing the cipher
     */
    Cipher getCipher(EncryptionMethod encryptionMethod, SecretKey key, byte[] iv, boolean encryptMode) throws Exception;

    /**
     * Returns an initialized cipher for the specified algorithm. The IV will be generated internally (for encryption). If decryption is requested, it will throw an exception.
     *
     * @param encryptionMethod the {@link EncryptionMethod}
     * @param key              the key
     * @param encryptMode      true for encrypt, false for decrypt
     * @return the initialized cipher
     * @throws Exception if there is a problem initializing the cipher or if decryption is requested
     */
    Cipher getCipher(EncryptionMethod encryptionMethod, SecretKey key, boolean encryptMode) throws Exception;

    /**
     * Generates a new random IV of the correct length.
     *
     * @return the IV
     */
    byte[] generateIV();
}
