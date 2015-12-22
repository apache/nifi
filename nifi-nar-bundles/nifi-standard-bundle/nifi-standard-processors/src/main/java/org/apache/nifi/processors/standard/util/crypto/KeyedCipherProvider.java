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

import javax.crypto.Cipher;
import javax.crypto.SecretKey;

public interface KeyedCipherProvider {
    /**
     * Returns an initialized cipher for the specified algorithm.
     *
     * @param algorithm   the algorithm name
     * @param provider    the provider name
     * @param key         the key
     * @param encryptMode true for encrypt, false for decrypt
     * @return the initialized cipher
     * @throws Exception if there is a problem initializing the cipher
     */
    Cipher getCipher(String algorithm, String provider, SecretKey key, boolean encryptMode) throws Exception;

    /**
     * Returns an initialized cipher for the specified algorithm.
     *
     * @param algorithm   the algorithm name
     * @param provider    the provider name
     * @param key         the key
     * @param iv          the IV
     * @param encryptMode true for encrypt, false for decrypt
     * @return the initialized cipher
     * @throws Exception if there is a problem initializing the cipher
     */
    Cipher getCipher(String algorithm, String provider, SecretKey key, byte[] iv, boolean encryptMode) throws Exception;
}
