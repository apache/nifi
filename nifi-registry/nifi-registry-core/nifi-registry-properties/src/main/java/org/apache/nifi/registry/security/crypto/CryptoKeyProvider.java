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
package org.apache.nifi.registry.security.crypto;

/**
 * A simple interface that wraps a key that can be used for encryption and decryption.
 * This allows for more flexibility with the lifecycle of keys and how other classes
 * can declare dependencies for keys, by depending on a CryptoKeyProvider that will provided
 * at runtime.
 */
public interface CryptoKeyProvider {

    /**
     * A string literal that indicates the contents of a key are empty.
     * Can also be used in contexts that a null key is undesirable.
     */
    String EMPTY_KEY = "";

    /**
     * @return The crypto key known to this CryptoKeyProvider instance in hexadecimal format, or
     *         {@link #EMPTY_KEY} if the key is empty.
     * @throws MissingCryptoKeyException if the key cannot be provided or determined for any reason.
     *         If the key is known to be empty, {@link #EMPTY_KEY} will be returned and a
     *         CryptoKeyMissingException will not be thrown
     */
    String getKey() throws MissingCryptoKeyException;

    /**
     * @return A boolean indicating if the key value held by this CryptoKeyProvider is empty,
     *         such as 'null' or empty string.
     */
    default boolean isEmpty() {
        String key;
        try {
            key = getKey();
        } catch (MissingCryptoKeyException e) {
            return true;
        }
        return EMPTY_KEY.equals(key);
    }

    /**
     * A string representation of this CryptoKeyProvider instance.
     * <p>
     * <p>
     * Note: Implementations of this interface should take care not to leak sensitive
     * key material in any strings they emmit, including in the toString implementation.
     *
     * @return A string representation of this CryptoKeyProvider instance.
     */
    @Override
    public String toString();

}
