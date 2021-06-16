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
package org.apache.nifi.security.util.crypto;

/**
 * An interface which specifies that implementations should provide a
 * cryptographic hash function (CHF) which accepts input and returns a
 * deterministic, (mathematically-difficult) irreversible value.
 *
 * While SHA-256, SHA-512, and Blake2 are CHF implementations, this interface is intended to
 * be used by password protection or key derivation functions (KDF) like
 * {@link PBKDF2CipherProvider}, {@link BcryptCipherProvider}, {@link ScryptCipherProvider},
 * or {@link Argon2SecureHasher}. These classes implement iterative processes which make use
 * of cryptographic primitives to return an irreversible value which can either securely
 * store a password representation or be used as an encryption key derived from a password.
 */
public interface SecureHasher {

    /**
     * Returns a String representation of {@code CHF(input)} in hex-encoded format.
     *
     * @param input the input
     * @return the hex-encoded hash
     */
    String hashHex(String input);

    /**
     * Returns a String representation of {@code CHF(input)} in hex-encoded format.
     *
     * @param input the input
     * @param salt  the provided salt
     *
     * @return the hex-encoded hash
     */
    String hashHex(String input, String salt);

    /**
     * Returns a String representation of {@code CHF(input)} in Base 64-encoded format.
     *
     * @param input the input
     * @return the Base 64-encoded hash
     */
    String hashBase64(String input);

    /**
     * Returns a String representation of {@code CHF(input)} in Base 64-encoded format.
     *
     * @param input the input
     * @param salt  the provided salt
     *
     * @return the Base 64-encoded hash
     */
    String hashBase64(String input, String salt);

    /**
     * Returns a byte[] representation of {@code CHF(input)}.
     *
     * @param input the input
     * @return the hash
     */
    byte[] hashRaw(byte[] input);

    /**
     * Returns a byte[] representation of {@code CHF(input)}.
     *
     * @param input the input
     * @param salt  the provided salt
     *
     * @return the hash
     */
    byte[] hashRaw(byte[] input, byte[] salt);
}
