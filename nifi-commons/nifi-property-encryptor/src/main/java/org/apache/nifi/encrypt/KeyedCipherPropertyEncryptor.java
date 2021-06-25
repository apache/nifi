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
package org.apache.nifi.encrypt;

import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.crypto.KeyedCipherProvider;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Objects;

/**
 * Property Encryptor implementation using Keyed Cipher Provider
 */
class KeyedCipherPropertyEncryptor extends CipherPropertyEncryptor {
    private static final int INITIALIZATION_VECTOR_LENGTH = 16;

    private static final int ARRAY_START = 0;

    private static final boolean ENCRYPT = true;

    private static final boolean DECRYPT = false;

    private final KeyedCipherProvider cipherProvider;

    private final EncryptionMethod encryptionMethod;

    private final SecretKey secretKey;

    private final SecureRandom secureRandom;

    private final String description;

    protected KeyedCipherPropertyEncryptor(final KeyedCipherProvider cipherProvider,
                                           final EncryptionMethod encryptionMethod,
                                           final SecretKey secretKey) {
        this.cipherProvider = cipherProvider;
        this.encryptionMethod = encryptionMethod;
        this.secretKey = secretKey;
        this.secureRandom = new SecureRandom();
        this.description = String.format("%s Encryption Method [%s] Key Algorithm [%s] Key Bytes [%d]",
                getClass().getSimpleName(),
                encryptionMethod.getAlgorithm(),
                secretKey.getAlgorithm(),
                secretKey.getEncoded().length);
    }

    /**
     * Get Cipher for Decryption based on encrypted binary
     *
     * @param encryptedBinary Encrypted Binary
     * @return Cipher for Decryption
     */
    @Override
    protected Cipher getDecryptionCipher(final byte[] encryptedBinary) {
        final byte[] initializationVector = readInitializationVector(encryptedBinary);
        return getCipher(initializationVector, DECRYPT);
    }

    /**
     * Get Cipher for Encryption using encoded parameters containing initialization vector
     *
     * @param encodedParameters Binary encoded parameters containing random initialization vector
     * @return Cipher for Encryption
     */
    @Override
    protected Cipher getEncryptionCipher(byte[] encodedParameters) {
        return getCipher(encodedParameters, ENCRYPT);
    }

    /**
     * Get Cipher Binary from encrypted binary
     *
     * @param encryptedBinary Encrypted Binary containing cipher binary and other information
     * @return Cipher Binary for decryption
     */
    @Override
    protected byte[] getCipherBinary(byte[] encryptedBinary) {
        return Arrays.copyOfRange(encryptedBinary, INITIALIZATION_VECTOR_LENGTH, encryptedBinary.length);
    }

    /**
     * Get Encoded Parameters returns a random initialization vector
     *
     * @return Initialization Vector for encoded parameters
     */
    @Override
    protected byte[] getEncodedParameters() {
        final byte[] initializationVector = new byte[INITIALIZATION_VECTOR_LENGTH];
        secureRandom.nextBytes(initializationVector);
        return initializationVector;
    }

    private Cipher getCipher(final byte[] initializationVector, final boolean encrypt) {
        try {
            return cipherProvider.getCipher(encryptionMethod, secretKey, initializationVector, encrypt);
        } catch (final Exception e) {
            final String message = String.format("Failed to get Cipher for Algorithm [%s]", encryptionMethod.getAlgorithm());
            throw new EncryptionException(message, e);
        }
    }

    private byte[] readInitializationVector(final byte[] binary) {
        final byte[] initializationVector = new byte[INITIALIZATION_VECTOR_LENGTH];
        System.arraycopy(binary, ARRAY_START, initializationVector, ARRAY_START, INITIALIZATION_VECTOR_LENGTH);
        return initializationVector;
    }

    /**
     * Return object equality based on Encryption Method and Secret Key
     *
     * @param object Object for comparison
     * @return Object equality status
     */
    @Override
    public boolean equals(final Object object) {
        boolean equals = false;
        if (this == object) {
            equals = true;
        } else if (object instanceof KeyedCipherPropertyEncryptor) {
            final KeyedCipherPropertyEncryptor encryptor = (KeyedCipherPropertyEncryptor) object;
            equals = Objects.equals(encryptionMethod, encryptor.encryptionMethod) && Objects.equals(secretKey, encryptor.secretKey);
        }
        return equals;
    }

    /**
     * Return hash code based on Encryption Method and Secret Key
     *
     * @return Hash Code based on Encryption Method and Secret Key
     */
    @Override
    public int hashCode() {
        return Objects.hash(encryptionMethod, secretKey);
    }

    /**
     * Return String containing object description
     *
     * @return Object description
     */
    @Override
    public String toString() {
        return description;
    }
}
