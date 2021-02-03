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

import org.apache.nifi.security.kms.CryptoUtils;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.crypto.CipherUtility;
import org.apache.nifi.security.util.crypto.PBECipherProvider;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Objects;

/**
 * Property Encryptor implementation using Password Based Encryption Cipher Provider
 */
class PasswordBasedCipherPropertyEncryptor extends CipherPropertyEncryptor {
    private static final int ARRAY_START = 0;

    private static final boolean ENCRYPT = true;

    private static final boolean DECRYPT = false;

    private final SecureRandom secureRandom;

    private final PBECipherProvider cipherProvider;

    private final EncryptionMethod encryptionMethod;

    private final String password;

    private final int keyLength;

    private final int saltLength;

    protected PasswordBasedCipherPropertyEncryptor(final PBECipherProvider cipherProvider,
                                                   final EncryptionMethod encryptionMethod,
                                                   final String password) {
        this.cipherProvider = cipherProvider;
        this.encryptionMethod = encryptionMethod;
        this.password = password;
        this.keyLength = CipherUtility.parseKeyLengthFromAlgorithm(encryptionMethod.getAlgorithm());
        this.saltLength = CipherUtility.getSaltLengthForAlgorithm(encryptionMethod.getAlgorithm());
        this.secureRandom = new SecureRandom();
    }

    /**
     * Encrypt binary and return enciphered binary prefixed with salt
     *
     * @param binary Binary to be encrypted
     * @return Enciphered binary prefixed with salt
     */
    @Override
    protected byte[] encrypt(final byte[] binary) {
        final byte[] salt = generateSalt();
        final Cipher cipher = getCipher(salt, ENCRYPT);
        try {
            final byte[] encrypted = cipher.doFinal(binary);
            return CryptoUtils.concatByteArrays(salt, encrypted);
        } catch (final BadPaddingException | IllegalBlockSizeException e) {
            final String message = String.format("Encryption Failed with Algorithm [%s]", cipher.getAlgorithm());
            throw new EncryptionException(message, e);
        }
    }

    /**
     * Get Cipher for Decryption based on encrypted binary
     *
     * @param encryptedBinary Encrypted Binary
     * @return Cipher for Decryption
     */
    @Override
    protected Cipher getDecryptionCipher(final byte[] encryptedBinary) {
        final byte[] salt = readSalt(encryptedBinary);
        return getCipher(salt, DECRYPT);
    }

    /**
     * Get Cipher Binary from encrypted binary
     *
     * @param encryptedBinary Encrypted Binary containing cipher binary and other information
     * @return Cipher Binary for decryption
     */
    @Override
    protected byte[] getCipherBinary(byte[] encryptedBinary) {
        return Arrays.copyOfRange(encryptedBinary, saltLength, encryptedBinary.length);
    }

    private Cipher getCipher(final byte[] salt, final boolean encrypt) {
        try {
            return cipherProvider.getCipher(encryptionMethod, password, salt, keyLength, encrypt);
        } catch (final Exception e) {
            final String message = String.format("Failed to get Cipher for Algorithm [%s]", encryptionMethod.getAlgorithm());
            throw new EncryptionException(message, e);
        }
    }

    private byte[] generateSalt() {
        final byte[] salt;

        if (cipherProvider instanceof org.apache.nifi.security.util.crypto.NiFiLegacyCipherProvider) {
            salt = ((org.apache.nifi.security.util.crypto.NiFiLegacyCipherProvider) cipherProvider).generateSalt(encryptionMethod);
        } else {
            salt = cipherProvider.generateSalt();
        }

        return salt;
    }

    private byte[] readSalt(final byte[] binary) {
        final byte[] salt = new byte[saltLength];
        System.arraycopy(binary, ARRAY_START, salt, ARRAY_START, saltLength);
        return salt;
    }

    /**
     * Return object equality based on Encryption Method and Password
     *
     * @param object Object for comparison
     * @return Object equality status
     */
    @Override
    public boolean equals(final Object object) {
        boolean equals = false;
        if (this == object) {
            equals = true;
        } else if (object instanceof PasswordBasedCipherPropertyEncryptor) {
            final PasswordBasedCipherPropertyEncryptor encryptor = (PasswordBasedCipherPropertyEncryptor) object;
            equals = Objects.equals(encryptionMethod, encryptor.encryptionMethod) && Objects.equals(password, encryptor.password);
        }
        return equals;
    }

    /**
     * Return hash code based on Encryption Method and Password
     *
     * @return Hash Code based on Encryption Method and Password
     */
    @Override
    public int hashCode() {
        return Objects.hash(encryptionMethod, password);
    }

    /**
     * Return String containing class and Encryption Method
     *
     * @return String description
     */
    @Override
    public String toString() {
        return String.format("%s Encryption Method [%s]", getClass().getSimpleName(), encryptionMethod.getAlgorithm());
    }
}
