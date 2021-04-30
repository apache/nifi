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

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.security.kms.CryptoUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.Security;

/**
 * Cipher Property Encryptor provides hexadecimal encoding and decoding around cipher operations
 */
abstract class CipherPropertyEncryptor implements PropertyEncryptor {
    private static final Charset PROPERTY_CHARSET = StandardCharsets.UTF_8;

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    /**
     * Encrypt property and encode as a hexadecimal string
     *
     * @param property Property value to be encrypted
     * @return Encrypted and hexadecimal string
     */
    @Override
    public String encrypt(final String property) {
        final byte[] binary = property.getBytes(PROPERTY_CHARSET);

        final byte[] encodedParameters = getEncodedParameters();
        final Cipher cipher = getEncryptionCipher(encodedParameters);
        try {
            final byte[] encrypted = cipher.doFinal(binary);
            return Hex.encodeHexString(CryptoUtils.concatByteArrays(encodedParameters, encrypted));
        } catch (final BadPaddingException | IllegalBlockSizeException e) {
            final String message = String.format("Encryption Failed with Algorithm [%s]", cipher.getAlgorithm());
            throw new EncryptionException(message, e);
        }
    }

    /**
     * Decrypt property from a hexadecimal string
     *
     * @param encryptedProperty Encrypted property value to be deciphered
     * @return Property decoded from hexadecimal string and deciphered from binary
     */
    @Override
    public String decrypt(final String encryptedProperty) {
        final byte[] binary = getDecodedBinary(encryptedProperty);
        final Cipher cipher = getDecryptionCipher(binary);
        final byte[] cipherBinary = getCipherBinary(binary);
        try {
            final byte[] deciphered = cipher.doFinal(cipherBinary);
            return new String(deciphered, PROPERTY_CHARSET);
        } catch (final BadPaddingException | IllegalBlockSizeException e) {
            final String message = String.format("Decryption Failed with Algorithm [%s]", cipher.getAlgorithm());
            throw new EncryptionException(message, e);
        }
    }

    private byte[] getDecodedBinary(final String encryptedProperty) {
        try {
            return Hex.decodeHex(encryptedProperty);
        } catch (final DecoderException e) {
            throw new EncryptionException("Hexadecimal decoding failed", e);
        }
    }

    /**
     * Get Encoded Parameters based on cipher implementation
     *
     * @return Encoded Parameters
     */
    protected abstract byte[] getEncodedParameters();

    /**
     * Get Cipher for Encryption using encoded parameters
     *
     * @param encodedParameters Binary encoded parameters
     * @return Cipher for Encryption
     */
    protected abstract Cipher getEncryptionCipher(byte[] encodedParameters);

    /**
     * Get Cipher for Decryption based on encrypted binary
     *
     * @param encryptedBinary Encrypted Binary
     * @return Cipher for Decryption
     */
    protected abstract Cipher getDecryptionCipher(byte[] encryptedBinary);

    /**
     * Get Cipher Binary from encrypted binary
     *
     * @param encryptedBinary Encrypted Binary containing cipher binary and other information
     * @return Cipher Binary for decryption
     */
    protected abstract byte[] getCipherBinary(byte[] encryptedBinary);
}
