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
package org.apache.nifi.properties;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

/**
 * Sensitive Property Provider implementation using AES-GCM with configurable key sizes
 */
class AesGcmSensitivePropertyProvider implements SensitivePropertyProvider {
    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private static final String SECRET_KEY_ALGORITHM = "AES";
    private static final String DELIMITER = "||"; // "|" is not a valid Base64 character, so ensured not to be present in cipher text
    private static final int IV_LENGTH = 12;
    private static final int TAG_LENGTH = 128;
    private static final int MIN_CIPHER_TEXT_LENGTH = IV_LENGTH * 4 / 3 + DELIMITER.length() + 1;
    private static final List<Integer> VALID_KEY_LENGTHS = Arrays.asList(128, 192, 256);

    private static final String IDENTIFIER_KEY_FORMAT = "aes/gcm/%d";
    private static final int BITS_PER_BYTE = 8;

    private static final Base64.Encoder BASE_64_ENCODER = Base64.getEncoder();
    private static final Base64.Decoder BASE_64_DECODER = Base64.getDecoder();

    private final SecureRandom secureRandom;
    private final Cipher cipher;
    private final SecretKey key;
    private final String identifierKey;

    public AesGcmSensitivePropertyProvider(final String keyHex) {
        byte[] keyBytes = validateKey(keyHex);

        secureRandom = new SecureRandom();
        try {
            cipher = Cipher.getInstance(ALGORITHM);
            key = new SecretKeySpec(keyBytes, SECRET_KEY_ALGORITHM);

            final int keySize = keyBytes.length * BITS_PER_BYTE;
            identifierKey = String.format(IDENTIFIER_KEY_FORMAT, keySize);
        } catch (final NoSuchAlgorithmException | NoSuchPaddingException e) {
            throw new SensitivePropertyProtectionException(String.format("Cipher [%s] initialization failed", ALGORITHM), e);
        }
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    /**
     * Returns the key used to identify the provider implementation in {@code nifi.properties}.
     *
     * @return the key to persist in the sibling property
     */
    @Override
    public String getIdentifierKey() {
        return identifierKey;
    }

    /**
     * Returns the encrypted cipher text.
     *
     * @param unprotectedValue the sensitive value
     * @param context The property context, unused in this provider
     * @return the value to persist in the {@code nifi.properties} file
     * @throws SensitivePropertyProtectionException if there is an exception encrypting the value
     */
    @Override
    public String protect(final String unprotectedValue, final ProtectedPropertyContext context) throws SensitivePropertyProtectionException {
        Objects.requireNonNull(unprotectedValue, "Value required");

        final byte[] iv = generateIV();
        try {
            cipher.init(Cipher.ENCRYPT_MODE, this.key, new GCMParameterSpec(TAG_LENGTH, iv));

            byte[] plainBytes = unprotectedValue.getBytes(StandardCharsets.UTF_8);
            byte[] cipherBytes = cipher.doFinal(plainBytes);
            return BASE_64_ENCODER.encodeToString(iv) + DELIMITER + BASE_64_ENCODER.encodeToString(cipherBytes);
        } catch (final BadPaddingException | IllegalBlockSizeException | InvalidAlgorithmParameterException | InvalidKeyException e) {
            throw new SensitivePropertyProtectionException("AES-GCM encryption failed", e);
        }
    }

    /**
     * Returns the decrypted plaintext.
     *
     * @param protectedValue the cipher text read from the {@code nifi.properties} file
     * @param context The property context, unused in this provider
     * @return the raw value to be used by the application
     * @throws SensitivePropertyProtectionException if there is an error decrypting the cipher text
     */
    @Override
    public String unprotect(final String protectedValue, final ProtectedPropertyContext context) throws SensitivePropertyProtectionException {
        if (protectedValue == null || protectedValue.trim().length() < MIN_CIPHER_TEXT_LENGTH) {
            throw new IllegalArgumentException("Cannot decrypt a cipher text shorter than " + MIN_CIPHER_TEXT_LENGTH + " chars");
        }

        if (!protectedValue.contains(DELIMITER)) {
            throw new IllegalArgumentException("The cipher text does not contain the delimiter " + DELIMITER + " -- it should be of the form Base64(IV) || Base64(cipherText)");
        }
        final String trimmedProtectedValue = protectedValue.trim();

        final String armoredIV = trimmedProtectedValue.substring(0, trimmedProtectedValue.indexOf(DELIMITER));
        final byte[] iv = BASE_64_DECODER.decode(armoredIV);
        if (iv.length < IV_LENGTH) {
            throw new IllegalArgumentException(String.format("The IV (%s bytes) must be at least %s bytes", iv.length, IV_LENGTH));
        }

        final String encodedCipherBytes = trimmedProtectedValue.substring(trimmedProtectedValue.indexOf(DELIMITER) + 2);

        try {
            final byte[] cipherBytes = BASE_64_DECODER.decode(encodedCipherBytes);

            cipher.init(Cipher.DECRYPT_MODE, this.key, new GCMParameterSpec(TAG_LENGTH, iv));
            final byte[] plainBytes = cipher.doFinal(cipherBytes);
            return new String(plainBytes, StandardCharsets.UTF_8);
        } catch (final BadPaddingException | IllegalBlockSizeException | InvalidAlgorithmParameterException | InvalidKeyException e) {
            throw new SensitivePropertyProtectionException("AES-GCM decryption failed", e);
        }
    }

    /**
     * No cleanup necessary
     */
    @Override
    public void cleanUp() { }

    private byte[] generateIV() {
        final byte[] iv = new byte[IV_LENGTH];
        secureRandom.nextBytes(iv);
        return iv;
    }

    private byte[] validateKey(String keyHex) {
        if (keyHex == null || keyHex.isEmpty()) {
            throw new SensitivePropertyProtectionException("AES Key not provided");
        }
        keyHex = formatHexKey(keyHex);
        if (!isHexKeyValid(keyHex)) {
            throw new SensitivePropertyProtectionException("AES Key not hexadecimal");
        }
        final byte[] key;
        try {
            key = Hex.decodeHex(keyHex);
        } catch (final DecoderException e) {
            throw new SensitivePropertyProtectionException("AES Key Hexadecimal decoding failed", e);
        }
        final int keyLengthBits = key.length * BITS_PER_BYTE;
        if (!VALID_KEY_LENGTHS.contains(keyLengthBits)) {
            throw new SensitivePropertyProtectionException(String.format("AES Key length not valid [%d]", keyLengthBits));
        }
        return key;
    }

    private static String formatHexKey(String input) {
        if (input == null || input.isEmpty()) {
            return "";
        }
        return input.replaceAll("[^0-9a-fA-F]", "").toLowerCase();
    }

    private static boolean isHexKeyValid(String key) {
        if (key == null || key.isEmpty()) {
            return false;
        }
        return key.matches("^[0-9a-fA-F]*$");
    }
}
