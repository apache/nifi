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

import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.encoders.DecoderException;
import org.bouncycastle.util.encoders.EncoderException;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class AESSensitivePropertyProvider extends AbstractSensitivePropertyProvider {
    private static final Logger logger = LoggerFactory.getLogger(AESSensitivePropertyProvider.class);

    private static final String IMPLEMENTATION_NAME = "AES Sensitive Property Provider";
    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private static final String PROVIDER = "BC";
    private static final String DELIMITER = "||"; // "|" is not a valid Base64 character, so ensured not to be present in cipher text
    private static final int IV_LENGTH = 12;
    private static final int MIN_CIPHER_TEXT_LENGTH = IV_LENGTH * 4 / 3 + DELIMITER.length() + 1;

    private final Cipher cipher;
    private final SecretKey key;
    private final int keySize;

    AESSensitivePropertyProvider(final byte[] keyHex) {
        this(keyHex == null ? "" : Hex.toHexString(keyHex));
    }

    AESSensitivePropertyProvider(final String keyHex) {
        super(null);

        byte[] keyBytes = validateKey(keyHex);

        try {
            this.cipher = Cipher.getInstance(ALGORITHM, PROVIDER);
            // Only store the key if the cipher was initialized successfully
            this.key = new SecretKeySpec(keyBytes, "AES");
            this.keySize = getKeySize(Hex.toHexString(this.key.getEncoded()));
        } catch (NoSuchAlgorithmException | NoSuchProviderException | NoSuchPaddingException e) {
            logger.error("Encountered an error initializing the {}: {}", IMPLEMENTATION_NAME, e.getMessage());
            throw new SensitivePropertyProtectionException("Error initializing the protection cipher", e);
        }
    }

    @Override
    protected PropertyProtectionScheme getProtectionScheme() {
        return PropertyProtectionScheme.AES_GCM;
    }

    @Override
    public boolean isSupported() {
        return true; // AES protection is always supported
    }

    private byte[] validateKey(String keyHex) {
        if (keyHex == null || StringUtils.isBlank(keyHex)) {
            throw new SensitivePropertyProtectionException("The key cannot be empty");
        }
        keyHex = formatHexKey(keyHex);
        if (!isHexKeyValid(keyHex)) {
            throw new SensitivePropertyProtectionException("The key must be a valid hexadecimal key");
        }
        byte[] key = Hex.decode(keyHex);
        final List<Integer> validKeyLengths = getValidKeyLengths();
        if (!validKeyLengths.contains(key.length * 8)) {
            List<String> validKeyLengthsAsStrings = validKeyLengths.stream().map(i -> Integer.toString(i)).collect(Collectors.toList());
            throw new SensitivePropertyProtectionException("The key (" + key.length * 8 + " bits) must be a valid length: " + StringUtils.join(validKeyLengthsAsStrings, ", "));
        }
        return key;
    }

    private static String formatHexKey(String input) {
        if (input == null || StringUtils.isBlank(input)) {
            return "";
        }
        return input.replaceAll("[^0-9a-fA-F]", "").toLowerCase();
    }

    private static boolean isHexKeyValid(String key) {
        if (key == null || StringUtils.isBlank(key)) {
            return false;
        }
        // Key length is in "nibbles" (i.e. one hex char = 4 bits)
        return getValidKeyLengths().contains(key.length() * 4) && key.matches("^[0-9a-fA-F]*$");
    }

    private static List<Integer> getValidKeyLengths() {
        List<Integer> validLengths = new ArrayList<>();
        validLengths.add(128);

        try {
            if (Cipher.getMaxAllowedKeyLength("AES") > 128) {
                validLengths.add(192);
                validLengths.add(256);
            } else {
                logger.warn("JCE Unlimited Strength Cryptography Jurisdiction policies are not available, so the max key length is 128 bits");
            }
        } catch (NoSuchAlgorithmException e) {
            logger.warn("Encountered an error determining the max key length", e);
        }

        return validLengths;
    }

    /**
     * Returns the key used to identify the provider implementation in {@code nifi.properties}.
     *
     * @return the key to persist in the sibling property
     */
    @Override
    public String getIdentifierKey() {
        return getProtectionScheme().getIdentifier(String.valueOf(keySize));
    }

    private static int getKeySize(final String key) {
        // A key in hexadecimal format has one char per nibble (4 bits)
        return StringUtils.isBlank(key) ? 0 : formatHexKey(key).length() * 4;
    }

    /**
     * Returns the encrypted cipher text.
     *
     * @param unprotectedValue the sensitive value
     * @return the value to persist in the {@code nifi.properties} file
     * @throws SensitivePropertyProtectionException if there is an exception encrypting the value
     */
    @Override
    public String protect(final String unprotectedValue) throws SensitivePropertyProtectionException {
        if (StringUtils.isBlank(unprotectedValue)) {
            throw new IllegalArgumentException("Cannot encrypt an empty value");
        }

        // Generate IV
        byte[] iv = generateIV();
        if (iv.length < IV_LENGTH) {
            throw new IllegalArgumentException("The IV (" + iv.length + " bytes) must be at least " + IV_LENGTH + " bytes");
        }

        try {
            // Initialize cipher for encryption
            cipher.init(Cipher.ENCRYPT_MODE, this.key, new IvParameterSpec(iv));

            byte[] plainBytes = unprotectedValue.getBytes(StandardCharsets.UTF_8);
            byte[] cipherBytes = cipher.doFinal(plainBytes);
            logger.debug(getName() + " encrypted a sensitive value successfully");
            return base64Encode(iv) + DELIMITER + base64Encode(cipherBytes);
            // return Base64.toBase64String(iv) + DELIMITER + Base64.toBase64String(cipherBytes);
        } catch (BadPaddingException | IllegalBlockSizeException | EncoderException | InvalidAlgorithmParameterException | InvalidKeyException e) {
            final String msg = "Error encrypting a protected value";
            logger.error(msg, e);
            throw new SensitivePropertyProtectionException(msg, e);
        }
    }

    private String base64Encode(final byte[] input) {
        return Base64.toBase64String(input).replaceAll("=", "");
    }

    /**
     * Generates a new random IV of 12 bytes using {@link java.security.SecureRandom}.
     *
     * @return the IV
     */
    private byte[] generateIV() {
        final byte[] iv = new byte[IV_LENGTH];
        new SecureRandom().nextBytes(iv);
        return iv;
    }

    /**
     * Returns the decrypted plaintext.
     *
     * @param protectedValue the cipher text read from the {@code nifi.properties} file
     * @return the raw value to be used by the application
     * @throws SensitivePropertyProtectionException if there is an error decrypting the cipher text
     */
    @Override
    public String unprotect(final String protectedValue) throws SensitivePropertyProtectionException {
        if (protectedValue == null || protectedValue.trim().length() < MIN_CIPHER_TEXT_LENGTH) {
            throw new IllegalArgumentException("Cannot decrypt a cipher text shorter than " + MIN_CIPHER_TEXT_LENGTH + " chars");
        }

        if (!protectedValue.contains(DELIMITER)) {
            throw new IllegalArgumentException("The cipher text does not contain the delimiter " + DELIMITER + " -- it should be of the form Base64(IV) || Base64(cipherText)");
        }
        final String trimmedProtectedValue = protectedValue.trim();

        final String armoredIV = trimmedProtectedValue.substring(0, trimmedProtectedValue.indexOf(DELIMITER));
        final byte[] iv = Base64.decode(armoredIV);
        if (iv.length < IV_LENGTH) {
            throw new IllegalArgumentException(String.format("The IV (%s bytes) must be at least %s bytes", iv.length, IV_LENGTH));
        }

        String armoredCipherText = trimmedProtectedValue.substring(trimmedProtectedValue.indexOf(DELIMITER) + 2);

        // Restore the = padding if necessary to reconstitute the GCM MAC check
        if (armoredCipherText.length() % 4 != 0) {
            final int paddedLength = armoredCipherText.length() + 4 - (armoredCipherText.length() % 4);
            armoredCipherText = StringUtils.rightPad(armoredCipherText, paddedLength, '=');
        }

        try {
            final byte[] cipherBytes = Base64.decode(armoredCipherText);

            cipher.init(Cipher.DECRYPT_MODE, this.key, new IvParameterSpec(iv));
            final byte[] plainBytes = cipher.doFinal(cipherBytes);
            logger.debug(getName() + " decrypted a sensitive value successfully");
            return new String(plainBytes, StandardCharsets.UTF_8);
        } catch (BadPaddingException | IllegalBlockSizeException | DecoderException | InvalidAlgorithmParameterException | InvalidKeyException e) {
            final String msg = "Error decrypting a protected value";
            logger.error(msg, e);
            throw new SensitivePropertyProtectionException(msg, e);
        }
    }

    public static int getIvLength() {
        return IV_LENGTH;
    }

    public static int getMinCipherTextLength() {
        return MIN_CIPHER_TEXT_LENGTH;
    }

    public static String getDelimiter() {
        return DELIMITER;
    }
}
