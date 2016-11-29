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

import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.encoders.DecoderException;
import org.bouncycastle.util.encoders.EncoderException;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AESSensitivePropertyProvider implements SensitivePropertyProvider {
    private static final Logger logger = LoggerFactory.getLogger(AESSensitivePropertyProvider.class);

    private static final String IMPLEMENTATION_NAME = "AES Sensitive Property Provider";
    private static final String IMPLEMENTATION_KEY = "aes/gcm/";
    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private static final String PROVIDER = "BC";
    private static final String DELIMITER = "||"; // "|" is not a valid Base64 character, so ensured not to be present in cipher text
    private static final int IV_LENGTH = 12;
    private static final int MIN_CIPHER_TEXT_LENGTH = IV_LENGTH * 4 / 3 + DELIMITER.length() + 1;

    private Cipher cipher;
    private final SecretKey key;

    public AESSensitivePropertyProvider(String keyHex) throws NoSuchPaddingException, NoSuchAlgorithmException, NoSuchProviderException {
        byte[] key = validateKey(keyHex);

        try {
            cipher = Cipher.getInstance(ALGORITHM, PROVIDER);
            // Only store the key if the cipher was initialized successfully
            this.key = new SecretKeySpec(key, "AES");
        } catch (NoSuchAlgorithmException | NoSuchProviderException | NoSuchPaddingException e) {
            logger.error("Encountered an error initializing the {}: {}", IMPLEMENTATION_NAME, e.getMessage());
            throw new SensitivePropertyProtectionException("Error initializing the protection cipher", e);
        }
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

    public AESSensitivePropertyProvider(byte[] key) throws NoSuchPaddingException, NoSuchAlgorithmException, NoSuchProviderException {
        this(key == null ? "" : Hex.toHexString(key));
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
     * Returns the name of the underlying implementation.
     *
     * @return the name of this sensitive property provider
     */
    @Override
    public String getName() {
        return IMPLEMENTATION_NAME;
    }

    /**
     * Returns the key used to identify the provider implementation in {@code nifi.properties}.
     *
     * @return the key to persist in the sibling property
     */
    @Override
    public String getIdentifierKey() {
        return IMPLEMENTATION_KEY + getKeySize(Hex.toHexString(key.getEncoded()));
    }

    private int getKeySize(String key) {
        if (StringUtils.isBlank(key)) {
            return 0;
        } else {
            // A key in hexadecimal format has one char per nibble (4 bits)
            return formatHexKey(key).length() * 4;
        }
    }

    /**
     * Returns the encrypted cipher text.
     *
     * @param unprotectedValue the sensitive value
     * @return the value to persist in the {@code nifi.properties} file
     * @throws SensitivePropertyProtectionException if there is an exception encrypting the value
     */
    @Override
    public String protect(String unprotectedValue) throws SensitivePropertyProtectionException {
        if (unprotectedValue == null || unprotectedValue.trim().length() == 0) {
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
            logger.info(getName() + " encrypted a sensitive value successfully");
            return base64Encode(iv) + DELIMITER + base64Encode(cipherBytes);
            // return Base64.toBase64String(iv) + DELIMITER + Base64.toBase64String(cipherBytes);
        } catch (BadPaddingException | IllegalBlockSizeException | EncoderException | InvalidAlgorithmParameterException | InvalidKeyException e) {
            final String msg = "Error encrypting a protected value";
            logger.error(msg, e);
            throw new SensitivePropertyProtectionException(msg, e);
        }
    }

    private String base64Encode(byte[] input) {
        return Base64.toBase64String(input).replaceAll("=", "");
    }

    /**
     * Generates a new random IV of 12 bytes using {@link java.security.SecureRandom}.
     *
     * @return the IV
     */
    private byte[] generateIV() {
        byte[] iv = new byte[IV_LENGTH];
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
    public String unprotect(String protectedValue) throws SensitivePropertyProtectionException {
        if (protectedValue == null || protectedValue.trim().length() < MIN_CIPHER_TEXT_LENGTH) {
            throw new IllegalArgumentException("Cannot decrypt a cipher text shorter than " + MIN_CIPHER_TEXT_LENGTH + " chars");
        }

        if (!protectedValue.contains(DELIMITER)) {
            throw new IllegalArgumentException("The cipher text does not contain the delimiter " + DELIMITER + " -- it should be of the form Base64(IV) || Base64(cipherText)");
        }

        protectedValue = protectedValue.trim();

        final String IV_B64 = protectedValue.substring(0, protectedValue.indexOf(DELIMITER));
        byte[] iv = Base64.decode(IV_B64);
        if (iv.length < IV_LENGTH) {
            throw new IllegalArgumentException("The IV (" + iv.length + " bytes) must be at least " + IV_LENGTH + " bytes");
        }

        String CIPHERTEXT_B64 = protectedValue.substring(protectedValue.indexOf(DELIMITER) + 2);

        // Restore the = padding if necessary to reconstitute the GCM MAC check
        if (CIPHERTEXT_B64.length() % 4 != 0) {
            final int paddedLength = CIPHERTEXT_B64.length() + 4 - (CIPHERTEXT_B64.length() % 4);
            CIPHERTEXT_B64 = StringUtils.rightPad(CIPHERTEXT_B64, paddedLength, '=');
        }

        try {
            byte[] cipherBytes = Base64.decode(CIPHERTEXT_B64);

            cipher.init(Cipher.DECRYPT_MODE, this.key, new IvParameterSpec(iv));
            byte[] plainBytes = cipher.doFinal(cipherBytes);
            logger.info(getName() + " decrypted a sensitive value successfully");
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
