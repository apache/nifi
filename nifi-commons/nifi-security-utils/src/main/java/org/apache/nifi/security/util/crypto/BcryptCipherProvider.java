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

import at.favre.lib.crypto.bcrypt.Radix64Encoder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.security.util.EncryptionMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BcryptCipherProvider extends RandomIVPBECipherProvider {
    private static final Logger logger = LoggerFactory.getLogger(BcryptCipherProvider.class);

    private final int workFactor;
    /**
     * This can be calculated automatically using the code {@see BcryptCipherProviderGroovyTest#calculateMinimumWorkFactor} or manually updated by a maintainer
     */
    private static final int DEFAULT_WORK_FACTOR = 12;
    private static final int DEFAULT_SALT_LENGTH = 16;

    private static final Pattern BCRYPT_SALT_FORMAT = Pattern.compile("^\\$\\d\\w\\$\\d{2}\\$[\\w\\/\\.]{22}");
    private static final String BCRYPT_SALT_FORMAT_MSG = "The salt must be of the format $2a$10$gUVbkVzp79H8YaCOsCVZNu. To generate a salt, use BcryptCipherProvider#generateSalt()";

    /**
     * Instantiates a Bcrypt cipher provider with the default work factor 12 (2^12 key expansion rounds).
     */
    public BcryptCipherProvider() {
        this(DEFAULT_WORK_FACTOR);
    }

    /**
     * Instantiates a Bcrypt cipher provider with the specified work factor w (2^w key expansion rounds).
     *
     * @param workFactor the (log) number of key expansion rounds [4..30]
     */
    public BcryptCipherProvider(int workFactor) {
        this.workFactor = workFactor;
        if (workFactor < DEFAULT_WORK_FACTOR) {
            logger.warn("The provided work factor {} is below the recommended minimum {}", workFactor, DEFAULT_WORK_FACTOR);
        }
    }

    @Override
    Logger getLogger() {
        return logger;
    }

    /**
     * Returns an initialized cipher for the specified algorithm. The key is derived by the KDF of the implementation. The IV is provided externally to allow for non-deterministic IVs, as IVs
     * deterministically derived from the password are a potential vulnerability and compromise semantic security. See
     * <a href="http://crypto.stackexchange.com/a/3970/12569">Ilmari Karonen's answer on Crypto Stack Exchange</a>
     *
     * @param encryptionMethod the {@link EncryptionMethod}
     * @param password         the secret input
     * @param salt             the complete salt (e.g. {@code "$2a$10$gUVbkVzp79H8YaCOsCVZNu".getBytes(StandardCharsets.UTF_8)})
     * @param iv               the IV
     * @param keyLength        the desired key length in bits
     * @param encryptMode      true for encrypt, false for decrypt
     * @return the initialized cipher
     * @throws Exception if there is a problem initializing the cipher
     */
    @Override
    public Cipher getCipher(EncryptionMethod encryptionMethod, String password, byte[] salt, byte[] iv, int keyLength, boolean encryptMode) throws Exception {
        return createCipherAndHandleExceptions(encryptionMethod, password, salt, iv, keyLength, encryptMode, false);
    }

    private Cipher createCipherAndHandleExceptions(EncryptionMethod encryptionMethod, String password, byte[] salt, byte[] iv, int keyLength, boolean encryptMode, boolean useLegacyKeyDerivation) {
        try {
            return getInitializedCipher(encryptionMethod, password, salt, iv, keyLength, encryptMode, useLegacyKeyDerivation);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new ProcessException("Error initializing the cipher", e);
        }
    }

    /**
     * Returns an initialized cipher for the specified algorithm. The key (and IV if necessary) are derived by the KDF of the implementation.
     * <p>
     * The IV can be retrieved by the calling method using {@link Cipher#getIV()}.
     *
     * @param encryptionMethod the {@link EncryptionMethod}
     * @param password         the secret input
     * @param salt             the complete salt (e.g. {@code "$2a$10$gUVbkVzp79H8YaCOsCVZNu".getBytes(StandardCharsets.UTF_8)})
     * @param keyLength        the desired key length in bits
     * @param encryptMode      true for encrypt, false for decrypt
     * @return the initialized cipher
     * @throws Exception if there is a problem initializing the cipher
     */
    @Override
    public Cipher getCipher(EncryptionMethod encryptionMethod, String password, byte[] salt, int keyLength, boolean encryptMode) throws Exception {
        return getCipher(encryptionMethod, password, salt, new byte[0], keyLength, encryptMode);
    }

    /**
     * Returns a {@link Cipher} instance in {@code Cipher.DECRYPT_MODE} configured with the provided inputs and using <em>the
     * legacy key derivation process for {@code Bcrypt}</em> where the complete Bcrypt hash output (including algorithm, work
     * factor, and salt) was used as the input to the key stretching SHA-512 digest function. This is only used for
     * backward-compatibility decryptions for NiFi versions prior to 1.12.0. All encryption operations moving forward use the
     * correct key derivation process.
     *
     * @param encryptionMethod the {@link EncryptionMethod}
     * @param password         the secret input
     * @param salt             the complete salt (e.g. {@code "$2a$10$gUVbkVzp79H8YaCOsCVZNu".getBytes(StandardCharsets.UTF_8)})
     * @param iv               the Initialization Vector in bits
     * @param keyLength        the desired key length in bits
     * @return the initialized cipher using the legacy key derivation process
     * @throws Exception if there is a problem initializing the cipher
     */
    public Cipher getLegacyDecryptCipher(EncryptionMethod encryptionMethod, String password, byte[] salt, byte[] iv, int keyLength) {
        return createCipherAndHandleExceptions(encryptionMethod, password, salt, iv, keyLength, false, true);
    }

    protected Cipher getInitializedCipher(EncryptionMethod encryptionMethod, String password, byte[] salt, byte[] iv,
                                          int keyLength, boolean encryptMode, boolean useLegacyKeyDerivation) throws Exception {
        if (encryptionMethod == null) {
            throw new IllegalArgumentException("The encryption method must be specified");
        }
        if (!encryptionMethod.isCompatibleWithStrongKDFs()) {
            throw new IllegalArgumentException(encryptionMethod.name() + " is not compatible with Bcrypt");
        }

        if (StringUtils.isEmpty(password)) {
            throw new IllegalArgumentException("Encryption with an empty password is not supported");
        }

        String algorithm = encryptionMethod.getAlgorithm();
        String provider = encryptionMethod.getProvider();

        final String cipherName = CipherUtility.parseCipherFromAlgorithm(algorithm);
        if (!CipherUtility.isValidKeyLength(keyLength, cipherName)) {
            throw new IllegalArgumentException(keyLength + " is not a valid key length for " + cipherName);
        }

        if (salt == null || salt.length == 0) {
            throw new IllegalArgumentException(BCRYPT_SALT_FORMAT_MSG);
        }
        String saltString = new String(salt, StandardCharsets.UTF_8);
        byte[] rawSalt = new byte[DEFAULT_SALT_LENGTH];
        int workFactor = 0;
        if (isBcryptFormattedSalt(saltString)) {
            // parseSalt will extract workFactor from salt
            workFactor = parseSalt(saltString, rawSalt);
        } else {
            throw new IllegalArgumentException(BCRYPT_SALT_FORMAT_MSG);
        }

        try {
            SecretKey tempKey = deriveKey(password, keyLength, algorithm, rawSalt, workFactor, useLegacyKeyDerivation);
            KeyedCipherProvider keyedCipherProvider = new AESKeyedCipherProvider();
            return keyedCipherProvider.getCipher(encryptionMethod, tempKey, iv, encryptMode);
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains("salt must be exactly")) {
                throw new IllegalArgumentException(BCRYPT_SALT_FORMAT_MSG, e);
            } else if (e.getMessage().contains("The salt length")) {
                throw new IllegalArgumentException("The raw salt must be greater than or equal to 16 bytes", e);
            } else {
                logger.error("Encountered an error generating the Bcrypt hash", e);
                throw e;
            }
        }
    }

    private SecretKey deriveKey(String password, int keyLength, String algorithm, byte[] rawSalt,
                                int workFactor, boolean useLegacyKeyDerivation) {

        final int derivedKeyLength = keyLength / 8;
        final SecureHasher secureHasher = new KeyDerivationBcryptSecureHasher(derivedKeyLength, workFactor, useLegacyKeyDerivation);
        byte[] derivedKey = secureHasher.hashRaw(password.getBytes(StandardCharsets.UTF_8), rawSalt);
        return new SecretKeySpec(derivedKey, algorithm);
    }

    /**
     * Returns {@code true} if the salt string is a valid Bcrypt salt string ({@code $2a$10$abcdefghi..{22}}).
     *
     * @param salt the salt string to evaluate
     * @return true if valid Bcrypt salt
     */
    public static boolean isBcryptFormattedSalt(String salt) {
        if (salt == null || salt.length() == 0) {
            throw new IllegalArgumentException("The salt cannot be empty. To generate a salt, use BcryptCipherProvider#generateSalt()");
        }
        Matcher matcher = BCRYPT_SALT_FORMAT.matcher(salt);
        return matcher.find();
    }

    private int parseSalt(String bcryptSalt, byte[] rawSalt) {
        if (StringUtils.isEmpty(bcryptSalt)) {
            throw new IllegalArgumentException("Cannot parse empty salt");
        }

        /** Salt format is $2a$10$saltB64 */
        byte[] salt = extractRawSalt(bcryptSalt);
        if (rawSalt.length < salt.length) {
            byte[] tempBytes = new byte[salt.length];
            System.arraycopy(rawSalt, 0, tempBytes, 0, rawSalt.length);
            rawSalt = tempBytes;
        }
        // Copy salt from full bcryptSalt into rawSalt byte[]
        System.arraycopy(salt, 0, rawSalt, 0, salt.length);

        // Parse the workfactor and return
        final String[] saltComponents = bcryptSalt.split("\\$");
        return Integer.parseInt(saltComponents[2]);
    }

    public static String formatSaltForBcrypt(byte[] salt, int workFactor) {
        String rawSaltString = new String(salt, StandardCharsets.UTF_8);
        if (isBcryptFormattedSalt(rawSaltString)) {
            return rawSaltString;
        } else {
            // TODO: This library allows for 2a, 2b, and 2y versions so this should be changed to be configurable
            String saltString = "$2a$" +
                    StringUtils.leftPad(String.valueOf(workFactor), 2, "0") +
                    "$" + new String(new Radix64Encoder.Default().encode(salt), StandardCharsets.UTF_8);
            return saltString;
        }
    }

    /**
     * Returns the full salt in a {@code byte[]} for this cipher provider (i.e. {@code $2a$10$abcdef...} format).
     *
     * @return the full salt as a byte[]
     */
    @Override
    public byte[] generateSalt() {
        byte[] salt = new byte[DEFAULT_SALT_LENGTH];
        SecureRandom sr = new SecureRandom();
        sr.nextBytes(salt);
        String saltString = formatSaltForBcrypt(salt, getWorkFactor());
        return saltString.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Returns the raw salt as a {@code byte[]} extracted from the Bcrypt formatted salt byte[].
     *
     * @param fullSalt the Bcrypt salt sequence as bytes
     * @return the raw salt (16 bytes) without Radix 64 encoding
     */
    public static byte[] extractRawSalt(String fullSalt) {
        final String[] saltComponents = fullSalt.split("\\$");
        if (saltComponents.length < 4) {
            throw new IllegalArgumentException("Could not parse salt");
        }

        // Bcrypt uses a custom Radix64 encoding alphabet that is not compatible with default Base64
        return new Radix64Encoder.Default().decode(saltComponents[3].getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public int getDefaultSaltLength() {
        return DEFAULT_SALT_LENGTH;
    }

    protected int getWorkFactor() {
        return workFactor;
    }
}
