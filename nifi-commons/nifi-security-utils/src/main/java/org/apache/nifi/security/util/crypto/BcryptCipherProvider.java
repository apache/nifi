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

import at.favre.lib.crypto.bcrypt.BCrypt;
import at.favre.lib.crypto.bcrypt.Radix64Encoder;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.security.util.EncryptionMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BcryptCipherProvider extends RandomIVPBECipherProvider {
    private static final Logger logger = LoggerFactory.getLogger(BcryptCipherProvider.class);

    private final int workFactor;
    /**
     * This can be calculated automatically using the code {@see BcryptCipherProviderGroovyTest#calculateMinimumWorkFactor} or manually updated by a maintainer
     */
    private static final int DEFAULT_WORK_FACTOR = 12;
    private static final int DEFAULT_SALT_LENGTH = 16;

    private static final Pattern BCRYPT_SALT_FORMAT = Pattern.compile("^\\$\\d\\w\\$\\d{2}\\$[\\w\\/\\.]{22}");

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
        try {
            return getInitializedCipher(encryptionMethod, password, salt, iv, keyLength, encryptMode);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new ProcessException("Error initializing the cipher", e);
        }
    }

    @Override
    Logger getLogger() {
        return logger;
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

    protected Cipher getInitializedCipher(EncryptionMethod encryptionMethod, String password, byte[] salt, byte[] iv, int keyLength, boolean encryptMode) throws Exception {
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

        byte[] rawSalt = extractRawSalt(salt);
        String hash = new String(BCrypt.withDefaults().hash(workFactor, rawSalt, password.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);

        /* The SHA-512 hash is required in order to derive a key longer than 184 bits (the resulting size of the Bcrypt hash) and ensuring the avalanche effect causes higher key entropy (if all
        derived keys follow a consistent pattern, it weakens the strength of the encryption) */
        MessageDigest digest = MessageDigest.getInstance("SHA-512", provider);
        byte[] dk = digest.digest(hash.getBytes(StandardCharsets.UTF_8));
        dk = Arrays.copyOf(dk, keyLength / 8);
        SecretKey tempKey = new SecretKeySpec(dk, algorithm);

        KeyedCipherProvider keyedCipherProvider = new AESKeyedCipherProvider();
        return keyedCipherProvider.getCipher(encryptionMethod, tempKey, iv, encryptMode);
    }

    private static String formatSaltForBcrypt(byte[] salt) {
        if (salt == null || salt.length == 0) {
            throw new IllegalArgumentException("The salt cannot be empty. To generate a salt, use BcryptCipherProvider#generateSalt()");
        }

        String rawSalt = new String(salt, StandardCharsets.UTF_8);
        Matcher matcher = BCRYPT_SALT_FORMAT.matcher(rawSalt);

        if (matcher.find()) {
            return rawSalt;
        } else {
            throw new IllegalArgumentException("The salt must be of the format $2a$10$gUVbkVzp79H8YaCOsCVZNu. To generate a salt, use BcryptCipherProvider#generateSalt()");
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
        // TODO: This library allows for 2a, 2b, and 2y versions so this should be changed to be configurable
        String saltString = "$2a$" +
                StringUtils.leftPad(String.valueOf(workFactor), 2, "0") +
                "$" + new String(new Radix64Encoder.Default().encode(salt), StandardCharsets.UTF_8);
        return saltString.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Returns the raw salt as a {@code byte[]} extracted from the Bcrypt formatted salt byte[].
     *
     * @param fullSalt the Bcrypt salt sequence as bytes
     * @return the raw salt (16 bytes) without Radix 64 encoding
     */
    public static byte[] extractRawSalt(byte[] fullSalt) {
        try {
            String formattedSalt = formatSaltForBcrypt(fullSalt);
            String rawSalt = formattedSalt.substring(formattedSalt.lastIndexOf("$") + 1);
            if (rawSalt.length() != 22) {
                throw new IllegalArgumentException("The formatted salt did not contain a raw salt");
            }
            return new Radix64Encoder.Default().decode(rawSalt.getBytes(StandardCharsets.UTF_8));
        } catch (IllegalArgumentException e) {
            logger.warn("Unable to extract a raw salt from bcrypt salt {}", new String(fullSalt, StandardCharsets.UTF_8));
            throw e;
        }
    }

    /**
     * Returns the raw salt as a {@code byte[]} extracted from the Bcrypt formatted salt String.
     *
     * @param fullSalt the Bcrypt salt sequence
     * @return the raw salt (16 bytes)
     */
    public static byte[] extractRawSalt(String fullSalt) {
        if (fullSalt == null) {
            return new byte[0];
        }
        return extractRawSalt(fullSalt.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public int getDefaultSaltLength() {
        return DEFAULT_SALT_LENGTH;
    }

    protected int getWorkFactor() {
        return workFactor;
    }
}
