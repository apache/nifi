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

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.crypto.scrypt.Scrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScryptCipherProvider extends RandomIVPBECipherProvider {
    private static final Logger logger = LoggerFactory.getLogger(ScryptCipherProvider.class);

    private final int n;
    private final int r;
    private final int p;
    /**
     * These values can be calculated automatically using the code {@see ScryptCipherProviderGroovyTest#calculateMinimumParameters} or manually updated by a maintainer
     */
    private static final int DEFAULT_N = Double.valueOf(Math.pow(2, 14)).intValue();
    private static final int DEFAULT_R = 8;
    private static final int DEFAULT_P = 1;

    private static final Pattern SCRYPT_SALT_FORMAT = Pattern.compile("^\\$s0\\$[a-f0-9]{5,16}\\$[\\w\\/\\.]{12,44}");
    private static final Pattern MCRYPT_SALT_FORMAT = Pattern.compile("^\\$\\d+\\$\\d+\\$\\d+\\$[a-f0-9]{16,64}");

    /**
     * Instantiates a Scrypt cipher provider with the default parameters N=2^14, r=8, p=1.
     */
    public ScryptCipherProvider() {
        this(DEFAULT_N, DEFAULT_R, DEFAULT_P);
    }

    /**
     * Instantiates a Scrypt cipher provider with the specified N, r, p values.
     *
     * @param n the number of iterations
     * @param r the block size in bytes
     * @param p the parallelization factor
     */
    public ScryptCipherProvider(int n, int r, int p) {
        this.n = n;
        this.r = r;
        this.p = p;
        if (n < DEFAULT_N) {
            logger.warn("The provided iteration count {} is below the recommended minimum {}", n, DEFAULT_N);
        }
        if (r < DEFAULT_R) {
            logger.warn("The provided block size {} is below the recommended minimum {}", r, DEFAULT_R);
        }
        if (p < DEFAULT_P) {
            logger.warn("The provided parallelization factor {} is below the recommended minimum {}", p, DEFAULT_P);
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
     *
     * The IV can be retrieved by the calling method using {@link Cipher#getIV()}.
     *
     * @param encryptionMethod the {@link EncryptionMethod}
     * @param password         the secret input
     * @param salt             the complete salt (e.g. {@code "$s0$20101$gUVbkVzp79H8YaCOsCVZNu".getBytes(StandardCharsets.UTF_8)})
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
            throw new IllegalArgumentException(encryptionMethod.name() + " is not compatible with Scrypt");
        }

        if (StringUtils.isEmpty(password)) {
            throw new IllegalArgumentException("Encryption with an empty password is not supported");
        }

        String algorithm = encryptionMethod.getAlgorithm();

        final String cipherName = CipherUtility.parseCipherFromAlgorithm(algorithm);
        if (!CipherUtility.isValidKeyLength(keyLength, cipherName)) {
            throw new IllegalArgumentException(String.valueOf(keyLength) + " is not a valid key length for " + cipherName);
        }

        String scryptSalt = formatSaltForScrypt(salt);
        List<Integer> params = new ArrayList<>(3);
        byte[] rawSalt = new byte[Scrypt.getDefaultSaltLength()];

        parseSalt(scryptSalt, rawSalt, params);

        String hash = Scrypt.scrypt(password, rawSalt, params.get(0), params.get(1), params.get(2), keyLength);

        // Split out the derived key from the hash and form a key object
        final String[] hashComponents = hash.split("\\$");
        final int HASH_INDEX = 4;
        if (hashComponents.length < HASH_INDEX) {
            throw new ProcessException("There was an error generating a scrypt hash -- the resulting hash was not properly formatted");
        }
        byte[] keyBytes = Base64.decodeBase64(hashComponents[HASH_INDEX]);
        SecretKey tempKey = new SecretKeySpec(keyBytes, algorithm);

        KeyedCipherProvider keyedCipherProvider = new AESKeyedCipherProvider();
        return keyedCipherProvider.getCipher(encryptionMethod, tempKey, iv, encryptMode);
    }

    private void parseSalt(String scryptSalt, byte[] rawSalt, List<Integer> params) {
        if (StringUtils.isEmpty(scryptSalt)) {
            throw new IllegalArgumentException("Cannot parse empty salt");
        }

        /** Salt format is $s0$params$saltB64 where params is encoded according to
         *  {@link Scrypt#parseParameters(String)}*/
        final String[] saltComponents = scryptSalt.split("\\$");
        if (saltComponents.length < 4) {
            throw new IllegalArgumentException("Could not parse salt");
        }
        byte[] salt = Base64.decodeBase64(saltComponents[3]);
        if (rawSalt.length < salt.length) {
            byte[] tempBytes = new byte[salt.length];
            System.arraycopy(rawSalt, 0, tempBytes, 0, rawSalt.length);
            rawSalt = tempBytes;
        }
        System.arraycopy(salt, 0, rawSalt, 0, salt.length);

        if (params == null) {
            params = new ArrayList<>(3);
        }
        params.addAll(Scrypt.parseParameters(saltComponents[2]));
    }

    /**
     * Formats the salt into a string which Scrypt can understand containing the N, r, p values along with the salt value. If the provided salt contains all values, the response will be unchanged.
     * If it only contains the raw salt value, the resulting return value will also include the current instance version, N, r, and p.
     *
     * @param salt the provided salt
     * @return the properly-formatted and complete salt
     */
    private String formatSaltForScrypt(byte[] salt) {
        if (salt == null || salt.length == 0) {
            throw new IllegalArgumentException("The salt cannot be empty. To generate a salt, use ScryptCipherProvider#generateSalt()");
        }

        String saltString = new String(salt, StandardCharsets.UTF_8);
        Matcher matcher = SCRYPT_SALT_FORMAT.matcher(saltString);

        if (matcher.find()) {
            return saltString;
        } else {
            if (saltString.startsWith("$")) {
                logger.warn("Salt starts with $ but is not valid scrypt salt");
                matcher = MCRYPT_SALT_FORMAT.matcher(saltString);
                if (matcher.find()) {
                    logger.warn("The salt appears to be of the modified mcrypt format. Use ScryptCipherProvider#translateSalt(mcryptSalt) to form a valid salt");
                    return translateSalt(saltString);
                }

                logger.info("Salt is not modified mcrypt format");
            }
            logger.info("Treating as raw salt bytes");

            // Ensure the length of the salt
            int saltLength = salt.length;
            if (saltLength < 8 || saltLength > 32) {
                throw new IllegalArgumentException("The raw salt must be between 8 and 32 bytes");
            }
            return Scrypt.formatSalt(salt, n, r, p);
        }
    }

    /**
     * Translates a salt from the mcrypt format {@code $n$r$p$salt_hex} to the Java scrypt format {@code $s0$params$saltBase64}.
     *
     * @param mcryptSalt the mcrypt-formatted salt string
     * @return the formatted salt to use with Java Scrypt
     */
    public String translateSalt(String mcryptSalt) {
        if (StringUtils.isEmpty(mcryptSalt)) {
            throw new IllegalArgumentException("Cannot translate empty salt");
        }

        // Format should be $n$r$p$saltHex
        Matcher matcher = MCRYPT_SALT_FORMAT.matcher(mcryptSalt);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Salt is not valid mcrypt format of $n$r$p$saltHex");
        }

        String[] components = mcryptSalt.split("\\$");
        try {
            return Scrypt.formatSalt(Hex.decodeHex(components[4].toCharArray()), Integer.valueOf(components[1]), Integer.valueOf(components[2]), Integer.valueOf(components[3]));
        } catch (DecoderException e) {
            final String msg = "Mcrypt salt was not properly hex-encoded";
            logger.warn(msg);
            throw new IllegalArgumentException(msg);
        }
    }

    @Override
    public byte[] generateSalt() {
        byte[] salt = new byte[Scrypt.getDefaultSaltLength()];
        new SecureRandom().nextBytes(salt);
        return Scrypt.formatSalt(salt, n, r, p).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public int getDefaultSaltLength() {
        return Scrypt.getDefaultSaltLength();
    }

    protected int getN() {
        return n;
    }

    protected int getR() {
        return r;
    }

    protected int getP() {
        return p;
    }
}
