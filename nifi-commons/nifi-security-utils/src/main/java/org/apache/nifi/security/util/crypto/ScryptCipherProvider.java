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

    private static final Pattern SCRYPT_SALT_FORMAT = Pattern.compile("^\\$s0\\$[a-f0-9]{5,16}\\$[\\w\\/\\+]{12,44}");
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
        if (!isPValid(r, p)) {
            logger.warn("Based on the provided block size {}, the provided parallelization factor {} is out of bounds", r, p);
            throw new IllegalArgumentException("Invalid p value exceeds p boundary");
        }
    }

    /**
     * Returns whether the provided parallelization factor (p value) is within boundaries. The lower bound > 0 and the
     * upper bound is calculated based on the provided block size (r value).
     *
     * @param r the block size in bytes
     * @param p the parallelization factor
     * @return true if p is within boundaries
     */
    public static boolean isPValid(int r, int p) {
        if (!isRValid(r)) {
            logger.warn("The provided block size {} must be greater than 0", r);
            throw new IllegalArgumentException("Invalid r value; must be greater than 0");
        }
        // Calculate p boundary
        double pBoundary = ((Math.pow(2, 32)) - 1) * (32.0 / (r * 128));
        return p <= pBoundary && p > 0;
    }

    /**
     * Returns whether the provided block size (r value) is a positive integer or not.
     *
     * @param r the block size in bytes
     * @return true if r is a positive integer
     */
    public static boolean isRValid(int r) {
        return r > 0;
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

        /** 1. Parse the incoming salt $s0$20101$ABCDEF... into rawSalt, N, r, p
         *  2. Generate the hash (derived key) from Scrypt using password, rawSalt, N, r, p, dkLen
         *  3. Pass derived key into cipher
         */
        String saltString = new String(salt, StandardCharsets.UTF_8);
        byte[] rawSalt = new byte[getDefaultSaltLength()];
        int n, r, p;
        if (isScryptFormattedSalt(saltString)) {
            List<Integer> params = new ArrayList<>(3);
            parseSalt(saltString, rawSalt, params);
            n = params.get(0);
            r = params.get(1);
            p = params.get(2);
        } else {
            rawSalt = salt;
            n = getN();
            r = getR();
            p = getP();
        }

        ScryptSecureHasher scryptSecureHasher = new ScryptSecureHasher(n, r, p, keyLength / 8);
        try {
            byte[] keyBytes = scryptSecureHasher.hashRaw(password.getBytes(StandardCharsets.UTF_8), rawSalt);
            SecretKey tempKey = new SecretKeySpec(keyBytes, algorithm);

            KeyedCipherProvider keyedCipherProvider = new AESKeyedCipherProvider();
            return keyedCipherProvider.getCipher(encryptionMethod, tempKey, iv, encryptMode);
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains("The salt length")) {
                throw new IllegalArgumentException("The raw salt must be greater than or equal to 8 bytes", e);
            } else {
                logger.error("Encountered an error generating the Scrypt hash", e);
                throw e;
            }
        }
    }

    /**
     * Returns the raw salt contained in the provided Scrypt salt string.
     *
     * @param scryptSalt the full Scrypt salt
     * @return the raw salt decoded from Base64
     */
    public static byte[] extractRawSaltFromScryptSalt(String scryptSalt) {
        final String[] saltComponents = scryptSalt.split("\\$");
        if (saltComponents.length < 4) {
            throw new IllegalArgumentException("Could not parse salt");
        }
        return Base64.decodeBase64(saltComponents[3]);
    }

    /**
     * Returns {@code true} if the salt string is a valid Scrypt salt string ({@code $s0$e0801$abcdefghi..{22}}).
     *
     * @param salt the salt string to evaluate
     * @return true if valid Scrypt salt
     */
    public static boolean isScryptFormattedSalt(String salt) {
        if (salt == null || salt.length() == 0) {
            throw new IllegalArgumentException("The salt cannot be empty. To generate a salt, use ScryptCipherProvider#generateSalt()");
        }
        Matcher matcher = SCRYPT_SALT_FORMAT.matcher(salt);
        return matcher.find();
    }

    private void parseSalt(String scryptSalt, byte[] rawSalt, List<Integer> params) {
        if (StringUtils.isEmpty(scryptSalt)) {
            throw new IllegalArgumentException("Cannot parse empty salt");
        }

        /** Salt format is $s0$params$saltB64 where params is encoded according to
         *  {@link Scrypt#parseParameters(String)}*/
        byte[] salt = extractRawSaltFromScryptSalt(scryptSalt);
        if (rawSalt.length < salt.length) {
            byte[] tempBytes = new byte[salt.length];
            System.arraycopy(rawSalt, 0, tempBytes, 0, rawSalt.length);
            rawSalt = tempBytes;
        }
        System.arraycopy(salt, 0, rawSalt, 0, salt.length);

        if (params == null) {
            params = new ArrayList<>(3);
        }
        final String[] saltComponents = scryptSalt.split("\\$");
        params.addAll(Scrypt.parseParameters(saltComponents[2]));
    }

    /**
     * Formats the salt into a string which Scrypt can understand containing the N, r, p values along with the salt
     * value. If the provided salt contains all values, the response will be unchanged.
     * If it only contains the raw salt value, the resulting return value will also include the current instance
     * version, and provided N, r, and p.
     * <p>
     * The salt is expected to be in the format {@code new String(saltBytes, StandardCharsets.UTF_8) => "$s0$e0801$ABCDEF...."}.
     *
     * @param salt the provided salt
     * @return the properly-formatted and complete salt
     */
    public String formatSaltForScrypt(byte[] salt) {
        String saltString = new String(salt, StandardCharsets.UTF_8);
        if (isScryptFormattedSalt(saltString)) {
            return saltString;
        } else {
            // The provided salt is not complete, so get the current instance cost parameters
            return formatSaltForScrypt(salt, getN(), getR(), getP());
        }
    }

    /**
     * Formats the salt into a string which Scrypt can understand containing the N, r, p values along with the salt
     * value. If the provided salt contains all values, the response will be unchanged.
     * If it only contains the raw salt value, the resulting return value will also include the  provided N, r, and p.
     * <p>
     * The salt is expected to be in the format {@code new String(saltBytes, StandardCharsets.UTF_8) => "$s0$e0801$ABCDEF...."}.
     *
     * @param salt the provided salt
     * @param n    the N param
     * @param r    the r param
     * @param p    the p param
     * @return the properly-formatted and complete salt
     */
    public static String formatSaltForScrypt(byte[] salt, int n, int r, int p) {
        String saltString = new String(salt, StandardCharsets.UTF_8);
        if (isScryptFormattedSalt(saltString)) {
            return saltString;
        } else {
            if (saltString.startsWith("$")) {
                logger.warn("Salt starts with $ but is not valid scrypt salt");
                Matcher matcher = MCRYPT_SALT_FORMAT.matcher(saltString);
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
    public static String translateSalt(String mcryptSalt) {
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
            return Scrypt.formatSalt(Hex.decodeHex(components[4].toCharArray()), Integer.parseInt(components[1]), Integer.parseInt(components[2]), Integer.parseInt(components[3]));
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
