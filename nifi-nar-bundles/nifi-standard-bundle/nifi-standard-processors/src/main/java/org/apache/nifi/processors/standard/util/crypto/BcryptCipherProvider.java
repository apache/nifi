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
package org.apache.nifi.processors.standard.util.crypto;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.security.util.EncryptionMethod;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BcryptCipherProvider implements RandomIVPBECipherProvider {
    private static final Logger logger = LoggerFactory.getLogger(BcryptCipherProvider.class);

    private final int workFactor;
    // TODO: Change default work factor to 12
    private static final int DEFAULT_WORK_FACTOR = 10;

    private static final Pattern BCRYPT_SALT_FORMAT = Pattern.compile("^\\$\\d\\w\\$\\d{2}\\$");

    /**
     * Instantiates a Bcrypt cipher provider with the default work factor 10 (2^10 key expansion rounds).
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
    }

    /**
     * Returns an initialized cipher for the specified algorithm. The key is derived by the KDF of the implementation. The IV is provided externally to allow for non-deterministic IVs, as IVs
     * deterministically derived from the password are a potential vulnerability and compromise semantic security. See
     * <a href="http://crypto.stackexchange.com/a/3970/12569">Ilmari Karonen's answer on Crypto Stack Exchange</a>
     *
     * @param encryptionMethod the {@link EncryptionMethod}
     * @param password         the secret input
     * @param salt             the complete salt (e.g. {@code "$2a$10$gUVbkVzp79H8YaCOsCVZNu".getBytes("UTF-8")})
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

    /**
     * Returns an initialized cipher for the specified algorithm. The key (and IV if necessary) are derived by the KDF of the implementation.
     *
     * @param encryptionMethod the {@link EncryptionMethod}
     * @param password         the secret input
     * @param keyLength        the desired key length in bits
     * @param encryptMode      true for encrypt, false for decrypt
     * @return the initialized cipher
     * @throws Exception if there is a problem initializing the cipher
     */
    @Override
    public Cipher getCipher(EncryptionMethod encryptionMethod, String password, int keyLength, boolean encryptMode) throws Exception {
        return getCipher(encryptionMethod, password, new byte[0], new byte[0], keyLength, encryptMode);
    }

    /**
     * Returns an initialized cipher for the specified algorithm. The key (and IV if necessary) are derived by the KDF of the implementation.
     *
     * @param encryptionMethod the {@link EncryptionMethod}
     * @param password         the secret input
     * @param salt             the complete salt (e.g. {@code "$2a$10$gUVbkVzp79H8YaCOsCVZNu".getBytes("UTF-8")})
     * @param keyLength        the desired key length in bits
     * @param encryptMode      true for encrypt, false for decrypt
     * @return the initialized cipher
     * @throws Exception if there is a problem initializing the cipher
     */
    @Override
    public Cipher getCipher(EncryptionMethod encryptionMethod, String password, byte[] salt, int keyLength, boolean encryptMode) throws Exception {
        return getCipher(encryptionMethod, password, salt, new byte[0], keyLength, encryptMode);
    }

    protected Cipher getInitializedCipher(EncryptionMethod encryptionMethod, String password, byte[] salt, byte[] iv, int keyLength,
                                          boolean encryptMode) throws NoSuchAlgorithmException, NoSuchProviderException,
            InvalidKeySpecException, NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException, UnsupportedEncodingException {
        if (encryptionMethod == null) {
            throw new IllegalArgumentException("The encryption method must be specified");
        }
        if (!encryptionMethod.isCompatibleWithStrongKDFs()) {
            throw new IllegalArgumentException(encryptionMethod.name() + " is not compatible with Bcrypt");
        }

        // TODO: Update documentation and check backward compatibility
        if (StringUtils.isEmpty(password)) {
            throw new IllegalArgumentException("Encryption with an empty password is not supported");
        }

        // TODO: Check key length validity

        String algorithm = encryptionMethod.getAlgorithm();
        String provider = encryptionMethod.getProvider();

        // TODO: Ignore provided salt?
        String bcryptSalt = formatSaltForBcrypt(salt);

        logger.warn("REMOVE Salt: {}", bcryptSalt);
        String hash = BCrypt.hashpw(password, bcryptSalt);
        logger.warn("REMOVE Hash: {}", hash);
        MessageDigest digest = MessageDigest.getInstance("SHA-512", provider);
        byte[] dk = digest.digest(hash.getBytes("UTF-8"));

        dk = Arrays.copyOf(dk, keyLength / 8);
        SecretKey tempKey = new SecretKeySpec(dk, algorithm);

        // TODO: May be able to refactor below into KeyedCipherProvider
        Cipher cipher = Cipher.getInstance(algorithm, provider);

        int ivLength = cipher.getBlockSize();
        // If an IV was not provided already, generate a random IV and inject it in the cipher
        if (iv.length == 0) {
            if (encryptMode) {
                iv = new byte[ivLength];
                new SecureRandom().nextBytes(iv);
            } else {
                // Can't decrypt without an IV
                throw new IllegalArgumentException("Cannot decrypt without an IV");
            }
        }
        cipher.init(encryptMode ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, tempKey, new IvParameterSpec(iv));

        return cipher;
    }

    private String formatSaltForBcrypt(byte[] salt) throws UnsupportedEncodingException {
        if (salt == null || salt.length == 0) {
            throw new IllegalArgumentException("The salt cannot be empty. To generate a salt, use BcryptCipherProvider#generateSalt()");
        }

        String rawSalt = new String(salt, "UTF-8");
        Matcher matcher = BCRYPT_SALT_FORMAT.matcher(rawSalt);

        if (matcher.find()) {
            return rawSalt;
        } else {
            String base64EncodedSalt = Base64.getEncoder().withoutPadding().encodeToString(salt);
            return "$2a$" + StringUtils.leftPad(String.valueOf(workFactor), 2, "0") + "$" + base64EncodedSalt;
        }
    }

    public String generateSalt() {
        return BCrypt.gensalt(workFactor);
    }

    protected int getWorkFactor() {
        return workFactor;
    }
}
