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
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.MD5Digest;
import org.bouncycastle.crypto.digests.SHA1Digest;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.digests.SHA384Digest;
import org.bouncycastle.crypto.digests.SHA512Digest;
import org.bouncycastle.crypto.generators.PKCS5S2ParametersGenerator;
import org.bouncycastle.crypto.params.KeyParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;

public class PBKDF2CipherProvider implements RandomIVPBECipherProvider {
    private static final Logger logger = LoggerFactory.getLogger(PBKDF2CipherProvider.class);

    private final int iterationCount;
    private final Digest prf;

    private static final String DEFAULT_PRF = "SHA-256";
    private static final int DEFAULT_ITERATION_COUNT = 10000;

    /**
     * Instantiates a PBKDF2 cipher provider with the default number of iterations and the default PRF. Currently 10,000 iterations and SHA-256.
     */
    public PBKDF2CipherProvider() {
        this(DEFAULT_PRF, DEFAULT_ITERATION_COUNT);
    }

    /**
     * Instantiates a PBKDF2 cipher provider with the specified number of iterations and the specified PRF. Currently supports MD5, SHA1, SHA256, SHA384, and SHA512. Unknown PRFs will default to
     * SHA256.
     *
     * @param prf            a String representation of the PRF name, e.g. "SHA256", "SHA-384" "sha_512"
     * @param iterationCount the number of iterations
     */
    public PBKDF2CipherProvider(String prf, int iterationCount) {
        this.iterationCount = iterationCount;
        try {
            this.prf = resolvePRF(prf);
        } catch (NoSuchAlgorithmException e) {
            logger.warn("Error resolving PRF for PBKDF2. No such algorithm for '{}'", prf);
            throw new IllegalArgumentException("'" + prf + "' is not a valid PRF", e);
        }
    }

    /**
     * Returns an initialized cipher for the specified algorithm. The key is derived by the KDF of the implementation. The IV is provided externally to allow for non-deterministic IVs, as IVs
     * deterministically derived from the password are a potential vulnerability and compromise semantic security. See
     * <a href="http://crypto.stackexchange.com/a/3970/12569">Ilmari Karonen's answer on Crypto Stack Exchange</a>
     *
     * @param encryptionMethod the {@link EncryptionMethod}
     * @param password         the secret input
     * @param salt             the salt
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
     * @param salt             the salt
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
            throw new IllegalArgumentException(encryptionMethod.name() + " is not compatible with PBKDF2");
        }

        // TODO: Check key length validity

        // TODO: Update documentation and check backward compatibility
        if (StringUtils.isEmpty(password)) {
            throw new IllegalArgumentException("Encryption with an empty password is not supported");
        }

        String algorithm = encryptionMethod.getAlgorithm();
        String provider = encryptionMethod.getProvider();

        // TODO: The non-PBE algorithms don't contain key sizes in the names
        final int keySize = keyLength != -1 ? keyLength : getKeySize(algorithm);

        PKCS5S2ParametersGenerator gen = new PKCS5S2ParametersGenerator(this.prf);
        gen.init(password.getBytes("UTF-8"), salt, getIterationCount());
        byte[] dk = ((KeyParameter) gen.generateDerivedParameters(keySize)).getKey();
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

    protected int getIterationCount() {
        return iterationCount;
    }

    protected int getKeySize(String algorithm) throws NoSuchAlgorithmException {
        String cipher = CipherUtility.parseCipherFromAlgorithm(algorithm);
        KeyGenerator keyGenerator = KeyGenerator.getInstance(cipher);
        return keyGenerator.generateKey().getEncoded().length * 8;
    }

    protected String getPRFName() {
        if (prf != null) {
            return prf.getAlgorithmName();
        } else {
            return "No PRF enabled";
        }
    }

    private Digest resolvePRF(String prf) throws NoSuchAlgorithmException {
        // TODO: May refactor to provide other PRFs
        prf = prf.toLowerCase().replaceAll("[\\W]+", "");
        switch (prf) {
            case "md5":
                return new MD5Digest();
            case "sha1":
                return new SHA1Digest();
            case "sha384":
                return new SHA384Digest();
            case "sha512":
                return new SHA512Digest();
            case "sha256":
            default:
                return new SHA256Digest();
        }
    }
}
