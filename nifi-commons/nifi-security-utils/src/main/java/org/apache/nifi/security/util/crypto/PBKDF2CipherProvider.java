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
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
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

public class PBKDF2CipherProvider extends RandomIVPBECipherProvider {
    private static final Logger logger = LoggerFactory.getLogger(PBKDF2CipherProvider.class);
    private static final int DEFAULT_SALT_LENGTH = 16;

    private final int iterationCount;
    private final Digest prf;

    private static final String DEFAULT_PRF = "SHA-512";
    /**
     * This can be calculated automatically using the code {@see PBKDF2CipherProviderGroovyTest#calculateMinimumIterationCount} or manually updated by a maintainer
     */
    private static final int DEFAULT_ITERATION_COUNT = 160_000;

    /**
     * Instantiates a PBKDF2 cipher provider with the default number of iterations and the default PRF. Currently 128,000 iterations and SHA-512.
     */
    public PBKDF2CipherProvider() {
        this(DEFAULT_PRF, DEFAULT_ITERATION_COUNT);
    }

    /**
     * Instantiates a PBKDF2 cipher provider with the specified number of iterations and the specified PRF. Currently supports MD5, SHA1, SHA256, SHA384, and SHA512. Unknown PRFs will default to
     * SHA512.
     *
     * @param prf            a String representation of the PRF name, e.g. "SHA256", "SHA-384" "sha_512"
     * @param iterationCount the number of iterations
     */
    public PBKDF2CipherProvider(String prf, int iterationCount) {
        this.iterationCount = iterationCount;
        if (iterationCount < DEFAULT_ITERATION_COUNT) {
            logger.warn("The provided iteration count {} is below the recommended minimum {}", iterationCount, DEFAULT_ITERATION_COUNT);
        }
        this.prf = resolvePRF(prf);
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

    protected Cipher getInitializedCipher(EncryptionMethod encryptionMethod, String password, byte[] salt, byte[] iv, int keyLength, boolean encryptMode) throws Exception {
        if (encryptionMethod == null) {
            throw new IllegalArgumentException("The encryption method must be specified");
        }

        if (!encryptionMethod.isCompatibleWithStrongKDFs()) {
            throw new IllegalArgumentException(encryptionMethod.name() + " is not compatible with PBKDF2");
        }

        String algorithm = encryptionMethod.getAlgorithm();

        final String cipherName = CipherUtility.parseCipherFromAlgorithm(algorithm);
        if (!CipherUtility.isValidKeyLength(keyLength, cipherName)) {
            throw new IllegalArgumentException(String.valueOf(keyLength) + " is not a valid key length for " + cipherName);
        }

        if (StringUtils.isEmpty(password)) {
            throw new IllegalArgumentException("Encryption with an empty password is not supported");
        }

        if (salt == null || salt.length < DEFAULT_SALT_LENGTH) {
            throw new IllegalArgumentException("The salt must be at least " + DEFAULT_SALT_LENGTH + " bytes. To generate a salt, use PBKDF2CipherProvider#generateSalt()");
        }

        PKCS5S2ParametersGenerator gen = new PKCS5S2ParametersGenerator(this.prf);
        gen.init(password.getBytes(StandardCharsets.UTF_8), salt, getIterationCount());
        byte[] dk = ((KeyParameter) gen.generateDerivedParameters(keyLength)).getKey();
        SecretKey tempKey = new SecretKeySpec(dk, algorithm);

        KeyedCipherProvider keyedCipherProvider = new AESKeyedCipherProvider();
        return keyedCipherProvider.getCipher(encryptionMethod, tempKey, iv, encryptMode);
    }

    @Override
    public byte[] generateSalt() {
        byte[] salt = new byte[DEFAULT_SALT_LENGTH];
        new SecureRandom().nextBytes(salt);
        return salt;
    }

    @Override
    public int getDefaultSaltLength() {
        return DEFAULT_SALT_LENGTH;
    }

    protected int getIterationCount() {
        return iterationCount;
    }

    protected String getPRFName() {
        if (prf != null) {
            return prf.getAlgorithmName();
        } else {
            return "No PRF enabled";
        }
    }

    private Digest resolvePRF(final String prf) {
        if (StringUtils.isEmpty(prf)) {
            throw new IllegalArgumentException("Cannot resolve empty PRF");
        }
        String formattedPRF = prf.toLowerCase().replaceAll("[\\W]+", "");
        logger.debug("Resolved PRF {} to {}", prf, formattedPRF);
        switch (formattedPRF) {
            case "md5":
                return new MD5Digest();
            case "sha1":
                return new SHA1Digest();
            case "sha384":
                return new SHA384Digest();
            case "sha256":
                return new SHA256Digest();
            case "sha512":
                return new SHA512Digest();
            default:
                logger.warn("Could not resolve PRF {}. Using default PRF {} instead", prf, DEFAULT_PRF);
                return new SHA512Digest();
        }
    }
}
