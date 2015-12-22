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

import org.apache.nifi.processor.exception.ProcessException;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.generators.PKCS5S2ParametersGenerator;
import org.bouncycastle.crypto.params.KeyParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.spec.InvalidKeySpecException;

public class PBKDF2CipherProvider implements PBECipherProvider {
    private static final Logger logger = LoggerFactory.getLogger(PBKDF2CipherProvider.class);

    private final int iterationCount;
    private final MessageDigest prf;

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
     * Returns an initialized cipher for the specified algorithm. The key (and IV if necessary) are derived using PBKDF2 with the PRF and iteration count specified in the constructor.
     *
     * @param algorithm   the algorithm name
     * @param provider    the provider name
     * @param password    the secret input
     * @param encryptMode true for encrypt, false for decrypt
     * @return the initialized cipher
     * @throws Exception if there is a problem initializing the cipher
     */
    @Override
    public Cipher getCipher(String algorithm, String provider, String password, boolean encryptMode) throws Exception {
        return getCipher(algorithm, provider, password, new byte[0], encryptMode);
    }

    /**
     * Returns an initialized cipher for the specified algorithm. The key (and IV if necessary) are derived using PBKDF2 with the PRF and iteration count specified in the constructor.
     *
     * @param algorithm   the algorithm name
     * @param provider    the provider name
     * @param password    the secret input
     * @param salt        the salt
     * @param encryptMode true for encrypt, false for decrypt
     * @return the initialized cipher
     * @throws Exception if there is a problem initializing the cipher
     */
    @Override
    public Cipher getCipher(String algorithm, String provider, String password, byte[] salt, boolean encryptMode) throws Exception {
        try {
            return getInitializedCipher(algorithm, provider, password, salt, encryptMode);
        } catch (Exception e) {
            throw new ProcessException("Error initializing the cipher", e);
        }
    }

    protected Cipher getInitializedCipher(String algorithm, String provider, String password, byte[] salt, boolean encryptMode) throws NoSuchAlgorithmException, NoSuchProviderException,
            InvalidKeySpecException, NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException, UnsupportedEncodingException {
        PKCS5S2ParametersGenerator gen = new PKCS5S2ParametersGenerator(new SHA256Digest());
        gen.init(password.getBytes("UTF-8"), salt, getIterationCount());
        byte[] dk = ((KeyParameter) gen.generateDerivedParameters(getKeySize(algorithm))).getKey();
        SecretKey tempKey = new SecretKeySpec(dk, algorithm);

        Cipher cipher = Cipher.getInstance(algorithm, provider);
        cipher.init(encryptMode ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, tempKey);
        return cipher;
    }

    protected int getIterationCount() {
        return iterationCount;
    }

    protected int getKeySize(String algorithm) throws NoSuchAlgorithmException {
        KeyGenerator keyGenerator = KeyGenerator.getInstance(algorithm);
        return keyGenerator.generateKey().getEncoded().length;
    }

    private MessageDigest resolvePRF(String prf) throws NoSuchAlgorithmException {
        // TODO: May refactor to provide other PRFs
        return MessageDigest.getInstance(prf);
    }
}
