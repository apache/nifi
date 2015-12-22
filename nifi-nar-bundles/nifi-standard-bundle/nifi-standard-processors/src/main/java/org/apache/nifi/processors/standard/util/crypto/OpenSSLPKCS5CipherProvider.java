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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.spec.InvalidKeySpecException;

public class OpenSSLPKCS5CipherProvider implements PBECipherProvider {
    private static final Logger logger = LoggerFactory.getLogger(OpenSSLPKCS5CipherProvider.class);

    // Legacy magic number value
    private static final int ITERATION_COUNT = 0;

    /**
     * Returns an initialized cipher for the specified algorithm. The key (and IV if necessary) are derived using the <a href="https://www.openssl.org/docs/manmaster/crypto/EVP_BytesToKey.html">OpenSSL EVP_BytesToKey proprietary KDF</a> [essentially {@code MD5(password || salt) }].
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
     * Returns an initialized cipher for the specified algorithm. The key (and IV if necessary) are derived using the <a href="https://www.openssl.org/docs/manmaster/crypto/EVP_BytesToKey.html">OpenSSL EVP_BytesToKey proprietary KDF</a> [essentially {@code MD5(password || salt) }].
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

    protected Cipher getInitializedCipher(String algorithm, String provider, String password, byte[] salt, boolean encryptMode) throws NoSuchAlgorithmException, NoSuchProviderException, InvalidKeySpecException, NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException {
        // Initialize secret key from password
        final PBEKeySpec pbeKeySpec = new PBEKeySpec(password.toCharArray());
        final SecretKeyFactory factory = SecretKeyFactory.getInstance(algorithm, provider);
        SecretKey tempKey = factory.generateSecret(pbeKeySpec);

        final PBEParameterSpec parameterSpec = new PBEParameterSpec(salt, getIterationCount());
        Cipher cipher = Cipher.getInstance(algorithm, provider);
        cipher.init(encryptMode ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, tempKey, parameterSpec);
        return cipher;
    }

    protected int getIterationCount() {
        return ITERATION_COUNT;
    }
}
