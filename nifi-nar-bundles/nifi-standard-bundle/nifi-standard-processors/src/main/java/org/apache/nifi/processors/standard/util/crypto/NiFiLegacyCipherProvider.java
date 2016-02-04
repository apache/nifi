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
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.stream.io.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Provides a cipher initialized with the original NiFi key derivation process for password-based encryption (MD5 @ 1000 iterations). This is not a secure
 * {@link org.apache.nifi.security.util.KeyDerivationFunction} (KDF) and should no longer be used.
 * It is provided only for backward-compatibility with legacy data. A strong KDF should be selected for any future use.
 *
 * @see BcryptCipherProvider
 * @see ScryptCipherProvider
 * @see PBKDF2CipherProvider
 */
@Deprecated
public class NiFiLegacyCipherProvider extends OpenSSLPKCS5CipherProvider implements PBECipherProvider {
    private static final Logger logger = LoggerFactory.getLogger(NiFiLegacyCipherProvider.class);

    // Legacy magic number value
    private static final int ITERATION_COUNT = 1000;

    /**
     * Returns an initialized cipher for the specified algorithm. The key (and IV if necessary) are derived using the NiFi legacy code, based on @see org.apache.nifi.processors.standard.util.crypto
     * .OpenSSLPKCS5CipherProvider#getCipher(java.lang.String, java.lang.String, java.lang.String, boolean) [essentially {@code MD5(password || salt) * 1000 }].
     *
     * @param encryptionMethod the {@link EncryptionMethod}
     * @param password         the secret input
     * @param keyLength        the desired key length in bits (ignored because OpenSSL ciphers provide key length in algorithm name)
     * @param encryptMode      true for encrypt, false for decrypt
     * @return the initialized cipher
     * @throws Exception if there is a problem initializing the cipher
     */
    @Override
    public Cipher getCipher(EncryptionMethod encryptionMethod, String password, int keyLength, boolean encryptMode) throws Exception {
        return getCipher(encryptionMethod, password, new byte[0], keyLength, encryptMode);
    }

    /**
     * Returns an initialized cipher for the specified algorithm. The key (and IV if necessary) are derived using the NiFi legacy code, based on @see org.apache.nifi.processors.standard.util.crypto
     * .OpenSSLPKCS5CipherProvider#getCipher(java.lang.String, java.lang.String, java.lang.String, byte[], boolean) [essentially {@code MD5(password || salt) * 1000 }].
     *
     * @param encryptionMethod the {@link EncryptionMethod}
     * @param password         the secret input
     * @param salt             the salt
     * @param keyLength        the desired key length in bits (ignored because OpenSSL ciphers provide key length in algorithm name)
     * @param encryptMode      true for encrypt, false for decrypt
     * @return the initialized cipher
     * @throws Exception if there is a problem initializing the cipher
     */
    @Override
    public Cipher getCipher(EncryptionMethod encryptionMethod, String password, byte[] salt, int keyLength, boolean encryptMode) throws Exception {
        try {
            // This method is defined in the OpenSSL implementation and just uses a locally-overridden iteration count
            return getInitializedCipher(encryptionMethod, password, salt, encryptMode);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new ProcessException("Error initializing the cipher", e);
        }
    }

    @Override
    public byte[] readSalt(InputStream in) throws IOException, ProcessException {
        if (in == null) {
            throw new IllegalArgumentException("Cannot read salt from null InputStream");
        }

        // The first 16 bytes of the input stream are the salt
        if (in.available() < getDefaultSaltLength()) {
            throw new ProcessException("The cipher stream is too small to contain the salt");
        }
        byte[] salt = new byte[getDefaultSaltLength()];
        StreamUtils.fillBuffer(in, salt);
        return salt;
    }

    @Override
    public void writeSalt(byte[] salt, OutputStream out) throws IOException {
        if (out == null) {
            throw new IllegalArgumentException("Cannot write salt to null OutputStream");
        }
        out.write(salt);
    }

    protected int getIterationCount() {
        return ITERATION_COUNT;
    }
}
