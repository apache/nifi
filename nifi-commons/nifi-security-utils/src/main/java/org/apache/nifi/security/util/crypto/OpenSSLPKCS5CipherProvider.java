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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.stream.io.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSSLPKCS5CipherProvider implements PBECipherProvider {
    private static final Logger logger = LoggerFactory.getLogger(OpenSSLPKCS5CipherProvider.class);

    // Legacy magic number value
    private static final int ITERATION_COUNT = 0;
    private static final int DEFAULT_SALT_LENGTH = 8;
    private static final byte[] EMPTY_SALT = new byte[8];

    private static final String OPENSSL_EVP_HEADER_MARKER = "Salted__";
    private static final int OPENSSL_EVP_HEADER_SIZE = 8;

    /**
     * Returns an initialized cipher for the specified algorithm. The key (and IV if necessary) are derived using the
     * <a href="https://www.openssl.org/docs/manmaster/crypto/EVP_BytesToKey.html">OpenSSL EVP_BytesToKey proprietary KDF</a> [essentially {@code MD5(password || salt) }].
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
            return getInitializedCipher(encryptionMethod, password, salt, encryptMode);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new ProcessException("Error initializing the cipher", e);
        }
    }

    /**
     * Convenience method without key length parameter. See {@link OpenSSLPKCS5CipherProvider#getCipher(EncryptionMethod, String, int, boolean)}
     *
     * @param encryptionMethod the {@link EncryptionMethod}
     * @param password         the secret input
     * @param encryptMode      true for encrypt, false for decrypt
     * @return the initialized cipher
     * @throws Exception if there is a problem initializing the cipher
     */
    public Cipher getCipher(EncryptionMethod encryptionMethod, String password, boolean encryptMode) throws Exception {
        return getCipher(encryptionMethod, password, new byte[0], -1, encryptMode);
    }

    /**
     * Convenience method without key length parameter. See {@link OpenSSLPKCS5CipherProvider#getCipher(EncryptionMethod, String, byte[], int, boolean)}
     *
     * @param encryptionMethod the {@link EncryptionMethod}
     * @param password         the secret input
     * @param salt             the salt
     * @param encryptMode      true for encrypt, false for decrypt
     * @return the initialized cipher
     * @throws Exception if there is a problem initializing the cipher
     */
    public Cipher getCipher(EncryptionMethod encryptionMethod, String password, byte[] salt, boolean encryptMode) throws Exception {
        return getCipher(encryptionMethod, password, salt, -1, encryptMode);
    }

    protected Cipher getInitializedCipher(EncryptionMethod encryptionMethod, String password, byte[] salt, boolean encryptMode)
            throws NoSuchAlgorithmException, NoSuchProviderException, InvalidKeySpecException, NoSuchPaddingException, InvalidKeyException,
            InvalidAlgorithmParameterException {
        if (encryptionMethod == null) {
            throw new IllegalArgumentException("The encryption method must be specified");
        }

        if (StringUtils.isEmpty(password)) {
            throw new IllegalArgumentException("Encryption with an empty password is not supported");
        }

        validateSalt(encryptionMethod, salt);

        String algorithm = encryptionMethod.getAlgorithm();
        String provider = encryptionMethod.getProvider();

        // Initialize secret key from password
        final PBEKeySpec pbeKeySpec = new PBEKeySpec(password.toCharArray());
        final SecretKeyFactory factory = SecretKeyFactory.getInstance(algorithm, provider);
        SecretKey tempKey = factory.generateSecret(pbeKeySpec);

        final PBEParameterSpec parameterSpec = new PBEParameterSpec(salt, getIterationCount());
        Cipher cipher = Cipher.getInstance(algorithm, provider);
        cipher.init(encryptMode ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, tempKey, parameterSpec);
        return cipher;
    }

    protected void validateSalt(EncryptionMethod encryptionMethod, byte[] salt) {
        if (salt.length != DEFAULT_SALT_LENGTH && salt.length != 0) {
            // This does not enforce ASCII encoding, just length
            throw new IllegalArgumentException("Salt must be 8 bytes US-ASCII encoded or empty");
        }
    }

    protected int getIterationCount() {
        return ITERATION_COUNT;
    }

    @Override
    public byte[] generateSalt() {
        byte[] salt = new byte[getDefaultSaltLength()];
        new SecureRandom().nextBytes(salt);
        return salt;
    }

    @Override
    public int getDefaultSaltLength() {
        return DEFAULT_SALT_LENGTH;
    }

    /**
     * Returns the salt provided as part of the cipher stream, or throws an exception if one cannot be detected.
     *
     * @param in the cipher InputStream
     * @return the salt
     */
    @Override
    public byte[] readSalt(InputStream in) throws IOException {
        if (in == null) {
            throw new IllegalArgumentException("Cannot read salt from null InputStream");
        }

        // The header and salt format is "Salted__salt x8b" in ASCII
        byte[] salt = new byte[DEFAULT_SALT_LENGTH];

        // Try to read the header and salt from the input
        byte[] header = new byte[OPENSSL_EVP_HEADER_SIZE];

        // Mark the stream in case there is no salt
        in.mark(OPENSSL_EVP_HEADER_SIZE + 1);
        StreamUtils.fillBuffer(in, header);

        final byte[] headerMarkerBytes = OPENSSL_EVP_HEADER_MARKER.getBytes(StandardCharsets.US_ASCII);

        if (!Arrays.equals(headerMarkerBytes, header)) {
            // No salt present
            salt = new byte[0];
            // Reset the stream because we skipped 8 bytes of cipher text
            in.reset();
        }

        StreamUtils.fillBuffer(in, salt);
        return salt;
    }

    @Override
    public void writeSalt(byte[] salt, OutputStream out) throws IOException {
        if (out == null) {
            throw new IllegalArgumentException("Cannot write salt to null OutputStream");
        }

        out.write(OPENSSL_EVP_HEADER_MARKER.getBytes(StandardCharsets.US_ASCII));
        out.write(salt);
    }
}
