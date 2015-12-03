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
package org.apache.nifi.processors.standard.util;

import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processors.standard.EncryptContent.Encryptor;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.apache.nifi.stream.io.StreamUtils;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;

public class PasswordBasedEncryptor implements Encryptor {

    private Cipher cipher;
    private int saltSize;
    private SecretKey secretKey;
    private KeyDerivationFunction kdf;

    public static final String SECURE_RANDOM_ALGORITHM = "SHA1PRNG";
    public static final int DEFAULT_SALT_SIZE = 8;
    // TODO: Eventually KDF-specific values should be refactored into injectable interface impls
    public static final int OPENSSL_EVP_HEADER_SIZE = 8;
    public static final int OPENSSL_EVP_SALT_SIZE = 8;
    public static final String OPENSSL_EVP_HEADER_MARKER = "Salted__";

    public PasswordBasedEncryptor(final String algorithm, final String providerName, final char[] password, KeyDerivationFunction kdf) {
        super();
        try {
            // initialize cipher
            this.cipher = Cipher.getInstance(algorithm, providerName);
            this.kdf = kdf;

            if (KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY.equals(kdf)) {
                this.saltSize = OPENSSL_EVP_SALT_SIZE;
            } else {
                int algorithmBlockSize = cipher.getBlockSize();
                this.saltSize = (algorithmBlockSize > 0) ? algorithmBlockSize : DEFAULT_SALT_SIZE;
            }

            // initialize SecretKey from password
            PBEKeySpec pbeKeySpec = new PBEKeySpec(password);
            SecretKeyFactory factory = SecretKeyFactory.getInstance(algorithm, providerName);
            this.secretKey = factory.generateSecret(pbeKeySpec);
        } catch (Exception e) {
            throw new ProcessException(e);
        }
    }

    @Override
    public StreamCallback getEncryptionCallback() throws ProcessException {
        try {
            byte[] salt = new byte[saltSize];
            SecureRandom secureRandom = SecureRandom.getInstance(SECURE_RANDOM_ALGORITHM);
            secureRandom.nextBytes(salt);
            return new EncryptCallback(salt);
        } catch (Exception e) {
            throw new ProcessException(e);
        }
    }

    @Override
    public StreamCallback getDecryptionCallback() throws ProcessException {
        return new DecryptCallback();
    }

    private class DecryptCallback implements StreamCallback {

        public DecryptCallback() {
        }

        private void initCipher(final byte[] salt) {
            PBEParameterSpec saltParameterSpec;

            // TODO: Handle other KDFs
            // If the KDF is OpenSSL, derive the OpenSSL PBE Parameters
            if (KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY.equals(kdf)) {
                saltParameterSpec = new PBEParameterSpec(salt, 0);
            } else {
                // Else use the legacy KDF
                saltParameterSpec = new PBEParameterSpec(salt, 1000);
            }

            try {
                cipher.init(Cipher.DECRYPT_MODE, secretKey, saltParameterSpec);
            } catch (final Exception e) {
                throw new ProcessException(e);
            }
        }

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            byte[] salt = new byte[saltSize];

            // The legacy default value
            int kdfIterations = 1000;
            try {
                // If the KDF is OpenSSL, try to read the salt from the input stream
                if (KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY.equals(kdf)) {
                    // Set the iteration count to 0
                    kdfIterations = 0;

                    // Try to read the header and salt from the input
                    byte[] header = new byte[PasswordBasedEncryptor.OPENSSL_EVP_HEADER_SIZE];

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
                }

                StreamUtils.fillBuffer(in, salt);
            } catch (final EOFException e) {
                throw new ProcessException("Cannot decrypt because file size is smaller than salt size", e);
            }

            final PBEParameterSpec parameterSpec = new PBEParameterSpec(salt, kdfIterations);
            try {
                cipher.init(Cipher.DECRYPT_MODE, secretKey, parameterSpec);
            } catch (final Exception e) {
                throw new ProcessException(e);
            }

            final byte[] buffer = new byte[65536];
            int len;
            while ((len = in.read(buffer)) > 0) {
                final byte[] decryptedBytes = cipher.update(buffer, 0, len);
                if (decryptedBytes != null) {
                    out.write(decryptedBytes);
                }
            }

            try {
                out.write(cipher.doFinal());
            } catch (final Exception e) {
                throw new ProcessException(e);
            }
        }
    }

    private class EncryptCallback implements StreamCallback {

        private final byte[] salt;

        public EncryptCallback(final byte[] salt) {
            this.salt = salt;
        }

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            final PBEParameterSpec parameterSpec = new PBEParameterSpec(salt, 1000);
            try {
                cipher.init(Cipher.ENCRYPT_MODE, secretKey, parameterSpec);
            } catch (final Exception e) {
                throw new ProcessException(e);
            }

            out.write(salt);

            final byte[] buffer = new byte[65536];
            int len;
            while ((len = in.read(buffer)) > 0) {
                final byte[] encryptedBytes = cipher.update(buffer, 0, len);
                if (encryptedBytes != null) {
                    out.write(encryptedBytes);
                }
            }

            try {
                out.write(cipher.doFinal());
            } catch (final IllegalBlockSizeException | BadPaddingException e) {
                throw new ProcessException(e);
            }
        }
    }
}
