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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.SecureRandom;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;

import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processors.standard.EncryptContent.Encryptor;
import org.apache.nifi.stream.io.StreamUtils;

public class PasswordBasedEncryptor implements Encryptor {

    private Cipher cipher;
    private int saltSize;
    private SecretKey secretKey;

    public static final String SECURE_RANDOM_ALGORITHM = "SHA1PRNG";
    public static final int DEFAULT_SALT_SIZE = 8;

    public PasswordBasedEncryptor(final String algorithm, final String providerName, final char[] password) {
        super();
        try {
            // initialize cipher
            this.cipher = Cipher.getInstance(algorithm, providerName);
            int algorithmBlockSize = cipher.getBlockSize();
            this.saltSize = (algorithmBlockSize > 0) ? algorithmBlockSize : DEFAULT_SALT_SIZE;

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
            secureRandom.setSeed(System.currentTimeMillis());
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

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            final byte[] salt = new byte[saltSize];
            try {
                StreamUtils.fillBuffer(in, salt);
            } catch (final EOFException e) {
                throw new ProcessException("Cannot decrypt because file size is smaller than salt size", e);
            }

            final PBEParameterSpec parameterSpec = new PBEParameterSpec(salt, 1000);
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
