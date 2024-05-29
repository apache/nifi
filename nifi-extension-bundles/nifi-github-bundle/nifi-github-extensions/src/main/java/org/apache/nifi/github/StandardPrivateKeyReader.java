/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.nifi.github;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.interfaces.RSAPrivateKey;
import java.util.Base64;
import java.util.Objects;

/**
 * Standard implementation of Private Key Reader supporting RSA PKCS #1 encoding
 */
class StandardPrivateKeyReader implements PrivateKeyReader {

    private static final Base64.Decoder DECODER = Base64.getDecoder();

    private static final String RSA_ALGORITHM = "RSA";

    private static final String PKCS1_FORMAT = "PKCS#1";

    private static final String PEM_BOUNDARY_PREFIX = "-----";

    /**
     * Read RSA Private Key from PEM-encoded PKCS #1 string
     *
     * @param inputPrivateKey PEM-encoded string
     * @return RSA Private Key
     * @throws GeneralSecurityException Thrown on failures to parse private key
     */
    @Override
    public PrivateKey readPrivateKey(final String inputPrivateKey) throws GeneralSecurityException {
        Objects.requireNonNull(inputPrivateKey, "Private Key required");

        final byte[] decoded = getDecoded(inputPrivateKey);

        final PrivateKey encodedPrivateKey = new PKCS1EncodedPrivateKey(decoded);
        final KeyFactory keyFactory = KeyFactory.getInstance(RSA_ALGORITHM);
        final Key translatedKey = keyFactory.translateKey(encodedPrivateKey);
        if (translatedKey instanceof RSAPrivateKey) {
            return (RSAPrivateKey) translatedKey;
        } else {
            throw new InvalidKeyException("Failed to parse encoded RSA Private Key: unsupported class [%s]".formatted(translatedKey.getClass()));
        }
    }

    private byte[] getDecoded(final String inputPrivateKey) throws GeneralSecurityException {
        try (BufferedReader bufferedReader = new BufferedReader(new StringReader(inputPrivateKey))) {
            final StringBuilder encodedBuilder = new StringBuilder();

            String line = bufferedReader.readLine();
            while (line != null) {
                if (!line.startsWith(PEM_BOUNDARY_PREFIX)) {
                    encodedBuilder.append(line);
                }

                line = bufferedReader.readLine();
            }

            final String encoded = encodedBuilder.toString();
            return DECODER.decode(encoded);
        } catch (final IOException e) {
            throw new InvalidKeyException("Failed to read Private Key", e);
        }
    }

    private static class PKCS1EncodedPrivateKey implements PrivateKey {

        private final byte[] encoded;

        private PKCS1EncodedPrivateKey(final byte[] encoded) {
            this.encoded = encoded;
        }

        @Override
        public String getAlgorithm() {
            return RSA_ALGORITHM;
        }

        @Override
        public String getFormat() {
            return PKCS1_FORMAT;
        }

        @Override
        public byte[] getEncoded() {
            return encoded.clone();
        }
    }
}
