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
package org.apache.nifi.key.service.reader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;

/**
 * Standard implementation of Private Key Reader supporting PEM encoded PKCS8 Private Keys using Java Cryptography Architecture components
 */
public class StandardPrivateKeyReader implements PrivateKeyReader {
    static final String KEY_PASSWORD_REQUIRED = "Key Password required for encrypted Private Key";

    private static final String PRIVATE_KEY_TYPE = "PRIVATE KEY";

    private static final String ENCRYPTED_PRIVATE_KEY_TYPE = "ENCRYPTED PRIVATE KEY";

    private static final String BEGIN_BOUNDARY_PREFIX = "-----BEGIN ";

    private static final String BOUNDARY_SUFFIX = "-----";

    private static final String END_BOUNDARY_FORMAT = "-----END %s-----";

    private static final char[] EMPTY_PASSWORD = new char[0];

    private static final Map<String, String> KEY_ALGORITHMS = Map.of(
            "1.2.840.113549.1.1.1", "RSA",
            "1.2.840.113549.1.1.10", "RSASSA-PSS",
            "1.2.840.10045.2.1", "EC",
            "1.2.840.10040.4.1", "DSA",
            "1.3.101.110", "X25519",
            "1.3.101.111", "X448",
            "1.3.101.112", "Ed25519",
            "1.3.101.113", "Ed448"
    );

    private static final Base64.Decoder DECODER = Base64.getDecoder();

    private static final EncryptedPrivateKeyDecryptor ENCRYPTED_PRIVATE_KEY_DECRYPTOR = new EncryptedPrivateKeyDecryptor();

    /**
     * Read Private Key from the first PEM Private Key object in the stream with optional password for encrypted keys
     *
     * @param inputStream Key stream
     * @param keyPassword Password for encrypted keys or empty when not encrypted
     * @return Private Key
     */
    @Override
    public PrivateKey readPrivateKey(final InputStream inputStream, final char[] keyPassword) {
        Objects.requireNonNull(inputStream, "Input Stream required");
        final char[] password = keyPassword == null ? EMPTY_PASSWORD : keyPassword;

        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.US_ASCII))) {
            final String type = readPrivateKeyType(reader);
            return switch (type) {
                case PRIVATE_KEY_TYPE -> getPrivateKey(readContent(reader, type));
                case ENCRYPTED_PRIVATE_KEY_TYPE -> getEncryptedPrivateKey(readContent(reader, type), password);
                default -> throw new IllegalArgumentException("Private Key [%s] not supported".formatted(type));
            };
        } catch (final IOException e) {
            throw new UncheckedIOException("Read Private Key stream failed", e);
        }
    }

    private String readPrivateKeyType(final BufferedReader reader) throws IOException {
        String line = reader.readLine();
        while (line != null) {
            final String boundary = line.trim();
            if (boundary.startsWith(BEGIN_BOUNDARY_PREFIX) && boundary.endsWith(BOUNDARY_SUFFIX)) {
                final String type = boundary.substring(BEGIN_BOUNDARY_PREFIX.length(), boundary.length() - BOUNDARY_SUFFIX.length());
                if (type.endsWith(PRIVATE_KEY_TYPE)) {
                    return type;
                }
            }

            line = reader.readLine();
        }

        throw new PrivateKeyException("PEM Private Key not found");
    }

    private byte[] readContent(final BufferedReader reader, final String type) throws IOException {
        final String endBoundary = END_BOUNDARY_FORMAT.formatted(type);
        final StringBuilder encoded = new StringBuilder();

        String line = reader.readLine();
        while (line != null) {
            final String trimmedLine = line.trim();
            if (endBoundary.equals(trimmedLine)) {
                return decode(type, encoded.toString());
            }

            encoded.append(trimmedLine);
            line = reader.readLine();
        }

        throw new PrivateKeyException("PEM boundary [%s] not found".formatted(endBoundary));
    }

    private byte[] decode(final String type, final String encoded) {
        try {
            return DECODER.decode(encoded);
        } catch (final IllegalArgumentException e) {
            throw new PrivateKeyException("PEM [%s] Base64 decoding failed".formatted(type), e);
        }
    }

    private PrivateKey getEncryptedPrivateKey(final byte[] encryptedPrivateKeyInfo, final char[] password) {
        try {
            final byte[] privateKeyInfo = ENCRYPTED_PRIVATE_KEY_DECRYPTOR.decrypt(encryptedPrivateKeyInfo, password);
            return getPrivateKey(privateKeyInfo);
        } catch (final PrivateKeyException e) {
            if (password.length == 0) {
                throw new PrivateKeyException(KEY_PASSWORD_REQUIRED, e);
            }

            throw e;
        }
    }

    private PrivateKey getPrivateKey(final byte[] privateKeyInfo) {
        final String algorithmIdentifier = DerElement.read(privateKeyInfo).getElement(1).getElement(0).getObjectIdentifier();
        final String keyAlgorithm = KEY_ALGORITHMS.getOrDefault(algorithmIdentifier, algorithmIdentifier);
        try {
            final KeyFactory keyFactory = KeyFactory.getInstance(keyAlgorithm);
            return keyFactory.generatePrivate(new PKCS8EncodedKeySpec(privateKeyInfo));
        } catch (final NoSuchAlgorithmException e) {
            throw new PrivateKeyException("Private Key Algorithm [%s] not supported".formatted(keyAlgorithm), e);
        } catch (final InvalidKeySpecException e) {
            throw new PrivateKeyException("Private Key Algorithm [%s] parsing failed".formatted(keyAlgorithm), e);
        }
    }
}
