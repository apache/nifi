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
package org.apache.nifi.security.ssl;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.spec.ECGenParameterSpec;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StandardPemPrivateKeyReaderTest {

    private static final String RSA_ALGORITHM = "RSA";

    private static final int RSA_KEY_SIZE = 3072;

    private static final String ED25519_ALGORITHM = "Ed25519";

    private static final String EDDSA_ALGORITHM = "EdDSA";

    private static final String EC_ALGORITHM = "EC";

    private static final String NIST_CURVE_P_256 = "secp256r1";

    private static final String NIST_CURVE_P_384 = "secp384r1";

    private static final String NIST_CURVE_P_512 = "secp521r1";

    private static final String CERTIFICATE_HEADER = "-----BEGIN CERTIFICATE-----";

    private static final String LINE_LENGTH_PATTERN = "(?<=\\G.{64})";

    private static final char LINE_FEED = 10;

    private static final int PKCS8_RSA_DER_HEADER_LENGTH = 26;

    private static final Base64.Encoder encoder = Base64.getEncoder();

    private static String PKCS1_RSA_PRIVATE_KEY_ENCODED;

    private static String PKCS8_ED25519_PRIVATE_KEY_ENCODED;

    private final StandardPemPrivateKeyReader reader = new StandardPemPrivateKeyReader();

    @BeforeAll
    static void setPrivateKey() throws Exception {
        final KeyPairGenerator rsaKeyPairGenerator = KeyPairGenerator.getInstance(RSA_ALGORITHM);
        rsaKeyPairGenerator.initialize(RSA_KEY_SIZE);
        final KeyPair rsaKeyPair = rsaKeyPairGenerator.generateKeyPair();
        final PrivateKey rsaPrivateKey = rsaKeyPair.getPrivate();
        final byte[] rsaPrivateKeyEncoded = rsaPrivateKey.getEncoded();

        PKCS1_RSA_PRIVATE_KEY_ENCODED = getRsaPrivateKeyPemEncoded(rsaPrivateKeyEncoded);

        final KeyPairGenerator ed25519KeyPairGenerator = KeyPairGenerator.getInstance(ED25519_ALGORITHM);
        final KeyPair ed25519KeyPair = ed25519KeyPairGenerator.generateKeyPair();
        final PrivateKey ed25519PrivateKey = ed25519KeyPair.getPrivate();
        final byte[] ed25519PrivateKeyEncoded = ed25519PrivateKey.getEncoded();

        PKCS8_ED25519_PRIVATE_KEY_ENCODED = getPrivateKeyPemEncoded(ed25519PrivateKeyEncoded);
    }

    @Test
    void testReadPrivateKeyHeaderException() {
        final InputStream inputStream = new ByteArrayInputStream(CERTIFICATE_HEADER.getBytes(StandardCharsets.US_ASCII));

        assertThrows(ReadEntityException.class, () -> reader.readPrivateKey(inputStream));
    }

    @Test
    void testReadPrivateKeyPkcs8NotSupportedException() {
        final String privateKeyEncoded = getPrivateKeyPemEncoded(String.class.getName().getBytes(StandardCharsets.US_ASCII));

        final InputStream inputStream = new ByteArrayInputStream(privateKeyEncoded.getBytes(StandardCharsets.US_ASCII));

        final ReadEntityException exception = assertThrows(ReadEntityException.class, () -> reader.readPrivateKey(inputStream));
        assertInstanceOf(UnrecoverableKeyException.class, exception.getCause());
    }

    @Test
    void testReadPrivateKeyPkcs1Rsa() {
        final InputStream inputStream = new ByteArrayInputStream(PKCS1_RSA_PRIVATE_KEY_ENCODED.getBytes(StandardCharsets.US_ASCII));

        final PrivateKey privateKey = reader.readPrivateKey(inputStream);

        assertNotNull(privateKey);
        assertEquals(RSA_ALGORITHM, privateKey.getAlgorithm());
    }

    @ParameterizedTest
    @ValueSource(ints = {2048, 3072})
    void testReadPrivateKeyPkcs8Rsa(final int keySize) throws GeneralSecurityException {
        final String privateKeyEncoded = getRsaPrivateKeyEncoded(keySize);
        final InputStream inputStream = new ByteArrayInputStream(privateKeyEncoded.getBytes(StandardCharsets.US_ASCII));

        final PrivateKey privateKey = reader.readPrivateKey(inputStream);

        assertNotNull(privateKey);
        assertEquals(RSA_ALGORITHM, privateKey.getAlgorithm());
    }

    @Test
    void testReadPrivateKeyPkcs8Ed25519() {
        final InputStream inputStream = new ByteArrayInputStream(PKCS8_ED25519_PRIVATE_KEY_ENCODED.getBytes(StandardCharsets.US_ASCII));

        final PrivateKey privateKey = reader.readPrivateKey(inputStream);

        assertNotNull(privateKey);
        assertEquals(EDDSA_ALGORITHM, privateKey.getAlgorithm());
    }

    @ParameterizedTest
    @ValueSource(strings = {NIST_CURVE_P_256, NIST_CURVE_P_384, NIST_CURVE_P_512})
    void testReadPrivateKeyPkcs8EllipticCurve(final String curveName) throws GeneralSecurityException {
        final String privateKeyEncoded = getEllipticCurvePrivateKeyEncoded(curveName);
        final InputStream inputStream = new ByteArrayInputStream(privateKeyEncoded.getBytes(StandardCharsets.US_ASCII));

        final PrivateKey privateKey = reader.readPrivateKey(inputStream);

        assertNotNull(privateKey);
        assertEquals(EC_ALGORITHM, privateKey.getAlgorithm());
    }

    static String getRsaPrivateKeyEncoded(final int keySize) throws GeneralSecurityException {
        final KeyPairGenerator rsaKeyPairGenerator = KeyPairGenerator.getInstance(RSA_ALGORITHM);
        rsaKeyPairGenerator.initialize(keySize);
        final KeyPair rsaKeyPair = rsaKeyPairGenerator.generateKeyPair();
        final PrivateKey rsaPrivateKey = rsaKeyPair.getPrivate();
        final byte[] rsaPrivateKeyEncoded = rsaPrivateKey.getEncoded();
        return getPrivateKeyPemEncoded(rsaPrivateKeyEncoded);
    }

    private String getEllipticCurvePrivateKeyEncoded(final String curveName) throws GeneralSecurityException {
        final KeyPairGenerator ecKeyPairGenerator = KeyPairGenerator.getInstance(EC_ALGORITHM);
        ecKeyPairGenerator.initialize(new ECGenParameterSpec(curveName));
        final KeyPair ecKeyPair = ecKeyPairGenerator.generateKeyPair();
        final PrivateKey ecPrivateKey = ecKeyPair.getPrivate();
        final byte[] ecPrivateKeyEncoded = ecPrivateKey.getEncoded();
        return getPrivateKeyPemEncoded(ecPrivateKeyEncoded);
    }

    private static String getRsaPrivateKeyPemEncoded(final byte[] privateKeyEncoded) {
        final int rsaPrivateKeyLength = privateKeyEncoded.length - PKCS8_RSA_DER_HEADER_LENGTH;
        final byte[] rsaPrivateKey = new byte[rsaPrivateKeyLength];
        System.arraycopy(privateKeyEncoded, PKCS8_RSA_DER_HEADER_LENGTH, rsaPrivateKey, 0, rsaPrivateKey.length);
        final String formatted = encoder.encodeToString(rsaPrivateKey);

        final String[] lines = formatted.split(LINE_LENGTH_PATTERN);

        final StringBuilder builder = new StringBuilder();
        builder.append(StandardPemPrivateKeyReader.RSA_PRIVATE_KEY_HEADER);
        builder.append(LINE_FEED);

        for (final String line : lines) {
            builder.append(line);
            builder.append(LINE_FEED);
        }

        builder.append(StandardPemPrivateKeyReader.RSA_PRIVATE_KEY_FOOTER);
        builder.append(LINE_FEED);

        return builder.toString();
    }

    private static String getPrivateKeyPemEncoded(final byte[] privateKeyEncoded) {
        final String formatted = encoder.encodeToString(privateKeyEncoded);

        final String[] lines = formatted.split(LINE_LENGTH_PATTERN);

        final StringBuilder builder = new StringBuilder();
        builder.append(StandardPemPrivateKeyReader.PRIVATE_KEY_HEADER);
        builder.append(LINE_FEED);

        for (final String line : lines) {
            builder.append(line);
            builder.append(LINE_FEED);
        }

        builder.append(StandardPemPrivateKeyReader.PRIVATE_KEY_FOOTER);
        builder.append(LINE_FEED);

        return builder.toString();
    }
}
