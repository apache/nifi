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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.AlgorithmParameters;
import java.security.GeneralSecurityException;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.spec.ECGenParameterSpec;
import java.util.Base64;
import java.util.HexFormat;
import java.util.UUID;
import java.util.stream.Stream;
import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StandardPrivateKeyReaderTest {
    private static final HexFormat HEX_FORMAT = HexFormat.of();

    private static final Base64.Encoder ENCODER = Base64.getEncoder();

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private static final char[] KEY_PASSWORD = UUID.randomUUID().toString().toCharArray();

    private static final char[] INCORRECT_KEY_PASSWORD = UUID.randomUUID().toString().toCharArray();

    private static final char[] EMPTY_KEY_PASSWORD = new char[0];

    private static final char[] NULL_KEY_PASSWORD = null;

    private static final String PBES2_HMAC_SHA1_AES_128 = "PBEWithHmacSHA1AndAES_128";

    private static final String PBES2_HMAC_SHA224_AES_256 = "PBEWithHmacSHA224AndAES_256";

    private static final String PBES2_HMAC_SHA256_AES_256 = "PBEWithHmacSHA256AndAES_256";

    private static final String PBES2_HMAC_SHA384_AES_128 = "PBEWithHmacSHA384AndAES_128";

    private static final String PBES2_HMAC_SHA512_AES_256 = "PBEWithHmacSHA512AndAES_256";

    private static final String PBES2_HMAC_SHA512_224_AES_128 = "PBEWithHmacSHA512/224AndAES_128";

    private static final String PBES2_HMAC_SHA512_256_AES_256 = "PBEWithHmacSHA512/256AndAES_256";

    private static final String PBE_SHA1_DESEDE = "PBEWithSHA1AndDESede";

    private static final String PBES2_ALGORITHM = "PBES2";

    private static final String PBES2_CIPHER_PREFIX = "PBEWithHmac";

    private static final String PBKDF2_HMAC_SHA256 = "PBKDF2WithHmacSHA256";

    private static final int PBKDF2_ITERATION_COUNT = 2048;

    private static final int SALT_LENGTH = 16;

    private static final String DES_EDE3_ALGORITHM = "DESede";

    private static final String DES_EDE3_CBC_TRANSFORMATION = "DESede/CBC/PKCS5Padding";

    private static final int DES_EDE3_KEY_SIZE = 192;

    private static final byte[] PBES2_OBJECT_IDENTIFIER = HEX_FORMAT.parseHex("06092a864886f70d01050d");

    private static final byte[] PBKDF2_OBJECT_IDENTIFIER = HEX_FORMAT.parseHex("06092a864886f70d01050c");

    private static final byte[] HMAC_SHA256_OBJECT_IDENTIFIER = HEX_FORMAT.parseHex("06082a864886f70d0209");

    private static final byte[] DES_EDE3_CBC_OBJECT_IDENTIFIER = HEX_FORMAT.parseHex("06082a864886f70d0307");

    private static final byte[] NULL_PARAMETERS = HEX_FORMAT.parseHex("0500");

    private static final int DER_LONG_FORM_LENGTH_FLAG = 0x80;

    private static final String RSA_ALGORITHM = "RSA";

    private static final int RSA_KEY_SIZE = 2048;

    private static final String EC_ALGORITHM = "EC";

    private static final String ED25519_ALGORITHM = "Ed25519";

    private static final String CURVE_P_256 = "secp256r1";

    private static final String CURVE_P_384 = "secp384r1";

    private static final String PRIVATE_KEY_TYPE = "PRIVATE KEY";

    private static final String ENCRYPTED_PRIVATE_KEY_TYPE = "ENCRYPTED PRIVATE KEY";

    private static final String CERTIFICATE_TYPE = "CERTIFICATE";

    private static final String PEM_FORMAT = "-----BEGIN %s-----\n%s\n-----END %s-----\n";

    private static final String BAG_ATTRIBUTES = "Bag Attributes\n    localKeyID: 01 00 00 00\nKey Attributes: <No Attributes>\n";

    private static final String BASE64_NOT_VALID = "-----BEGIN PRIVATE KEY-----\n!!!!\n-----END PRIVATE KEY-----\n";

    private static final String END_BOUNDARY_NOT_FOUND = "-----BEGIN PRIVATE KEY-----\nMAA=\n";

    private static final byte[] CONTENT = {1};

    private static final byte[] DER_NOT_VALID = {0x30};

    private static PrivateKey rsaPrivateKey;

    private static PrivateKey ecCurveP256PrivateKey;

    private static PrivateKey ecCurveP384PrivateKey;

    private static PrivateKey ed25519PrivateKey;

    private final StandardPrivateKeyReader reader = new StandardPrivateKeyReader();

    @BeforeAll
    static void setPrivateKeys() throws GeneralSecurityException {
        final KeyPairGenerator rsaKeyPairGenerator = KeyPairGenerator.getInstance(RSA_ALGORITHM);
        rsaKeyPairGenerator.initialize(RSA_KEY_SIZE);
        rsaPrivateKey = rsaKeyPairGenerator.generateKeyPair().getPrivate();

        ecCurveP256PrivateKey = getEcPrivateKey(CURVE_P_256);
        ecCurveP384PrivateKey = getEcPrivateKey(CURVE_P_384);
        ed25519PrivateKey = KeyPairGenerator.getInstance(ED25519_ALGORITHM).generateKeyPair().getPrivate();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("privateKeyArguments")
    void testReadPrivateKey(final String description, final String pem, final char[] keyPassword, final PrivateKey expectedPrivateKey) {
        final PrivateKey privateKey = readPrivateKey(pem, keyPassword);

        assertEquals(expectedPrivateKey, privateKey);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("privateKeyPasswordRequiredArguments")
    void testReadPrivateKeyPasswordRequired(final String description, final String pem) {
        final PrivateKeyException exception = assertThrows(PrivateKeyException.class, () -> readPrivateKey(pem, EMPTY_KEY_PASSWORD));

        assertEquals(StandardPrivateKeyReader.KEY_PASSWORD_REQUIRED, exception.getMessage());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("privateKeyExceptionArguments")
    void testReadPrivateKeyException(final String description, final String pem, final char[] keyPassword) {
        assertThrows(PrivateKeyException.class, () -> readPrivateKey(pem, keyPassword));
    }

    static Stream<Arguments> privateKeyArguments() throws GeneralSecurityException, IOException {
        return Stream.of(
                Arguments.of("RSA", getPkcs8Pem(rsaPrivateKey), EMPTY_KEY_PASSWORD, rsaPrivateKey),
                Arguments.of("RSA null password", getPkcs8Pem(rsaPrivateKey), NULL_KEY_PASSWORD, rsaPrivateKey),
                Arguments.of("EC P-256", getPkcs8Pem(ecCurveP256PrivateKey), EMPTY_KEY_PASSWORD, ecCurveP256PrivateKey),
                Arguments.of("Ed25519", getPkcs8Pem(ed25519PrivateKey), EMPTY_KEY_PASSWORD, ed25519PrivateKey),
                Arguments.of("PBES2 HMAC-SHA1 AES-128-CBC RSA", getEncryptedPkcs8Pem(rsaPrivateKey, PBES2_HMAC_SHA1_AES_128), KEY_PASSWORD, rsaPrivateKey),
                Arguments.of("PBES2 HMAC-SHA224 AES-256-CBC RSA", getEncryptedPkcs8Pem(rsaPrivateKey, PBES2_HMAC_SHA224_AES_256), KEY_PASSWORD, rsaPrivateKey),
                Arguments.of("PBES2 HMAC-SHA256 AES-256-CBC RSA", getEncryptedPkcs8Pem(rsaPrivateKey, PBES2_HMAC_SHA256_AES_256), KEY_PASSWORD, rsaPrivateKey),
                Arguments.of("PBES2 HMAC-SHA384 AES-128-CBC RSA", getEncryptedPkcs8Pem(rsaPrivateKey, PBES2_HMAC_SHA384_AES_128), KEY_PASSWORD, rsaPrivateKey),
                Arguments.of("PBES2 HMAC-SHA512 AES-256-CBC RSA", getEncryptedPkcs8Pem(rsaPrivateKey, PBES2_HMAC_SHA512_AES_256), KEY_PASSWORD, rsaPrivateKey),
                Arguments.of("PBES2 HMAC-SHA512/224 AES-128-CBC RSA", getEncryptedPkcs8Pem(rsaPrivateKey, PBES2_HMAC_SHA512_224_AES_128), KEY_PASSWORD, rsaPrivateKey),
                Arguments.of("PBES2 HMAC-SHA512/256 AES-256-CBC RSA", getEncryptedPkcs8Pem(rsaPrivateKey, PBES2_HMAC_SHA512_256_AES_256), KEY_PASSWORD, rsaPrivateKey),
                Arguments.of("PBES2 HMAC-SHA256 DES-EDE3-CBC RSA", getPbes2DesEde3Pem(rsaPrivateKey), KEY_PASSWORD, rsaPrivateKey),
                Arguments.of("PBES2 HMAC-SHA256 AES-256-CBC EC P-384", getEncryptedPkcs8Pem(ecCurveP384PrivateKey, PBES2_HMAC_SHA256_AES_256), KEY_PASSWORD, ecCurveP384PrivateKey),
                Arguments.of("PBES2 HMAC-SHA256 AES-256-CBC Ed25519", getEncryptedPkcs8Pem(ed25519PrivateKey, PBES2_HMAC_SHA256_AES_256), KEY_PASSWORD, ed25519PrivateKey),
                Arguments.of("PBE-SHA1-3DES RSA", getEncryptedPkcs8Pem(rsaPrivateKey, PBE_SHA1_DESEDE), KEY_PASSWORD, rsaPrivateKey),
                Arguments.of("RSA with preceding Bag Attributes", BAG_ATTRIBUTES + getPkcs8Pem(rsaPrivateKey), EMPTY_KEY_PASSWORD, rsaPrivateKey),
                Arguments.of("RSA with preceding Certificate", getPem(CERTIFICATE_TYPE, CONTENT) + getPkcs8Pem(rsaPrivateKey), EMPTY_KEY_PASSWORD, rsaPrivateKey)
        );
    }

    static Stream<Arguments> privateKeyPasswordRequiredArguments() throws GeneralSecurityException, IOException {
        return Stream.of(
                Arguments.of("PBES2 HMAC-SHA256 AES-256-CBC", getEncryptedPkcs8Pem(rsaPrivateKey, PBES2_HMAC_SHA256_AES_256)),
                Arguments.of("PBE-SHA1-3DES", getEncryptedPkcs8Pem(rsaPrivateKey, PBE_SHA1_DESEDE))
        );
    }

    static Stream<Arguments> privateKeyExceptionArguments() throws GeneralSecurityException, IOException {
        return Stream.of(
                Arguments.of("PBES2 HMAC-SHA256 AES-256-CBC incorrect password", getEncryptedPkcs8Pem(rsaPrivateKey, PBES2_HMAC_SHA256_AES_256), INCORRECT_KEY_PASSWORD),
                Arguments.of("PBE-SHA1-3DES incorrect password", getEncryptedPkcs8Pem(rsaPrivateKey, PBE_SHA1_DESEDE), INCORRECT_KEY_PASSWORD),
                Arguments.of("DER not valid", getPem(PRIVATE_KEY_TYPE, DER_NOT_VALID), EMPTY_KEY_PASSWORD),
                Arguments.of("PEM Base64 not valid", BASE64_NOT_VALID, EMPTY_KEY_PASSWORD),
                Arguments.of("PEM end boundary not found", END_BOUNDARY_NOT_FOUND, EMPTY_KEY_PASSWORD),
                Arguments.of("PEM Private Key not found", getPem(CERTIFICATE_TYPE, CONTENT), EMPTY_KEY_PASSWORD)
        );
    }

    private PrivateKey readPrivateKey(final String pem, final char[] keyPassword) {
        return reader.readPrivateKey(new ByteArrayInputStream(pem.getBytes(StandardCharsets.US_ASCII)), keyPassword);
    }

    private static PrivateKey getEcPrivateKey(final String curveName) throws GeneralSecurityException {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(EC_ALGORITHM);
        keyPairGenerator.initialize(new ECGenParameterSpec(curveName));
        return keyPairGenerator.generateKeyPair().getPrivate();
    }

    private static String getPem(final String type, final byte[] content) {
        return PEM_FORMAT.formatted(type, ENCODER.encodeToString(content), type);
    }

    private static String getPkcs8Pem(final PrivateKey privateKey) {
        return getPem(PRIVATE_KEY_TYPE, privateKey.getEncoded());
    }

    private static String getEncryptedPkcs8Pem(final PrivateKey privateKey, final String encryptionAlgorithm) throws GeneralSecurityException, IOException {
        final SecretKey secretKey = SecretKeyFactory.getInstance(encryptionAlgorithm).generateSecret(new PBEKeySpec(KEY_PASSWORD));
        final Cipher cipher = Cipher.getInstance(encryptionAlgorithm);
        cipher.init(Cipher.ENCRYPT_MODE, secretKey);
        final byte[] encrypted = cipher.doFinal(privateKey.getEncoded());

        final EncryptedPrivateKeyInfo encryptedPrivateKeyInfo = new EncryptedPrivateKeyInfo(getEncryptionParameters(cipher), encrypted);
        return getPem(ENCRYPTED_PRIVATE_KEY_TYPE, encryptedPrivateKeyInfo.getEncoded());
    }

    private static AlgorithmParameters getEncryptionParameters(final Cipher cipher) throws GeneralSecurityException, IOException {
        final AlgorithmParameters cipherParameters = cipher.getParameters();

        final AlgorithmParameters encryptionParameters;
        if (cipherParameters.getAlgorithm().startsWith(PBES2_CIPHER_PREFIX)) {
            encryptionParameters = AlgorithmParameters.getInstance(PBES2_ALGORITHM);
            encryptionParameters.init(cipherParameters.getEncoded());
        } else {
            encryptionParameters = cipherParameters;
        }

        return encryptionParameters;
    }

    private static String getPbes2DesEde3Pem(final PrivateKey privateKey) throws GeneralSecurityException {
        final byte[] salt = new byte[SALT_LENGTH];
        SECURE_RANDOM.nextBytes(salt);
        final PBEKeySpec keySpec = new PBEKeySpec(KEY_PASSWORD, salt, PBKDF2_ITERATION_COUNT, DES_EDE3_KEY_SIZE);
        final byte[] key = SecretKeyFactory.getInstance(PBKDF2_HMAC_SHA256).generateSecret(keySpec).getEncoded();

        final Cipher cipher = Cipher.getInstance(DES_EDE3_CBC_TRANSFORMATION);
        cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, DES_EDE3_ALGORITHM));
        final byte[] encrypted = cipher.doFinal(privateKey.getEncoded());

        final byte[] pseudoRandomFunction = getSequence(HMAC_SHA256_OBJECT_IDENTIFIER, NULL_PARAMETERS);
        final byte[] pbkdf2Parameters = getSequence(getOctetString(salt), getInteger(), pseudoRandomFunction);
        final byte[] keyDerivationFunction = getSequence(PBKDF2_OBJECT_IDENTIFIER, pbkdf2Parameters);
        final byte[] encryptionScheme = getSequence(DES_EDE3_CBC_OBJECT_IDENTIFIER, getOctetString(cipher.getIV()));
        final byte[] encryptionAlgorithm = getSequence(PBES2_OBJECT_IDENTIFIER, getSequence(keyDerivationFunction, encryptionScheme));
        final byte[] encryptedPrivateKeyInfo = getSequence(encryptionAlgorithm, getOctetString(encrypted));
        return getPem(ENCRYPTED_PRIVATE_KEY_TYPE, encryptedPrivateKeyInfo);
    }

    private static byte[] getSequence(final byte[]... elements) {
        return getDerEncoded(DerElement.SEQUENCE_TAG, elements);
    }

    private static byte[] getOctetString(final byte[] contents) {
        return getDerEncoded(DerElement.OCTET_STRING_TAG, contents);
    }

    private static byte[] getInteger() {
        return getDerEncoded(DerElement.INTEGER_TAG, BigInteger.valueOf(PBKDF2_ITERATION_COUNT).toByteArray());
    }

    private static byte[] getDerEncoded(final int tag, final byte[]... contents) {
        final ByteArrayOutputStream contentsStream = new ByteArrayOutputStream();
        for (final byte[] content : contents) {
            contentsStream.writeBytes(content);
        }

        final ByteArrayOutputStream encodedStream = new ByteArrayOutputStream();
        encodedStream.write(tag);

        final int length = contentsStream.size();
        if (length < DER_LONG_FORM_LENGTH_FLAG) {
            encodedStream.write(length);
        } else {
            final byte[] signedLength = BigInteger.valueOf(length).toByteArray();
            final int lengthOffset = signedLength[0] == 0 ? 1 : 0;
            final int lengthBytes = signedLength.length - lengthOffset;
            encodedStream.write(DER_LONG_FORM_LENGTH_FLAG | lengthBytes);
            encodedStream.write(signedLength, lengthOffset, lengthBytes);
        }

        encodedStream.writeBytes(contentsStream.toByteArray());
        return encodedStream.toByteArray();
    }
}
