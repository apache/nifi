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
package org.apache.nifi.processors.standard;

import groovy.time.TimeCategory;
import groovy.time.TimeDuration;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.apache.nifi.security.util.crypto.Argon2CipherProvider;
import org.apache.nifi.security.util.crypto.Argon2SecureHasher;
import org.apache.nifi.security.util.crypto.CipherUtility;
import org.apache.nifi.security.util.crypto.KeyedEncryptor;
import org.apache.nifi.security.util.crypto.PasswordBasedEncryptor;
import org.apache.nifi.security.util.crypto.RandomIVPBECipherProvider;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.Security;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestEncryptContent {

    private static final List<EncryptionMethod> SUPPORTED_KEYED_ENCRYPTION_METHODS = Arrays
            .stream(EncryptionMethod.values())
            .filter(method -> method.isKeyedCipher() && method != EncryptionMethod.AES_CBC_NO_PADDING)
            .collect(Collectors.toList());

    @BeforeEach
    public void setUp() {
        Security.addProvider(new BouncyCastleProvider());
    }

    @Test
    public void testRoundTrip() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent());
        testRunner.setProperty(EncryptContent.PASSWORD, "short");
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NIFI_LEGACY.name());
        // Must be allowed or short password will cause validation errors
        testRunner.setProperty(EncryptContent.ALLOW_WEAK_CRYPTO, "allowed");

        for (final EncryptionMethod encryptionMethod : EncryptionMethod.values()) {
            if (encryptionMethod.isUnlimitedStrength()) {
                continue;   // cannot test unlimited strength in unit tests because it's not enabled by the JVM by default.
            }

            // KeyedCiphers tested in TestEncryptContentGroovy.groovy
            if (encryptionMethod.isKeyedCipher()) {
                continue;
            }

            testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name());
            testRunner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE);

            testRunner.enqueue(Paths.get("src/test/resources/hello.txt"));
            testRunner.clearTransferState();
            testRunner.run();

            testRunner.assertAllFlowFilesTransferred(EncryptContent.REL_SUCCESS, 1);

            MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(EncryptContent.REL_SUCCESS).get(0);
            testRunner.assertQueueEmpty();

            testRunner.setProperty(EncryptContent.MODE, EncryptContent.DECRYPT_MODE);
            testRunner.enqueue(flowFile);
            testRunner.clearTransferState();
            testRunner.run();
            testRunner.assertAllFlowFilesTransferred(EncryptContent.REL_SUCCESS, 1);

            flowFile = testRunner.getFlowFilesForRelationship(EncryptContent.REL_SUCCESS).get(0);
            flowFile.assertContentEquals(new File("src/test/resources/hello.txt"));
        }
    }

    @Test
    public void testKeyedCiphersRoundTrip() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent());
        final String RAW_KEY_HEX = StringUtils.repeat("ab", 16);
        testRunner.setProperty(EncryptContent.RAW_KEY_HEX, RAW_KEY_HEX);
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NONE.name());

        for (final EncryptionMethod encryptionMethod : SUPPORTED_KEYED_ENCRYPTION_METHODS) {
            testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name());
            testRunner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE);

            testRunner.enqueue(Paths.get("src/test/resources/hello.txt"));
            testRunner.clearTransferState();
            testRunner.run();

            testRunner.assertAllFlowFilesTransferred(EncryptContent.REL_SUCCESS, 1);

            MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(EncryptContent.REL_SUCCESS).get(0);
            testRunner.assertQueueEmpty();

            testRunner.setProperty(EncryptContent.MODE, EncryptContent.DECRYPT_MODE);
            testRunner.enqueue(flowFile);
            testRunner.clearTransferState();
            testRunner.run();
            testRunner.assertAllFlowFilesTransferred(EncryptContent.REL_SUCCESS, 1);

            flowFile = testRunner.getFlowFilesForRelationship(EncryptContent.REL_SUCCESS).get(0);
            flowFile.assertContentEquals(new File("src/test/resources/hello.txt"));
        }
    }

    @Test
    public void testShouldDetermineMaxKeySizeForAlgorithms() {
        final String AES_ALGORITHM = EncryptionMethod.MD5_256AES.getAlgorithm();
        final String DES_ALGORITHM = EncryptionMethod.MD5_DES.getAlgorithm();

        final int AES_MAX_LENGTH = Integer.MAX_VALUE;
        final int DES_MAX_LENGTH = Integer.MAX_VALUE;

        int determinedAESMaxLength = PasswordBasedEncryptor.getMaxAllowedKeyLength(AES_ALGORITHM);
        int determinedTDESMaxLength = PasswordBasedEncryptor.getMaxAllowedKeyLength(DES_ALGORITHM);

        assertEquals(AES_MAX_LENGTH, determinedAESMaxLength);
        assertEquals(DES_MAX_LENGTH, determinedTDESMaxLength);
    }

    @Test
    public void testShouldDecryptOpenSSLRawSalted() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent());

        final String password = "thisIsABadPassword";
        final EncryptionMethod method = EncryptionMethod.MD5_256AES;
        final KeyDerivationFunction kdf = KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY;

        testRunner.setProperty(EncryptContent.PASSWORD, password);
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, kdf.name());
        testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, method.name());
        testRunner.setProperty(EncryptContent.MODE, EncryptContent.DECRYPT_MODE);

        testRunner.enqueue(Paths.get("src/test/resources/TestEncryptContent/salted_raw.enc"));
        testRunner.clearTransferState();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EncryptContent.REL_SUCCESS, 1);
        testRunner.assertQueueEmpty();

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(EncryptContent.REL_SUCCESS).get(0);

        flowFile.assertContentEquals(new File("src/test/resources/TestEncryptContent/plain.txt"));
    }

    @Test
    public void testShouldDecryptOpenSSLRawUnsalted() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent());

        final String password = "thisIsABadPassword";
        final EncryptionMethod method = EncryptionMethod.MD5_256AES;
        final KeyDerivationFunction kdf = KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY;

        testRunner.setProperty(EncryptContent.PASSWORD, password);
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, kdf.name());
        testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, method.name());
        testRunner.setProperty(EncryptContent.MODE, EncryptContent.DECRYPT_MODE);

        testRunner.enqueue(Paths.get("src/test/resources/TestEncryptContent/unsalted_raw.enc"));
        testRunner.clearTransferState();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EncryptContent.REL_SUCCESS, 1);
        testRunner.assertQueueEmpty();

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(EncryptContent.REL_SUCCESS).get(0);

        flowFile.assertContentEquals(new File("src/test/resources/TestEncryptContent/plain.txt"));
    }

    @Test
    public void testDecryptSmallerThanSaltSize() {
        final TestRunner runner = TestRunners.newTestRunner(EncryptContent.class);
        runner.setProperty(EncryptContent.PASSWORD, "Hello, World!");
        runner.setProperty(EncryptContent.MODE, EncryptContent.DECRYPT_MODE);
        runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NIFI_LEGACY.name());
        runner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, EncryptionMethod.MD5_128AES.name());
        runner.enqueue(new byte[4]);
        runner.run();
        runner.assertAllFlowFilesTransferred(EncryptContent.REL_FAILURE, 1);
    }

    @Test
    public void testValidation() {
        final TestRunner runner = TestRunners.newTestRunner(EncryptContent.class);
        Collection<ValidationResult> results;
        MockProcessContext pc;

        runner.enqueue(new byte[0]);
        pc = (MockProcessContext) runner.getProcessContext();
        results = pc.validate();

        // The default validation error is:
        // Raw key hex cannot be empty
        final String RAW_KEY_ERROR = "'raw-key-hex' is invalid because Raw Key (hexadecimal) is " +
                "required when using algorithm AES/GCM/NoPadding and KDF KeyDerivationFunction[KDF " +
                "Name=None,Description=The cipher is given a raw key conforming to the algorithm " +
                "specifications]. See Admin Guide.";

        final Set<String>  EXPECTED_ERRORS = new HashSet<>();
        EXPECTED_ERRORS.add(RAW_KEY_ERROR);

        assertEquals(EXPECTED_ERRORS.size(), results.size(), results.toString());
        for (final ValidationResult vr : results) {
            assertTrue(EXPECTED_ERRORS.contains(vr.toString()));
        }

        runner.enqueue(new byte[0]);
        final EncryptionMethod encryptionMethod = EncryptionMethod.MD5_128AES;
        runner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name());
        runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NIFI_LEGACY.name());
        runner.setProperty(EncryptContent.PASSWORD, "ThisIsAPasswordThatIsLongerThanSixteenCharacters");
        pc = (MockProcessContext) runner.getProcessContext();
        results = pc.validate();

        assertEquals(0, results.size(), results.toString());
    }

    @Test
    void testShouldValidateKeyFormatAndSizeForAlgorithms() {
        final TestRunner runner = TestRunners.newTestRunner(EncryptContent.class);
        Collection<ValidationResult> results;
        MockProcessContext pc;

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        final int INVALID_KEY_LENGTH = 120;
        final String INVALID_KEY_HEX = StringUtils.repeat("ab", (INVALID_KEY_LENGTH / 8));

        runner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE);
        runner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name());
        runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NONE.name());
        runner.setProperty(EncryptContent.RAW_KEY_HEX, INVALID_KEY_HEX);

        runner.enqueue(new byte[0]);
        pc = (MockProcessContext) runner.getProcessContext();

        results = pc.validate();

        assertEquals(1, results.size());
        ValidationResult keyLengthInvalidVR = results.iterator().next();

        String expectedResult = "'raw-key-hex' is invalid because Key must be valid length [128, 192, 256]";
        String message = "'" + keyLengthInvalidVR.toString() + "' contains '" + expectedResult + "'";
        assertTrue(keyLengthInvalidVR.toString().contains(expectedResult), message);
    }

    @Test
    void testShouldValidateKDFWhenKeyedCipherSelected() {
        final TestRunner runner = TestRunners.newTestRunner(EncryptContent.class);
        Collection<ValidationResult> results;
        MockProcessContext pc;

        final int VALID_KEY_LENGTH = 128;
        final String VALID_KEY_HEX = StringUtils.repeat("ab", (VALID_KEY_LENGTH / 8));

        runner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE);

        for (final EncryptionMethod encryptionMethod : SUPPORTED_KEYED_ENCRYPTION_METHODS) {
            runner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name());

            // Scenario 1: Legacy KDF + keyed cipher -> validation error
            final List<KeyDerivationFunction> invalidKDFs = Arrays.asList(KeyDerivationFunction.NIFI_LEGACY, KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY);
            for (final KeyDerivationFunction invalidKDF : invalidKDFs) {
                runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, invalidKDF.name());
                runner.setProperty(EncryptContent.RAW_KEY_HEX, VALID_KEY_HEX);
                runner.removeProperty(EncryptContent.PASSWORD);

                runner.enqueue(new byte[0]);
                pc = (MockProcessContext) runner.getProcessContext();

                results = pc.validate();

                assertEquals(1, results.size());
                ValidationResult keyLengthInvalidVR = results.iterator().next();

                String expectedResult = String.format("'key-derivation-function' is invalid because Key Derivation Function is required to be BCRYPT, SCRYPT, PBKDF2, ARGON2, NONE when using " +
                        "algorithm %s", encryptionMethod.getAlgorithm());
                String message = "'" + keyLengthInvalidVR.toString() + "' contains '" + expectedResult + "'";
                assertTrue(keyLengthInvalidVR.toString().contains(expectedResult), message);
            }

            // Scenario 2: No KDF + keyed cipher + raw-key-hex -> valid

            runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NONE.name());
            runner.setProperty(EncryptContent.RAW_KEY_HEX, VALID_KEY_HEX);
            runner.removeProperty(EncryptContent.PASSWORD);

            runner.enqueue(new byte[0]);
            pc = (MockProcessContext) runner.getProcessContext();

            results = pc.validate();

            assertTrue(results.isEmpty());

            // Scenario 3: Strong KDF + keyed cipher + password -> valid
            final List<KeyDerivationFunction> validKDFs = Arrays.asList(KeyDerivationFunction.BCRYPT,
                    KeyDerivationFunction.SCRYPT,
                    KeyDerivationFunction.PBKDF2,
                    KeyDerivationFunction.ARGON2);
            for (final KeyDerivationFunction validKDF : validKDFs) {
                runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, validKDF.name());
                runner.setProperty(EncryptContent.PASSWORD, "thisIsABadPassword");
                runner.removeProperty(EncryptContent.RAW_KEY_HEX);

                runner.enqueue(new byte[0]);
                pc = (MockProcessContext) runner.getProcessContext();

                results = pc.validate();

                assertTrue(results.isEmpty());
            }
        }
    }

    @Test
    void testShouldValidateKeyMaterialSourceWhenKeyedCipherSelected() {
        final TestRunner runner = TestRunners.newTestRunner(EncryptContent.class);
        Collection<ValidationResult> results;
        MockProcessContext pc;

        final int VALID_KEY_LENGTH = 128;
        final String VALID_KEY_HEX = StringUtils.repeat("ab", (VALID_KEY_LENGTH / 8));

        final String VALID_PASSWORD = "thisIsABadPassword";

        runner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE);
        KeyDerivationFunction none = KeyDerivationFunction.NONE;

        // Scenario 1 - RKH w/ KDF NONE & em in [CBC, CTR, GCM] (no password)
        for (final EncryptionMethod kem : SUPPORTED_KEYED_ENCRYPTION_METHODS) {
            runner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, kem.name());
            runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, none.name());

            runner.setProperty(EncryptContent.RAW_KEY_HEX, VALID_KEY_HEX);
            runner.removeProperty(EncryptContent.PASSWORD);

            runner.enqueue(new byte[0]);
            pc = (MockProcessContext) runner.getProcessContext();

            results = pc.validate();

            assertTrue(results.isEmpty());

            // Scenario 2 - PW w/ KDF in [BCRYPT, SCRYPT, PBKDF2, ARGON2] & em in [CBC, CTR, GCM] (no RKH)
            final List<KeyDerivationFunction> validKDFs = Arrays
                    .stream(KeyDerivationFunction.values())
                    .filter(KeyDerivationFunction::isStrongKDF)
                    .collect(Collectors.toList());
            for (final KeyDerivationFunction kdf : validKDFs) {
                runner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, kem.name());
                runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, kdf.name());

                runner.removeProperty(EncryptContent.RAW_KEY_HEX);
                runner.setProperty(EncryptContent.PASSWORD, VALID_PASSWORD);

                runner.enqueue(new byte[0]);
                pc = (MockProcessContext) runner.getProcessContext();

                results = pc.validate();

                assertTrue(results.isEmpty());
            }
        }
    }

    @Test
    void testShouldValidateKDFWhenPBECipherSelected() {
        final TestRunner runner = TestRunners.newTestRunner(EncryptContent.class);
        Collection<ValidationResult> results;
        MockProcessContext pc;
        final String PASSWORD = "short";

        final List<EncryptionMethod> encryptionMethods = Arrays
                .stream(EncryptionMethod.values())
                .filter(it -> it.getAlgorithm().startsWith("PBE"))
                .collect(Collectors.toList());

        runner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE);
        runner.setProperty(EncryptContent.PASSWORD, PASSWORD);
        runner.setProperty(EncryptContent.ALLOW_WEAK_CRYPTO, "allowed");

        for (final EncryptionMethod encryptionMethod : encryptionMethods) {
            runner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name());

            final List<KeyDerivationFunction> invalidKDFs = Arrays.asList(
                    KeyDerivationFunction.NONE,
                    KeyDerivationFunction.BCRYPT,
                    KeyDerivationFunction.SCRYPT,
                    KeyDerivationFunction.PBKDF2,
                    KeyDerivationFunction.ARGON2
            );
            for (final KeyDerivationFunction invalidKDF : invalidKDFs) {
                runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, invalidKDF.name());

                runner.enqueue(new byte[0]);
                pc = (MockProcessContext) runner.getProcessContext();

                results = pc.validate();

                assertEquals(1, results.size());
                ValidationResult keyLengthInvalidVR = results.iterator().next();

                String expectedResult = String.format("'Key Derivation Function' is invalid because Key Derivation Function is required to be NIFI_LEGACY, OPENSSL_EVP_BYTES_TO_KEY when using " +
                        "algorithm %s", encryptionMethod.getAlgorithm());
                String message = "'" + keyLengthInvalidVR.toString() + "' contains '" + expectedResult + "'";
                assertTrue(keyLengthInvalidVR.toString().contains(expectedResult), message);
            }

            final List<KeyDerivationFunction> validKDFs = Arrays.asList(KeyDerivationFunction.NIFI_LEGACY, KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY);
            for (final KeyDerivationFunction validKDF : validKDFs) {
                runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, validKDF.name());

                runner.enqueue(new byte[0]);
                pc = (MockProcessContext) runner.getProcessContext();

                results = pc.validate();

                assertEquals(0, results.size());
            }
        }
    }

    @Test
    void testDecryptAesCbcNoPadding() throws DecoderException, IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent());
        final String RAW_KEY_HEX = StringUtils.repeat("ab", 16);
        testRunner.setProperty(EncryptContent.RAW_KEY_HEX, RAW_KEY_HEX);
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NONE.name());
        testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, EncryptionMethod.AES_CBC_NO_PADDING.name());
        testRunner.setProperty(EncryptContent.MODE, EncryptContent.DECRYPT_MODE);

        final String content = "ExactBlockSizeRequiredForProcess";
        final byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        final KeyedEncryptor encryptor = new KeyedEncryptor(EncryptionMethod.AES_CBC_NO_PADDING, Hex.decodeHex(RAW_KEY_HEX));
        encryptor.getEncryptionCallback().process(inputStream, outputStream);
        outputStream.close();

        final byte[] encrypted = outputStream.toByteArray();
        testRunner.enqueue(encrypted);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EncryptContent.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(EncryptContent.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(content);
    }

    @Test
    void testArgon2EncryptionShouldWriteAttributesWithEncryptionMetadata() throws ParseException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent());
        KeyDerivationFunction kdf = KeyDerivationFunction.ARGON2;
        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        testRunner.setProperty(EncryptContent.PASSWORD, "thisIsABadPassword");
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, kdf.name());
        testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name());
        testRunner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE);

        String PLAINTEXT = "This is a plaintext message. ";

        testRunner.enqueue(PLAINTEXT);
        testRunner.clearTransferState();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EncryptContent.REL_SUCCESS, 1);

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(EncryptContent.REL_SUCCESS).get(0);
        testRunner.assertQueueEmpty();

        byte[] flowfileContentBytes = flowFile.getData();

        int ivDelimiterStart = CipherUtility.findSequence(flowfileContentBytes, RandomIVPBECipherProvider.IV_DELIMITER);

        final byte[] EXPECTED_KDF_SALT_BYTES = extractFullSaltFromCipherBytes(flowfileContentBytes);
        final String EXPECTED_KDF_SALT = new String(EXPECTED_KDF_SALT_BYTES);
        final String EXPECTED_SALT_HEX = extractRawSaltHexFromFullSalt(EXPECTED_KDF_SALT_BYTES, kdf);

        final String EXPECTED_IV_HEX = Hex.encodeHexString(Arrays.copyOfRange(flowfileContentBytes, ivDelimiterStart - 16, ivDelimiterStart));

        // Assert the timestamp attribute was written and is accurate
        final TimeDuration diff = calculateTimestampDifference(new Date(), flowFile.getAttribute("encryptcontent.timestamp"));
        assertTrue(diff.toMilliseconds() < 1_000);
        assertEquals(encryptionMethod.name(), flowFile.getAttribute("encryptcontent.algorithm"));
        assertEquals(kdf.name(), flowFile.getAttribute("encryptcontent.kdf"));
        assertEquals("encrypted", flowFile.getAttribute("encryptcontent.action"));
        assertEquals(EXPECTED_SALT_HEX, flowFile.getAttribute("encryptcontent.salt"));
        assertEquals("16", flowFile.getAttribute("encryptcontent.salt_length"));
        assertEquals(EXPECTED_KDF_SALT, flowFile.getAttribute("encryptcontent.kdf_salt"));
        final int kdfSaltLength = Integer.parseInt(flowFile.getAttribute("encryptcontent.kdf_salt_length"));
        assertTrue(kdfSaltLength >= 29 && kdfSaltLength <= 54);
        assertEquals(EXPECTED_IV_HEX, flowFile.getAttribute("encryptcontent.iv"));
        assertEquals("16", flowFile.getAttribute("encryptcontent.iv_length"));
        assertEquals(String.valueOf(PLAINTEXT.length()), flowFile.getAttribute("encryptcontent.plaintext_length"));
        assertEquals(String.valueOf(flowfileContentBytes.length), flowFile.getAttribute("encryptcontent.cipher_text_length"));
    }

    @Test
    void testKeyedEncryptionShouldWriteAttributesWithEncryptionMetadata() throws ParseException {
        // Arrange
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent());
        KeyDerivationFunction kdf = KeyDerivationFunction.NONE;
        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        testRunner.setProperty(EncryptContent.RAW_KEY_HEX, "0123456789ABCDEFFEDCBA9876543210");
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, kdf.name());
        testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name());
        testRunner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE);

        String PLAINTEXT = "This is a plaintext message. ";

        testRunner.enqueue(PLAINTEXT);
        testRunner.clearTransferState();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EncryptContent.REL_SUCCESS, 1);

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(EncryptContent.REL_SUCCESS).get(0);
        testRunner.assertQueueEmpty();

        byte[] flowfileContentBytes = flowFile.getData();

        int ivDelimiterStart = CipherUtility.findSequence(flowfileContentBytes, RandomIVPBECipherProvider.IV_DELIMITER);
        assertEquals(16, ivDelimiterStart);

        final TimeDuration diff = calculateTimestampDifference(new Date(), flowFile.getAttribute("encryptcontent.timestamp"));

        // Assert the timestamp attribute was written and is accurate
        assertTrue(diff.toMilliseconds() < 1_000);

        final String EXPECTED_IV_HEX = Hex.encodeHexString(Arrays.copyOfRange(flowfileContentBytes, 0, ivDelimiterStart));
        final int EXPECTED_CIPHER_TEXT_LENGTH = CipherUtility.calculateCipherTextLength(PLAINTEXT.length(), 0);
        assertEquals(encryptionMethod.name(), flowFile.getAttribute("encryptcontent.algorithm"));
        assertEquals(kdf.name(), flowFile.getAttribute("encryptcontent.kdf"));
        assertEquals("encrypted", flowFile.getAttribute("encryptcontent.action"));
        assertEquals(EXPECTED_IV_HEX, flowFile.getAttribute("encryptcontent.iv"));
        assertEquals("16", flowFile.getAttribute("encryptcontent.iv_length"));
        assertEquals(String.valueOf(PLAINTEXT.length()), flowFile.getAttribute("encryptcontent.plaintext_length"));
        assertEquals(String.valueOf(EXPECTED_CIPHER_TEXT_LENGTH), flowFile.getAttribute("encryptcontent.cipher_text_length"));
    }

    @Test
    void testDifferentCompatibleConfigurations() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent());
        KeyDerivationFunction argon2 = KeyDerivationFunction.ARGON2;
        EncryptionMethod aesCbcEM = EncryptionMethod.AES_CBC;
        int keyLength = CipherUtility.parseKeyLengthFromAlgorithm(aesCbcEM.getAlgorithm());

        final String PASSWORD = "thisIsABadPassword";
        testRunner.setProperty(EncryptContent.PASSWORD, PASSWORD);
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, argon2.name());
        testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, aesCbcEM.name());
        testRunner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE);

        String PLAINTEXT = "This is a plaintext message. ";

        testRunner.enqueue(PLAINTEXT);
        testRunner.clearTransferState();
        testRunner.run();

        MockFlowFile encryptedFlowFile = testRunner.getFlowFilesForRelationship(EncryptContent.REL_SUCCESS).get(0);
        byte[] fullCipherBytes = encryptedFlowFile.getData();

        // Extract the KDF salt from the encryption metadata in the flowfile attribute
        String argon2Salt = encryptedFlowFile.getAttribute("encryptcontent.kdf_salt");
        Argon2SecureHasher a2sh = new Argon2SecureHasher(keyLength / 8);
        byte[] fullSaltBytes = argon2Salt.getBytes(StandardCharsets.UTF_8);
        byte[] rawSaltBytes = Hex.decodeHex(encryptedFlowFile.getAttribute("encryptcontent.salt"));
        byte[] keyBytes = a2sh.hashRaw(PASSWORD.getBytes(StandardCharsets.UTF_8), rawSaltBytes);
        String keyHex = Hex.encodeHexString(keyBytes);

        byte[] ivBytes = Hex.decodeHex(encryptedFlowFile.getAttribute("encryptcontent.iv"));

        // Sanity check the encryption
        Argon2CipherProvider a2cp = new Argon2CipherProvider();
        Cipher sanityCipher = a2cp.getCipher(aesCbcEM, PASSWORD, fullSaltBytes, ivBytes, CipherUtility.parseKeyLengthFromAlgorithm(aesCbcEM.getAlgorithm()), false);
        byte[] cipherTextBytes = Arrays.copyOfRange(fullCipherBytes, fullCipherBytes.length - 32, fullCipherBytes.length);
        byte[] recoveredBytes = sanityCipher.doFinal(cipherTextBytes);

        // Configure decrypting processor with raw key
        KeyDerivationFunction kdf = KeyDerivationFunction.NONE;
        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        testRunner.setProperty(EncryptContent.RAW_KEY_HEX, keyHex);
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, kdf.name());
        testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name());
        testRunner.setProperty(EncryptContent.MODE, EncryptContent.DECRYPT_MODE);
        testRunner.removeProperty(EncryptContent.PASSWORD);

        testRunner.enqueue(fullCipherBytes);
        testRunner.clearTransferState();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EncryptContent.REL_SUCCESS, 1);

        MockFlowFile decryptedFlowFile = testRunner.getFlowFilesForRelationship(EncryptContent.REL_SUCCESS).get(0);
        testRunner.assertQueueEmpty();

        byte[] flowfileContentBytes = decryptedFlowFile.getData();

        assertArrayEquals(recoveredBytes, flowfileContentBytes);
    }

    @Test
    void testShouldCheckLengthOfPasswordWhenNotAllowed() {
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent());
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NIFI_LEGACY.name());

        Collection<ValidationResult> results;
        MockProcessContext pc;

        final List<EncryptionMethod> encryptionMethods = Arrays
                .stream(EncryptionMethod.values())
                .filter(it -> it.getAlgorithm().startsWith("PBE"))
                .collect(Collectors.toList());

        testRunner.setProperty(EncryptContent.ALLOW_WEAK_CRYPTO, "not-allowed");

        // Use .find instead of .each to allow "breaks" using return false
        for (final EncryptionMethod encryptionMethod : encryptionMethods) {
            // Determine the minimum of the algorithm-accepted length or the global safe minimum to ensure only one validation result
            final int shortPasswordLength = Math.min(PasswordBasedEncryptor.getMinimumSafePasswordLength() - 1,
                    CipherUtility.getMaximumPasswordLengthForAlgorithmOnLimitedStrengthCrypto(encryptionMethod) - 1);
            String shortPassword = StringUtils.repeat("x", shortPasswordLength);
            if (encryptionMethod.isUnlimitedStrength() || encryptionMethod.isKeyedCipher()) {
                continue;
                // cannot test unlimited strength in unit tests because it's not enabled by the JVM by default.
            }

            testRunner.setProperty(EncryptContent.PASSWORD, shortPassword);
            testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name());
            testRunner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE);

            testRunner.clearTransferState();
            testRunner.enqueue(new byte[0]);
            pc = (MockProcessContext) testRunner.getProcessContext();

            results = pc.validate();

            assertEquals(1, results.size());
            ValidationResult passwordLengthVR = results.iterator().next();

            String expectedResult = String.format("'Password' is invalid because Password length less than %s characters is potentially unsafe. " +
                    "See Admin Guide.", PasswordBasedEncryptor.getMinimumSafePasswordLength());
            String message = "'" + passwordLengthVR.toString() + "' contains '" + expectedResult + "'";
            assertTrue(passwordLengthVR.toString().contains(expectedResult), message);
        }
    }

    @Test
    void testShouldNotCheckLengthOfPasswordWhenAllowed() {
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent());
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NIFI_LEGACY.name());

        Collection<ValidationResult> results;
        MockProcessContext pc;

        final List<EncryptionMethod> encryptionMethods = Arrays
                .stream(EncryptionMethod.values())
                .filter(it -> it.getAlgorithm().startsWith("PBE"))
                .collect(Collectors.toList());

        testRunner.setProperty(EncryptContent.ALLOW_WEAK_CRYPTO, "allowed");

        for (final EncryptionMethod encryptionMethod : encryptionMethods) {
            // Determine the minimum of the algorithm-accepted length or the global safe minimum to ensure only one validation result
            final int shortPasswordLength = Math.min(PasswordBasedEncryptor.getMinimumSafePasswordLength() - 1,
                    CipherUtility.getMaximumPasswordLengthForAlgorithmOnLimitedStrengthCrypto(encryptionMethod) - 1);
            String shortPassword = StringUtils.repeat("x", shortPasswordLength);
            if (encryptionMethod.isUnlimitedStrength() || encryptionMethod.isKeyedCipher()) {
                continue;
                // cannot test unlimited strength in unit tests because it's not enabled by the JVM by default.
            }

            testRunner.setProperty(EncryptContent.PASSWORD, shortPassword);
            testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name());
            testRunner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE);

            testRunner.clearTransferState();
            testRunner.enqueue(new byte[0]);
            pc = (MockProcessContext) testRunner.getProcessContext();

            results = pc.validate();

            assertEquals(0, results.size(), results.toString());
        }
    }

    @Test
    void testArgon2ShouldIncludeFullSalt() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent());
        testRunner.setProperty(EncryptContent.PASSWORD, "thisIsABadPassword");
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.ARGON2.name());

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name());
        testRunner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE);

        testRunner.enqueue(Paths.get("src/test/resources/hello.txt"));
        testRunner.clearTransferState();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EncryptContent.REL_SUCCESS, 1);

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(EncryptContent.REL_SUCCESS).get(0);
        testRunner.assertQueueEmpty();

        final String flowFileContent = flowFile.getContent();

        final String fullSalt = flowFileContent.substring(0, flowFileContent.indexOf(new String(RandomIVPBECipherProvider.SALT_DELIMITER, StandardCharsets.UTF_8)));

        boolean isValidFormattedSalt = Argon2CipherProvider.isArgon2FormattedSalt(fullSalt);
        assertTrue(isValidFormattedSalt);

        boolean fullSaltIsValidLength = fullSalt.getBytes().length >= 49 && fullSalt.getBytes().length <= 57;
        assertTrue(fullSaltIsValidLength);
    }

    private static byte[] extractFullSaltFromCipherBytes(byte[] cipherBytes) {
        int saltDelimiterStart = CipherUtility.findSequence(cipherBytes, RandomIVPBECipherProvider.SALT_DELIMITER);
        return Arrays.copyOfRange(cipherBytes, 0, saltDelimiterStart);
    }

    private static String extractRawSaltHexFromFullSalt(byte[] fullSaltBytes, KeyDerivationFunction kdf) {
        // Salt will be in Base64 (or Radix64) for strong KDFs
        byte[] rawSaltBytes = CipherUtility.extractRawSalt(fullSaltBytes, kdf);
        return Hex.encodeHexString(rawSaltBytes);
    }

    private static TimeDuration calculateTimestampDifference(Date date, String timestamp) throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z");
        Date parsedTimestamp = formatter.parse(timestamp);

        return TimeCategory.minus(date, parsedTimestamp);
    }
}
