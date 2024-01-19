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
package org.apache.nifi.processors.cipher;

import org.apache.nifi.processors.cipher.compatibility.CompatibilityModeEncryptionScheme;
import org.apache.nifi.processors.cipher.compatibility.CompatibilityModeKeyDerivationStrategy;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.SecureRandom;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

class DecryptContentCompatibilityTest {
    private static final BouncyCastleProvider BOUNCY_CASTLE_PROVIDER = new BouncyCastleProvider();

    private static final String PASSWORD = "password";

    private static final byte[] RAW = DecryptContentCompatibility.class.getSimpleName().getBytes(StandardCharsets.UTF_8);

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private static final CompatibilityModeEncryptionScheme SIMPLE_ENCRYPTION_SCHEME = CompatibilityModeEncryptionScheme.PBE_WITH_MD5_AND_DES;

    private static final byte[] EMPTY_SALT = {};

    private static final byte[] OPENSSL_SALT = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};

    private static final int BLOCK_SIZE_UNDEFINED = 0;

    private static final byte[] WORD = {'W', 'O', 'R', 'D'};

    private static final int WORD_COUNT = 2048;

    TestRunner runner;

    @BeforeEach
    void setRunner() {
        runner = TestRunners.newTestRunner(DecryptContentCompatibility.class);
    }

    @ParameterizedTest
    @EnumSource(CompatibilityModeEncryptionScheme.class)
    void testRunKeyDerivationOpenSslUnsalted(final CompatibilityModeEncryptionScheme encryptionScheme) throws Exception {
        final CompatibilityModeKeyDerivationStrategy keyDerivationStrategy = CompatibilityModeKeyDerivationStrategy.OPENSSL_EVP_BYTES_TO_KEY;
        runner.setProperty(DecryptContentCompatibility.KEY_DERIVATION_STRATEGY, keyDerivationStrategy);
        runner.setProperty(DecryptContentCompatibility.ENCRYPTION_SCHEME, encryptionScheme);
        runner.setProperty(DecryptContentCompatibility.PASSWORD, PASSWORD);

        final Cipher cipher = getCipher(encryptionScheme);
        final byte[] encrypted = getEncrypted(cipher, keyDerivationStrategy, EMPTY_SALT, RAW);
        assertDecrypted(RAW, encrypted, encryptionScheme);
    }

    @ParameterizedTest
    @EnumSource(CompatibilityModeEncryptionScheme.class)
    void testRunKeyDerivationOpenSslSalted(final CompatibilityModeEncryptionScheme encryptionScheme) throws Exception {
        final CompatibilityModeKeyDerivationStrategy keyDerivationStrategy = CompatibilityModeKeyDerivationStrategy.OPENSSL_EVP_BYTES_TO_KEY;
        runner.setProperty(DecryptContentCompatibility.KEY_DERIVATION_STRATEGY, keyDerivationStrategy);
        runner.setProperty(DecryptContentCompatibility.ENCRYPTION_SCHEME, encryptionScheme.getValue());
        runner.setProperty(DecryptContentCompatibility.PASSWORD, PASSWORD);

        final Cipher cipher = getCipher(encryptionScheme);
        final byte[] encrypted = getEncrypted(cipher, keyDerivationStrategy, OPENSSL_SALT, RAW);
        assertDecrypted(RAW, encrypted, encryptionScheme);
    }

    @Test
    void testRunKeyDerivationOpenSslSaltedStream() throws Exception {
        final CompatibilityModeKeyDerivationStrategy keyDerivationStrategy = CompatibilityModeKeyDerivationStrategy.OPENSSL_EVP_BYTES_TO_KEY;
        runner.setProperty(DecryptContentCompatibility.KEY_DERIVATION_STRATEGY, keyDerivationStrategy);
        runner.setProperty(DecryptContentCompatibility.ENCRYPTION_SCHEME, SIMPLE_ENCRYPTION_SCHEME);
        runner.setProperty(DecryptContentCompatibility.PASSWORD, PASSWORD);

        final Cipher cipher = getCipher(SIMPLE_ENCRYPTION_SCHEME);

        final byte[] words = getWords();
        final byte[] encrypted = getEncrypted(cipher, keyDerivationStrategy, OPENSSL_SALT, words);
        assertDecrypted(words, encrypted, SIMPLE_ENCRYPTION_SCHEME);
    }

    @ParameterizedTest
    @EnumSource(CompatibilityModeEncryptionScheme.class)
    void testRunKeyDerivationJasyptStandard(final CompatibilityModeEncryptionScheme encryptionScheme) throws Exception {
        final CompatibilityModeKeyDerivationStrategy keyDerivationStrategy = CompatibilityModeKeyDerivationStrategy.JASYPT_STANDARD;
        runner.setProperty(DecryptContentCompatibility.KEY_DERIVATION_STRATEGY, keyDerivationStrategy);
        runner.setProperty(DecryptContentCompatibility.ENCRYPTION_SCHEME, encryptionScheme);
        runner.setProperty(DecryptContentCompatibility.PASSWORD, PASSWORD);

        final Cipher cipher = getCipher(encryptionScheme);
        final byte[] salt = getSalt(cipher, keyDerivationStrategy);
        final byte[] encrypted = getEncrypted(cipher, keyDerivationStrategy, salt, RAW);
        assertDecrypted(RAW, encrypted, encryptionScheme);
    }

    @Test
    void testRunKeyDerivationJasyptStandardSaltMissing() throws Exception {
        setSimpleEncryptionScheme();

        final Cipher cipher = getCipher(SIMPLE_ENCRYPTION_SCHEME);
        final byte[] encrypted = getEncrypted(cipher, CompatibilityModeKeyDerivationStrategy.JASYPT_STANDARD, EMPTY_SALT, RAW);

        runner.enqueue(encrypted);
        runner.run();

        runner.assertAllFlowFilesTransferred(DecryptContentCompatibility.FAILURE);

        assertErrorLogged();
    }

    @Test
    void testRunKeyDerivationJasyptStandardFileEnd() {
        setSimpleEncryptionScheme();

        runner.enqueue(EMPTY_SALT);
        runner.run();

        runner.assertAllFlowFilesTransferred(DecryptContentCompatibility.FAILURE);

        assertErrorLogged();
    }

    @Test
    void testRunKeyDerivationJasyptStandardSaltLengthMissing() {
        setSimpleEncryptionScheme();

        runner.enqueue(WORD);
        runner.run();

        runner.assertAllFlowFilesTransferred(DecryptContentCompatibility.FAILURE);

        assertErrorLogged();
    }

    private void setSimpleEncryptionScheme() {
        runner.setProperty(DecryptContentCompatibility.KEY_DERIVATION_STRATEGY, CompatibilityModeKeyDerivationStrategy.JASYPT_STANDARD);
        runner.setProperty(DecryptContentCompatibility.ENCRYPTION_SCHEME, SIMPLE_ENCRYPTION_SCHEME);
        runner.setProperty(DecryptContentCompatibility.PASSWORD, PASSWORD);
    }

    private void assertErrorLogged() {
        final List<LogMessage> errorMessages = runner.getLogger().getErrorMessages();
        final Optional<LogMessage> firstErrorMessage = errorMessages.stream().findFirst();
        assertTrue(firstErrorMessage.isPresent());

        final LogMessage errorLogMessage = firstErrorMessage.get();
        final String message = errorLogMessage.getMsg();
        assertTrue(message.contains(SIMPLE_ENCRYPTION_SCHEME.getValue()));
    }

    private void assertDecrypted(final byte[] expected, final byte[] encrypted, final CompatibilityModeEncryptionScheme encryptionScheme) throws IOException {
        runner.enqueue(encrypted);
        runner.run();

        runner.assertAllFlowFilesTransferred(DecryptContentCompatibility.SUCCESS);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(DecryptContentCompatibility.SUCCESS).get(0);

        flowFile.assertContentEquals(expected);

        flowFile.assertAttributeEquals(CipherAttributeKey.PBE_SCHEME, encryptionScheme.getValue());
        flowFile.assertAttributeEquals(CipherAttributeKey.PBE_SYMMETRIC_CIPHER, encryptionScheme.getSymmetricCipher().getValue());
        flowFile.assertAttributeEquals(CipherAttributeKey.PBE_DIGEST_ALGORITHM, encryptionScheme.getDigestAlgorithm().getValue());
    }

    private byte[] getSalt(final Cipher cipher, final CompatibilityModeKeyDerivationStrategy strategy) {
        final int blockSize = cipher.getBlockSize();
        final int saltLength = blockSize == BLOCK_SIZE_UNDEFINED ? strategy.getSaltStandardLength() : blockSize;
        final byte[] salt = new byte[saltLength];
        SECURE_RANDOM.nextBytes(salt);
        return salt;
    }

    private byte[] getEncrypted(
            final Cipher cipher,
            final CompatibilityModeKeyDerivationStrategy keyDerivationStrategy,
            final byte[] salt,
            final byte[] raw
    ) throws GeneralSecurityException, IOException {
        initCipher(cipher, keyDerivationStrategy, salt);
        final byte[] encrypted = cipher.doFinal(raw);

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        if (CompatibilityModeKeyDerivationStrategy.JASYPT_STANDARD == keyDerivationStrategy || salt.length == 0) {
            outputStream.write(salt);
        } else {
            outputStream.write(keyDerivationStrategy.getSaltHeader());
            outputStream.write(salt);
        }

        outputStream.write(encrypted);
        return outputStream.toByteArray();
    }

    private Cipher getCipher(final CompatibilityModeEncryptionScheme encryptionScheme) throws GeneralSecurityException {
        final String algorithm = encryptionScheme.getValue();
        return Cipher.getInstance(algorithm, BOUNCY_CASTLE_PROVIDER);
    }

    private void initCipher(final Cipher cipher, final CompatibilityModeKeyDerivationStrategy strategy, final byte[] salt) throws GeneralSecurityException {
        final String algorithm = cipher.getAlgorithm();
        final Key key = getKey(algorithm);
        final PBEParameterSpec parameterSpec = new PBEParameterSpec(salt, strategy.getIterations());
        cipher.init(Cipher.ENCRYPT_MODE, key, parameterSpec);
    }

    private Key getKey(final String algorithm) throws GeneralSecurityException  {
        final SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance(algorithm, BOUNCY_CASTLE_PROVIDER);
        final PBEKeySpec keySpec = new PBEKeySpec(PASSWORD.toCharArray());
        return secretKeyFactory.generateSecret(keySpec);
    }

    private byte[] getWords() throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        for (int i = 0; i < WORD_COUNT; i++) {
            outputStream.write(WORD);
        }

        return outputStream.toByteArray();
    }
}
