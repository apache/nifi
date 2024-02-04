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

import org.apache.nifi.processors.cipher.algorithm.CipherAlgorithmMode;
import org.apache.nifi.processors.cipher.algorithm.CipherAlgorithmPadding;
import org.apache.nifi.processors.cipher.encoded.EncodedDelimiter;
import org.apache.nifi.processors.cipher.encoded.KeySpecificationFormat;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bouncycastle.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

class DecryptContentTest {

    private static final byte[] WORD = {'W', 'O', 'R', 'D'};

    private static final byte[] INITIALIZATION_VECTOR = {'I', 'N', 'I', 'T', 'I', 'A', 'L', 'I', 'Z', 'A', 'T', 'I', 'O', 'N', 'I', 'V'};

    private static final String HEXADECIMAL_KEY_SPECIFICATION = "0123456789abcdef0123456789abcdef";

    private static final String SECRET_KEY_SPECIFICATION = "ContentEncrypted";

    private static final String UNENCRYPTED = "unencrypted";

    private static final String ARGON2_PARAMETERS = "$argon2id$v=19$m=65536,t=5,p=8$OCXIIeGgaovfpBZXCDxGDg";

    private static final String ARGON2_IV_ENCODED = "veGFZ+EWI0qShgT5AUJ+Qg==";

    private static final String ARGON2_CIPHERED_ENCODED = "I32fSCCeZEkKFwP04MtnK9rbL283yXBTis4T";

    private static final String BCRYPT_PARAMETERS = "$2a$12$JhHkHwk6ojTSWn9seyQV2O";

    private static final String BCRYPT_IV_ENCODED = "8bDMMSPI+dKfNL3iC3hBow==";

    private static final String BCRYPT_CIPHERED_ENCODED = "IeNNMCulpLoehwKg/A0e5dhIztA6fpqtBENl";

    private static final String PBKDF2_SALT_ENCODED = "yoHJ1TbaLMc9qpDNAhV5bQ==";

    private static final String PBKDF2_IV_ENCODED = "tgN6TnR6EbXnjKUBT4mq6Q==";

    private static final String PBKDF2_CIPHERED_ENCODED = "++5mqUPqtG4bXNwJ7ruq4cSWIncOpFhiQRDR";

    private static final String SCRYPT_PARAMETERS = "$s0$e0801$RWV0Tnr5u0fbQ3oPsHG9FA";

    private static final String SCRYPT_IV_ENCODED = "oZxuUWK5le9eCkkckKoLhw==";

    private static final String SCRYPT_CIPHERED_ENCODED = "Z4de2Bo+Zs5tpcgSp8jasXuea8tJl+vj6wqh";

    private static final String HEXADECIMAL_KEY_IV_ENCODED = "z5sz+mXn3GdEWq/qvFUbvw==";

    private static final String HEXADECIMAL_KEY_CIPHERED_ENCODED = "GtpE6cPs8zSYO2U=";

    private static final Base64.Decoder decoder = Base64.getDecoder();

    TestRunner runner;

    @BeforeEach
    void setRunner() {
        runner = TestRunners.newTestRunner(DecryptContent.class);
    }

    @Test
    void testRunInitializationVectorNotFound() {
        runner.setProperty(DecryptContent.KEY_SPECIFICATION_FORMAT, KeySpecificationFormat.PASSWORD);
        runner.setProperty(DecryptContent.KEY_SPECIFICATION, SECRET_KEY_SPECIFICATION);

        runner.enqueue(WORD);
        runner.run();

        runner.assertAllFlowFilesTransferred(DecryptContent.FAILURE);
        assertErrorLogged();
    }

    @Test
    void testRunInputNotFound() {
        runner.setProperty(DecryptContent.KEY_SPECIFICATION_FORMAT, KeySpecificationFormat.RAW);
        runner.setProperty(DecryptContent.KEY_SPECIFICATION, HEXADECIMAL_KEY_SPECIFICATION);

        final byte[] bytes = Arrays.concatenate(INITIALIZATION_VECTOR, EncodedDelimiter.IV.getDelimiter());
        runner.enqueue(bytes);
        runner.run();

        runner.assertAllFlowFilesTransferred(DecryptContent.FAILURE);
        assertErrorLogged();
    }

    @Test
    void testRunInputInvalid() {
        runner.setProperty(DecryptContent.KEY_SPECIFICATION_FORMAT, KeySpecificationFormat.RAW);
        runner.setProperty(DecryptContent.KEY_SPECIFICATION, HEXADECIMAL_KEY_SPECIFICATION);

        final byte[] bytes = Arrays.concatenate(INITIALIZATION_VECTOR, EncodedDelimiter.IV.getDelimiter(), INITIALIZATION_VECTOR);
        runner.enqueue(bytes);
        runner.run();

        runner.assertAllFlowFilesTransferred(DecryptContent.FAILURE);
        assertErrorLogged();
    }

    @Test
    void testRunAesGcmNoPaddingPasswordArgon2() throws IOException {
        setGcmNoPaddingPassword();

        final byte[] encryptedBytes = getEncryptedBytes(ARGON2_PARAMETERS.getBytes(StandardCharsets.UTF_8), ARGON2_IV_ENCODED, ARGON2_CIPHERED_ENCODED);
        runner.enqueue(encryptedBytes);
        runner.run();

        assertDecryptedSuccess();
    }

    @Test
    void testRunAesGcmNoPaddingPasswordBcrypt() throws IOException {
        setGcmNoPaddingPassword();

        final byte[] encryptedBytes = getEncryptedBytes(BCRYPT_PARAMETERS.getBytes(StandardCharsets.UTF_8), BCRYPT_IV_ENCODED, BCRYPT_CIPHERED_ENCODED);
        runner.enqueue(encryptedBytes);
        runner.run();

        assertDecryptedSuccess();
    }

    @Test
    void testRunAesGcmNoPaddingPasswordPbkdf2() throws IOException {
        setGcmNoPaddingPassword();

        final byte[] encryptedBytes = getEncryptedBytes(decoder.decode(PBKDF2_SALT_ENCODED), PBKDF2_IV_ENCODED, PBKDF2_CIPHERED_ENCODED);
        runner.enqueue(encryptedBytes);
        runner.run();

        assertDecryptedSuccess();
    }

    @Test
    void testRunAesGcmNoPaddingPasswordScrypt() throws IOException {
        setGcmNoPaddingPassword();

        final byte[] encryptedBytes = getEncryptedBytes(SCRYPT_PARAMETERS.getBytes(StandardCharsets.UTF_8), SCRYPT_IV_ENCODED, SCRYPT_CIPHERED_ENCODED);
        runner.enqueue(encryptedBytes);
        runner.run();

        assertDecryptedSuccess();
    }

    @Test
    void testRunAesCtrNoPaddingRaw() throws IOException {
        runner.setProperty(DecryptContent.KEY_SPECIFICATION_FORMAT, KeySpecificationFormat.RAW);
        runner.setProperty(DecryptContent.KEY_SPECIFICATION, HEXADECIMAL_KEY_SPECIFICATION);
        runner.setProperty(DecryptContent.CIPHER_ALGORITHM_MODE, CipherAlgorithmMode.CTR);
        runner.setProperty(DecryptContent.CIPHER_ALGORITHM_PADDING, CipherAlgorithmPadding.NO_PADDING);

        final byte[] encryptedBytes = getHexadecimalKeyEncryptedBytes();
        runner.enqueue(encryptedBytes);
        runner.run();

        assertDecryptedSuccess();
    }

    private void setGcmNoPaddingPassword() {
        runner.setProperty(DecryptContent.KEY_SPECIFICATION_FORMAT, KeySpecificationFormat.PASSWORD);
        runner.setProperty(DecryptContent.KEY_SPECIFICATION, SECRET_KEY_SPECIFICATION);
        runner.setProperty(DecryptContent.CIPHER_ALGORITHM_MODE, CipherAlgorithmMode.GCM);
        runner.setProperty(DecryptContent.CIPHER_ALGORITHM_PADDING, CipherAlgorithmPadding.NO_PADDING);
    }

    private void assertDecryptedSuccess() {
        runner.assertAllFlowFilesTransferred(DecryptContent.SUCCESS);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(DecryptContent.SUCCESS).getFirst();
        flowFile.assertContentEquals(UNENCRYPTED);
    }

    private void assertErrorLogged() {
        final List<LogMessage> errorMessages = runner.getLogger().getErrorMessages();
        final Optional<LogMessage> firstErrorMessage = errorMessages.stream().findFirst();
        assertTrue(firstErrorMessage.isPresent());
    }

    private byte[] getEncryptedBytes(
            final byte[] serializedParameters,
            final String initializationVectorEncoded,
            final String cipheredEncoded
    ) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        outputStream.write(serializedParameters);
        outputStream.write(EncodedDelimiter.SALT.getDelimiter());

        final byte[] initializationVector = decoder.decode(initializationVectorEncoded);
        outputStream.write(initializationVector);
        outputStream.write(EncodedDelimiter.IV.getDelimiter());

        final byte[] ciphered = decoder.decode(cipheredEncoded);
        outputStream.write(ciphered);

        return outputStream.toByteArray();
    }

    private byte[] getHexadecimalKeyEncryptedBytes() throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        final byte[] initializationVector = decoder.decode(HEXADECIMAL_KEY_IV_ENCODED);
        outputStream.write(initializationVector);
        outputStream.write(EncodedDelimiter.IV.getDelimiter());

        final byte[] ciphered = decoder.decode(HEXADECIMAL_KEY_CIPHERED_ENCODED);
        outputStream.write(ciphered);

        return outputStream.toByteArray();
    }
}
