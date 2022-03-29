/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Security;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PasswordBasedEncryptorTest {
    private static final String TEST_RESOURCES_PATH = "src/test/resources/TestEncryptContent";

    private static final char[] FILE_PASSWORD = "thisIsABadPassword".toCharArray();

    private static final Path TEST_SALTED_PATH = Paths.get(String.format("%s/salted_128_raw.enc", TEST_RESOURCES_PATH));

    private static final Path TEST_UNSALTED_PATH = Paths.get(String.format("%s/unsalted_128_raw.enc", TEST_RESOURCES_PATH));

    private static final Path TEST_PLAIN_PATH = Paths.get(String.format("%s/plain.txt", TEST_RESOURCES_PATH));

    private static final byte[] PLAINTEXT = new byte[]{9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

    private static final char[] PASSWORD = new char[]{'a', 'b', 'c', 'd', 'e', 'f', 'g'};

    private static final int SALT_LENGTH = RandomIVPBECipherProvider.SALT_DELIMITER.length;

    private static final String INITIALIZATION_VECTOR_LENGTH = Integer.toString(RandomIVPBECipherProvider.MAX_IV_LIMIT);

    private static final String IV_ATTRIBUTE = "iv";

    private static final EncryptionMethod PBE_ENCRYPTION_METHOD = EncryptionMethod.SHA256_256AES;

    private static final EncryptionMethod KDF_ENCRYPTION_METHOD = EncryptionMethod.AES_GCM;

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    @Test
    public void testEncryptDecryptLegacy() throws IOException {
        final PasswordBasedEncryptor encryptor = new PasswordBasedEncryptor(PBE_ENCRYPTION_METHOD, PASSWORD, KeyDerivationFunction.NIFI_LEGACY);

        assertEncryptDecryptMatched(encryptor);
    }

    @Test
    public void testEncryptDecryptOpenSsl() throws IOException {
        final PasswordBasedEncryptor encryptor = new PasswordBasedEncryptor(PBE_ENCRYPTION_METHOD, PASSWORD, KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY);

        assertEncryptDecryptMatched(encryptor);
    }

    @Test
    public void testEncryptDecryptBcrypt() throws IOException {
        final PasswordBasedEncryptor encryptor = new PasswordBasedEncryptor(KDF_ENCRYPTION_METHOD, PASSWORD, KeyDerivationFunction.BCRYPT);

        assertEncryptDecryptMatched(encryptor);
    }

    @Test
    public void testEncryptDecryptScrypt() throws IOException {
        final PasswordBasedEncryptor encryptor = new PasswordBasedEncryptor(KDF_ENCRYPTION_METHOD, PASSWORD, KeyDerivationFunction.SCRYPT);

        assertEncryptDecryptMatched(encryptor);
    }

    @Test
    public void testEncryptDecryptPbkdf2() throws IOException {
        final PasswordBasedEncryptor encryptor = new PasswordBasedEncryptor(KDF_ENCRYPTION_METHOD, PASSWORD, KeyDerivationFunction.PBKDF2);

        assertEncryptDecryptMatched(encryptor);
    }

    @Test
    public void testDecryptOpenSslSalted() throws IOException {
        final PasswordBasedEncryptor encryptor = new PasswordBasedEncryptor(EncryptionMethod.MD5_128AES, FILE_PASSWORD, KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY);

        final byte[] plainBytes = Files.readAllBytes(TEST_PLAIN_PATH);
        final byte[] encryptedBytes = Files.readAllBytes(TEST_SALTED_PATH);

        assertDecryptMatched(encryptor, encryptedBytes, plainBytes);
    }

    @Test
    public void testDecryptOpenSslUnsalted() throws IOException {
        final PasswordBasedEncryptor encryptor = new PasswordBasedEncryptor(EncryptionMethod.MD5_128AES, FILE_PASSWORD, KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY);

        final byte[] plainBytes = Files.readAllBytes(TEST_PLAIN_PATH);
        final byte[] encryptedBytes = Files.readAllBytes(TEST_UNSALTED_PATH);

        assertDecryptMatched(encryptor, encryptedBytes, plainBytes);
    }

    @Test
    public void testEncryptDecryptArgon2SkippedSaltMissing() throws IOException {
        final PasswordBasedEncryptor encryptor = new PasswordBasedEncryptor(KDF_ENCRYPTION_METHOD, PASSWORD, KeyDerivationFunction.ARGON2);
        final StreamCallback decryption = encryptor.getDecryptionCallback();

        final byte[] encryptedBytes = encryptBytes(encryptor);

        final ByteArrayInputStream encryptedInputStream = new ByteArrayInputStream(encryptedBytes);
        final long skipped = encryptedInputStream.skip(SALT_LENGTH);
        assertEquals(SALT_LENGTH, skipped);

        final ByteArrayOutputStream decryptedOutputStream = new ByteArrayOutputStream();
        assertThrows(ProcessException.class, () -> decryption.process(encryptedInputStream, decryptedOutputStream));
    }

    @Test
    public void testEncryptDecryptArgon2SaltDelimiterMissing() throws IOException {
        final PasswordBasedEncryptor encryptor = new PasswordBasedEncryptor(KDF_ENCRYPTION_METHOD, PASSWORD, KeyDerivationFunction.ARGON2);
        final StreamCallback decryption = encryptor.getDecryptionCallback();

        final byte[] encryptedBytes = encryptBytes(encryptor);
        final byte[] delimiterRemoved = ArrayUtils.removeElements(encryptedBytes, RandomIVPBECipherProvider.SALT_DELIMITER);

        final ByteArrayInputStream encryptedInputStream = new ByteArrayInputStream(delimiterRemoved);
        final ByteArrayOutputStream decryptedOutputStream = new ByteArrayOutputStream();
        assertThrows(ProcessException.class, () -> decryption.process(encryptedInputStream, decryptedOutputStream));
    }

    @Test
    public void testEncryptDecryptArgon2InitializationVectorMissing() throws IOException {
        final PasswordBasedEncryptor encryptor = new PasswordBasedEncryptor(KDF_ENCRYPTION_METHOD, PASSWORD, KeyDerivationFunction.ARGON2);
        final StreamCallback decryption = encryptor.getDecryptionCallback();

        final StreamCallback encryption = encryptor.getEncryptionCallback();

        final ByteArrayOutputStream encryptedOutputStream = new ByteArrayOutputStream();
        encryption.process(new ByteArrayInputStream(PLAINTEXT), encryptedOutputStream);
        final byte[] encryptedBytes = encryptedOutputStream.toByteArray();

        final String initializationVectorAttribute = encryptor.flowfileAttributes.get(getAttributeName(IV_ATTRIBUTE));
        final byte[] initializationVector = initializationVectorAttribute.getBytes(StandardCharsets.UTF_8);

        final byte[] encryptedBytesUpdated = ArrayUtils.removeElements(encryptedBytes, initializationVector);

        final ByteArrayInputStream encryptedInputStream = new ByteArrayInputStream(encryptedBytesUpdated);
        final ByteArrayOutputStream decryptedOutputStream = new ByteArrayOutputStream();
        assertThrows(ProcessException.class, () -> decryption.process(encryptedInputStream, decryptedOutputStream));
    }

    @Test
    public void testEncryptDecryptArgon2InitializationVectorDelimiterMissing() throws IOException {
        final PasswordBasedEncryptor encryptor = new PasswordBasedEncryptor(KDF_ENCRYPTION_METHOD, PASSWORD, KeyDerivationFunction.ARGON2);
        final StreamCallback decryption = encryptor.getDecryptionCallback();

        final byte[] encryptedBytes = encryptBytes(encryptor);
        final byte[] encryptedBytesUpdated = ArrayUtils.removeElements(encryptedBytes, RandomIVPBECipherProvider.IV_DELIMITER);

        final ByteArrayInputStream encryptedInputStream = new ByteArrayInputStream(encryptedBytesUpdated);
        final ByteArrayOutputStream decryptedOutputStream = new ByteArrayOutputStream();
        assertThrows(ProcessException.class, () -> decryption.process(encryptedInputStream, decryptedOutputStream));
    }

    private byte[] encryptBytes(final PasswordBasedEncryptor encryptor) throws IOException {
        final StreamCallback encryption = encryptor.getEncryptionCallback();

        final ByteArrayOutputStream encryptedOutputStream = new ByteArrayOutputStream();
        encryption.process(new ByteArrayInputStream(PLAINTEXT), encryptedOutputStream);

        return encryptedOutputStream.toByteArray();
    }

    private void assertEncryptDecryptMatched(final PasswordBasedEncryptor encryptor) throws IOException {
        final StreamCallback encryption = encryptor.getEncryptionCallback();
        final StreamCallback decryption = encryptor.getDecryptionCallback();

        final ByteArrayOutputStream encryptedOutputStream = new ByteArrayOutputStream();
        encryption.process(new ByteArrayInputStream(PLAINTEXT), encryptedOutputStream);

        final ByteArrayInputStream encryptedInputStream = new ByteArrayInputStream(encryptedOutputStream.toByteArray());
        final ByteArrayOutputStream decryptedOutputStream = new ByteArrayOutputStream();
        decryption.process(encryptedInputStream, decryptedOutputStream);

        final byte[] decrypted = decryptedOutputStream.toByteArray();
        assertArrayEquals(PLAINTEXT, decrypted);

        assertFlowFileAttributesFound(encryptor.flowfileAttributes);
    }

    private void assertFlowFileAttributesFound(final Map<String, String> attributes) {
        assertTrue(attributes.containsKey(getAttributeName("algorithm")));
        assertTrue(attributes.containsKey(getAttributeName("timestamp")));
        assertTrue(attributes.containsKey(getAttributeName("cipher_text_length")));
        assertEquals("decrypted", attributes.get(getAttributeName("action")));
        assertEquals(Integer.toString(PLAINTEXT.length), attributes.get(getAttributeName("plaintext_length")));
        assertTrue(attributes.containsKey(getAttributeName("salt")));
        assertTrue(attributes.containsKey(getAttributeName("salt_length")));
        assertTrue(attributes.containsKey(getAttributeName(IV_ATTRIBUTE)));
        assertEquals(INITIALIZATION_VECTOR_LENGTH, attributes.get(getAttributeName("iv_length")));
        assertTrue(attributes.containsKey(getAttributeName("kdf")));
    }

    private void assertDecryptMatched(final PasswordBasedEncryptor encryptor, final byte[] encrypted, final byte[] expected) throws IOException {
        final StreamCallback decryption = encryptor.getDecryptionCallback();

        final ByteArrayOutputStream decryptedOutputStream = new ByteArrayOutputStream();
        decryption.process(new ByteArrayInputStream(encrypted), decryptedOutputStream);

        final byte[] decrypted = decryptedOutputStream.toByteArray();
        assertArrayEquals(expected, decrypted);
    }

    private String getAttributeName(final String name) {
        return String.format("encryptcontent.%s", name);
    }
}
