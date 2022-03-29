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
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.Security;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KeyedEncryptorTest {
    private static final byte[] SECRET_KEY_BYTES = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 7, 6, 5, 4, 3, 2, 1, 0};

    private static final SecretKey SECRET_KEY = new SecretKeySpec(SECRET_KEY_BYTES, "AES");

    private static final byte[] INITIALIZATION_VECTOR = new byte[]{7, 6, 5, 4, 3, 2, 1, 0, 0, 1, 2, 3, 4, 5, 6, 7};

    private static final byte[] PLAINTEXT = new byte[]{9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

    private static final EncryptionMethod ENCRYPTION_METHOD = EncryptionMethod.AES_GCM;

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    @Test
    public void testEncryptDecrypt() throws IOException {
        final KeyedEncryptor encryptor = new KeyedEncryptor(ENCRYPTION_METHOD, SECRET_KEY);

        assertEncryptDecryptMatched(encryptor, encryptor);
    }

    @Test
    public void testEncryptDecryptWithInitializationVector() throws IOException {
        final KeyedEncryptor encryptor = new KeyedEncryptor(ENCRYPTION_METHOD, SECRET_KEY, INITIALIZATION_VECTOR);
        final KeyedEncryptor decryptor = new KeyedEncryptor(ENCRYPTION_METHOD, SECRET_KEY);

        assertEncryptDecryptMatched(encryptor, decryptor);
    }

    @Test
    public void testEncryptDecryptInitializationVectorRemoved() throws IOException {
        final KeyedEncryptor encryptor = new KeyedEncryptor(ENCRYPTION_METHOD, SECRET_KEY);

        final StreamCallback encryption = encryptor.getEncryptionCallback();
        final StreamCallback decryption = encryptor.getDecryptionCallback();

        final ByteArrayOutputStream encryptedOutputStream = new ByteArrayOutputStream();
        encryption.process(new ByteArrayInputStream(PLAINTEXT), encryptedOutputStream);

        final byte[] encryptedBytes = encryptedOutputStream.toByteArray();
        final byte[] encryptedBytesInitializationVectorRemoved = ArrayUtils.subarray(encryptedBytes, INITIALIZATION_VECTOR.length, encryptedBytes.length);

        final ByteArrayInputStream encryptedInputStream = new ByteArrayInputStream(encryptedBytesInitializationVectorRemoved);
        final ByteArrayOutputStream decryptedOutputStream = new ByteArrayOutputStream();
        assertThrows(ProcessException.class, () -> decryption.process(encryptedInputStream, decryptedOutputStream));
    }

    @Test
    public void testEncryptDecryptInitializationVectorDelimiterRemoved() throws IOException {
        final KeyedEncryptor encryptor = new KeyedEncryptor(ENCRYPTION_METHOD, SECRET_KEY);

        final StreamCallback encryption = encryptor.getEncryptionCallback();
        final StreamCallback decryption = encryptor.getDecryptionCallback();

        final ByteArrayOutputStream encryptedOutputStream = new ByteArrayOutputStream();
        encryption.process(new ByteArrayInputStream(PLAINTEXT), encryptedOutputStream);

        final byte[] encryptedBytes = encryptedOutputStream.toByteArray();
        final int startIndex = INITIALIZATION_VECTOR.length + KeyedCipherProvider.IV_DELIMITER.length;
        final byte[] encryptedBytesInitializationVectorRemoved = ArrayUtils.subarray(encryptedBytes, startIndex, encryptedBytes.length);

        final ByteArrayInputStream encryptedInputStream = new ByteArrayInputStream(encryptedBytesInitializationVectorRemoved);
        final ByteArrayOutputStream decryptedOutputStream = new ByteArrayOutputStream();
        assertThrows(ProcessException.class, () -> decryption.process(encryptedInputStream, decryptedOutputStream));
    }

    private void assertEncryptDecryptMatched(final KeyedEncryptor encryptor, final KeyedEncryptor decryptor) throws IOException {
        final StreamCallback encryption = encryptor.getEncryptionCallback();
        final StreamCallback decryption = decryptor.getDecryptionCallback();

        final ByteArrayOutputStream encryptedOutputStream = new ByteArrayOutputStream();
        encryption.process(new ByteArrayInputStream(PLAINTEXT), encryptedOutputStream);

        final ByteArrayInputStream encryptedInputStream = new ByteArrayInputStream(encryptedOutputStream.toByteArray());
        final ByteArrayOutputStream decryptedOutputStream = new ByteArrayOutputStream();
        decryption.process(encryptedInputStream, decryptedOutputStream);

        final byte[] decrypted = decryptedOutputStream.toByteArray();
        assertArrayEquals(PLAINTEXT, decrypted);
    }
}
