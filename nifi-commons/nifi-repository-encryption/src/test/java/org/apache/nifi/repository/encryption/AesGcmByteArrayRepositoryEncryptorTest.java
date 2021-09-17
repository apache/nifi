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
package org.apache.nifi.repository.encryption;

import org.apache.nifi.repository.encryption.configuration.EncryptionMetadataHeader;
import org.apache.nifi.security.kms.KeyProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.SecureRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AesGcmByteArrayRepositoryEncryptorTest {
    private static final String RECORD_ID = "primary-record";

    private static final String KEY_ID = "primary-key";

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private static final int KEY_LENGTH = 16;

    private static final String KEY_ALGORITHM = "AES";

    private static final String PLAINTEXT = "RECORD";

    private static final Charset CHARSET = StandardCharsets.UTF_8;

    private static final byte[] PLAINTEXT_RECORD = PLAINTEXT.getBytes(CHARSET);

    private static SecretKey secretKey;

    @Mock
    private KeyProvider keyProvider;

    @BeforeAll
    public static void setKey() {
        final byte[] key = new byte[KEY_LENGTH];
        SECURE_RANDOM.nextBytes(key);
        secretKey = new SecretKeySpec(key, KEY_ALGORITHM);
    }

    @Test
    public void testEncryptDecryptContentRecord() throws KeyManagementException {
        assertEncryptDecryptEquals(EncryptionMetadataHeader.CONTENT);
    }

    @Test
    public void testEncryptDecryptProvenanceRecord() throws KeyManagementException {
        assertEncryptDecryptEquals(EncryptionMetadataHeader.PROVENANCE);
    }

    @Test
    public void testDecryptEmptyByteArrayFailed() {
        final AesGcmByteArrayRepositoryEncryptor encryptor = new AesGcmByteArrayRepositoryEncryptor(keyProvider, EncryptionMetadataHeader.CONTENT);
        assertThrows(RepositoryEncryptionException.class, () -> encryptor.decrypt(new byte[0], RECORD_ID));
    }

    private void assertEncryptDecryptEquals(final EncryptionMetadataHeader encryptionMetadataHeader) throws KeyManagementException {
        setKeyProvider();
        final AesGcmByteArrayRepositoryEncryptor encryptor = new AesGcmByteArrayRepositoryEncryptor(keyProvider, encryptionMetadataHeader);
        final byte[] encrypted = encryptor.encrypt(PLAINTEXT_RECORD, RECORD_ID, KEY_ID);
        final byte[] decrypted = encryptor.decrypt(encrypted, RECORD_ID);
        assertEquals(PLAINTEXT_RECORD.length, decrypted.length);
        final String decryptedRecord = new String(decrypted, CHARSET);
        assertEquals(PLAINTEXT, decryptedRecord);
    }

    private void setKeyProvider() throws KeyManagementException {
        when(keyProvider.keyExists(eq(KEY_ID))).thenReturn(true);
        when(keyProvider.getKey(eq(KEY_ID))).thenReturn(secretKey);
    }
}
