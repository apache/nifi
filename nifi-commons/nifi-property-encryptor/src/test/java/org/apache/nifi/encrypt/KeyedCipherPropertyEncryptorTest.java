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
package org.apache.nifi.encrypt;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KeyedCipherPropertyEncryptorTest {
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    private static final String PROPERTY = String.class.getName();

    private static final int ENCRYPTED_BINARY_LENGTH = 48;

    private static final String KEY_ALGORITHM = "AES";

    private static final byte[] STATIC_KEY = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6};

    private static final SecretKey SECRET_KEY = new SecretKeySpec(STATIC_KEY, KEY_ALGORITHM);

    private static final SecretKey INVALID_SECRET_KEY = new SecretKeySpec(KEY_ALGORITHM.getBytes(DEFAULT_CHARSET), KEY_ALGORITHM);

    private KeyedCipherPropertyEncryptor encryptor;

    @BeforeEach
    public void setUp() {
        encryptor = new KeyedCipherPropertyEncryptor(SECRET_KEY);
    }

    @Test
    public void testEncryptDecrypt() {
        final String encrypted = encryptor.encrypt(PROPERTY);
        final String decrypted = encryptor.decrypt(encrypted);
        assertEquals(PROPERTY, decrypted);
    }

    @Test
    public void testEncryptHexadecimalEncoded() {
        final String encrypted = encryptor.encrypt(PROPERTY);
        final byte[] decoded = HexFormat.of().parseHex(encrypted);
        assertEquals(ENCRYPTED_BINARY_LENGTH, decoded.length);
    }

    @Test
    public void testDecryptEncryptionException() {
        final String encodedProperty = HexFormat.of().formatHex(PROPERTY.getBytes(DEFAULT_CHARSET));
        assertThrows(Exception.class, () -> encryptor.decrypt(encodedProperty));
    }

    @Test
    public void testDecryptHexadecimalInvalid() {
        final String invalidProperty = String.class.getName();
        final EncryptionException exception = assertThrows(EncryptionException.class, () -> encryptor.decrypt(invalidProperty));
        assertInstanceOf(IllegalArgumentException.class, exception.getCause());
    }

    @Test
    public void testGetCipherEncryptionException() {
        encryptor = new KeyedCipherPropertyEncryptor(INVALID_SECRET_KEY);
        assertThrows(EncryptionException.class, () -> encryptor.encrypt(PROPERTY));
    }

    @Test
    public void testEqualsHashCode() {
        final KeyedCipherPropertyEncryptor equivalentEncryptor = new KeyedCipherPropertyEncryptor(SECRET_KEY);
        assertEquals(encryptor, equivalentEncryptor);
        assertEquals(encryptor.hashCode(), equivalentEncryptor.hashCode());
    }

    @Test
    public void testEqualsHashCodeDifferentSecretKey() {
        final SecretKey secretKey = new SecretKeySpec(String.class.getSimpleName().getBytes(StandardCharsets.UTF_8), KEY_ALGORITHM);
        final KeyedCipherPropertyEncryptor differentEncryptor = new KeyedCipherPropertyEncryptor(secretKey);
        assertNotEquals(encryptor, differentEncryptor);
        assertNotEquals(encryptor.hashCode(), differentEncryptor.hashCode());
    }
}
