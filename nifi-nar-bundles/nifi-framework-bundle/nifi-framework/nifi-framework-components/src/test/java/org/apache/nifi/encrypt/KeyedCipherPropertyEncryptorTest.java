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

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.crypto.AESKeyedCipherProvider;
import org.apache.nifi.security.util.crypto.KeyedCipherProvider;
import org.apache.nifi.util.StringUtils;
import org.junit.Before;
import org.junit.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

public class KeyedCipherPropertyEncryptorTest {
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    private static final String PROPERTY = String.class.getName();

    private static final int ENCRYPTED_BINARY_LENGTH = 48;

    private static final KeyedCipherProvider CIPHER_PROVIDER = new AESKeyedCipherProvider();

    private static final EncryptionMethod ENCRYPTION_METHOD = EncryptionMethod.AES_GCM;

    private static final String KEY_ALGORITHM = "AES";

    private static final byte[] STATIC_KEY = StringUtils.repeat("KEY", 8).getBytes(DEFAULT_CHARSET);

    private static final SecretKey SECRET_KEY = new SecretKeySpec(STATIC_KEY, KEY_ALGORITHM);

    private static final SecretKey INVALID_SECRET_KEY = new SecretKeySpec(KEY_ALGORITHM.getBytes(DEFAULT_CHARSET), KEY_ALGORITHM);

    private KeyedCipherPropertyEncryptor encryptor;

    @Before
    public void setUp() {
        encryptor = new KeyedCipherPropertyEncryptor(CIPHER_PROVIDER, ENCRYPTION_METHOD, SECRET_KEY);
    }

    @Test
    public void testEncryptDecrypt() {
        final String encrypted = encryptor.encrypt(PROPERTY);
        final String decrypted = encryptor.decrypt(encrypted);
        assertEquals(PROPERTY, decrypted);
    }

    @Test
    public void testEncryptHexadecimalEncoded() throws DecoderException {
        final String encrypted = encryptor.encrypt(PROPERTY);
        final byte[] decoded = Hex.decodeHex(encrypted);
        assertEquals(ENCRYPTED_BINARY_LENGTH, decoded.length);
    }

    @Test
    public void testDecryptEncryptionException() {
        final String encodedProperty = Hex.encodeHexString(PROPERTY.getBytes(DEFAULT_CHARSET));
        assertThrows(EncryptionException.class, () -> encryptor.decrypt(encodedProperty));
    }

    @Test
    public void testGetCipherEncryptionException() {
        encryptor = new KeyedCipherPropertyEncryptor(CIPHER_PROVIDER, ENCRYPTION_METHOD, INVALID_SECRET_KEY);
        assertThrows(EncryptionException.class, () -> encryptor.encrypt(PROPERTY));
    }

    @Test
    public void testEqualsHashCode() {
        final KeyedCipherPropertyEncryptor equivalentEncryptor = new KeyedCipherPropertyEncryptor(CIPHER_PROVIDER, ENCRYPTION_METHOD, SECRET_KEY);
        assertEquals(encryptor, equivalentEncryptor);
        assertEquals(encryptor.hashCode(), equivalentEncryptor.hashCode());
    }

    @Test
    public void testEqualsHashCodeDifferentSecretKey() {
        final SecretKey secretKey = new SecretKeySpec(String.class.getSimpleName().getBytes(StandardCharsets.UTF_8), KEY_ALGORITHM);
        final KeyedCipherPropertyEncryptor differentEncryptor = new KeyedCipherPropertyEncryptor(CIPHER_PROVIDER, ENCRYPTION_METHOD, secretKey);
        assertNotEquals(encryptor, differentEncryptor);
        assertNotEquals(encryptor.hashCode(), differentEncryptor.hashCode());
    }
}
