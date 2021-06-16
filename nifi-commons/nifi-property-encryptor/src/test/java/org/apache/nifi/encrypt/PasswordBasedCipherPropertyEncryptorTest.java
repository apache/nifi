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
import org.apache.nifi.security.util.crypto.PBECipherProvider;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

public class PasswordBasedCipherPropertyEncryptorTest {
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    private static final String PROPERTY = String.class.getName();

    private static final int ENCRYPTED_BINARY_LENGTH = 48;

    @SuppressWarnings("deprecation")
    private static final PBECipherProvider CIPHER_PROVIDER = new org.apache.nifi.security.util.crypto.NiFiLegacyCipherProvider();

    private static final EncryptionMethod ENCRYPTION_METHOD = EncryptionMethod.MD5_256AES;

    private static final String PASSWORD = String.class.getName();

    private PasswordBasedCipherPropertyEncryptor encryptor;

    @Before
    public void setUp() {
        encryptor = new PasswordBasedCipherPropertyEncryptor(CIPHER_PROVIDER, ENCRYPTION_METHOD, PASSWORD);
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
    public void testEqualsHashCode() {
        final PasswordBasedCipherPropertyEncryptor equivalentEncryptor = new PasswordBasedCipherPropertyEncryptor(CIPHER_PROVIDER, ENCRYPTION_METHOD, PASSWORD);
        assertEquals(encryptor, equivalentEncryptor);
        assertEquals(encryptor.hashCode(), equivalentEncryptor.hashCode());
    }

    @Test
    public void testEqualsHashCodeDifferentPassword() {
        final PasswordBasedCipherPropertyEncryptor differentEncryptor = new PasswordBasedCipherPropertyEncryptor(CIPHER_PROVIDER, ENCRYPTION_METHOD, String.class.getSimpleName());
        assertNotEquals(encryptor, differentEncryptor);
        assertNotEquals(encryptor.hashCode(), differentEncryptor.hashCode());
    }
}
