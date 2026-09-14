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
package org.apache.nifi.security.encryption;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SensitivePropertyCodecTest {
    private static final String VALUE = "Sensitive Value";

    private static final String MULTIBYTE_VALUE = "Sensitive \u00e4\u00f6\u00fc Value";

    private static final SensitivePropertyContext CONTEXT = new SensitivePropertyContext(SensitivePropertyCategory.COMPONENT_PROPERTY, Map.of());

    private final PropertyEncryptionProvider provider = new ReversingPropertyEncryptionProvider();

    @Test
    void testEncryptDecrypt() {
        final String encrypted = SensitivePropertyCodec.encrypt(provider, VALUE, CONTEXT);
        assertNotNull(encrypted);

        final String decrypted = SensitivePropertyCodec.decrypt(provider, encrypted, CONTEXT);
        assertEquals(VALUE, decrypted);
    }

    @Test
    void testEncryptDecryptMultibyteCharacters() {
        final String encrypted = SensitivePropertyCodec.encrypt(provider, MULTIBYTE_VALUE, CONTEXT);

        final String decrypted = SensitivePropertyCodec.decrypt(provider, encrypted, CONTEXT);
        assertEquals(MULTIBYTE_VALUE, decrypted);
    }

    /**
     * The encoded representation must be the hexadecimal encoding of the bytes returned from the Provider, which is the
     * representation written inside the encryption wrapper of persisted flow configurations.
     */
    @Test
    void testEncryptHexadecimalRepresentation() {
        final PropertyEncryptionProvider passThroughProvider = new InternalPassThroughPropertyEncryptionProvider();

        final String encrypted = SensitivePropertyCodec.encrypt(passThroughProvider, VALUE, CONTEXT);

        assertEquals(HexFormat.of().formatHex(VALUE.getBytes(StandardCharsets.UTF_8)), encrypted);
    }

    @Test
    void testDecryptHexadecimalNotValid() {
        final PropertyEncryptionException exception = assertThrows(
                PropertyEncryptionException.class,
                () -> SensitivePropertyCodec.decrypt(provider, "Not Hexadecimal", CONTEXT)
        );

        assertNotNull(exception.getCause());
    }

    @Test
    void testEncryptProviderRequired() {
        assertThrows(NullPointerException.class, () -> SensitivePropertyCodec.encrypt(null, VALUE, CONTEXT));
    }

    @Test
    void testDecryptProviderRequired() {
        assertThrows(NullPointerException.class, () -> SensitivePropertyCodec.decrypt(null, "00", CONTEXT));
    }

    /**
     * Provider that reverses the supplied bytes, which distinguishes the encoded representation from the plain value
     */
    private static class ReversingPropertyEncryptionProvider implements PropertyEncryptionProvider {
        @Override
        public void initialize(final PropertyEncryptionProviderInitializationContext context) {
        }

        @Override
        public byte[] encrypt(final byte[] property, final SensitivePropertyContext context) {
            return getReversed(property);
        }

        @Override
        public byte[] decrypt(final byte[] encryptedProperty, final SensitivePropertyContext context) {
            return getReversed(encryptedProperty);
        }

        private byte[] getReversed(final byte[] bytes) {
            final byte[] reversed = new byte[bytes.length];
            for (int i = 0; i < bytes.length; i++) {
                reversed[i] = bytes[bytes.length - 1 - i];
            }
            return reversed;
        }
    }
}
