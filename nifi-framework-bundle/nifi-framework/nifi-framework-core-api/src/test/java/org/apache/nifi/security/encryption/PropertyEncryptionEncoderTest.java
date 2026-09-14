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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PropertyEncryptionEncoderTest {

    private static final String ENCRYPTED_VALUE = "0123456789abcdef";

    private static final String ENCODED_VALUE = "enc{0123456789abcdef}";

    private static final String PARAMETER_REFERENCE = "#{Parameter}";

    @Test
    void testIsEncrypted() {
        assertTrue(PropertyEncryptionEncoder.isEncrypted(ENCODED_VALUE));
    }

    @Test
    void testIsEncryptedNull() {
        assertFalse(PropertyEncryptionEncoder.isEncrypted(null));
    }

    @Test
    void testIsEncryptedPlaintext() {
        assertFalse(PropertyEncryptionEncoder.isEncrypted(ENCRYPTED_VALUE));
        assertFalse(PropertyEncryptionEncoder.isEncrypted(PARAMETER_REFERENCE));
    }

    @Test
    void testIsEncryptedSuffixNotFound() {
        assertFalse(PropertyEncryptionEncoder.isEncrypted("enc{0123456789abcdef"));
    }

    @Test
    void testGetEncoded() {
        assertEquals(ENCODED_VALUE, PropertyEncryptionEncoder.getEncoded(ENCRYPTED_VALUE));
    }

    @Test
    void testGetEncodedNull() {
        assertThrows(NullPointerException.class, () -> PropertyEncryptionEncoder.getEncoded(null));
    }

    @Test
    void testGetDecoded() {
        assertEquals(ENCRYPTED_VALUE, PropertyEncryptionEncoder.getDecoded(ENCODED_VALUE));
    }

    @Test
    void testGetDecodedNotEncoded() {
        assertThrows(IllegalArgumentException.class, () -> PropertyEncryptionEncoder.getDecoded(ENCRYPTED_VALUE));
        assertThrows(IllegalArgumentException.class, () -> PropertyEncryptionEncoder.getDecoded(null));
    }

    @Test
    void testGetEncodedGetDecodedRoundTrip() {
        final String encoded = PropertyEncryptionEncoder.getEncoded(ENCRYPTED_VALUE);
        assertTrue(PropertyEncryptionEncoder.isEncrypted(encoded));
        assertEquals(ENCRYPTED_VALUE, PropertyEncryptionEncoder.getDecoded(encoded));
    }
}
