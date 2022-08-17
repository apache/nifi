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
package org.apache.nifi.properties;

import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AesGcmSensitivePropertyProviderTest {
    private static final String HEXADECIMAL_KEY_32 = "12345678";

    private static final String HEXADECIMAL_KEY_128 = "12345678123456788765432187654321";

    private static final String HEXADECIMAL_KEY_256 = "1234567812345678876543218765432112345678123456788765432187654321";

    private static final String AES_GCM_128 = "aes/gcm/128";

    private static final String AES_GCM_256 = "aes/gcm/256";

    private static final ProtectedPropertyContext PROPERTY_CONTEXT = ProtectedPropertyContext.defaultContext("propertyName");

    private static final String PROPERTY_VALUE = "propertyValue";

    private static final String DELIMITER = "||";

    private static final String DELIMITER_PATTERN = "\\|\\|";

    private static final int DELIMITED_ELEMENTS = 2;

    private static final int INITIALIZATION_VECTOR_LENGTH = 12;

    @Test
    public void testInvalidKeyLength() {
        assertThrows(SensitivePropertyProtectionException.class, () -> new AesGcmSensitivePropertyProvider(HEXADECIMAL_KEY_32));
    }

    @Test
    public void testIsSupported() {
        final AesGcmSensitivePropertyProvider provider = new AesGcmSensitivePropertyProvider(HEXADECIMAL_KEY_128);
        assertTrue(provider.isSupported());
    }

    @Test
    public void testGetIdentifierKeyAesGcm128() {
        final AesGcmSensitivePropertyProvider provider = new AesGcmSensitivePropertyProvider(HEXADECIMAL_KEY_128);
        final String identifierKey = provider.getIdentifierKey();
        assertEquals(AES_GCM_128, identifierKey);
    }

    @Test
    public void testGetIdentifierKeyAesGcm256() {
        final AesGcmSensitivePropertyProvider provider = new AesGcmSensitivePropertyProvider(HEXADECIMAL_KEY_256);
        final String identifierKey = provider.getIdentifierKey();
        assertEquals(AES_GCM_256, identifierKey);
    }

    @Test
    public void testProtectUnprotectSuccess() {
        final AesGcmSensitivePropertyProvider provider = new AesGcmSensitivePropertyProvider(HEXADECIMAL_KEY_128);

        final String protectedPropertyValue = provider.protect(PROPERTY_VALUE, PROPERTY_CONTEXT);
        final String unprotectedPropertyValue = provider.unprotect(protectedPropertyValue, PROPERTY_CONTEXT);

        assertEquals(PROPERTY_VALUE, unprotectedPropertyValue);
        assertTrue(protectedPropertyValue.contains(DELIMITER));

        final String[] elements = protectedPropertyValue.split(DELIMITER_PATTERN);
        assertEquals(DELIMITED_ELEMENTS, elements.length);

        final String initializationVectorEncoded = elements[0];
        final byte[] initializationVector = Base64.getDecoder().decode(initializationVectorEncoded);
        assertEquals(INITIALIZATION_VECTOR_LENGTH, initializationVector.length);
    }

    @Test
    public void testProtectUnprotectDifferentKeyFailed() {
        final AesGcmSensitivePropertyProvider provider = new AesGcmSensitivePropertyProvider(HEXADECIMAL_KEY_128);

        final String protectedPropertyValue = provider.protect(PROPERTY_VALUE, PROPERTY_CONTEXT);

        final AesGcmSensitivePropertyProvider secondProvider = new AesGcmSensitivePropertyProvider(HEXADECIMAL_KEY_256);
        assertThrows(SensitivePropertyProtectionException.class, () -> secondProvider.unprotect(protectedPropertyValue, PROPERTY_CONTEXT));
    }

    @Test
    public void testUnprotectMinLengthRequired() {
        final AesGcmSensitivePropertyProvider provider = new AesGcmSensitivePropertyProvider(HEXADECIMAL_KEY_128);

        assertThrows(IllegalArgumentException.class, () -> provider.unprotect(HEXADECIMAL_KEY_32, PROPERTY_CONTEXT));
    }
}
