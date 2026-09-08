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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class InternalPassThroughPropertyEncryptionProviderTest {
    private static final String VALUE = "Sensitive Value";

    private static final SensitivePropertyContext CONTEXT = new SensitivePropertyContext(SensitivePropertyCategory.COMPONENT_PROPERTY, Map.of());

    private final InternalPassThroughPropertyEncryptionProvider provider = new InternalPassThroughPropertyEncryptionProvider();

    @Test
    void testEncryptReturnsProperty() {
        final byte[] property = VALUE.getBytes(StandardCharsets.UTF_8);

        assertArrayEquals(property, provider.encrypt(property, CONTEXT));
    }

    @Test
    void testDecryptReturnsEncryptedProperty() {
        final byte[] encryptedProperty = VALUE.getBytes(StandardCharsets.UTF_8);

        assertArrayEquals(encryptedProperty, provider.decrypt(encryptedProperty, CONTEXT));
    }

    @Test
    void testCodecRoundTrip() {
        final String encoded = SensitivePropertyCodec.encrypt(provider, VALUE, CONTEXT);
        assertNotEquals(VALUE, encoded);

        final String decoded = SensitivePropertyCodec.decrypt(provider, encoded, CONTEXT);
        assertEquals(VALUE, decoded);
    }
}
