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
package org.apache.nifi.security.crypto.key.pbkdf2;

import org.apache.nifi.security.crypto.key.DerivedKey;
import org.apache.nifi.security.crypto.key.DerivedKeySpec;
import org.apache.nifi.security.crypto.key.StandardDerivedKeySpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class Pbkdf2DerivedKeyProviderTest {
    private static final char[] PASSWORD = String.class.getSimpleName().toCharArray();

    private static final byte[] SALT = String.class.getName().getBytes(StandardCharsets.UTF_8);

    private static final int DERIVED_KEY_LENGTH = 16;

    private static final String SERIALIZED = "yeoZx0KZR9IzcuaOWOJbFg";

    private static final String ALGORITHM = "AES";

    private static final int ITERATIONS = 160000;

    private static final Base64.Decoder decoder = Base64.getDecoder();

    Pbkdf2DerivedKeyProvider provider;

    @BeforeEach
    void setReader() {
        provider = new Pbkdf2DerivedKeyProvider();
    }

    @Test
    void testGetDerivedKey() {
        final Pbkdf2DerivedKeyParameterSpec parameterSpec = new Pbkdf2DerivedKeyParameterSpec(ITERATIONS, SALT);
        final DerivedKeySpec<Pbkdf2DerivedKeyParameterSpec> derivedKeySpec = new StandardDerivedKeySpec<>(
                PASSWORD,
                DERIVED_KEY_LENGTH,
                ALGORITHM,
                parameterSpec
        );

        final DerivedKey derivedKey = provider.getDerivedKey(derivedKeySpec);

        assertNotNull(derivedKey);
        assertEquals(ALGORITHM, derivedKey.getAlgorithm());
        assertEquals(SERIALIZED, derivedKey.getSerialized());

        final byte[] decoded = decoder.decode(SERIALIZED);
        assertArrayEquals(decoded, derivedKey.getEncoded());
    }
}
