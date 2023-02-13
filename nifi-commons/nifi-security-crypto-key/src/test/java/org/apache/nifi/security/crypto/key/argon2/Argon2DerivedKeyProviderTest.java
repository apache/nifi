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
package org.apache.nifi.security.crypto.key.argon2;

import org.apache.nifi.security.crypto.key.DerivedKey;
import org.apache.nifi.security.crypto.key.DerivedKeySpec;
import org.apache.nifi.security.crypto.key.StandardDerivedKeySpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class Argon2DerivedKeyProviderTest {
    private static final char[] PASSWORD = String.class.getSimpleName().toCharArray();

    private static final int DERIVED_KEY_LENGTH = 16;

    private static final String ALGORITHM = "AES";

    private static final int MEMORY = 65536;

    private static final int ITERATIONS = 2;

    private static final int PARALLELISM = 1;

    private static final String ARGON2_SALT_STRING = "QXJnb24yU2FsdFN0cmluZw";

    private static final String PARAMETERS = String.format("$argon2id$v=19$m=%d,t=%d,p=%d$%s", MEMORY, ITERATIONS, PARALLELISM, ARGON2_SALT_STRING);

    private static final String HASH = "6LOmoOXYJV0tXBJtxtD1Mg";

    private static final String SERIALIZED = String.format("%s$%s", PARAMETERS, HASH);

    Base64.Decoder decoder = Base64.getDecoder();

    Argon2DerivedKeyProvider provider;

    @BeforeEach
    void setReader() {
        provider = new Argon2DerivedKeyProvider();
    }

    @Test
    void testGetDerivedKey() {
        final byte[] salt = decoder.decode(ARGON2_SALT_STRING);
        final Argon2DerivedKeyParameterSpec parameterSpec = new Argon2DerivedKeyParameterSpec(MEMORY, ITERATIONS, PARALLELISM, salt);
        final DerivedKeySpec<Argon2DerivedKeyParameterSpec> derivedKeySpec = new StandardDerivedKeySpec<>(
                PASSWORD,
                DERIVED_KEY_LENGTH,
                ALGORITHM,
                parameterSpec
        );

        final DerivedKey derivedKey = provider.getDerivedKey(derivedKeySpec);

        assertNotNull(derivedKey);
        assertEquals(ALGORITHM, derivedKey.getAlgorithm());
        assertEquals(SERIALIZED, derivedKey.getSerialized());

        final byte[] hashBytes = decoder.decode(HASH);
        assertArrayEquals(hashBytes, derivedKey.getEncoded());
    }
}
