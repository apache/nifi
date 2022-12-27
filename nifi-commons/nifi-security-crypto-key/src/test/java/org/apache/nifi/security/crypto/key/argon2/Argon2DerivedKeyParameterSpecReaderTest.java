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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class Argon2DerivedKeyParameterSpecReaderTest {
    private static final byte[] STRING_PARAMETERS = String.class.getSimpleName().getBytes(StandardCharsets.US_ASCII);

    private static final int MEMORY = 65536;

    private static final int ITERATIONS = 2;

    private static final int PARALLELISM = 1;

    private static final String ARGON2_SALT_STRING = "QXJnb24yU2FsdFN0cmluZw";

    private static final String PARAMETERS = String.format("$argon2id$v=19$m=%d,t=%d,p=%d$%s", MEMORY, ITERATIONS, PARALLELISM, ARGON2_SALT_STRING);

    private static final String PARAMETERS_HASH = String.format("%s$6LOmoOXYJV0tXBJtxtD1Mg", PARAMETERS);

    Base64.Decoder decoder = Base64.getDecoder();

    Argon2DerivedKeyParameterSpecReader reader;

    @BeforeEach
    void setReader() {
        reader = new Argon2DerivedKeyParameterSpecReader();
    }

    @Test
    void testReadException() {
        assertThrows(IllegalArgumentException.class, () -> reader.read(STRING_PARAMETERS));
    }

    @Test
    void testRead() {
        final byte[] serializedParameters = PARAMETERS.getBytes(StandardCharsets.US_ASCII);

        final Argon2DerivedKeyParameterSpec parameterSpec = reader.read(serializedParameters);

        assertParameterSpecEquals(parameterSpec);
    }

    @Test
    void testReadHash() {
        final byte[] serializedParameters = PARAMETERS_HASH.getBytes(StandardCharsets.US_ASCII);

        final Argon2DerivedKeyParameterSpec parameterSpec = reader.read(serializedParameters);

        assertParameterSpecEquals(parameterSpec);
    }

    private void assertParameterSpecEquals(final Argon2DerivedKeyParameterSpec parameterSpec) {
        assertNotNull(parameterSpec);
        assertEquals(MEMORY, parameterSpec.getMemory());
        assertEquals(ITERATIONS, parameterSpec.getIterations());
        assertEquals(PARALLELISM, parameterSpec.getParallelism());

        final byte[] salt = decoder.decode(ARGON2_SALT_STRING);
        assertArrayEquals(salt, parameterSpec.getSalt());
    }
}
