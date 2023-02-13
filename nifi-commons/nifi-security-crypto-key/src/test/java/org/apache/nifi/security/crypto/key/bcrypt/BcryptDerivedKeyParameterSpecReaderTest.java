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
package org.apache.nifi.security.crypto.key.bcrypt;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BcryptDerivedKeyParameterSpecReaderTest {
    private static final byte[] STRING_PARAMETERS = String.class.getSimpleName().getBytes(StandardCharsets.US_ASCII);

    private static final int COST = 12;

    private static final String SALT_ENCODED = "R9h/cIPz0gi.URNNX3kh2O";

    private static final String HASH = "PST9/PgBkqquzi.Ss7KIUgO2t0jWMUW";

    private static final String SERIALIZED = String.format("$2a$%d$%s", COST, SALT_ENCODED);

    private static final String SERIALIZED_HASH = String.format("%s%s", SERIALIZED, HASH);

    BcryptDerivedKeyParameterSpecReader reader;

    @BeforeEach
    void setReader() {
        reader = new BcryptDerivedKeyParameterSpecReader();
    }

    @Test
    void testReadException() {
        assertThrows(IllegalArgumentException.class, () -> reader.read(STRING_PARAMETERS));
    }

    @Test
    void testRead() {
        final byte[] serializedParameters = SERIALIZED.getBytes(StandardCharsets.US_ASCII);

        final BcryptDerivedKeyParameterSpec parameterSpec = reader.read(serializedParameters);

        assertParameterSpecEquals(parameterSpec);
    }

    @Test
    void testReadHash() {
        final byte[] serializedParameters = SERIALIZED_HASH.getBytes(StandardCharsets.US_ASCII);

        final BcryptDerivedKeyParameterSpec parameterSpec = reader.read(serializedParameters);

        assertParameterSpecEquals(parameterSpec);
    }

    private void assertParameterSpecEquals(final BcryptDerivedKeyParameterSpec parameterSpec) {
        assertNotNull(parameterSpec);
        assertEquals(COST, parameterSpec.getCost());

        final byte[] salt = BcryptBase64Decoder.decode(SALT_ENCODED);
        assertArrayEquals(salt, parameterSpec.getSalt());
    }
}
