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
package org.apache.nifi.security.crypto.key.scrypt;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ScryptDerivedKeyParameterSpecReaderTest {
    private static final byte[] STRING_PARAMETERS = String.class.getSimpleName().getBytes(StandardCharsets.US_ASCII);

    private static final int COST = 16384;

    private static final int BLOCK_SIZE = 8;

    private static final int PARALLELIZATION = 1;

    private static final String SALT_ENCODED = "epIxT/h6HbbwHaehFnh/bw";

    private static final String HASH = "7H0vsXlY8UxxyW/BWx/9GuY7jEvGjT71GFd6O4SZND0";

    private static final String SERIALIZED = String.format("$s0$e0801$%s", SALT_ENCODED);

    private static final String SERIALIZED_HASH = String.format("%s$%s", SERIALIZED, HASH);

    private static final Base64.Decoder decoder = Base64.getDecoder();

    ScryptDerivedKeyParameterSpecReader reader;

    @BeforeEach
    void setReader() {
        reader = new ScryptDerivedKeyParameterSpecReader();
    }

    @Test
    void testReadException() {
        assertThrows(IllegalArgumentException.class, () -> reader.read(STRING_PARAMETERS));
    }

    @Test
    void testRead() {
        final byte[] serializedParameters = SERIALIZED.getBytes(StandardCharsets.US_ASCII);

        final ScryptDerivedKeyParameterSpec parameterSpec = reader.read(serializedParameters);

        assertParameterSpecEquals(parameterSpec);
    }

    @Test
    void testReadHash() {
        final byte[] serializedParameters = SERIALIZED_HASH.getBytes(StandardCharsets.US_ASCII);

        final ScryptDerivedKeyParameterSpec parameterSpec = reader.read(serializedParameters);

        assertParameterSpecEquals(parameterSpec);
    }

    private void assertParameterSpecEquals(final ScryptDerivedKeyParameterSpec parameterSpec) {
        assertNotNull(parameterSpec);
        assertEquals(COST, parameterSpec.getCost());
        assertEquals(BLOCK_SIZE, parameterSpec.getBlockSize());
        assertEquals(PARALLELIZATION, parameterSpec.getParallelization());

        final byte[] salt = decoder.decode(SALT_ENCODED);
        assertArrayEquals(salt, parameterSpec.getSalt());
    }
}
