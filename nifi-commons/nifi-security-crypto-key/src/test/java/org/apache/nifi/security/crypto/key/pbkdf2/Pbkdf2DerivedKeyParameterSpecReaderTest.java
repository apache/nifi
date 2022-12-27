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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class Pbkdf2DerivedKeyParameterSpecReaderTest {
    private static final String STRING_PARAMETERS = String.class.getName();

    Pbkdf2DerivedKeyParameterSpecReader reader;

    @BeforeEach
    void setReader() {
        reader = new Pbkdf2DerivedKeyParameterSpecReader();
    }

    @Test
    void testRead() {
        final byte[] serializedParameters = STRING_PARAMETERS.getBytes(StandardCharsets.US_ASCII);

        final Pbkdf2DerivedKeyParameterSpec parameterSpec = reader.read(serializedParameters);

        assertNotNull(parameterSpec);
        assertEquals(Pbkdf2DerivedKeyParameterSpecReader.VERSION_0_5_0_ITERATIONS, parameterSpec.getIterations());
        assertArrayEquals(serializedParameters, parameterSpec.getSalt());
    }
}
