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
package org.apache.nifi.toolkit.config.crypto;

import org.junit.jupiter.api.Test;

import java.util.HexFormat;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StandardDerivedKeyGeneratorTest {

    private static final int KEY_LENGTH = 32;

    @Test
    void testGetDerivedKeyEncoded() {
        final StandardDerivedKeyGenerator generator = new StandardDerivedKeyGenerator();

        final char[] random = UUID.randomUUID().toString().toCharArray();

        final String firstDerivedKey = generator.getDerivedKeyEncoded(random);
        final String secondDerivedKey = generator.getDerivedKeyEncoded(random);

        assertEquals(firstDerivedKey, secondDerivedKey);

        final byte[] decoded = HexFormat.of().parseHex(firstDerivedKey);
        assertEquals(KEY_LENGTH, decoded.length);
    }
}
