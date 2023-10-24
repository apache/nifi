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
package org.apache.nifi.pgp.service.api;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KeyIdentifierConverterTest {
    private static final long KEY_ID = Long.MAX_VALUE;

    private static final String KEY_ID_FORMATTED = "7FFFFFFFFFFFFFFF";

    private static final String INVALID = Long.class.getSimpleName();

    @Test
    public void testFormat() {
        final String formatted = KeyIdentifierConverter.format(KEY_ID);
        assertEquals(KEY_ID_FORMATTED, formatted);
    }

    @Test
    public void testParse() {
        final long parsed = KeyIdentifierConverter.parse(KEY_ID_FORMATTED);
        assertEquals(KEY_ID, parsed);
    }

    @Test
    public void testParseNumberFormatException() {
        assertThrows(NumberFormatException.class, () -> KeyIdentifierConverter.parse(INVALID));
    }
}
