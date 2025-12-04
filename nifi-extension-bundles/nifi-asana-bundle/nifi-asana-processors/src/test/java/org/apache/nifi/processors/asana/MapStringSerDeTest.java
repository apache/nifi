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
package org.apache.nifi.processors.asana;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class MapStringSerDeTest {

    private final MapStringSerDe serDe = new MapStringSerDe();

    @Test
    void testSingletonMap() throws IOException {
        final Map<String, String> expected = singletonMap("Lorem", "Ipsum");
        final Map<String, String> actual = serializeAndThenDeserialize(expected);
        assertEquals(expected, actual);
    }

    @Test
    void testMultipleMap() throws IOException {
        final Map<String, String> expected = Map.of(
                "Lorel", "Ipsum",
                "Foo", "Bar"
        );
        final Map<String, String> actual = serializeAndThenDeserialize(expected);
        assertEquals(expected, actual);
    }

    @Test
    void testDeserializingNullInput() throws IOException {
        assertNull(serDe.deserialize(null));
    }

    @Test
    void testDeserializingEmptyByteArray() throws IOException {
        assertNull(serDe.deserialize(new byte[0]));
    }

    private Map<String, String> serializeAndThenDeserialize(final Map<String, String> value) throws IOException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            serDe.serialize(value, outputStream);
            return serDe.deserialize(outputStream.toByteArray());
        }
    }
}
