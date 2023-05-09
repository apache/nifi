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

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class GenericObjectSerDeTest {

    @Test
    public void testString1() throws IOException {
        String expected = "Lorem Ipsum";
        String actual = serializeAndThenDeserialize(expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testString2() throws IOException {
        String expected = "Foo Bar";
        String actual = serializeAndThenDeserialize(expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testMap1() throws IOException {
        Map<String, String> expected = singletonMap("Lorem", "Ipsum");
        Map<String, String> actual = serializeAndThenDeserialize(expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testMap2() throws IOException {
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("Lorem", "Ipsum");
        expected.put("Foo", "Bar");
        Map<String, String> actual = serializeAndThenDeserialize(expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testMap3() throws IOException {
        Map<String, Map<String, Integer>> expected = new LinkedHashMap<>();
        expected.put("Lorem", singletonMap("Ipsum", 1));
        expected.put("Foo", singletonMap("Bar", 2));
        Map<String, Map<String, Integer>> actual = serializeAndThenDeserialize(expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testDeserializingNullInput() throws IOException {
        assertNull(new GenericObjectSerDe<>().deserialize(null));
    }

    @Test
    public void testDeserializingEmptyByteArray() throws IOException {
        assertNull(new GenericObjectSerDe<>().deserialize(new byte[0]));
    }

    private <V> V serializeAndThenDeserialize(V value) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            new GenericObjectSerDe<V>().serialize(value, bos);
            return new GenericObjectSerDe<V>().deserialize(bos.toByteArray());
        }
    }
}
