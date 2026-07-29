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
package org.apache.nifi.util;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestPackageUnpackageV1 {
    @Test
    @SuppressWarnings("unchecked")
    void testPackageWithNonStringAttributes() {
        final FlowFilePackager packager = new FlowFilePackagerV1();
        final byte[] data = "Hello, World!".getBytes(StandardCharsets.UTF_8);
        final Map<Object, Object> map = new HashMap<>();
        map.put(12, 34);
        map.put(56, 78);
        map.put(9, 10);

        final Map<String, String> cast = (Map) map;

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ByteArrayInputStream in = new ByteArrayInputStream(data);

        assertThrows(ClassCastException.class, () -> packager.packageFlowFile(in, baos, cast, data.length));
    }

    @Test
    void testPackageAndUnpackage() throws Exception {
        final FlowFilePackager packager = new FlowFilePackagerV1();
        final FlowFileUnpackager unpackager = new FlowFileUnpackagerV1();

        final byte[] data = "Hello, World!".getBytes(StandardCharsets.UTF_8);
        final Map<String, String> map = new HashMap<>();
        map.put("abc", "cba");
        map.put("123", null);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ByteArrayInputStream in = new ByteArrayInputStream(data);
        packager.packageFlowFile(in, baos, map, data.length);

        final byte[] encoded = baos.toByteArray();
        final ByteArrayInputStream encodedIn = new ByteArrayInputStream(encoded);
        final ByteArrayOutputStream decodedOut = new ByteArrayOutputStream();
        final Map<String, String> unpackagedAttributes = unpackager.unpackageFlowFile(encodedIn, decodedOut);
        final byte[] decoded = decodedOut.toByteArray();

        /*Since the properties serialized as XML has the value null specified between the entry tags,
          when loaded into a java.util.Properties it is read as string whose value is "null" hence for
          verification must change to the expected value.*/
        map.put("123", "null");
        assertEquals(map, unpackagedAttributes);
        assertArrayEquals(data, decoded);
    }
}
