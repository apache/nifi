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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPackageUnpackageV3 {

    @Test
    public void test() throws IOException {
        final FlowFilePackager packager = new FlowFilePackagerV3();
        final FlowFileUnpackager unpackager = new FlowFileUnpackagerV3();

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

        map.put("123", ""); // replace null attribute for verification, because it is packaged as empty string
        assertEquals(map, unpackagedAttributes);
        assertArrayEquals(data, decoded);
    }

}
