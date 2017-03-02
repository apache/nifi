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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class TestPackageUnpackageV3 {

    @Test
    public void test() throws IOException {
        final FlowFilePackager packager = new FlowFilePackagerV3();
        final FlowFileUnpackager unpackager = new FlowFileUnpackagerV3();

        final byte[] data = "Hello, World!".getBytes("UTF-8");
        final Map<String, String> map = new HashMap<>();
        map.put("abc", "cba");

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ByteArrayInputStream in = new ByteArrayInputStream(data);
        packager.packageFlowFile(in, baos, map, data.length);

        final byte[] encoded = baos.toByteArray();
        final ByteArrayInputStream encodedIn = new ByteArrayInputStream(encoded);
        final ByteArrayOutputStream decodedOut = new ByteArrayOutputStream();
        final Map<String, String> unpackagedAttributes = unpackager.unpackageFlowFile(encodedIn, decodedOut);
        final byte[] decoded = decodedOut.toByteArray();

        assertEquals(map, unpackagedAttributes);
        assertTrue(Arrays.equals(data, decoded));
    }

}
