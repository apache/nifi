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

package org.apache.nifi.toolkit.s2s;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class DataPacketImplTest {
    private Map<String, String> testAttributes;

    @Before
    public void setup() {
        testAttributes = new HashMap<>();
        testAttributes.put("testKey", "testVal");
    }

    @Test
    public void testPacketNulls() throws IOException {
        DataPacketImpl dataPacket = new DataPacketImpl(null, null, null);
        assertEquals(0, dataPacket.getAttributes().size());
        assertEquals(-1, dataPacket.getData().read(new byte[1]));
        assertEquals(0, dataPacket.getSize());
    }

    @Test
    public void testPacketAttributes() {
        assertEquals(Collections.unmodifiableMap(testAttributes), new DataPacketImpl(testAttributes, null, null).getAttributes());
    }

    @Test
    public void testPacketData() throws IOException {
        byte[] testData = "test data".getBytes(StandardCharsets.UTF_8);
        DataPacketImpl dataPacket = new DataPacketImpl(null, testData, null);
        assertEquals(testData.length, dataPacket.getSize());
        assertArrayEquals(testData, IOUtils.toByteArray(dataPacket.getData()));
    }

    @Test
    public void testDataFile() throws IOException {
        byte[] testData = "test data".getBytes(StandardCharsets.UTF_8);
        File tempFile = File.createTempFile("abc", "def");
        try {
            try (FileOutputStream fileOutputStream = new FileOutputStream(tempFile)) {
                fileOutputStream.write(testData);
            }
            DataPacketImpl dataPacket = new DataPacketImpl(null, null, tempFile.getAbsolutePath());
            assertEquals(testData.length, dataPacket.getSize());
            try (InputStream input = dataPacket.getData()) {
                assertArrayEquals(testData, IOUtils.toByteArray(input));
            }
        } finally {
            if (!tempFile.delete()) {
                tempFile.deleteOnExit();
            }
        }
    }
}
