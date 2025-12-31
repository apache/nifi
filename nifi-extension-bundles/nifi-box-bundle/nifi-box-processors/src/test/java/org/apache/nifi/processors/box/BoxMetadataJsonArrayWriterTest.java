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
package org.apache.nifi.processors.box;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BoxMetadataJsonArrayWriterTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private ByteArrayOutputStream out;
    private BoxMetadataJsonArrayWriter writer;

    @BeforeEach
    void setUp() throws IOException {
        out = new ByteArrayOutputStream();
        writer = BoxMetadataJsonArrayWriter.create(out);
    }

    @Test
    void writeNoMetadata() throws IOException {
        writer.close();

        ArrayNode resultArray = (ArrayNode) OBJECT_MAPPER.readTree(out.toString());
        assertEquals(0, resultArray.size());
    }

    @Test
    void writeSingleMetadata() throws IOException {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("$id", "test-metadata-id-1");
        metadata.put("$type", "test-metadata-type");
        metadata.put("$scope", "enterprise");
        metadata.put("$template", "testTemplate");
        metadata.put("$parent", "file_12345");
        metadata.put("testField1", "value1");
        metadata.put("testField2", "value2");

        writer.write(metadata);
        writer.close();

        ArrayNode resultArray = (ArrayNode) OBJECT_MAPPER.readTree(out.toString());
        assertEquals(1, resultArray.size());

        JsonNode resultObject = resultArray.get(0);
        assertEquals("test-metadata-id-1", resultObject.get("$id").asText());
        assertEquals("test-metadata-type", resultObject.get("$type").asText());
        assertEquals("enterprise", resultObject.get("$scope").asText());
        assertEquals("testTemplate", resultObject.get("$template").asText());
        assertEquals("file_12345", resultObject.get("$parent").asText());
        assertEquals("value1", resultObject.get("testField1").asText());
        assertEquals("value2", resultObject.get("testField2").asText());
    }

    @Test
    void writeMultipleMetadata() throws IOException {
        Map<String, Object> metadata1 = new HashMap<>();
        metadata1.put("$id", "test-metadata-id-1");
        metadata1.put("$type", "test-type-1");
        metadata1.put("$scope", "enterprise");
        metadata1.put("$template", "testTemplate1");
        metadata1.put("$parent", "file_12345");
        metadata1.put("field1", "value1");
        metadata1.put("field2", "value2");

        Map<String, Object> metadata2 = new HashMap<>();
        metadata2.put("$id", "test-metadata-id-2");
        metadata2.put("$type", "test-type-2");
        metadata2.put("$scope", "global");
        metadata2.put("$template", "testTemplate2");
        metadata2.put("$parent", "file_12345");
        metadata2.put("field3", "value3");
        metadata2.put("field4", "value4");

        writer.write(metadata1);
        writer.write(metadata2);
        writer.close();

        ArrayNode resultArray = (ArrayNode) OBJECT_MAPPER.readTree(out.toString());
        assertEquals(2, resultArray.size());

        Set<String> foundIds = new HashSet<>();
        for (JsonNode node : resultArray) {
            String id = node.get("$id").asText();
            foundIds.add(id);
            if (id.equals("test-metadata-id-1")) {
                assertEquals("test-type-1", node.get("$type").asText());
                assertEquals("enterprise", node.get("$scope").asText());
                assertEquals("testTemplate1", node.get("$template").asText());
                assertEquals("file_12345", node.get("$parent").asText());
                assertEquals("value1", node.get("field1").asText());
                assertEquals("value2", node.get("field2").asText());
            } else if (id.equals("test-metadata-id-2")) {
                assertEquals("test-type-2", node.get("$type").asText());
                assertEquals("global", node.get("$scope").asText());
                assertEquals("testTemplate2", node.get("$template").asText());
                assertEquals("file_12345", node.get("$parent").asText());
                assertEquals("value3", node.get("field3").asText());
                assertEquals("value4", node.get("field4").asText());
            }
        }
        assertEquals(2, foundIds.size());
        assertTrue(foundIds.contains("test-metadata-id-1"));
        assertTrue(foundIds.contains("test-metadata-id-2"));
    }
}
