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

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
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

        assertEquals(Json.array(), actualJson());
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

        JsonArray resultArray = actualJson().asArray();
        assertEquals(1, resultArray.size());

        JsonObject resultObject = resultArray.get(0).asObject();
        assertEquals("test-metadata-id-1", resultObject.get("$id").asString());
        assertEquals("test-metadata-type", resultObject.get("$type").asString());
        assertEquals("enterprise", resultObject.get("$scope").asString());
        assertEquals("testTemplate", resultObject.get("$template").asString());
        assertEquals("file_12345", resultObject.get("$parent").asString());
        assertEquals("value1", resultObject.get("testField1").asString());
        assertEquals("value2", resultObject.get("testField2").asString());
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

        JsonArray resultArray = actualJson().asArray();
        assertEquals(2, resultArray.size());

        Set<String> foundIds = new HashSet<>();
        for (JsonValue value : resultArray) {
            JsonObject obj = value.asObject();
            String id = obj.get("$id").asString();
            foundIds.add(id);
            if (id.equals("test-metadata-id-1")) {
                assertEquals("test-type-1", obj.get("$type").asString());
                assertEquals("enterprise", obj.get("$scope").asString());
                assertEquals("testTemplate1", obj.get("$template").asString());
                assertEquals("file_12345", obj.get("$parent").asString());
                assertEquals("value1", obj.get("field1").asString());
                assertEquals("value2", obj.get("field2").asString());
            } else if (id.equals("test-metadata-id-2")) {
                assertEquals("test-type-2", obj.get("$type").asString());
                assertEquals("global", obj.get("$scope").asString());
                assertEquals("testTemplate2", obj.get("$template").asString());
                assertEquals("file_12345", obj.get("$parent").asString());
                assertEquals("value3", obj.get("field3").asString());
                assertEquals("value4", obj.get("field4").asString());
            }
        }
        assertEquals(2, foundIds.size());
        assertTrue(foundIds.contains("test-metadata-id-1"));
        assertTrue(foundIds.contains("test-metadata-id-2"));
    }

    private JsonValue actualJson() {
        return Json.parse(out.toString());
    }
}
