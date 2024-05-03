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

package org.apache.nifi.serialization.record;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.avro.Schema;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TestMockSchemaRegistry {
    @Test
    void testGetSchemaByName() throws IOException, SchemaNotFoundException {
        final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        final MockSchemaRegistry registry = new MockSchemaRegistry();
        final Map<String, Serializable> map = new LinkedHashMap<>(3);
        map.put("name", "TestSchema");
        map.put("type", "record");
        final Map<String, String> map1 = new LinkedHashMap<>(2);
        map1.put("name", "msg");
        map1.put("type", "string");
        map.put("fields", new ArrayList<>(Collections.singletonList(map1)));
        final String schema = mapper.writeValueAsString(map);
        final RecordSchema recordSchema = AvroTypeUtil.createSchema(new Schema.Parser().parse(schema));
        registry.addSchema("simple", recordSchema);

        final SchemaIdentifier identifier = SchemaIdentifier.builder().name("simple").build();
        final RecordSchema result = registry.retrieveSchemaByName(identifier);

        assertNotNull(result, "Failed to load schema.");
        assertEquals(result.getFieldNames(), recordSchema.getFieldNames());
    }
}
