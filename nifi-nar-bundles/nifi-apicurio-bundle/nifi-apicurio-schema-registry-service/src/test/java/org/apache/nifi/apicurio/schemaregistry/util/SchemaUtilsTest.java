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
package org.apache.nifi.apicurio.schemaregistry.util;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemaUtilsTest {

    @Test
    void testCreateRecordSchema() throws SchemaNotFoundException, IOException {
        final InputStream in = getResource("schema_response.json");

        final RecordSchema schema = SchemaUtils.createRecordSchema(in, "schema1", OptionalInt.of(3));

        assertEquals("schema1", schema.getSchemaName().orElseThrow(() -> new AssertionError("Schema Name is empty")));
        assertEquals("schema_namespace_1", schema.getSchemaNamespace().orElseThrow(() -> new AssertionError("Schema Namespace is empty")));
        assertEquals("avro", schema.getSchemaFormat().orElseThrow(() -> new AssertionError("Schema Format is empty")));

        final String expectedSchemaText = IOUtils.toString(getResource("schema_response.json"), Charset.defaultCharset())
                .replace("\n", "")
                .replaceAll(" +", "");
        assertEquals(expectedSchemaText, schema.getSchemaText().orElseThrow(() -> new AssertionError("Schema Text is empty")));
    }

    private InputStream getResource(final String resourceName) {
        return this.getClass().getClassLoader().getResourceAsStream(resourceName);
    }
}
