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
package org.apache.nifi.schemaregistry.services;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonSchemaRegistryServiceTest {

    private static final String SIMPLE_SCHEMA = """
        {
          "title": "Example",
          "type": "object",
          "properties": {
            "id": { "type": "string" }
          },
          "required": ["id"],
          "additionalProperties": false
        }
        """;

    @Test
    void testSchemaRetrievalByName() throws IOException, SchemaNotFoundException {
        final JsonSchemaRegistryService service = new JsonSchemaRegistryService();
        final PropertyDescriptor descriptor = service.getSupportedDynamicPropertyDescriptor("example");
        service.onPropertyModified(descriptor, null, SIMPLE_SCHEMA);

        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().name("example").build();
        final RecordSchema recordSchema = service.retrieveSchema(schemaIdentifier);

        assertNotNull(recordSchema);
        assertEquals("Example", recordSchema.getSchemaName().orElse(null));
        assertEquals("json-schema", recordSchema.getSchemaFormat().orElse(null));
    }

    @Test
    void testSchemaDefinitionRetrieval() throws IOException, SchemaNotFoundException {
        final JsonSchemaRegistryService service = new JsonSchemaRegistryService();
        final PropertyDescriptor descriptor = service.getSupportedDynamicPropertyDescriptor("example");
        service.onPropertyModified(descriptor, null, SIMPLE_SCHEMA);

        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().name("example").build();
        final SchemaDefinition schemaDefinition = service.retrieveSchemaDefinition(schemaIdentifier);

        assertNotNull(schemaDefinition);
        assertEquals(SchemaDefinition.SchemaType.JSON, schemaDefinition.getSchemaType());
        assertEquals(SIMPLE_SCHEMA, schemaDefinition.getText());
    }

    @Test
    void testMissingSchemaThrowsException() {
        final JsonSchemaRegistryService service = new JsonSchemaRegistryService();
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().name("missing").build();
        assertThrows(SchemaNotFoundException.class, () -> service.retrieveSchema(schemaIdentifier));
    }
}
