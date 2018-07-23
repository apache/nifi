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
package org.apache.nifi.schema.access;

import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.when;

public class TestHortonworksAttributeSchemaReferenceStrategy extends AbstractSchemaAccessStrategyTest {

    @Test
    public void testGetSchemaWithValidAttributes() throws IOException, SchemaNotFoundException {
        final long schemaId = 123456;
        final int version = 2;
        final int protocol = 1;

        final Map<String,String> attributes = new HashMap<>();
        attributes.put(HortonworksAttributeSchemaReferenceStrategy.SCHEMA_ID_ATTRIBUTE, String.valueOf(schemaId));
        attributes.put(HortonworksAttributeSchemaReferenceStrategy.SCHEMA_VERSION_ATTRIBUTE, String.valueOf(version));
        attributes.put(HortonworksAttributeSchemaReferenceStrategy.SCHEMA_PROTOCOL_VERSION_ATTRIBUTE, String.valueOf(protocol));

        final SchemaAccessStrategy schemaAccessStrategy = new HortonworksAttributeSchemaReferenceStrategy(schemaRegistry);

        final SchemaIdentifier expectedSchemaIdentifier = SchemaIdentifier.builder()
                .id(schemaId)
                .version(version)
                .build();

        when(schemaRegistry.retrieveSchema(argThat(new SchemaIdentifierMatcher(expectedSchemaIdentifier))))
                .thenReturn(recordSchema);

        final RecordSchema retrievedSchema = schemaAccessStrategy.getSchema(attributes, null, recordSchema);
        assertNotNull(retrievedSchema);
    }

    @Test(expected = SchemaNotFoundException.class)
    public void testGetSchemaMissingAttributes() throws IOException, SchemaNotFoundException {
        final SchemaAccessStrategy schemaAccessStrategy = new HortonworksAttributeSchemaReferenceStrategy(schemaRegistry);
        schemaAccessStrategy.getSchema(Collections.emptyMap(), null, recordSchema);
    }
}
