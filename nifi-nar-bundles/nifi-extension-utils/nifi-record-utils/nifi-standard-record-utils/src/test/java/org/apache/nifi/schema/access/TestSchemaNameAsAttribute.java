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

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestSchemaNameAsAttribute {

    private List<RecordField> fields;
    private SchemaAccessWriter schemaAccessWriter;

    @BeforeEach
    public void setup() {
        fields = new ArrayList<>();
        fields.add(new RecordField("firstName", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("lastName", RecordFieldType.STRING.getDataType()));

        schemaAccessWriter = new SchemaNameAsAttribute();
    }

    @Test
    public void testWriteNameBranchAndVersion() {
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder()
                .name("person").branch("master").version(1).id(1L).build();

        final RecordSchema schema = new SimpleRecordSchema(fields, schemaIdentifier);

        final Map<String,String> attributes = schemaAccessWriter.getAttributes(schema);
        assertEquals(3, attributes.size());
        assertEquals(schemaIdentifier.getName().get(), attributes.get(SchemaNameAsAttribute.SCHEMA_NAME_ATTRIBUTE));
        assertEquals(schemaIdentifier.getBranch().get(), attributes.get(SchemaNameAsAttribute.SCHEMA_BRANCH_ATTRIBUTE));
        assertEquals(String.valueOf(schemaIdentifier.getVersion().getAsInt()), attributes.get(SchemaNameAsAttribute.SCHEMA_VERSION_ATTRIBUTE));
    }

    @Test
    public void testWriteOnlyName() {
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().name("person").id(1L).build();

        final RecordSchema schema = new SimpleRecordSchema(fields, schemaIdentifier);

        final Map<String,String> attributes = schemaAccessWriter.getAttributes(schema);
        assertEquals(1, attributes.size());
        assertEquals(schemaIdentifier.getName().get(), attributes.get(SchemaNameAsAttribute.SCHEMA_NAME_ATTRIBUTE));
    }

    @Test
    public void testValidateSchemaWhenValid() throws SchemaNotFoundException {
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().name("person").id(1L).build();
        final RecordSchema schema = new SimpleRecordSchema(fields, schemaIdentifier);
        schemaAccessWriter.validateSchema(schema);
    }

    @Test
    public void testValidateSchemaWhenNoIdentifier() {
        final RecordSchema schema = new SimpleRecordSchema(fields, null);
        assertThrows(SchemaNotFoundException.class, () -> schemaAccessWriter.validateSchema(schema));
    }

    @Test
    public void testValidateSchemaWhenNoName() {
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().id(1L).build();
        final RecordSchema schema = new SimpleRecordSchema(fields, schemaIdentifier);
        assertThrows(SchemaNotFoundException.class, () -> schemaAccessWriter.validateSchema(schema));
    }
}
