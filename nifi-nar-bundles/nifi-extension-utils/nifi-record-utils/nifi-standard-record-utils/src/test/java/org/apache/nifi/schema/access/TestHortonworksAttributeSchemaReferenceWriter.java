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
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestHortonworksAttributeSchemaReferenceWriter {

    @Test
    public void testValidateWithValidSchema() throws SchemaNotFoundException {
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().id(123456L).version(2).build();
        final RecordSchema recordSchema = createRecordSchema(schemaIdentifier);

        final SchemaAccessWriter schemaAccessWriter = new HortonworksAttributeSchemaReferenceWriter();
        schemaAccessWriter.validateSchema(recordSchema);
    }

    @Test(expected = SchemaNotFoundException.class)
    public void testValidateWithInvalidSchema() throws SchemaNotFoundException {
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().name("test").build();
        final RecordSchema recordSchema = createRecordSchema(schemaIdentifier);

        final SchemaAccessWriter schemaAccessWriter = new HortonworksAttributeSchemaReferenceWriter();
        schemaAccessWriter.validateSchema(recordSchema);
    }

    @Test
    public void testGetAttributesWithoutBranch() {
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().id(123456L).version(2).build();
        final RecordSchema recordSchema = createRecordSchema(schemaIdentifier);

        final SchemaAccessWriter schemaAccessWriter = new HortonworksAttributeSchemaReferenceWriter();
        final Map<String,String> attributes = schemaAccessWriter.getAttributes(recordSchema);

        Assert.assertEquals(3, attributes.size());

        Assert.assertEquals(String.valueOf(schemaIdentifier.getIdentifier().getAsLong()),
                attributes.get(HortonworksAttributeSchemaReferenceStrategy.SCHEMA_ID_ATTRIBUTE));

        Assert.assertEquals(String.valueOf(schemaIdentifier.getVersion().getAsInt()),
                attributes.get(HortonworksAttributeSchemaReferenceStrategy.SCHEMA_VERSION_ATTRIBUTE));

        Assert.assertEquals(String.valueOf(HortonworksAttributeSchemaReferenceWriter.LATEST_PROTOCOL_VERSION),
                attributes.get(HortonworksAttributeSchemaReferenceStrategy.SCHEMA_PROTOCOL_VERSION_ATTRIBUTE));
    }

    @Test
    public void testGetAttributesWithBranch() {
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().id(123456L).version(2).branch("foo").build();
        final RecordSchema recordSchema = createRecordSchema(schemaIdentifier);

        final SchemaAccessWriter schemaAccessWriter = new HortonworksAttributeSchemaReferenceWriter();
        final Map<String,String> attributes = schemaAccessWriter.getAttributes(recordSchema);

        Assert.assertEquals(4, attributes.size());

        Assert.assertEquals(String.valueOf(schemaIdentifier.getIdentifier().getAsLong()),
                attributes.get(HortonworksAttributeSchemaReferenceStrategy.SCHEMA_ID_ATTRIBUTE));

        Assert.assertEquals(String.valueOf(schemaIdentifier.getVersion().getAsInt()),
                attributes.get(HortonworksAttributeSchemaReferenceStrategy.SCHEMA_VERSION_ATTRIBUTE));

        Assert.assertEquals(String.valueOf(HortonworksAttributeSchemaReferenceWriter.LATEST_PROTOCOL_VERSION),
                attributes.get(HortonworksAttributeSchemaReferenceStrategy.SCHEMA_PROTOCOL_VERSION_ATTRIBUTE));

        Assert.assertEquals("foo", attributes.get(HortonworksAttributeSchemaReferenceWriter.SCHEMA_BRANCH_ATTRIBUTE));
    }

    private RecordSchema createRecordSchema(final SchemaIdentifier schemaIdentifier) {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("firstName", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("lastName", RecordFieldType.STRING.getDataType()));
        return new SimpleRecordSchema(fields, schemaIdentifier);
    }
}
