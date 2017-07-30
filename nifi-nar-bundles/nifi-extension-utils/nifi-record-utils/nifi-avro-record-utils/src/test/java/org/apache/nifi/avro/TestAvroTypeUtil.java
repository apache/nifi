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

package org.apache.nifi.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.junit.Assert;
import org.junit.Test;

public class TestAvroTypeUtil {

    @Test
    public void testCreateAvroSchemaPrimitiveTypes() throws SchemaNotFoundException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("int", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("long", RecordFieldType.LONG.getDataType()));
        fields.add(new RecordField("string", RecordFieldType.STRING.getDataType(), "hola", Collections.singleton("greeting")));
        fields.add(new RecordField("byte", RecordFieldType.BYTE.getDataType()));
        fields.add(new RecordField("char", RecordFieldType.CHAR.getDataType()));
        fields.add(new RecordField("short", RecordFieldType.SHORT.getDataType()));
        fields.add(new RecordField("double", RecordFieldType.DOUBLE.getDataType()));
        fields.add(new RecordField("float", RecordFieldType.FLOAT.getDataType()));
        fields.add(new RecordField("time", RecordFieldType.TIME.getDataType()));
        fields.add(new RecordField("date", RecordFieldType.DATE.getDataType()));
        fields.add(new RecordField("timestamp", RecordFieldType.TIMESTAMP.getDataType()));

        final DataType arrayType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType());
        fields.add(new RecordField("strings", arrayType));

        final DataType mapType = RecordFieldType.MAP.getMapDataType(RecordFieldType.LONG.getDataType());
        fields.add(new RecordField("map", mapType));


        final List<RecordField> personFields = new ArrayList<>();
        personFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        personFields.add(new RecordField("dob", RecordFieldType.DATE.getDataType()));
        final RecordSchema personSchema = new SimpleRecordSchema(personFields);
        final DataType personType = RecordFieldType.RECORD.getRecordDataType(personSchema);
        fields.add(new RecordField("person", personType));


        final RecordSchema recordSchema = new SimpleRecordSchema(fields);

        final Schema avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);

        // everything should be a union, since it's nullable.
        for (final Field field : avroSchema.getFields()) {
            final Schema fieldSchema = field.schema();
            assertEquals(Type.UNION, fieldSchema.getType());
            assertTrue("Field " + field.name() + " does not contain NULL type", fieldSchema.getTypes().contains(Schema.create(Type.NULL)));
        }

        final RecordSchema afterConversion = AvroTypeUtil.createSchema(avroSchema);

        assertEquals(RecordFieldType.INT.getDataType(), afterConversion.getDataType("int").get());
        assertEquals(RecordFieldType.LONG.getDataType(), afterConversion.getDataType("long").get());
        assertEquals(RecordFieldType.STRING.getDataType(), afterConversion.getDataType("string").get());
        assertEquals(RecordFieldType.INT.getDataType(), afterConversion.getDataType("byte").get());
        assertEquals(RecordFieldType.STRING.getDataType(), afterConversion.getDataType("char").get());
        assertEquals(RecordFieldType.INT.getDataType(), afterConversion.getDataType("short").get());
        assertEquals(RecordFieldType.DOUBLE.getDataType(), afterConversion.getDataType("double").get());
        assertEquals(RecordFieldType.FLOAT.getDataType(), afterConversion.getDataType("float").get());
        assertEquals(RecordFieldType.TIME.getDataType(), afterConversion.getDataType("time").get());
        assertEquals(RecordFieldType.DATE.getDataType(), afterConversion.getDataType("date").get());
        assertEquals(RecordFieldType.TIMESTAMP.getDataType(), afterConversion.getDataType("timestamp").get());
        assertEquals(arrayType, afterConversion.getDataType("strings").get());
        assertEquals(mapType, afterConversion.getDataType("map").get());
        assertEquals(personType, afterConversion.getDataType("person").get());

        final RecordField stringField = afterConversion.getField("string").get();
        assertEquals("hola", stringField.getDefaultValue());
        assertEquals(Collections.singleton("greeting"), stringField.getAliases());
    }

    @Test
    // Simple recursion is a record A composing itself (similar to a LinkedList Node referencing 'next')
    public void testSimpleRecursiveSchema() {
        Schema recursiveSchema = new Schema.Parser().parse(
                "{\n" +
                "  \"namespace\": \"org.apache.nifi.testing\",\n" +
                "  \"name\": \"NodeRecord\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"name\": \"id\",\n" +
                "      \"type\": \"int\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"value\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"parent\",\n" +
                "      \"type\": [\n" +
                "        \"null\",\n" +
                "        \"NodeRecord\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}\n"
        );

        // Make sure the following doesn't throw an exception
        RecordSchema result = AvroTypeUtil.createSchema(recursiveSchema);

        // Make sure it parsed correctly
        Assert.assertEquals(3, result.getFieldCount());

        Optional<RecordField> idField = result.getField("id");
        Assert.assertTrue(idField.isPresent());
        Assert.assertEquals(RecordFieldType.INT, idField.get().getDataType().getFieldType());

        Optional<RecordField> valueField = result.getField("value");
        Assert.assertTrue(valueField.isPresent());
        Assert.assertEquals(RecordFieldType.STRING, valueField.get().getDataType().getFieldType());

        Optional<RecordField> parentField = result.getField("parent");
        Assert.assertTrue(parentField.isPresent());
        Assert.assertEquals(RecordFieldType.RECORD, parentField.get().getDataType().getFieldType());

        // The 'parent' field should have a circular schema reference to the top level record schema, similar to how Avro handles this
        Assert.assertEquals(result, ((RecordDataType)parentField.get().getDataType()).getChildSchema());
    }

    @Test
    // Complicated recursion is a record A composing record B, who composes a record A
    public void testComplicatedRecursiveSchema() {
        Schema recursiveSchema = new Schema.Parser().parse(
                "{\n" +
                "  \"namespace\": \"org.apache.nifi.testing\",\n" +
                "  \"name\": \"Record_A\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"name\": \"id\",\n" +
                "      \"type\": \"int\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"value\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"child\",\n" +
                "      \"type\": {\n" +
                "        \"namespace\": \"org.apache.nifi.testing\",\n" +
                "        \"name\": \"Record_B\",\n" +
                "        \"type\": \"record\",\n" +
                "        \"fields\": [\n" +
                "          {\n" +
                "            \"name\": \"id\",\n" +
                "            \"type\": \"int\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"name\": \"value\",\n" +
                "            \"type\": \"string\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"name\": \"parent\",\n" +
                "            \"type\": [\n" +
                "              \"null\",\n" +
                "              \"Record_A\"\n" +
                "            ]\n" +
                "          }\n" +
                "        ]\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}\n"
        );

        // Make sure the following doesn't throw an exception
        RecordSchema recordASchema = AvroTypeUtil.createSchema(recursiveSchema);

        // Make sure it parsed correctly
        Assert.assertEquals(3, recordASchema.getFieldCount());

        Optional<RecordField> recordAIdField = recordASchema.getField("id");
        Assert.assertTrue(recordAIdField.isPresent());
        Assert.assertEquals(RecordFieldType.INT, recordAIdField.get().getDataType().getFieldType());

        Optional<RecordField> recordAValueField = recordASchema.getField("value");
        Assert.assertTrue(recordAValueField.isPresent());
        Assert.assertEquals(RecordFieldType.STRING, recordAValueField.get().getDataType().getFieldType());

        Optional<RecordField> recordAChildField = recordASchema.getField("child");
        Assert.assertTrue(recordAChildField.isPresent());
        Assert.assertEquals(RecordFieldType.RECORD, recordAChildField.get().getDataType().getFieldType());

        // Get the child schema
        RecordSchema recordBSchema = ((RecordDataType)recordAChildField.get().getDataType()).getChildSchema();

        // Make sure it parsed correctly
        Assert.assertEquals(3, recordBSchema.getFieldCount());

        Optional<RecordField> recordBIdField = recordBSchema.getField("id");
        Assert.assertTrue(recordBIdField.isPresent());
        Assert.assertEquals(RecordFieldType.INT, recordBIdField.get().getDataType().getFieldType());

        Optional<RecordField> recordBValueField = recordBSchema.getField("value");
        Assert.assertTrue(recordBValueField.isPresent());
        Assert.assertEquals(RecordFieldType.STRING, recordBValueField.get().getDataType().getFieldType());

        Optional<RecordField> recordBParentField = recordBSchema.getField("parent");
        Assert.assertTrue(recordBParentField.isPresent());
        Assert.assertEquals(RecordFieldType.RECORD, recordBParentField.get().getDataType().getFieldType());

        // Make sure the 'parent' field has a schema reference back to the original top level record schema
        Assert.assertEquals(recordASchema, ((RecordDataType)recordBParentField.get().getDataType()).getChildSchema());
    }

}
