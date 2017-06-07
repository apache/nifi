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

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
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

}
