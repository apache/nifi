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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestAvroReaderWithExplicitSchema {

    @Test
    public void testAvroExplicitReaderWithSchemalessFile() throws Exception {
        File avroFileWithEmbeddedSchema = new File("src/test/resources/avro/avro_schemaless.avro");
        FileInputStream fileInputStream = new FileInputStream(avroFileWithEmbeddedSchema);
        Schema dataSchema = new Schema.Parser().parse(new File("src/test/resources/avro/avro_schemaless.avsc"));
        RecordSchema recordSchema = new SimpleRecordSchema(dataSchema.toString(), AvroTypeUtil.AVRO_SCHEMA_FORMAT, null);

        AvroReaderWithExplicitSchema avroReader = new AvroReaderWithExplicitSchema(fileInputStream, recordSchema, dataSchema);
        GenericRecord record = avroReader.nextAvroRecord();
        assertNotNull(record);
        assertEquals(1, record.get("id"));
        assertNotNull(record.get("key"));
        assertEquals("value", record.get("key").toString());

        record = avroReader.nextAvroRecord();
        assertNotNull(record);
        assertEquals(2, record.get("id"));
        assertNull(record.get("key"));

        record = avroReader.nextAvroRecord();
        assertEquals(3, record.get("id"));
        assertNotNull(record.get("key"));
        assertEquals("hello", record.get("key").toString());
        record = avroReader.nextAvroRecord();
        assertNull(record);
    }

    @Test
    public void testAvroExplicitReaderWithEmbeddedSchemaFile() throws Exception {
        File avroFileWithEmbeddedSchema = new File("src/test/resources/avro/avro_embed_schema.avro");
        FileInputStream fileInputStream = new FileInputStream(avroFileWithEmbeddedSchema);
        Schema dataSchema = new Schema.Parser().parse(new File("src/test/resources/avro/avro_schemaless.avsc"));
        RecordSchema recordSchema = new SimpleRecordSchema(dataSchema.toString(), AvroTypeUtil.AVRO_SCHEMA_FORMAT, null);

        AvroReaderWithExplicitSchema avroReader = new AvroReaderWithExplicitSchema(fileInputStream, recordSchema, dataSchema);
        GenericRecord record = avroReader.nextAvroRecord();
        assertNotNull(record);
        assertEquals(1, record.get("id"));
        assertNotNull(record.get("key"));
        assertEquals("value", record.get("key").toString());

        record = avroReader.nextAvroRecord();
        assertNotNull(record);
        assertEquals(2, record.get("id"));
        assertNull(record.get("key"));

        record = avroReader.nextAvroRecord();
        assertEquals(3, record.get("id"));
        assertNotNull(record.get("key"));
        assertEquals("hello", record.get("key").toString());
        record = avroReader.nextAvroRecord();
        assertNull(record);
    }

    @Test(expected = IOException.class)
    public void testAvroExplicitReaderWithEmbeddedSchemaFileDifferentFromExplicitSchema() throws Exception {
        File avroFileWithEmbeddedSchema = new File("src/test/resources/avro/avro_embed_schema.avro");
        FileInputStream fileInputStream = new FileInputStream(avroFileWithEmbeddedSchema);
        Schema dataSchema = new Schema.Parser().parse("{\"namespace\": \"nifi\",\"name\": \"test\",\"type\": \"record\",\"fields\": [{\"name\": \"id\",\"type\": \"int\"}]}");
        RecordSchema recordSchema = new SimpleRecordSchema(dataSchema.toString(), AvroTypeUtil.AVRO_SCHEMA_FORMAT, null);

        // Causes IOException in constructor due to schemas not matching
        new AvroReaderWithExplicitSchema(fileInputStream, recordSchema, dataSchema);
    }

    @Test
    public void testAvroExplicitReaderWithSchemalessFileDecimalValuesWithDifferentBufferSize() throws Exception {
        // GIVEN
        String avroFilePath = "src/test/resources/avro/avro_schemaless_decimal.avro";
        String avroSchemaPath = "src/test/resources/avro/avro_schemaless_decimal.avsc";

        List<Map<String, String>> expectedRecords = new ArrayList<>(4);
        Map<String, String> record = new HashMap<>();
        record.put("id", "AAAAAA");
        record.put("price", "0.000000");
        expectedRecords.add(record);
        record = new HashMap<>();
        record.put("id", "BBBBBB");
        record.put("price", "15000.000000");
        expectedRecords.add(record);
        record = new HashMap<>();
        record.put("id", "CCCCCC");
        record.put("price", "0.000000");
        expectedRecords.add(record);
        record = new HashMap<>();
        record.put("id", "DDDDDD");
        record.put("price", "12.340000");
        expectedRecords.add(record);

        // WHEN
        AvroReaderWithExplicitSchema avroReader = createAvroReaderWithExplicitSchema(avroFilePath, avroSchemaPath);
        List<Record> actualRecords = collectRecords(avroReader);

        // THEN
        assertEquals(expectedRecords.size(), actualRecords.size());
        for (int i = 0; i < expectedRecords.size(); ++i) {
            Map<String, String> expectedRecord = expectedRecords.get(i);
            Record actualRecord = actualRecords.get(i);

            assertEquals(expectedRecord.get("id"), actualRecord.getValue("id").toString());
            assertEquals(expectedRecord.get("price"), actualRecord.getValue("price").toString());
        }
    }

    private AvroReaderWithExplicitSchema createAvroReaderWithExplicitSchema(String avroFilePath, String avroSchemaPath) throws IOException {
        FileInputStream avroFileInputStream = new FileInputStream(avroFilePath);
        Schema avroSchema = new Schema.Parser().parse(new FileInputStream(avroSchemaPath));
        RecordSchema recordSchema = AvroTypeUtil.createSchema(avroSchema);
        return new AvroReaderWithExplicitSchema(avroFileInputStream, recordSchema, avroSchema);
    }

    private List<Record> collectRecords(RecordReader reader) throws IOException, MalformedRecordException {
        List<Record> records = new ArrayList<>();
        Record record = reader.nextRecord();
        while (record != null) {
            records.add(record);
            record = reader.nextRecord();
        }
        return records;
    }
}