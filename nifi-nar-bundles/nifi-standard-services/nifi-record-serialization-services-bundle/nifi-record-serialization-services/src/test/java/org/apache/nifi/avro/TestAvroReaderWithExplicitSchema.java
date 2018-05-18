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
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class TestAvroReaderWithExplicitSchema {

    @Test
    public void testAvroExplicitReaderWithSchemalessFile() throws Exception {
        File avroFileWithEmbeddedSchema = new File("src/test/resources/avro/avro_schemaless.avro");
        FileInputStream fileInputStream = new FileInputStream(avroFileWithEmbeddedSchema);
        Schema dataSchema = new Schema.Parser().parse(new File("src/test/resources/avro/avro_schemaless.avsc"));
        RecordSchema recordSchema = new SimpleRecordSchema(dataSchema.toString(), AvroTypeUtil.AVRO_SCHEMA_FORMAT, null);

        AvroReaderWithExplicitSchema avroReader = new AvroReaderWithExplicitSchema(fileInputStream, recordSchema, dataSchema);
        avroReader.nextAvroRecord();
    }

    @Test
    public void testAvroExplicitReaderWithEmbeddedSchemaFile() throws Exception {
        File avroFileWithEmbeddedSchema = new File("src/test/resources/avro/avro_embed_schema.avro");
        FileInputStream fileInputStream = new FileInputStream(avroFileWithEmbeddedSchema);
        Schema dataSchema = new Schema.Parser().parse(new File("src/test/resources/avro/avro_schemaless.avsc"));
        RecordSchema recordSchema = new SimpleRecordSchema(dataSchema.toString(), AvroTypeUtil.AVRO_SCHEMA_FORMAT, null);

        AvroReaderWithExplicitSchema avroReader = new AvroReaderWithExplicitSchema(fileInputStream, recordSchema, dataSchema);
        avroReader.nextAvroRecord();
    }

    @Test(expected = IOException.class)
    public void testAvroExplicitReaderWithEmbeddedSchemaFileDifferentFromExplicitSchema() throws Exception {
        File avroFileWithEmbeddedSchema = new File("src/test/resources/avro/avro_embed_schema.avro");
        FileInputStream fileInputStream = new FileInputStream(avroFileWithEmbeddedSchema);
        Schema dataSchema = new Schema.Parser().parse("{\"namespace\": \"nifi\",\"name\": \"test\",\"type\": \"record\",\"fields\": [{\"name\": \"id\",\"type\": \"int\"}]}");
        RecordSchema recordSchema = new SimpleRecordSchema(dataSchema.toString(), AvroTypeUtil.AVRO_SCHEMA_FORMAT, null);

        AvroReaderWithExplicitSchema avroReader = new AvroReaderWithExplicitSchema(fileInputStream, recordSchema, dataSchema);
        avroReader.nextAvroRecord();
    }
}