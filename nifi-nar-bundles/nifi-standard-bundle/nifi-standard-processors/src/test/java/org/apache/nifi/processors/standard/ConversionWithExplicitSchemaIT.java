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
package org.apache.nifi.processors.standard;

import org.apache.avro.Schema;
import org.apache.nifi.avro.AvroReaderWithExplicitSchema;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.csv.CSVUtils;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.TestRunner;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class ConversionWithExplicitSchemaIT extends AbstractConversionIT {
    private String schema;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        schema = new String(Files.readAllBytes(Paths.get("src/test/resources/TestConversions/explicit.schema.json")));
    }

    @Override
    protected String csvPostfix() {
        return "without_header.csv";
    }

    @Override
    protected String jsonPostfix() {
        return "json";
    }

    @Override
    protected String avroPostfix() {
        return "without_schema.avro";
    }

    @Override
    protected String xmlPostfix() {
        return "xml";
    }

    @Override
    protected void commonReaderConfiguration(TestRunner testRunner) {
        testRunner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        testRunner.setProperty(reader, SchemaAccessUtils.SCHEMA_TEXT, schema);
    }

    @Override
    protected void commonWriterConfiguration(TestRunner testRunner) {
        testRunner.setProperty(writer, "Schema Write Strategy", "no-schema");
        testRunner.setProperty(writer, CSVUtils.INCLUDE_HEADER_LINE, "false");
    }

    @Override
    protected List<Map<String, Object>> getRecords(byte[] avroData) throws IOException, MalformedRecordException {
        Schema avroSchema = new Schema.Parser().parse(schema);
        RecordSchema recordSchema = AvroTypeUtil.createSchema(avroSchema);

        try (RecordReader reader = new AvroReaderWithExplicitSchema(new ByteArrayInputStream(avroData), recordSchema, avroSchema);) {
            return getRecords(reader);
        }
    }
}
