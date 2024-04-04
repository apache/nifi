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
package org.apache.nifi.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestParquetRecordSetWriter {

    private static final String SCHEMA_PATH = "src/test/resources/avro/user.avsc";

    private static final int USERS = 10;

    private ComponentLog componentLog;
    private ParquetRecordSetWriter recordSetWriterFactory;

    @BeforeEach
    public void setup() {
        recordSetWriterFactory = new ParquetRecordSetWriter();
        componentLog = new MockComponentLog("1234", recordSetWriterFactory);
    }

    @Test
    public void testWriteUsers() throws IOException, SchemaNotFoundException, InitializationException {
        initRecordSetWriter(true);
        final RecordSchema writeSchema = recordSetWriterFactory.getSchema(Collections.emptyMap(), null);
        final File parquetFile = new File("target/testWriterUsers-" + System.currentTimeMillis());
        final WriteResult writeResult = writeUsers(writeSchema, parquetFile);
        assertWriteAttributesFound(writeResult);
        verifyParquetRecords(parquetFile);
    }

    @Test
    public void testWriteUsersWhenSchemaFormatNotAvro() throws IOException, SchemaNotFoundException, InitializationException {
        initRecordSetWriter(false);
        final RecordSchema writeSchema = recordSetWriterFactory.getSchema(Collections.emptyMap(), null);
        final RecordSchema writeSchemaWithOtherFormat = new SimpleRecordSchema(writeSchema.getFields(), null, "OTHER-FORMAT", SchemaIdentifier.EMPTY);
        final File parquetFile = new File("target/testWriterUsers-" + System.currentTimeMillis());
        writeUsers(writeSchemaWithOtherFormat, parquetFile);
        verifyParquetRecords(parquetFile);
    }

    private void initRecordSetWriter(final boolean writeSchemaNameStrategy) throws IOException, InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(new AbstractProcessor() {
            @Override
            public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
            }
        });

        runner.addControllerService("writer", recordSetWriterFactory);

        final File schemaFile = new File(SCHEMA_PATH);
        final Map<PropertyDescriptor, String> properties = createPropertiesWithSchema(schemaFile);
        properties.forEach((k, v) -> runner.setProperty(recordSetWriterFactory, k, v));

        if (writeSchemaNameStrategy) {
            runner.setProperty(recordSetWriterFactory, "Schema Write Strategy", "schema-name");
        }

        runner.enableControllerService(recordSetWriterFactory);
    }

    private WriteResult writeUsers(final RecordSchema writeSchema, final File parquetFile) throws IOException {
        final WriteResult writeResult;
        try(final OutputStream output = new FileOutputStream(parquetFile);
            final RecordSetWriter recordSetWriter = recordSetWriterFactory.createWriter(componentLog, writeSchema, output, Collections.emptyMap())) {
            recordSetWriter.beginRecordSet();
            for (int i = 0; i < USERS; i++) {
                final Map<String, Object> userFields = new HashMap<>();
                userFields.put("name", "user" + i);
                userFields.put("favorite_number", i);
                userFields.put("favorite_color", "blue");

                final Record userRecord = new MapRecord(writeSchema, userFields);
                recordSetWriter.write(userRecord);
            }

            recordSetWriter.flush();
            writeResult = recordSetWriter.finishRecordSet();
        }
        return writeResult;
    }

    private void verifyParquetRecords(final File parquetFile) throws IOException {
        final Configuration conf = new Configuration();
        final Path path = new Path(parquetFile.getPath());
        final InputFile inputFile = HadoopInputFile.fromPath(path, conf);

        try (final ParquetReader<GenericRecord> reader =
                AvroParquetReader.<GenericRecord>builder(inputFile).withConf(conf).build()){

            int recordCount = 0;
            while(reader.read() != null) {
                recordCount++;
            }
            assertEquals(USERS, recordCount);
        }
    }

    private Map<PropertyDescriptor,String> createPropertiesWithSchema(final File schemaFile) throws IOException {
        return createPropertiesWithSchema(IOUtils.toString(schemaFile.toURI(), StandardCharsets.UTF_8));
    }

    private Map<PropertyDescriptor,String> createPropertiesWithSchema(final String schemaText) {
        final Map<PropertyDescriptor,String> propertyValues = new HashMap<>();
        propertyValues.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY.getValue());
        propertyValues.put(SchemaAccessUtils.SCHEMA_TEXT, schemaText);
        return propertyValues;
    }

    private void assertWriteAttributesFound(final WriteResult writeResult) {
        final Map<String, String> attributes = writeResult.getAttributes();
        assertFalse(attributes.isEmpty(), "Write Attributes not found");
    }
}
