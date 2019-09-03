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
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestParquetRecordSetWriter {

    private ComponentLog componentLog;
    private ParquetRecordSetWriter recordSetWriterFactory;

    @Before
    public void setup() {
        recordSetWriterFactory = new ParquetRecordSetWriter();
        componentLog = new MockComponentLog("1234", recordSetWriterFactory);
    }

    @Test
    public void testWriteUsers() throws IOException, SchemaNotFoundException {
        initRecordSetWriter("src/test/resources/avro/user.avsc");

        // get the schema from the writer factory
        final RecordSchema writeSchema = recordSetWriterFactory.getSchema(Collections.emptyMap(), null);

        // write some records
        final int numUsers = 10;
        final File parquetFile = new File("target/testWriterUsers-" + System.currentTimeMillis());

        // write some records...
        writeUsers(writeSchema, parquetFile, numUsers);

        // read the records back in to verify
        verifyParquetRecords(parquetFile, numUsers);
    }

    @Test
    public void testWriteUsersWhenSchemaFormatNotAvro() throws IOException, SchemaNotFoundException {
        initRecordSetWriter("src/test/resources/avro/user.avsc");

        // get the schema from the writer factory
        final RecordSchema writeSchema = recordSetWriterFactory.getSchema(Collections.emptyMap(), null);
        final RecordSchema writeSchemaWithOtherFormat = new SimpleRecordSchema(writeSchema.getFields(), null, "OTHER-FORMAT", SchemaIdentifier.EMPTY);

        // write some records
        final int numUsers = 10;
        final File parquetFile = new File("target/testWriterUsers-" + System.currentTimeMillis());

        // write some records...
        writeUsers(writeSchemaWithOtherFormat, parquetFile, numUsers);

        // read the records back in to verify
        verifyParquetRecords(parquetFile, numUsers);
    }

    private void initRecordSetWriter(final String schemaFile) throws IOException {
        final ConfigurationContext configurationContext = getConfigurationContextWithSchema(schemaFile);

        // simulate enabling the service
        recordSetWriterFactory.onEnabled(configurationContext);
        recordSetWriterFactory.storeSchemaWriteStrategy(configurationContext);
        recordSetWriterFactory.storeSchemaAccessStrategy(configurationContext);
    }

    private void writeUsers(final RecordSchema writeSchema, final File parquetFile, final int numUsers) throws IOException {
        try(final OutputStream output = new FileOutputStream(parquetFile);
            final RecordSetWriter recordSetWriter = recordSetWriterFactory.createWriter(componentLog, writeSchema, output, Collections.emptyMap())) {
            for (int i = 0; i < numUsers; i++) {
                final Map<String, Object> userFields = new HashMap<>();
                userFields.put("name", "user" + i);
                userFields.put("favorite_number", i);
                userFields.put("favorite_color", "blue");

                final Record userRecord = new MapRecord(writeSchema, userFields);
                recordSetWriter.write(userRecord);
            }

            recordSetWriter.flush();
        }
    }

    private void verifyParquetRecords(final File parquetFile, final int expectedRecordCount) throws IOException {
        final Configuration conf = new Configuration();
        final Path path = new Path(parquetFile.getPath());
        final InputFile inputFile = HadoopInputFile.fromPath(path, conf);

        try (final ParquetReader<GenericRecord> reader =
                AvroParquetReader.<GenericRecord>builder(inputFile).withConf(conf).build()){

            int recordCount = 0;
            while(reader.read() != null) {
                recordCount++;
            }
            assertEquals(expectedRecordCount, recordCount);
        }
    }

    private ConfigurationContext getConfigurationContextWithSchema(String schemaPath) throws IOException {
        final File schemaFile = new File(schemaPath);
        final Map<PropertyDescriptor, String> properties = createPropertiesWithSchema(schemaFile);
        return new MockConfigurationContext(properties, null);
    }

    private Map<PropertyDescriptor,String> createPropertiesWithSchema(final File schemaFile) throws IOException {
        return createPropertiesWithSchema(IOUtils.toString(schemaFile.toURI()));
    }

    private Map<PropertyDescriptor,String> createPropertiesWithSchema(final String schemaText) {
        final Map<PropertyDescriptor,String> propertyValues = new HashMap<>();
        propertyValues.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY.getValue());
        propertyValues.put(SchemaAccessUtils.SCHEMA_TEXT, schemaText);
        return propertyValues;
    }
}
