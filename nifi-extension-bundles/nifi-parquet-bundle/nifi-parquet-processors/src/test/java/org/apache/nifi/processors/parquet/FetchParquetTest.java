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
package org.apache.nifi.processors.parquet;

import static org.apache.nifi.processors.hadoop.AbstractHadoopProcessor.HADOOP_FILE_URL_ATTRIBUTE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.parquet.ParquetTestUtils;
import org.apache.nifi.parquet.utils.ParquetAttribute;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.hadoop.record.HDFSRecordReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.mockito.AdditionalMatchers;
import org.mockito.Mockito;

@DisabledOnOs({ OS.WINDOWS })
public class FetchParquetTest {

    static final String DIRECTORY = "target";
    static final String TEST_CONF_PATH = "src/test/resources/core-site.xml";
    static final String RECORD_HEADER = "name,favorite_number,favorite_color";
    private static final int USERS = 10;

    private Schema schema;
    private Schema schemaWithArray;
    private Schema schemaWithNullableArray;
    private Schema schemaWithDecimal;

    private Configuration testConf;
    private FetchParquet proc;
    private TestRunner testRunner;

    @BeforeEach
    public void setup() throws IOException {
        final String avroSchema = IOUtils.toString(new FileInputStream("src/test/resources/avro/user.avsc"), StandardCharsets.UTF_8);
        schema = new Schema.Parser().parse(avroSchema);

        final String avroSchemaWithArray = IOUtils.toString(new FileInputStream("src/test/resources/avro/user-with-array.avsc"), StandardCharsets.UTF_8);
        schemaWithArray = new Schema.Parser().parse(avroSchemaWithArray);

        final String avroSchemaWithNullableArray = IOUtils.toString(new FileInputStream("src/test/resources/avro/user-with-nullable-array.avsc"), StandardCharsets.UTF_8);
        schemaWithNullableArray = new Schema.Parser().parse(avroSchemaWithNullableArray);

        final String avroSchemaWithDecimal = IOUtils.toString(new FileInputStream("src/test/resources/avro/user-with-fixed-decimal.avsc"), StandardCharsets.UTF_8);
        schemaWithDecimal = new Schema.Parser().parse(avroSchemaWithDecimal);

        testConf = new Configuration();
        testConf.addResource(new Path(TEST_CONF_PATH));

        proc = new FetchParquet();
    }

    private void configure(final FetchParquet fetchParquet) throws InitializationException {
        testRunner = TestRunners.newTestRunner(fetchParquet);
        testRunner.setProperty(FetchParquet.HADOOP_CONFIGURATION_RESOURCES, TEST_CONF_PATH);

        final RecordSetWriterFactory writerFactory = new MockRecordWriter(RECORD_HEADER, false);
        testRunner.addControllerService("mock-writer-factory", writerFactory);
        testRunner.enableControllerService(writerFactory);
        testRunner.setProperty(FetchParquet.RECORD_WRITER, "mock-writer-factory");
    }

    @Test
    public void testFetchParquetToCSV() throws IOException, InitializationException {
        configure(proc);

        final File parquetDir = new File(DIRECTORY);
        final File parquetFile = new File(parquetDir, "testFetchParquetToCSV.parquet");
        writeParquetUsers(parquetFile);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.PATH.key(), parquetDir.getAbsolutePath());
        attributes.put(CoreAttributes.FILENAME.key(), parquetFile.getName());

        testRunner.enqueue("TRIGGER", attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchParquet.REL_SUCCESS, 1);

        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchParquet.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(FetchParquet.RECORD_COUNT_ATTR, String.valueOf(USERS));
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain");
        assertTrue(flowFile.getAttribute(HADOOP_FILE_URL_ATTRIBUTE).endsWith(DIRECTORY + "/" + parquetFile.getName()));

        // the mock record writer will write the header for each record so replace those to get down to just the records
        String flowFileContent = new String(flowFile.toByteArray(), StandardCharsets.UTF_8);
        flowFileContent = flowFileContent.replaceAll(RECORD_HEADER + "\n", "");

        verifyCSVRecords(flowFileContent);
    }

    @Test
    public void testFetchParquetToCSVWithOffsetAndCount() throws IOException, InitializationException {
        configure(proc);

        final File parquetDir = new File(DIRECTORY);
        final File parquetFile = new File(parquetDir, "testFetchParquetToCSV.parquet");
        writeParquetUsers(parquetFile);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.PATH.key(), parquetDir.getAbsolutePath());
        attributes.put(CoreAttributes.FILENAME.key(), parquetFile.getName());
        attributes.put(ParquetAttribute.RECORD_OFFSET, "3");
        attributes.put(ParquetAttribute.RECORD_COUNT, "1");

        testRunner.enqueue("TRIGGER", attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchParquet.REL_SUCCESS, 1);

        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchParquet.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(FetchParquet.RECORD_COUNT_ATTR, "1");
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain");
        assertTrue(flowFile.getAttribute(HADOOP_FILE_URL_ATTRIBUTE).endsWith(DIRECTORY + "/" + parquetFile.getName()));

        // the mock record writer will write the header for each record so replace those to get down to just the records
        String flowFileContent = new String(flowFile.toByteArray(), StandardCharsets.UTF_8);
        flowFileContent = flowFileContent.replaceAll(RECORD_HEADER + "\n", "");

        assertEquals("Bob3,3,blue3\n", flowFileContent);
    }

    @Test
    public void testFetchParquetToCSVWithOffset() throws IOException, InitializationException {
        configure(proc);

        final File parquetDir = new File(DIRECTORY);
        final File parquetFile = new File(parquetDir, "testFetchParquetToCSV.parquet");
        writeParquetUsers(parquetFile);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.PATH.key(), parquetDir.getAbsolutePath());
        attributes.put(CoreAttributes.FILENAME.key(), parquetFile.getName());
        attributes.put(ParquetAttribute.RECORD_OFFSET, "9");

        testRunner.enqueue("TRIGGER", attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchParquet.REL_SUCCESS, 1);

        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchParquet.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(FetchParquet.RECORD_COUNT_ATTR, "1");
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain");
        assertTrue(flowFile.getAttribute(HADOOP_FILE_URL_ATTRIBUTE).endsWith(DIRECTORY + "/" + parquetFile.getName()));

        // the mock record writer will write the header for each record so replace those to get down to just the records
        String flowFileContent = new String(flowFile.toByteArray(), StandardCharsets.UTF_8);
        flowFileContent = flowFileContent.replaceAll(RECORD_HEADER + "\n", "");

        assertEquals("Bob9,9,blue9\n", flowFileContent);
    }

    @Test
    public void testFetchParquetToCSVWithCount() throws IOException, InitializationException {
        configure(proc);

        final File parquetDir = new File(DIRECTORY);
        final File parquetFile = new File(parquetDir, "testFetchParquetToCSV.parquet");
        writeParquetUsers(parquetFile);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.PATH.key(), parquetDir.getAbsolutePath());
        attributes.put(CoreAttributes.FILENAME.key(), parquetFile.getName());
        attributes.put(ParquetAttribute.RECORD_COUNT, "1");

        testRunner.enqueue("TRIGGER", attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchParquet.REL_SUCCESS, 1);

        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchParquet.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(FetchParquet.RECORD_COUNT_ATTR, "1");
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain");
        assertTrue(flowFile.getAttribute(HADOOP_FILE_URL_ATTRIBUTE).endsWith(DIRECTORY + "/" + parquetFile.getName()));

        // the mock record writer will write the header for each record so replace those to get down to just the records
        String flowFileContent = new String(flowFile.toByteArray(), StandardCharsets.UTF_8);
        flowFileContent = flowFileContent.replaceAll(RECORD_HEADER + "\n", "");

        assertEquals("Bob0,0,blue0\n", flowFileContent);
    }

    @Test
    public void testFetchParquetToCSVWithOffsetWithinFileRange() throws IOException, InitializationException {
        configure(proc);

        final File parquetFile = ParquetTestUtils.createUsersParquetFile(1000);
        final File parquetDir = parquetFile.getParentFile();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.PATH.key(), parquetDir.getAbsolutePath());
        attributes.put(CoreAttributes.FILENAME.key(), parquetFile.getName());
        attributes.put(ParquetAttribute.FILE_RANGE_START_OFFSET, "16543");
        attributes.put(ParquetAttribute.FILE_RANGE_END_OFFSET, "24784");
        attributes.put(ParquetAttribute.RECORD_OFFSET, "325");

        testRunner.enqueue("TRIGGER", attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchParquet.REL_SUCCESS, 1);

        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchParquet.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(FetchParquet.RECORD_COUNT_ATTR, "1");
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain");
        assertTrue(flowFile.getAttribute(HADOOP_FILE_URL_ATTRIBUTE).endsWith(DIRECTORY + "/" + parquetFile.getName()));

        // the mock record writer will write the header for each record so replace those to get down to just the records
        String flowFileContent = new String(flowFile.toByteArray(), StandardCharsets.UTF_8);
        flowFileContent = flowFileContent.replaceAll(RECORD_HEADER + "\n", "");

        assertEquals("Bob988,988,blue988\n", flowFileContent);
    }

    @Test
    public void testFetchParquetToCSVWithCountWithinFileRange() throws IOException, InitializationException {
        configure(proc);

        final File parquetFile = ParquetTestUtils.createUsersParquetFile(1000);
        final File parquetDir = parquetFile.getParentFile();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.PATH.key(), parquetDir.getAbsolutePath());
        attributes.put(CoreAttributes.FILENAME.key(), parquetFile.getName());
        attributes.put(ParquetAttribute.FILE_RANGE_START_OFFSET, "16543");
        attributes.put(ParquetAttribute.FILE_RANGE_END_OFFSET, "24784");
        attributes.put(ParquetAttribute.RECORD_COUNT, "1");

        testRunner.enqueue("TRIGGER", attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchParquet.REL_SUCCESS, 1);

        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchParquet.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(FetchParquet.RECORD_COUNT_ATTR, "1");
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain");
        assertTrue(flowFile.getAttribute(HADOOP_FILE_URL_ATTRIBUTE).endsWith(DIRECTORY + "/" + parquetFile.getName()));

        // the mock record writer will write the header for each record so replace those to get down to just the records
        String flowFileContent = new String(flowFile.toByteArray(), StandardCharsets.UTF_8);
        flowFileContent = flowFileContent.replaceAll(RECORD_HEADER + "\n", "");

        assertEquals("Bob663,663,blue663\n", flowFileContent);
    }

    @Test
    public void testFetchParquetToCSVWithOffsetAndCountWithinFileRange() throws IOException, InitializationException {
        configure(proc);

        final File parquetFile = ParquetTestUtils.createUsersParquetFile(1000);
        final File parquetDir = parquetFile.getParentFile();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.PATH.key(), parquetDir.getAbsolutePath());
        attributes.put(CoreAttributes.FILENAME.key(), parquetFile.getName());
        attributes.put(ParquetAttribute.FILE_RANGE_START_OFFSET, "16543");
        attributes.put(ParquetAttribute.FILE_RANGE_END_OFFSET, "24784");
        attributes.put(ParquetAttribute.RECORD_OFFSET, "3");
        attributes.put(ParquetAttribute.RECORD_COUNT, "1");

        testRunner.enqueue("TRIGGER", attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchParquet.REL_SUCCESS, 1);

        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchParquet.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(FetchParquet.RECORD_COUNT_ATTR, "1");
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain");
        assertTrue(flowFile.getAttribute(HADOOP_FILE_URL_ATTRIBUTE).endsWith(DIRECTORY + "/" + parquetFile.getName()));

        // the mock record writer will write the header for each record so replace those to get down to just the records
        String flowFileContent = new String(flowFile.toByteArray(), StandardCharsets.UTF_8);
        flowFileContent = flowFileContent.replaceAll(RECORD_HEADER + "\n", "");

        assertEquals("Bob666,666,blue666\n", flowFileContent);
    }

    @Test
    public void testFetchWhenELEvaluatesToEmptyShouldRouteFailure() throws InitializationException {
        configure(proc);
        testRunner.setProperty(FetchParquet.FILENAME, "${missing.attr}");

        testRunner.enqueue("TRIGGER");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchParquet.REL_FAILURE, 1);

        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchParquet.REL_FAILURE).get(0);
        flowFile.assertAttributeEquals(FetchParquet.FETCH_FAILURE_REASON_ATTR, "Can not create a Path from an empty string");
        flowFile.assertContentEquals("TRIGGER");
    }

    @Test
    public void testFetchWhenDoesNotExistShouldRouteToFailure() throws InitializationException {
        configure(proc);

        final String filename = "/tmp/does-not-exist-" + System.currentTimeMillis();
        testRunner.setProperty(FetchParquet.FILENAME, filename);

        testRunner.enqueue("TRIGGER");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchParquet.REL_FAILURE, 1);

        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchParquet.REL_FAILURE).get(0);
        flowFile.assertAttributeEquals(FetchParquet.FETCH_FAILURE_REASON_ATTR, "File " + filename + " does not exist");
        flowFile.assertContentEquals("TRIGGER");
    }

    @Test
    public void testIOExceptionCreatingReaderShouldRouteToRetry() throws InitializationException, IOException {
        final FetchParquet proc = new FetchParquet() {
            @Override
            public HDFSRecordReader createHDFSRecordReader(ProcessContext context, FlowFile flowFile, Configuration conf, Path path)
                    throws IOException {
                throw new IOException("IOException");
            }
        };

        configure(proc);

        final File parquetDir = new File(DIRECTORY);
        final File parquetFile = new File(parquetDir, "testFetchParquetToCSV.parquet");
        writeParquetUsers(parquetFile);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.PATH.key(), parquetDir.getAbsolutePath());
        attributes.put(CoreAttributes.FILENAME.key(), parquetFile.getName());

        testRunner.enqueue("TRIGGER", attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchParquet.REL_RETRY, 1);

        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchParquet.REL_RETRY).get(0);
        flowFile.assertContentEquals("TRIGGER");
    }

    @Test
    public void testIOExceptionWhileReadingShouldRouteToRetry() throws IOException, InitializationException {
        final FetchParquet proc = new FetchParquet() {
            @Override
            public HDFSRecordReader createHDFSRecordReader(ProcessContext context, FlowFile flowFile, Configuration conf, Path path) {
                return new HDFSRecordReader() {
                    @Override
                    public Record nextRecord() throws IOException {
                        throw new IOException("IOException");
                    }
                    @Override
                    public void close() {
                    }
                };
            }
        };

        configure(proc);

        final File parquetDir = new File(DIRECTORY);
        final File parquetFile = new File(parquetDir, "testFetchParquetToCSV.parquet");
        writeParquetUsers(parquetFile);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.PATH.key(), parquetDir.getAbsolutePath());
        attributes.put(CoreAttributes.FILENAME.key(), parquetFile.getName());

        testRunner.enqueue("TRIGGER", attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchParquet.REL_RETRY, 1);

        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchParquet.REL_RETRY).get(0);
        flowFile.assertContentEquals("TRIGGER");
    }

    @Test
    public void testIOExceptionWhileWritingShouldRouteToRetry() throws InitializationException, IOException, SchemaNotFoundException {
        configure(proc);

        final RecordSetWriter recordSetWriter = Mockito.mock(RecordSetWriter.class);
        when(recordSetWriter.write(any(Record.class))).thenThrow(new IOException("IOException"));

        final RecordSetWriterFactory recordSetWriterFactory = Mockito.mock(RecordSetWriterFactory.class);
        when(recordSetWriterFactory.getIdentifier()).thenReturn("mock-writer-factory");
        when(recordSetWriterFactory.createWriter(any(ComponentLog.class), AdditionalMatchers.or(any(RecordSchema.class), isNull()), any(OutputStream.class), any(FlowFile.class)))
                .thenReturn(recordSetWriter);

        testRunner.addControllerService("mock-writer-factory", recordSetWriterFactory);
        testRunner.enableControllerService(recordSetWriterFactory);
        testRunner.setProperty(FetchParquet.RECORD_WRITER, "mock-writer-factory");

        final File parquetDir = new File(DIRECTORY);
        final File parquetFile = new File(parquetDir, "testFetchParquetToCSV.parquet");
        writeParquetUsers(parquetFile);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.PATH.key(), parquetDir.getAbsolutePath());
        attributes.put(CoreAttributes.FILENAME.key(), parquetFile.getName());

        testRunner.enqueue("TRIGGER", attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchParquet.REL_RETRY, 1);

        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchParquet.REL_RETRY).get(0);
        flowFile.assertContentEquals("TRIGGER");
    }

    @Test
    public void testFetchWithArray() throws InitializationException, IOException {
        configure(proc);

        final File parquetDir = new File(DIRECTORY);
        final File parquetFile = new File(parquetDir, "testFetchParquetWithArrayToCSV.parquet");
        writeParquetUsersWithArray(parquetFile);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.PATH.key(), parquetDir.getAbsolutePath());
        attributes.put(CoreAttributes.FILENAME.key(), parquetFile.getName());

        testRunner.enqueue("TRIGGER", attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchParquet.REL_SUCCESS, 1);
    }

    @Test
    public void testFetchWithNullableArray() throws InitializationException, IOException {
        configure(proc);

        final File parquetDir = new File(DIRECTORY);
        final File parquetFile = new File(parquetDir, "testFetchParquetWithNullableArrayToCSV.parquet");
        writeParquetUsersWithNullableArray(parquetFile);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.PATH.key(), parquetDir.getAbsolutePath());
        attributes.put(CoreAttributes.FILENAME.key(), parquetFile.getName());

        testRunner.enqueue("TRIGGER", attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchParquet.REL_SUCCESS, 1);
    }

    @Test
    public void testFetchParquetWithDecimal() throws InitializationException, IOException {
        configure(proc);

        final File parquetDir = new File(DIRECTORY);
        final File parquetFile = new File(parquetDir, "testFetchParquetWithDecimal.parquet");
        writeParquetUsersWithDecimal(parquetFile);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.PATH.key(), parquetDir.getAbsolutePath());
        attributes.put(CoreAttributes.FILENAME.key(), parquetFile.getName());

        testRunner.enqueue("TRIGGER", attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchParquet.REL_SUCCESS, 1);
    }

    protected void verifyCSVRecords(String csvContent) {
        final String[] splits = csvContent.split("[\\n]");
        assertEquals(USERS, splits.length);

        for (int i = 0; i < USERS; i++) {
            final String line = splits[i];
            assertEquals("Bob" + i + "," + i + ",blue" + i, line);
        }
    }

    private AvroParquetWriter.Builder<GenericRecord> createAvroParquetWriter(final File parquetFile, final Schema schema) throws IOException {
        final Path parquetPath = new Path(parquetFile.getPath());

        return AvroParquetWriter
                .<GenericRecord>builder(HadoopOutputFile.fromPath(parquetPath, testConf))
                .withSchema(schema)
                .withConf(testConf);
    }

    private void writeParquetUsers(final File parquetFile) throws IOException {
        if (parquetFile.exists()) {
            assertTrue(parquetFile.delete());
        }

        final AvroParquetWriter.Builder<GenericRecord> writerBuilder = createAvroParquetWriter(parquetFile, schema);

        try (final ParquetWriter<GenericRecord> writer = writerBuilder.build()) {
            for (int i = 0; i < USERS; i++) {
                final GenericRecord user = new GenericData.Record(schema);
                user.put("name", "Bob" + i);
                user.put("favorite_number", i);
                user.put("favorite_color", "blue" + i);

                writer.write(user);
            }
        }
    }

    private void writeParquetUsersWithArray(final File parquetFile) throws IOException {
        if (parquetFile.exists()) {
            assertTrue(parquetFile.delete());
        }

        final AvroParquetWriter.Builder<GenericRecord> writerBuilder = createAvroParquetWriter(parquetFile, schemaWithArray);

        final Schema favoriteColorsSchema = schemaWithArray.getField("favorite_colors").schema();

        try (final ParquetWriter<GenericRecord> writer = writerBuilder.build()) {
            for (int i = 0; i < USERS; i++) {
                final GenericRecord user = new GenericData.Record(schema);
                user.put("name", "Bob" + i);
                user.put("favorite_number", i);


                final GenericData.Array<String> colors = new GenericData.Array<>(1, favoriteColorsSchema);
                colors.add("blue" + i);

                user.put("favorite_color", colors);

                writer.write(user);
            }
        }
    }

    private void writeParquetUsersWithNullableArray(final File parquetFile) throws IOException {
        if (parquetFile.exists()) {
            assertTrue(parquetFile.delete());
        }

        final AvroParquetWriter.Builder<GenericRecord> writerBuilder = createAvroParquetWriter(parquetFile, schemaWithNullableArray);

        // use the schemaWithArray here just to get the schema for the array part of the favorite_colors fields, the overall
        // schemaWithNullableArray has a union of the array schema and null
        final Schema favoriteColorsSchema = schemaWithArray.getField("favorite_colors").schema();

        try (final ParquetWriter<GenericRecord> writer = writerBuilder.build()) {
            for (int i = 0; i < USERS; i++) {
                final GenericRecord user = new GenericData.Record(schema);
                user.put("name", "Bob" + i);
                user.put("favorite_number", i);


                final GenericData.Array<String> colors = new GenericData.Array<>(1, favoriteColorsSchema);
                colors.add("blue" + i);

                user.put("favorite_color", colors);

                writer.write(user);
            }
        }
    }

    private void writeParquetUsersWithDecimal(final File parquetFile) throws IOException {
        if (parquetFile.exists()) {
            assertTrue(parquetFile.delete());
        }

        final BigDecimal initialAmount = new BigDecimal("1234567.0123456789");
        final AvroParquetWriter.Builder<GenericRecord> writerBuilder = createAvroParquetWriter(parquetFile, schemaWithDecimal);

        final List<Schema> amountSchemaUnion = schemaWithDecimal.getField("amount").schema().getTypes();
        final Schema amountSchema = amountSchemaUnion.stream().filter(s -> s.getType() == Schema.Type.FIXED).findFirst().orElse(null);
        assertNotNull(amountSchema);

        final Conversions.DecimalConversion decimalConversion = new Conversions.DecimalConversion();

        try (final ParquetWriter<GenericRecord> writer = writerBuilder.build()) {
            for (int i = 0; i < USERS; i++) {
                final BigDecimal incrementedAmount = initialAmount.add(BigDecimal.ONE);
                final GenericRecord user = new GenericData.Record(schemaWithDecimal);
                user.put("name", "Bob" + i);
                user.put("amount", decimalConversion.toFixed(incrementedAmount, amountSchema, amountSchema.getLogicalType()));

                writer.write(user);
            }
        }

    }
}
