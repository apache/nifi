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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.hadoop.exception.FailureException;
import org.apache.nifi.processors.hadoop.record.HDFSRecordWriter;
import org.apache.nifi.processors.parquet.utils.ParquetUtils;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;


public class PutParquetTest {

    static final String DIRECTORY = "target";
    static final String TEST_CONF_PATH = "src/test/resources/core-site.xml";

    private Schema schema;
    private Configuration testConf;
    private PutParquet proc;
    private MockRecordParser readerFactory;
    private TestRunner testRunner;

    @BeforeClass
    public static void setupLogging() {
        BasicConfigurator.configure();
    }

    @Before
    public void setup() throws IOException, InitializationException {
        final String avroSchema = IOUtils.toString(new FileInputStream("src/test/resources/avro/user.avsc"), StandardCharsets.UTF_8);
        schema = new Schema.Parser().parse(avroSchema);

        testConf = new Configuration();
        testConf.addResource(new Path(TEST_CONF_PATH));

        proc = new PutParquet();
    }

    private void configure(final PutParquet putParquet, final int numUsers) throws InitializationException {
        testRunner = TestRunners.newTestRunner(putParquet);
        testRunner.setProperty(PutParquet.HADOOP_CONFIGURATION_RESOURCES, TEST_CONF_PATH);
        testRunner.setProperty(PutParquet.DIRECTORY, DIRECTORY);

        readerFactory = new MockRecordParser();

        final RecordSchema recordSchema = AvroTypeUtil.createSchema(schema);
        for (final RecordField recordField : recordSchema.getFields()) {
            readerFactory.addSchemaField(recordField.getFieldName(), recordField.getDataType().getFieldType());
        }

        for (int i=0; i < numUsers; i++) {
            readerFactory.addRecord("name" + i, i, "blue" + i);
        }

        testRunner.addControllerService("mock-reader-factory", readerFactory);
        testRunner.enableControllerService(readerFactory);

        testRunner.setProperty(PutParquet.RECORD_READER, "mock-reader-factory");
    }

    @Test
    public void testWriteAvroParquetWithDefaults() throws IOException, InitializationException {
        configure(proc, 100);

        final String filename = "testWriteAvroWithDefaults-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_SUCCESS, 1);

        final Path avroParquetFile = new Path(DIRECTORY + "/" + filename);

        // verify the successful flow file has the expected attributes
        final MockFlowFile mockFlowFile = testRunner.getFlowFilesForRelationship(PutParquet.REL_SUCCESS).get(0);
        mockFlowFile.assertAttributeEquals(PutParquet.ABSOLUTE_HDFS_PATH_ATTRIBUTE, avroParquetFile.getParent().toString());
        mockFlowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), filename);
        mockFlowFile.assertAttributeEquals(PutParquet.RECORD_COUNT_ATTR, "100");

        // verify we generated a provenance event
        final List<ProvenanceEventRecord> provEvents = testRunner.getProvenanceEvents();
        Assert.assertEquals(1, provEvents.size());

        // verify it was a SEND event with the correct URI
        final ProvenanceEventRecord provEvent = provEvents.get(0);
        Assert.assertEquals(ProvenanceEventType.SEND, provEvent.getEventType());
        // If it runs with a real HDFS, the protocol will be "hdfs://", but with a local filesystem, just assert the filename.
        Assert.assertTrue(provEvent.getTransitUri().endsWith(DIRECTORY + "/" + filename));

        // verify the content of the parquet file by reading it back in
        verifyAvroParquetUsers(avroParquetFile, 100);

        // verify we don't have the temp dot file after success
        final File tempAvroParquetFile = new File(DIRECTORY + "/." + filename);
        Assert.assertFalse(tempAvroParquetFile.exists());

        // verify we DO have the CRC file after success
        final File crcAvroParquetFile = new File(DIRECTORY + "/." + filename + ".crc");
        Assert.assertTrue(crcAvroParquetFile.exists());
    }

    @Test
    public void testWriteAvroAndRemoveCRCFiles() throws IOException, InitializationException {
        configure(proc,100);
        testRunner.setProperty(PutParquet.REMOVE_CRC_FILES, "true");

        final String filename = "testWriteAvroAndRemoveCRCFiles-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_SUCCESS, 1);

        // verify we don't have the temp dot file after success
        final File tempAvroParquetFile = new File(DIRECTORY + "/." + filename);
        Assert.assertFalse(tempAvroParquetFile.exists());

        // verify we don't have the CRC file after success because we set remove to true
        final File crcAvroParquetFile = new File(DIRECTORY + "/." + filename + ".crc");
        Assert.assertFalse(crcAvroParquetFile.exists());
    }

    @Test
    public void testWriteAvroWithGZIPCompression() throws IOException, InitializationException {
        configure(proc, 100);
        testRunner.setProperty(ParquetUtils.COMPRESSION_TYPE, CompressionCodecName.GZIP.name());

        final String filename = "testWriteAvroWithGZIPCompression-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_SUCCESS, 1);

        // verify the content of the parquet file by reading it back in
        final Path avroParquetFile = new Path(DIRECTORY + "/" + filename);
        verifyAvroParquetUsers(avroParquetFile, 100);
    }

    @Test
    public void testInvalidAvroShouldRouteToFailure() throws InitializationException, SchemaNotFoundException, MalformedRecordException, IOException {
        configure(proc, 0);

        // simulate throwing an IOException when the factory creates a reader which is what would happen when
        // invalid Avro is passed to the Avro reader factory
        final RecordReaderFactory readerFactory = Mockito.mock(RecordReaderFactory.class);
        when(readerFactory.getIdentifier()).thenReturn("mock-reader-factory");
        when(readerFactory.createRecordReader(any(FlowFile.class), any(InputStream.class), any(ComponentLog.class))).thenThrow(new IOException("NOT AVRO"));

        testRunner.addControllerService("mock-reader-factory", readerFactory);
        testRunner.enableControllerService(readerFactory);
        testRunner.setProperty(PutParquet.RECORD_READER, "mock-reader-factory");

        final String filename = "testInvalidAvroShouldRouteToFailure-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_FAILURE, 1);
    }

    @Test
    public void testCreateDirectoryIOExceptionShouldRouteToRetry() throws InitializationException, IOException {
        final PutParquet proc = new PutParquet() {
            @Override
            protected void createDirectory(FileSystem fileSystem, Path directory, String remoteOwner, String remoteGroup)
                    throws IOException, FailureException {
                throw new IOException("IOException creating directory");
            }
        };

        configure(proc, 10);

        final String filename = "testCreateDirectoryIOExceptionShouldRouteToRetry-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_RETRY, 1);
    }

    @Test
    public void testCreateDirectoryFailureExceptionShouldRouteToFailure() throws InitializationException, IOException {
        final PutParquet proc = new PutParquet() {
            @Override
            protected void createDirectory(FileSystem fileSystem, Path directory, String remoteOwner, String remoteGroup)
                    throws IOException, FailureException {
                throw new FailureException("FailureException creating directory");
            }
        };

        configure(proc, 10);

        final String filename = "testCreateDirectoryFailureExceptionShouldRouteToFailure-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_FAILURE, 1);
    }

    @Test
    public void testDestinationExistsWithoutOverwriteShouldRouteFailure() throws IOException, InitializationException {
        configure(proc, 10);
        testRunner.setProperty(PutParquet.OVERWRITE, "false");

        final String filename = "testDestinationExistsWithoutOverwriteShouldRouteFailure-" + System.currentTimeMillis();

        // create a file in the directory with the same name
        final File avroParquetFile = new File(DIRECTORY + "/" + filename);
        Assert.assertTrue(avroParquetFile.createNewFile());

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_FAILURE, 1);
    }

    @Test
    public void testTempDestinationExistsWithoutOverwriteShouldRouteFailure() throws IOException, InitializationException {
        configure(proc, 10);
        testRunner.setProperty(PutParquet.OVERWRITE, "false");

        // use the dot filename
        final String filename = ".testDestinationExistsWithoutOverwriteShouldRouteFailure-" + System.currentTimeMillis();

        // create a file in the directory with the same name
        final File avroParquetFile = new File(DIRECTORY + "/" + filename);
        Assert.assertTrue(avroParquetFile.createNewFile());

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_FAILURE, 1);
    }

    @Test
    public void testDestinationExistsWithOverwriteShouldBeSuccessful() throws InitializationException, IOException {
        configure(proc, 10);
        testRunner.setProperty(PutParquet.OVERWRITE, "true");

        final String filename = "testDestinationExistsWithOverwriteShouldBeSuccessful-" + System.currentTimeMillis();

        // create a file in the directory with the same name
        final File avroParquetFile = new File(DIRECTORY + "/" + filename);
        Assert.assertTrue(avroParquetFile.createNewFile());

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_SUCCESS, 1);
    }

    @Test
    public void testValidSchemaWithELShouldBeSuccessful() throws InitializationException, IOException {
        configure(proc, 10);

        final String filename = "testValidSchemaWithELShouldBeSuccessful-" + System.currentTimeMillis();

        // don't provide my.schema as an attribute
        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);
        flowFileAttributes.put("my.schema", schema.toString());

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_SUCCESS, 1);
    }

    @Test
    public void testMalformedRecordExceptionFromReaderShouldRouteToFailure() throws InitializationException, IOException, MalformedRecordException, SchemaNotFoundException {
        configure(proc, 10);

        final RecordReader recordReader = Mockito.mock(RecordReader.class);
        when(recordReader.nextRecord()).thenThrow(new MalformedRecordException("ERROR"));

        final RecordReaderFactory readerFactory = Mockito.mock(RecordReaderFactory.class);
        when(readerFactory.getIdentifier()).thenReturn("mock-reader-factory");
        when(readerFactory.createRecordReader(any(FlowFile.class), any(InputStream.class), any(ComponentLog.class))).thenReturn(recordReader);

        testRunner.addControllerService("mock-reader-factory", readerFactory);
        testRunner.enableControllerService(readerFactory);
        testRunner.setProperty(PutParquet.RECORD_READER, "mock-reader-factory");

        final String filename = "testMalformedRecordExceptionShouldRouteToFailure-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_FAILURE, 1);
    }

    @Test
    public void testIOExceptionCreatingWriterShouldRouteToRetry() throws InitializationException, IOException, MalformedRecordException {
        final PutParquet proc = new PutParquet() {
            @Override
            public HDFSRecordWriter createHDFSRecordWriter(ProcessContext context, FlowFile flowFile, Configuration conf, Path path, RecordSchema schema)
                    throws IOException, SchemaNotFoundException {
                throw new IOException("IOException");
            }
        };
        configure(proc, 0);

        final String filename = "testMalformedRecordExceptionShouldRouteToFailure-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_RETRY, 1);
    }

    @Test
    public void testIOExceptionFromReaderShouldRouteToRetry() throws InitializationException, IOException, MalformedRecordException, SchemaNotFoundException {
        configure(proc, 10);

        final RecordSet recordSet = Mockito.mock(RecordSet.class);
        when(recordSet.next()).thenThrow(new IOException("ERROR"));

        final RecordReader recordReader = Mockito.mock(RecordReader.class);
        when(recordReader.createRecordSet()).thenReturn(recordSet);
        when(recordReader.getSchema()).thenReturn(AvroTypeUtil.createSchema(schema));

        final RecordReaderFactory readerFactory = Mockito.mock(RecordReaderFactory.class);
        when(readerFactory.getIdentifier()).thenReturn("mock-reader-factory");
        when(readerFactory.createRecordReader(any(FlowFile.class), any(InputStream.class), any(ComponentLog.class))).thenReturn(recordReader);

        testRunner.addControllerService("mock-reader-factory", readerFactory);
        testRunner.enableControllerService(readerFactory);
        testRunner.setProperty(PutParquet.RECORD_READER, "mock-reader-factory");

        final String filename = "testMalformedRecordExceptionShouldRouteToFailure-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_RETRY, 1);
    }

    @Test
    public void testIOExceptionRenamingShouldRouteToRetry() throws InitializationException, IOException {
        final PutParquet proc = new PutParquet() {
            @Override
            protected void rename(FileSystem fileSystem, Path srcFile, Path destFile)
                    throws IOException, InterruptedException, FailureException {
                throw new IOException("IOException renaming");
            }
        };

        configure(proc, 10);

        final String filename = "testIOExceptionRenamingShouldRouteToRetry-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_RETRY, 1);

        // verify we don't have the temp dot file after success
        final File tempAvroParquetFile = new File(DIRECTORY + "/." + filename);
        Assert.assertFalse(tempAvroParquetFile.exists());
    }

    @Test
    public void testFailureExceptionRenamingShouldRouteToFailure() throws InitializationException, IOException {
        final PutParquet proc = new PutParquet() {
            @Override
            protected void rename(FileSystem fileSystem, Path srcFile, Path destFile)
                    throws IOException, InterruptedException, FailureException {
                throw new FailureException("FailureException renaming");
            }
        };

        configure(proc, 10);

        final String filename = "testFailureExceptionRenamingShouldRouteToFailure-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_FAILURE, 1);

        // verify we don't have the temp dot file after success
        final File tempAvroParquetFile = new File(DIRECTORY + "/." + filename);
        Assert.assertFalse(tempAvroParquetFile.exists());
    }

    @Test
    public void testRowGroupSize() throws IOException, InitializationException {
        configure(proc, 10);
        testRunner.setProperty(ParquetUtils.ROW_GROUP_SIZE, "1024 B");

        final String filename = "testRowGroupSize-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_SUCCESS, 1);
    }

    @Test
    public void testInvalidRowGroupSizeFromELShouldRouteToFailure() throws IOException, InitializationException {
        configure(proc, 10);
        testRunner.setProperty(ParquetUtils.ROW_GROUP_SIZE, "${row.group.size}");

        final String filename = "testInvalidRowGroupSizeFromELShouldRouteToFailure" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);
        flowFileAttributes.put("row.group.size", "NOT A DATA SIZE");

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_FAILURE, 1);
    }

    @Test
    public void testPageSize() throws IOException, InitializationException {
        configure(proc, 10);
        testRunner.setProperty(ParquetUtils.PAGE_SIZE, "1024 B");

        final String filename = "testPageGroupSize-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_SUCCESS, 1);
    }

    @Test
    public void testInvalidPageSizeFromELShouldRouteToFailure() throws IOException, InitializationException {
        configure(proc, 10);
        testRunner.setProperty(ParquetUtils.PAGE_SIZE, "${page.size}");

        final String filename = "testInvalidPageSizeFromELShouldRouteToFailure" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);
        flowFileAttributes.put("page.size", "NOT A DATA SIZE");

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_FAILURE, 1);
    }

    @Test
    public void testDictionaryPageSize() throws IOException, InitializationException {
        configure(proc, 10);
        testRunner.setProperty(ParquetUtils.DICTIONARY_PAGE_SIZE, "1024 B");

        final String filename = "testDictionaryPageGroupSize-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_SUCCESS, 1);
    }

    @Test
    public void testInvalidDictionaryPageSizeFromELShouldRouteToFailure() throws IOException, InitializationException {
        configure(proc, 10);
        testRunner.setProperty(ParquetUtils.DICTIONARY_PAGE_SIZE, "${dictionary.page.size}");

        final String filename = "testInvalidDictionaryPageSizeFromELShouldRouteToFailure" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);
        flowFileAttributes.put("dictionary.page.size", "NOT A DATA SIZE");

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_FAILURE, 1);
    }

    @Test
    public void testMaxPaddingPageSize() throws IOException, InitializationException {
        configure(proc, 10);
        testRunner.setProperty(ParquetUtils.MAX_PADDING_SIZE, "1024 B");

        final String filename = "testMaxPaddingSize-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_SUCCESS, 1);
    }

    @Test
    public void testInvalidMaxPaddingSizeFromELShouldRouteToFailure() throws IOException, InitializationException {
        configure(proc, 10);
        testRunner.setProperty(ParquetUtils.MAX_PADDING_SIZE, "${max.padding.size}");

        final String filename = "testInvalidMaxPaddingSizeFromELShouldRouteToFailure" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);
        flowFileAttributes.put("max.padding.size", "NOT A DATA SIZE");

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_FAILURE, 1);
    }

    @Test
    public void testReadAsStringAndWriteAsInt() throws InitializationException, IOException {
        configure(proc, 0);

        // add the favorite color as a string
        readerFactory.addRecord("name0", "0", "blue0");

        final String filename = "testReadAsStringAndWriteAsInt-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutParquet.REL_SUCCESS, 1);

        final Path avroParquetFile = new Path(DIRECTORY + "/" + filename);

        // verify the content of the parquet file by reading it back in
        verifyAvroParquetUsers(avroParquetFile, 1);
    }

    private void verifyAvroParquetUsers(final Path avroParquetUsers, final int numExpectedUsers) throws IOException {
        final ParquetReader.Builder<GenericRecord> readerBuilder = AvroParquetReader
                .<GenericRecord>builder(avroParquetUsers)
                .withConf(testConf);

        int currUser = 0;

        try (final ParquetReader<GenericRecord> reader = readerBuilder.build()) {
            GenericRecord nextRecord;
            while((nextRecord = reader.read()) != null) {
                Assert.assertNotNull(nextRecord);
                Assert.assertEquals("name" + currUser, nextRecord.get("name").toString());
                Assert.assertEquals(currUser, nextRecord.get("favorite_number"));
                Assert.assertEquals("blue" + currUser, nextRecord.get("favorite_color").toString());
                currUser++;
            }
        }

        Assert.assertEquals(numExpectedUsers, currUser);
    }


}
