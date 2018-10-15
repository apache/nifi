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
package org.apache.nifi.processors.orc;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.BasicConfigurator;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.hadoop.exception.FailureException;
import org.apache.nifi.processors.hadoop.record.HDFSRecordWriter;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class PutORCTest {

    private static final String DIRECTORY = "target";
    private static final String TEST_CONF_PATH = "src/test/resources/core-site.xml";

    private Schema schema;
    private Configuration testConf;
    private PutORC proc;
    private TestRunner testRunner;

    @BeforeClass
    public static void setupLogging() {
        BasicConfigurator.configure();
    }

    @Before
    public void setup() throws IOException {
        final String avroSchema = IOUtils.toString(new FileInputStream("src/test/resources/user.avsc"), StandardCharsets.UTF_8);
        schema = new Schema.Parser().parse(avroSchema);

        testConf = new Configuration();
        testConf.addResource(new Path(TEST_CONF_PATH));

        proc = new PutORC();
    }

    private void configure(final PutORC putORC, final int numUsers) throws InitializationException {
        configure(putORC, numUsers, null);
    }

    private void configure(final PutORC putORC, final int numUsers, final BiFunction<Integer, MockRecordParser, Void> recordGenerator) throws InitializationException {
        testRunner = TestRunners.newTestRunner(putORC);
        testRunner.setProperty(PutORC.HADOOP_CONFIGURATION_RESOURCES, TEST_CONF_PATH);
        testRunner.setProperty(PutORC.DIRECTORY, DIRECTORY);

        MockRecordParser readerFactory = new MockRecordParser();

        final RecordSchema recordSchema = AvroTypeUtil.createSchema(schema);
        for (final RecordField recordField : recordSchema.getFields()) {
            readerFactory.addSchemaField(recordField.getFieldName(), recordField.getDataType().getFieldType(), recordField.isNullable());
        }

        if (recordGenerator == null) {
            for (int i = 0; i < numUsers; i++) {
                readerFactory.addRecord("name" + i, i, "blue" + i, i * 10.0);
            }
        } else {
            recordGenerator.apply(numUsers, readerFactory);
        }

        testRunner.addControllerService("mock-reader-factory", readerFactory);
        testRunner.enableControllerService(readerFactory);

        testRunner.setProperty(PutORC.RECORD_READER, "mock-reader-factory");
    }

    @Test
    public void testWriteORCWithDefaults() throws IOException, InitializationException {
        configure(proc, 100);

        final String filename = "testORCWithDefaults-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.setProperty(PutORC.HIVE_TABLE_NAME, "myTable");

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutORC.REL_SUCCESS, 1);

        final Path orcFile = new Path(DIRECTORY + "/" + filename);

        // verify the successful flow file has the expected attributes
        final MockFlowFile mockFlowFile = testRunner.getFlowFilesForRelationship(PutORC.REL_SUCCESS).get(0);
        mockFlowFile.assertAttributeEquals(PutORC.ABSOLUTE_HDFS_PATH_ATTRIBUTE, orcFile.getParent().toString());
        mockFlowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), filename);
        mockFlowFile.assertAttributeEquals(PutORC.RECORD_COUNT_ATTR, "100");
        mockFlowFile.assertAttributeEquals(PutORC.HIVE_DDL_ATTRIBUTE,
                "CREATE EXTERNAL TABLE IF NOT EXISTS `myTable` (`name` STRING, `favorite_number` INT, `favorite_color` STRING, `scale` DOUBLE) STORED AS ORC");

        // verify we generated a provenance event
        final List<ProvenanceEventRecord> provEvents = testRunner.getProvenanceEvents();
        assertEquals(1, provEvents.size());

        // verify it was a SEND event with the correct URI
        final ProvenanceEventRecord provEvent = provEvents.get(0);
        assertEquals(ProvenanceEventType.SEND, provEvent.getEventType());
        // If it runs with a real HDFS, the protocol will be "hdfs://", but with a local filesystem, just assert the filename.
        Assert.assertTrue(provEvent.getTransitUri().endsWith(DIRECTORY + "/" + filename));

        // verify the content of the ORC file by reading it back in
        verifyORCUsers(orcFile, 100);

        // verify we don't have the temp dot file after success
        final File tempOrcFile = new File(DIRECTORY + "/." + filename);
        Assert.assertFalse(tempOrcFile.exists());

        // verify we DO have the CRC file after success
        final File crcAvroORCFile = new File(DIRECTORY + "/." + filename + ".crc");
        Assert.assertTrue(crcAvroORCFile.exists());
    }

    @Test
    public void testWriteORCWithAvroLogicalTypes() throws IOException, InitializationException {
        final String avroSchema = IOUtils.toString(new FileInputStream("src/test/resources/user_logical_types.avsc"), StandardCharsets.UTF_8);
        schema = new Schema.Parser().parse(avroSchema);
        Calendar now = Calendar.getInstance();
        LocalTime nowTime = LocalTime.now();
        LocalDateTime nowDateTime = LocalDateTime.now();
        LocalDate epoch = LocalDate.ofEpochDay(0);
        LocalDate nowDate = LocalDate.now();

        final int timeMillis = nowTime.get(ChronoField.MILLI_OF_DAY);
        final Timestamp timestampMillis = Timestamp.valueOf(nowDateTime);
        final Date dt = Date.valueOf(nowDate);
        final double dec = 1234.56;

        configure(proc, 10, (numUsers, readerFactory) -> {
            for (int i = 0; i < numUsers; i++) {
                readerFactory.addRecord(
                        i,
                        timeMillis,
                        timestampMillis,
                        dt,
                        dec);
            }
            return null;
        });

        final String filename = "testORCWithDefaults-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.setProperty(PutORC.HIVE_TABLE_NAME, "myTable");

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutORC.REL_SUCCESS, 1);

        final Path orcFile = new Path(DIRECTORY + "/" + filename);

        // verify the successful flow file has the expected attributes
        final MockFlowFile mockFlowFile = testRunner.getFlowFilesForRelationship(PutORC.REL_SUCCESS).get(0);
        mockFlowFile.assertAttributeEquals(PutORC.ABSOLUTE_HDFS_PATH_ATTRIBUTE, orcFile.getParent().toString());
        mockFlowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), filename);
        mockFlowFile.assertAttributeEquals(PutORC.RECORD_COUNT_ATTR, "10");
        // DDL will be created with field names normalized (lowercased, e.g.) for Hive by default
        mockFlowFile.assertAttributeEquals(PutORC.HIVE_DDL_ATTRIBUTE,
                "CREATE EXTERNAL TABLE IF NOT EXISTS `myTable` (`id` INT, `timemillis` INT, `timestampmillis` TIMESTAMP, `dt` DATE, `dec` DOUBLE) STORED AS ORC");

        // verify we generated a provenance event
        final List<ProvenanceEventRecord> provEvents = testRunner.getProvenanceEvents();
        assertEquals(1, provEvents.size());

        // verify it was a SEND event with the correct URI
        final ProvenanceEventRecord provEvent = provEvents.get(0);
        assertEquals(ProvenanceEventType.SEND, provEvent.getEventType());
        // If it runs with a real HDFS, the protocol will be "hdfs://", but with a local filesystem, just assert the filename.
        Assert.assertTrue(provEvent.getTransitUri().endsWith(DIRECTORY + "/" + filename));

        // verify the content of the ORC file by reading it back in
        verifyORCUsers(orcFile, 10, (x, currUser) -> {
                    assertEquals((int) currUser, ((IntWritable) x.get(0)).get());
                    assertEquals(timeMillis, ((IntWritable) x.get(1)).get());
                    assertEquals(timestampMillis, ((TimestampWritableV2) x.get(2)).getTimestamp().toSqlTimestamp());
                    final DateFormat noTimeOfDayDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    noTimeOfDayDateFormat.setTimeZone(TimeZone.getTimeZone("gmt"));
                    assertEquals(noTimeOfDayDateFormat.format(dt), ((DateWritableV2) x.get(3)).get().toString());
                    assertEquals(dec, ((DoubleWritable) x.get(4)).get(), Double.MIN_VALUE);
                    return null;
                }
        );

        // verify we don't have the temp dot file after success
        final File tempOrcFile = new File(DIRECTORY + "/." + filename);
        Assert.assertFalse(tempOrcFile.exists());

        // verify we DO have the CRC file after success
        final File crcAvroORCFile = new File(DIRECTORY + "/." + filename + ".crc");
        Assert.assertTrue(crcAvroORCFile.exists());
    }

    @Test
    public void testValidSchemaWithELShouldBeSuccessful() throws InitializationException {
        configure(proc, 10);

        final String filename = "testValidSchemaWithELShouldBeSuccessful-" + System.currentTimeMillis();

        // don't provide my.schema as an attribute
        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);
        flowFileAttributes.put("my.schema", schema.toString());

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutORC.REL_SUCCESS, 1);
    }

    @Test
    public void testMalformedRecordExceptionFromReaderShouldRouteToFailure() throws InitializationException, IOException, MalformedRecordException, SchemaNotFoundException {
        configure(proc, 10);

        final org.apache.nifi.serialization.RecordReader recordReader = Mockito.mock(org.apache.nifi.serialization.RecordReader.class);
        when(recordReader.nextRecord()).thenThrow(new MalformedRecordException("ERROR"));

        final RecordReaderFactory readerFactory = Mockito.mock(RecordReaderFactory.class);
        when(readerFactory.getIdentifier()).thenReturn("mock-reader-factory");
        when(readerFactory.createRecordReader(any(FlowFile.class), any(InputStream.class), any(ComponentLog.class))).thenReturn(recordReader);

        testRunner.addControllerService("mock-reader-factory", readerFactory);
        testRunner.enableControllerService(readerFactory);
        testRunner.setProperty(PutORC.RECORD_READER, "mock-reader-factory");

        final String filename = "testMalformedRecordExceptionShouldRouteToFailure-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutORC.REL_FAILURE, 1);
    }

    @Test
    public void testIOExceptionCreatingWriterShouldRouteToRetry() throws InitializationException {
        final PutORC proc = new PutORC() {
            @Override
            public HDFSRecordWriter createHDFSRecordWriter(ProcessContext context, FlowFile flowFile, Configuration conf, Path path, RecordSchema schema)
                    throws IOException {
                throw new IOException("IOException");
            }
        };
        configure(proc, 0);

        final String filename = "testMalformedRecordExceptionShouldRouteToFailure-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutORC.REL_RETRY, 1);
    }

    @Test
    public void testIOExceptionFromReaderShouldRouteToRetry() throws InitializationException, IOException, MalformedRecordException, SchemaNotFoundException {
        configure(proc, 10);

        final RecordSet recordSet = Mockito.mock(RecordSet.class);
        when(recordSet.next()).thenThrow(new IOException("ERROR"));

        final org.apache.nifi.serialization.RecordReader recordReader = Mockito.mock(org.apache.nifi.serialization.RecordReader.class);
        when(recordReader.createRecordSet()).thenReturn(recordSet);
        when(recordReader.getSchema()).thenReturn(AvroTypeUtil.createSchema(schema));

        final RecordReaderFactory readerFactory = Mockito.mock(RecordReaderFactory.class);
        when(readerFactory.getIdentifier()).thenReturn("mock-reader-factory");
        when(readerFactory.createRecordReader(any(FlowFile.class), any(InputStream.class), any(ComponentLog.class))).thenReturn(recordReader);

        testRunner.addControllerService("mock-reader-factory", readerFactory);
        testRunner.enableControllerService(readerFactory);
        testRunner.setProperty(PutORC.RECORD_READER, "mock-reader-factory");

        final String filename = "testMalformedRecordExceptionShouldRouteToFailure-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutORC.REL_RETRY, 1);
    }

    @Test
    public void testIOExceptionRenamingShouldRouteToRetry() throws InitializationException {
        final PutORC proc = new PutORC() {
            @Override
            protected void rename(FileSystem fileSystem, Path srcFile, Path destFile)
                    throws IOException, InterruptedException, FailureException {
                throw new IOException("IOException renaming");
            }
        };

        configure(proc, 10);

        final String filename = "testIOExceptionRenamingShouldRouteToRetry-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutORC.REL_RETRY, 1);

        // verify we don't have the temp dot file after success
        final File tempAvroORCFile = new File(DIRECTORY + "/." + filename);
        Assert.assertFalse(tempAvroORCFile.exists());
    }

    @Test
    public void testNestedRecords() throws Exception {
        testRunner = TestRunners.newTestRunner(proc);
        testRunner.setProperty(PutORC.HADOOP_CONFIGURATION_RESOURCES, TEST_CONF_PATH);
        testRunner.setProperty(PutORC.DIRECTORY, DIRECTORY);

        MockRecordParser readerFactory = new MockRecordParser();

        final String avroSchema = IOUtils.toString(new FileInputStream("src/test/resources/nested_record.avsc"), StandardCharsets.UTF_8);
        schema = new Schema.Parser().parse(avroSchema);

        final RecordSchema recordSchema = AvroTypeUtil.createSchema(schema);
        for (final RecordField recordField : recordSchema.getFields()) {
            readerFactory.addSchemaField(recordField);
        }

        Map<String,Object> nestedRecordMap = new HashMap<>();
        nestedRecordMap.put("id", 11088000000001615L);
        nestedRecordMap.put("x", "Hello World!");

        RecordSchema nestedRecordSchema = AvroTypeUtil.createSchema(schema.getField("myField").schema());
        MapRecord nestedRecord = new MapRecord(nestedRecordSchema, nestedRecordMap);
        // This gets added in to its spot in the schema, which is already named "myField"
        readerFactory.addRecord(nestedRecord);

        testRunner.addControllerService("mock-reader-factory", readerFactory);
        testRunner.enableControllerService(readerFactory);

        testRunner.setProperty(PutORC.RECORD_READER, "mock-reader-factory");

        testRunner.enqueue("trigger");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutORC.REL_SUCCESS, 1);
    }

    private void verifyORCUsers(final Path orcUsers, final int numExpectedUsers) throws IOException {
        verifyORCUsers(orcUsers, numExpectedUsers, null);
    }

    private void verifyORCUsers(final Path orcUsers, final int numExpectedUsers, BiFunction<List<Object>, Integer, Void> assertFunction) throws IOException {
        Reader reader = OrcFile.createReader(orcUsers, OrcFile.readerOptions(testConf));
        RecordReader recordReader = reader.rows();

        TypeInfo typeInfo =
                TypeInfoUtils.getTypeInfoFromTypeString("struct<name:string,favorite_number:int,favorite_color:string,scale:double>");
        StructObjectInspector inspector = (StructObjectInspector)
                OrcStruct.createObjectInspector(typeInfo);

        int currUser = 0;
        Object nextRecord = null;
        while ((nextRecord = recordReader.next(nextRecord)) != null) {
            Assert.assertNotNull(nextRecord);
            Assert.assertTrue("Not an OrcStruct", nextRecord instanceof OrcStruct);
            List<Object> x = inspector.getStructFieldsDataAsList(nextRecord);

            if (assertFunction == null) {
                assertEquals("name" + currUser, x.get(0).toString());
                assertEquals(currUser, ((IntWritable) x.get(1)).get());
                assertEquals("blue" + currUser, x.get(2).toString());
                assertEquals(10.0 * currUser, ((DoubleWritable) x.get(3)).get(), Double.MIN_VALUE);
            } else {
                assertFunction.apply(x, currUser);
            }
            currUser++;
        }

        assertEquals(numExpectedUsers, currUser);
    }

}