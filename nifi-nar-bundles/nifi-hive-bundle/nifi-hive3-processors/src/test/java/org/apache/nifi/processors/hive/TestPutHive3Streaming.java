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
package org.apache.nifi.processors.hive;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.streaming.ConnectionStats;
import org.apache.hive.streaming.HiveRecordWriter;
import org.apache.hive.streaming.PartitionInfo;
import org.apache.hive.streaming.RecordWriter;
import org.apache.hive.streaming.StreamingConnection;
import org.apache.hive.streaming.StreamingException;
import org.apache.hive.streaming.StubConnectionError;
import org.apache.hive.streaming.StubSerializationError;
import org.apache.hive.streaming.StubStreamingIOFailure;
import org.apache.hive.streaming.StubTransactionError;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.hive.HiveConfigurator;
import org.apache.nifi.util.hive.HiveOptions;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.apache.nifi.processors.hive.AbstractHive3QLProcessor.ATTR_OUTPUT_TABLES;
import static org.apache.nifi.processors.hive.PutHive3Streaming.HIVE_STREAMING_RECORD_COUNT_ATTR;
import static org.apache.nifi.processors.hive.PutHive3Streaming.KERBEROS_CREDENTIALS_SERVICE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for PutHive3Streaming processor.
 */
public class TestPutHive3Streaming {

    private static final String TEST_CONF_PATH = "src/test/resources/core-site.xml";
    private static final String TARGET_HIVE = "target/hive";

    private TestRunner runner;
    private MockPutHive3Streaming processor;

    private HiveConfigurator hiveConfigurator;
    private HiveConf hiveConf;
    private UserGroupInformation ugi;
    private Schema schema;

    @Before
    public void setUp() throws Exception {

        final String avroSchema = IOUtils.toString(new FileInputStream("src/test/resources/user.avsc"), StandardCharsets.UTF_8);
        schema = new Schema.Parser().parse(avroSchema);

        Configuration testConf = new Configuration();
        testConf.addResource(new Path(TEST_CONF_PATH));

        // needed for calls to UserGroupInformation.setConfiguration() to work when passing in
        // config with Kerberos authentication enabled
        System.setProperty("java.security.krb5.realm", "nifi.com");
        System.setProperty("java.security.krb5.kdc", "nifi.kdc");

        ugi = null;
        processor = new MockPutHive3Streaming();
        hiveConfigurator = mock(HiveConfigurator.class);
        hiveConf = new HiveConf();
        when(hiveConfigurator.getConfigurationFromFiles(anyString())).thenReturn(hiveConf);
        processor.hiveConfigurator = hiveConfigurator;

        // Delete any temp files from previous tests
        try {
            FileUtils.deleteDirectory(new File(TARGET_HIVE));
        } catch (IOException ioe) {
            // Do nothing, directory may not have existed
        }
    }

    private void configure(final PutHive3Streaming processor, final int numUsers) throws InitializationException {
        configure(processor, numUsers, -1);
    }

    private void configure(final PutHive3Streaming processor, final int numUsers, int failAfter) throws InitializationException {
        configure(processor, numUsers, failAfter, null);
    }

    private void configure(final PutHive3Streaming processor, final int numUsers, final int failAfter,
                           final BiFunction<Integer, MockRecordParser, Void> recordGenerator) throws InitializationException {
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(PutHive3Streaming.HIVE_CONFIGURATION_RESOURCES, TEST_CONF_PATH);
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

        readerFactory.failAfter(failAfter);

        runner.addControllerService("mock-reader-factory", readerFactory);
        runner.enableControllerService(readerFactory);

        runner.setProperty(PutHive3Streaming.RECORD_READER, "mock-reader-factory");
    }

    private void configureComplex(final MockPutHive3Streaming processor, final int numUsers, final int failAfter,
                                  final BiFunction<Integer, MockRecordParser, Void> recordGenerator) throws IOException, InitializationException {
        final String avroSchema = IOUtils.toString(new FileInputStream("src/test/resources/array_of_records.avsc"), StandardCharsets.UTF_8);
        schema = new Schema.Parser().parse(avroSchema);
        processor.setFields(Arrays.asList(new FieldSchema("records",
                serdeConstants.LIST_TYPE_NAME + "<"
                        + serdeConstants.MAP_TYPE_NAME + "<"
                        + serdeConstants.STRING_TYPE_NAME + ","
                        +  serdeConstants.STRING_TYPE_NAME + ">>", "")));
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(PutHive3Streaming.HIVE_CONFIGURATION_RESOURCES, TEST_CONF_PATH);
        MockRecordParser readerFactory = new MockRecordParser();
        final RecordSchema recordSchema = AvroTypeUtil.createSchema(schema);
        for (final RecordField recordField : recordSchema.getFields()) {
            readerFactory.addSchemaField(recordField.getFieldName(), recordField.getDataType().getFieldType(), recordField.isNullable());
        }

        if (recordGenerator == null) {
            Object[] mapArray = new Object[numUsers];
            for (int i = 0; i < numUsers; i++) {
                final int x = i;
                Map<String, Object> map = new HashMap<String, Object>() {{
                    put("name", "name" + x);
                    put("age", x * 5);
                }};
                mapArray[i] = map;
            }
            readerFactory.addRecord((Object)mapArray);
        } else {
            recordGenerator.apply(numUsers, readerFactory);
        }

        readerFactory.failAfter(failAfter);

        runner.addControllerService("mock-reader-factory", readerFactory);
        runner.enableControllerService(readerFactory);

        runner.setProperty(PutHive3Streaming.RECORD_READER, "mock-reader-factory");
    }

    @Test
    public void testSetup() throws Exception {
        configure(processor, 0);
        runner.assertNotValid();
        runner.setProperty(PutHive3Streaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHive3Streaming.DB_NAME, "default");
        runner.assertNotValid();
        runner.setProperty(PutHive3Streaming.TABLE_NAME, "users");
        runner.assertValid();
        runner.run();
    }

    @Test
    public void testUgiGetsCleared() throws Exception {
        configure(processor, 0);
        runner.setProperty(PutHive3Streaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHive3Streaming.DB_NAME, "default");
        runner.setProperty(PutHive3Streaming.TABLE_NAME, "users");
        processor.ugi = mock(UserGroupInformation.class);
        runner.run();
        assertNull(processor.ugi);
    }

    @Test
    public void testUgiGetsSetIfSecure() throws Exception {
        configure(processor, 1);
        hiveConf.set(SecurityUtil.HADOOP_SECURITY_AUTHENTICATION, SecurityUtil.KERBEROS);
        KerberosCredentialsService kcs = new MockKerberosCredentialsService();
        runner.addControllerService("kcs", kcs);
        runner.setProperty(KERBEROS_CREDENTIALS_SERVICE, "kcs");
        runner.enableControllerService(kcs);
        ugi = mock(UserGroupInformation.class);
        when(hiveConfigurator.authenticate(eq(hiveConf), anyString(), anyString())).thenReturn(ugi);
        runner.setProperty(PutHive3Streaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHive3Streaming.DB_NAME, "default");
        runner.setProperty(PutHive3Streaming.TABLE_NAME, "users");
        runner.enqueue(new byte[0]);
        runner.run();
    }

    @Test(expected = AssertionError.class)
    public void testSetupWithKerberosAuthFailed() throws Exception {
        configure(processor, 0);
        runner.setProperty(PutHive3Streaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHive3Streaming.DB_NAME, "default");
        runner.setProperty(PutHive3Streaming.TABLE_NAME, "users");
        runner.setProperty(PutHive3Streaming.HIVE_CONFIGURATION_RESOURCES, "src/test/resources/core-site-security.xml, src/test/resources/hive-site-security.xml");

        hiveConf.set(SecurityUtil.HADOOP_SECURITY_AUTHENTICATION, SecurityUtil.KERBEROS);
        KerberosCredentialsService kcs = new MockKerberosCredentialsService(null, null);
        runner.addControllerService("kcs", kcs);
        runner.setProperty(KERBEROS_CREDENTIALS_SERVICE, "kcs");
        runner.enableControllerService(kcs);
        runner.assertNotValid();
        runner.run();
    }

    @Test
    public void onTrigger() throws Exception {
        configure(processor, 1);
        runner.setProperty(PutHive3Streaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHive3Streaming.DB_NAME, "default");
        runner.setProperty(PutHive3Streaming.TABLE_NAME, "users");
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutHive3Streaming.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutHive3Streaming.REL_SUCCESS).get(0);
        assertEquals("1", flowFile.getAttribute(HIVE_STREAMING_RECORD_COUNT_ATTR));
        assertEquals("default.users", flowFile.getAttribute(ATTR_OUTPUT_TABLES));
    }

    @Test
    public void onTriggerComplex() throws Exception {
        configureComplex(processor, 10, -1, null);
        runner.setProperty(PutHive3Streaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHive3Streaming.DB_NAME, "default");
        runner.setProperty(PutHive3Streaming.TABLE_NAME, "users");
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutHive3Streaming.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutHive3Streaming.REL_SUCCESS).get(0);
        // Schema is an array of size 10, so only one record is output
        assertEquals("1", flowFile.getAttribute(HIVE_STREAMING_RECORD_COUNT_ATTR));
        assertEquals("default.users", flowFile.getAttribute(ATTR_OUTPUT_TABLES));
    }

    @Test
    public void onTriggerBadInput() throws Exception {
        configure(processor, 1, 0);
        runner.setProperty(PutHive3Streaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHive3Streaming.DB_NAME, "default");
        runner.setProperty(PutHive3Streaming.TABLE_NAME, "users");
        runner.enqueue("I am not an Avro record".getBytes());
        runner.run();

        runner.assertTransferCount(PutHive3Streaming.REL_FAILURE, 1);
    }

    @Test
    public void onTriggerBadInputRollbackOnFailure() throws Exception {
        configure(processor, 1, 0);
        runner.setProperty(PutHive3Streaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHive3Streaming.DB_NAME, "default");
        runner.setProperty(PutHive3Streaming.TABLE_NAME, "users");

        runner.setProperty(PutHive3Streaming.ROLLBACK_ON_FAILURE, "true");
        runner.enqueue("I am not an Avro record".getBytes());
        try {
            runner.run();
            fail("ProcessException should be thrown");
        } catch (AssertionError e) {
            assertTrue(e.getCause() instanceof ProcessException);
        }

        runner.assertTransferCount(PutHive3Streaming.REL_FAILURE, 0);
        // Assert incoming FlowFile stays in input queue.
        assertEquals(1, runner.getQueueSize().getObjectCount());
    }


    @Test
    public void onTriggerMultipleRecordsSingleTransaction() throws Exception {
        configure(processor, 3);
        runner.setProperty(PutHive3Streaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHive3Streaming.DB_NAME, "default");
        runner.setProperty(PutHive3Streaming.TABLE_NAME, "users");
        Map<String, Object> user1 = new HashMap<String, Object>() {
            {
                put("name", "Joe");
                put("favorite_number", 146);
            }
        };
        Map<String, Object> user2 = new HashMap<String, Object>() {
            {
                put("name", "Mary");
                put("favorite_number", 42);
            }
        };
        Map<String, Object> user3 = new HashMap<String, Object>() {
            {
                put("name", "Matt");
                put("favorite_number", 3);
            }
        };
        final List<Map<String, Object>> users = Arrays.asList(user1, user2, user3);
        runner.enqueue(createAvroRecord(users));
        runner.run();

        runner.assertTransferCount(PutHive3Streaming.REL_SUCCESS, 1);
        MockFlowFile resultFlowFile = runner.getFlowFilesForRelationship(PutHive3Streaming.REL_SUCCESS).get(0);
        assertOutputAvroRecords(users, resultFlowFile);
    }

    @Test
    public void onTriggerMultipleRecordsFailInMiddle() throws Exception {
        configure(processor, 4);
        runner.setProperty(PutHive3Streaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHive3Streaming.DB_NAME, "default");
        runner.setProperty(PutHive3Streaming.TABLE_NAME, "users");
        processor.setGenerateWriteFailure(true);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutHive3Streaming.REL_SUCCESS, 0);
        runner.assertTransferCount(PutHive3Streaming.REL_FAILURE, 1);
        runner.assertTransferCount(PutHive3Streaming.REL_RETRY, 0);
    }

    @Test
    public void onTriggerMultipleRecordsFailInMiddleRollbackOnFailure() throws Exception {
        configure(processor, 3);
        runner.setProperty(PutHive3Streaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHive3Streaming.DB_NAME, "default");
        runner.setProperty(PutHive3Streaming.TABLE_NAME, "users");
        runner.setProperty(PutHive3Streaming.ROLLBACK_ON_FAILURE, "true");
        processor.setGenerateWriteFailure(true);
        runner.enqueue(new byte[0]);
        try {
            runner.run();
            fail("ProcessException should be thrown, because any Hive Transaction is committed yet.");
        } catch (AssertionError e) {
            assertTrue(e.getCause() instanceof ProcessException);
        }

        runner.assertTransferCount(PutHive3Streaming.REL_SUCCESS, 0);
        runner.assertTransferCount(PutHive3Streaming.REL_RETRY, 0);
        runner.assertTransferCount(PutHive3Streaming.REL_FAILURE, 0);
        // Assert incoming FlowFile stays in input queue.
        assertEquals(1, runner.getQueueSize().getObjectCount());
    }

    private void assertOutputAvroRecords(List<Map<String, Object>> expectedRecords, MockFlowFile resultFlowFile) throws IOException {
        assertEquals(String.valueOf(expectedRecords.size()), resultFlowFile.getAttribute(PutHive3Streaming.HIVE_STREAMING_RECORD_COUNT_ATTR));

        final DataFileStream<GenericRecord> reader = new DataFileStream<>(
                new ByteArrayInputStream(resultFlowFile.toByteArray()),
                new GenericDatumReader<>());

        Schema schema = reader.getSchema();

        // Verify that the schema is preserved
        assertEquals(schema, new Schema.Parser().parse(new File("src/test/resources/user.avsc")));

        GenericRecord record = null;
        for (Map<String, Object> expectedRecord : expectedRecords) {
            assertTrue(reader.hasNext());
            record = reader.next(record);
            final String name = record.get("name").toString();
            final Integer favorite_number = (Integer) record.get("favorite_number");
            assertNotNull(name);
            assertNotNull(favorite_number);
            assertNull(record.get("favorite_color"));
            assertNull(record.get("scale"));

            assertEquals(expectedRecord.get("name"), name);
            assertEquals(expectedRecord.get("favorite_number"), favorite_number);
        }
        assertFalse(reader.hasNext());
    }

    @Test
    public void onTriggerWithConnectFailure() throws Exception {
        configure(processor, 1);
        processor.setGenerateConnectFailure(true);
        runner.setProperty(PutHive3Streaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHive3Streaming.DB_NAME, "default");
        runner.setProperty(PutHive3Streaming.TABLE_NAME, "users");
        runner.enqueue(new byte[0]);
        try {
            runner.run();
            fail("ProcessException should be thrown");
        } catch (AssertionError e) {
            assertTrue(e.getCause() instanceof ProcessException);
        }
        runner.assertTransferCount(PutHive3Streaming.REL_RETRY, 0);
        runner.assertTransferCount(PutHive3Streaming.REL_FAILURE, 0);
        runner.assertTransferCount(PutHive3Streaming.REL_SUCCESS, 0);
        // Assert incoming FlowFile stays in input queue.
        assertEquals(1, runner.getQueueSize().getObjectCount());
    }

    @Test
    public void onTriggerWithConnectFailureRollbackOnFailure() throws Exception {
        configure(processor, 1);
        processor.setGenerateConnectFailure(true);
        runner.setProperty(PutHive3Streaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHive3Streaming.DB_NAME, "default");
        runner.setProperty(PutHive3Streaming.TABLE_NAME, "users");
        runner.setProperty(PutHive3Streaming.ROLLBACK_ON_FAILURE, "true");
        runner.enqueue(new byte[0]);
        try {
            runner.run();
            fail("ProcessException should be thrown");
        } catch (AssertionError e) {
            assertTrue(e.getCause() instanceof ProcessException);
        }

        runner.assertTransferCount(PutHive3Streaming.REL_RETRY, 0);
        runner.assertTransferCount(PutHive3Streaming.REL_FAILURE, 0);
        runner.assertTransferCount(PutHive3Streaming.REL_SUCCESS, 0);
        // Assert incoming FlowFile stays in input queue.
        assertEquals(1, runner.getQueueSize().getObjectCount());
    }

    @Test
    public void onTriggerWithWriteFailure() throws Exception {
        configure(processor, 2);
        processor.setGenerateWriteFailure(true);
        runner.setProperty(PutHive3Streaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHive3Streaming.DB_NAME, "default");
        runner.setProperty(PutHive3Streaming.TABLE_NAME, "users");
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutHive3Streaming.REL_FAILURE, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutHive3Streaming.REL_FAILURE).get(0);
        assertEquals("0", flowFile.getAttribute(HIVE_STREAMING_RECORD_COUNT_ATTR));
        assertEquals("default.users", flowFile.getAttribute(ATTR_OUTPUT_TABLES));
    }

    @Test
    public void onTriggerWithWriteFailureRollbackOnFailure() throws Exception {
        configure(processor, 2);
        processor.setGenerateWriteFailure(true);
        runner.setProperty(PutHive3Streaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHive3Streaming.DB_NAME, "default");
        runner.setProperty(PutHive3Streaming.TABLE_NAME, "users");
        runner.setProperty(PutHive3Streaming.ROLLBACK_ON_FAILURE, "true");
        Map<String, Object> user1 = new HashMap<String, Object>() {
            {
                put("name", "Joe");
                put("favorite_number", 146);
            }
        };
        Map<String, Object> user2 = new HashMap<String, Object>() {
            {
                put("name", "Mary");
                put("favorite_number", 42);
            }
        };
        runner.enqueue(createAvroRecord(Arrays.asList(user1, user2)));
        try {
            runner.run();
            fail("ProcessException should be thrown");
        } catch (AssertionError e) {
            assertTrue(e.getCause() instanceof ProcessException);
        }

        runner.assertTransferCount(PutHive3Streaming.REL_FAILURE, 0);
        // Assert incoming FlowFile stays in input queue.
        assertEquals(1, runner.getQueueSize().getObjectCount());
    }

    @Test
    public void onTriggerWithSerializationError() throws Exception {
        configure(processor, 1);
        processor.setGenerateSerializationError(true);
        runner.setProperty(PutHive3Streaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHive3Streaming.DB_NAME, "default");
        runner.setProperty(PutHive3Streaming.TABLE_NAME, "users");
        Map<String, Object> user1 = new HashMap<String, Object>() {
            {
                put("name", "Joe");
                put("favorite_number", 146);
            }
        };
        runner.enqueue(createAvroRecord(Collections.singletonList(user1)));
        runner.run();

        runner.assertTransferCount(PutHive3Streaming.REL_SUCCESS, 0);
        runner.assertTransferCount(PutHive3Streaming.REL_FAILURE, 1);
    }

    @Test
    public void onTriggerWithSerializationErrorRollbackOnFailure() throws Exception {
        configure(processor, 1);
        processor.setGenerateSerializationError(true);
        runner.setProperty(PutHive3Streaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHive3Streaming.DB_NAME, "default");
        runner.setProperty(PutHive3Streaming.TABLE_NAME, "users");
        runner.setProperty(PutHive3Streaming.ROLLBACK_ON_FAILURE, "true");
        Map<String, Object> user1 = new HashMap<String, Object>() {
            {
                put("name", "Joe");
                put("favorite_number", 146);
            }
        };
        runner.enqueue(createAvroRecord(Collections.singletonList(user1)));
        try {
            runner.run();
            fail("ProcessException should be thrown");
        } catch (AssertionError e) {
            assertTrue(e.getCause() instanceof ProcessException);
        }

        runner.assertTransferCount(PutHive3Streaming.REL_SUCCESS, 0);
        runner.assertTransferCount(PutHive3Streaming.REL_FAILURE, 0);
        // Assert incoming FlowFile stays in input queue.
        assertEquals(1, runner.getQueueSize().getObjectCount());
    }

    @Test
    public void onTriggerWithCommitFailure() throws Exception {
        configure(processor, 1);
        processor.setGenerateCommitFailure(true);
        runner.setProperty(PutHive3Streaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHive3Streaming.DB_NAME, "default");
        runner.setProperty(PutHive3Streaming.TABLE_NAME, "users");
        runner.setProperty(PutHive3Streaming.ROLLBACK_ON_FAILURE, "false");
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutHive3Streaming.REL_FAILURE, 0);
        runner.assertTransferCount(PutHive3Streaming.REL_SUCCESS, 0);
        runner.assertTransferCount(PutHive3Streaming.REL_RETRY, 1);
    }

    @Test
    public void onTriggerWithCommitFailureRollbackOnFailure() throws Exception {
        configure(processor, 1);
        processor.setGenerateCommitFailure(true);
        runner.setProperty(PutHive3Streaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHive3Streaming.DB_NAME, "default");
        runner.setProperty(PutHive3Streaming.TABLE_NAME, "users");
        runner.setProperty(PutHive3Streaming.ROLLBACK_ON_FAILURE, "true");
        runner.enqueue(new byte[0]);
        try {
            runner.run();
            fail("ProcessException should be thrown");
        } catch (AssertionError e) {
            assertTrue(e.getCause() instanceof ProcessException);
        }

        runner.assertTransferCount(PutHive3Streaming.REL_FAILURE, 0);
        runner.assertTransferCount(PutHive3Streaming.REL_SUCCESS, 0);
        runner.assertTransferCount(PutHive3Streaming.REL_RETRY, 0);
        // Assert incoming FlowFile stays in input queue.
        assertEquals(1, runner.getQueueSize().getObjectCount());
    }

    @Test
    public void cleanup() {
        processor.cleanup();
    }

    private byte[] createAvroRecord(List<Map<String, Object>> records) throws IOException {
        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/user.avsc"));

        List<GenericRecord> users = new LinkedList<>();
        for (Map<String, Object> record : records) {
            final GenericRecord user = new GenericData.Record(schema);
            user.put("name", record.get("name"));
            user.put("favorite_number", record.get("favorite_number"));
            user.put("favorite_color", record.get("favorite_color"));
            users.add(user);
        }
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, out);
            for (final GenericRecord user : users) {
                dataFileWriter.append(user);
            }
        }
        return out.toByteArray();

    }

    private class MockPutHive3Streaming extends PutHive3Streaming {

        private boolean generateConnectFailure = false;
        private boolean generateWriteFailure = false;
        private boolean generateSerializationError = false;
        private boolean generateCommitFailure = false;
        private List<FieldSchema> schema = Arrays.asList(
                new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""),
                new FieldSchema("favorite_number", serdeConstants.INT_TYPE_NAME, ""),
                new FieldSchema("favorite_color", serdeConstants.STRING_TYPE_NAME, ""),
                new FieldSchema("scale", serdeConstants.DOUBLE_TYPE_NAME, "")
        );

        @Override
        StreamingConnection makeStreamingConnection(HiveOptions options, RecordReader reader) throws StreamingException {

            if (generateConnectFailure) {
                throw new StubConnectionError("Unit Test - Connection Error");
            }

            HiveRecordWriter hiveRecordWriter = new HiveRecordWriter(reader, getLogger());
            MockHiveStreamingConnection hiveConnection = new MockHiveStreamingConnection(options, reader, hiveRecordWriter, schema);
            hiveConnection.setGenerateWriteFailure(generateWriteFailure);
            hiveConnection.setGenerateSerializationError(generateSerializationError);
            hiveConnection.setGenerateCommitFailure(generateCommitFailure);
            return hiveConnection;
        }

        void setGenerateConnectFailure(boolean generateConnectFailure) {
            this.generateConnectFailure = generateConnectFailure;
        }

        void setGenerateWriteFailure(boolean generateWriteFailure) {
            this.generateWriteFailure = generateWriteFailure;
        }

        void setGenerateSerializationError(boolean generateSerializationError) {
            this.generateSerializationError = generateSerializationError;
        }

        void setGenerateCommitFailure(boolean generateCommitFailure) {
            this.generateCommitFailure = generateCommitFailure;
        }

        void setFields(List<FieldSchema> schema) {
            this.schema = schema;
        }
    }

    private class MockHiveStreamingConnection implements StreamingConnection {

        private boolean generateWriteFailure = false;
        private boolean generateSerializationError = false;
        private boolean generateCommitFailure = false;
        private int writeAttemptCount = 0;
        private ConnectionStats connectionStats;
        private HiveOptions options;
        private RecordWriter writer;
        private HiveConf hiveConf;
        private Table table;
        private String metastoreURI;

        MockHiveStreamingConnection(HiveOptions options, RecordReader reader, RecordWriter recordWriter, List<FieldSchema> schema) {
            this.options = options;
            metastoreURI = options.getMetaStoreURI();
            this.writer = recordWriter;
            this.hiveConf = this.options.getHiveConf();
            connectionStats = new ConnectionStats();
            this.table = new Table(Table.getEmptyTable(options.getDatabaseName(), options.getTableName()));
            this.table.setFields(schema);
            StorageDescriptor sd = this.table.getSd();
            sd.setOutputFormat(OrcOutputFormat.class.getName());
            sd.setLocation(TARGET_HIVE);
        }

        @Override
        public HiveConf getHiveConf() {
            return hiveConf;
        }

        @Override
        public void beginTransaction() throws StreamingException {
            writer.init(this, 0, 100);
        }

        @Override
        public synchronized void write(byte[] record) throws StreamingException {
            throw new UnsupportedOperationException(this.getClass().getName() + " does not support writing of records via bytes, only via an InputStream");
        }

        @Override
        public void write(InputStream inputStream) throws StreamingException {
            try {
                if (generateWriteFailure) {
                    throw new StubStreamingIOFailure("Unit Test - Streaming IO Failure");
                }
                if (generateSerializationError) {
                    throw new StubSerializationError("Unit Test - Serialization error", new Exception());
                }
                this.writer.write(writeAttemptCount, inputStream);
            } finally {
                writeAttemptCount++;
            }
        }

        @Override
        public void commitTransaction() throws StreamingException {
            if (generateCommitFailure) {
                throw new StubTransactionError("Unit Test - Commit Failure");
            }
            connectionStats.incrementCommittedTransactions();
        }

        @Override
        public void abortTransaction() throws StreamingException {
            connectionStats.incrementAbortedTransactions();
        }

        @Override
        public void close() {
            // closing the connection shouldn't throw an exception
        }

        @Override
        public ConnectionStats getConnectionStats() {
            return connectionStats;
        }

        public void setGenerateWriteFailure(boolean generateWriteFailure) {
            this.generateWriteFailure = generateWriteFailure;
        }

        public void setGenerateSerializationError(boolean generateSerializationError) {
            this.generateSerializationError = generateSerializationError;
        }

        public void setGenerateCommitFailure(boolean generateCommitFailure) {
            this.generateCommitFailure = generateCommitFailure;
        }

        @Override
        public String getMetastoreUri() {
            return metastoreURI;
        }

        @Override
        public Table getTable() {
            return table;
        }

        @Override
        public List<String> getStaticPartitionValues() {
            return null;
        }

        @Override
        public boolean isPartitionedTable() {
            return false;
        }

        @Override
        public boolean isDynamicPartitioning() {
            return false;
        }

        @Override
        public String getAgentInfo() {
            return null;
        }

        @Override
        public PartitionInfo createPartitionIfNotExists(List<String> list) throws StreamingException {
            return null;
        }
    }

    private static class MockKerberosCredentialsService implements KerberosCredentialsService, ControllerService {

        private String keytab = "src/test/resources/fake.keytab";
        private String principal = "test@REALM.COM";

        public MockKerberosCredentialsService() {
        }

        public MockKerberosCredentialsService(String keytab, String principal) {
            this.keytab = keytab;
            this.principal = principal;
        }

        @Override
        public String getKeytab() {
            return keytab;
        }

        @Override
        public String getPrincipal() {
            return principal;
        }

        @Override
        public void initialize(ControllerServiceInitializationContext context) throws InitializationException {

        }

        @Override
        public Collection<ValidationResult> validate(ValidationContext context) {
            return Collections.EMPTY_LIST;
        }

        @Override
        public PropertyDescriptor getPropertyDescriptor(String name) {
            return null;
        }

        @Override
        public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {

        }

        @Override
        public List<PropertyDescriptor> getPropertyDescriptors() {
            return null;
        }

        @Override
        public String getIdentifier() {
            return "kcs";
        }
    }
}
