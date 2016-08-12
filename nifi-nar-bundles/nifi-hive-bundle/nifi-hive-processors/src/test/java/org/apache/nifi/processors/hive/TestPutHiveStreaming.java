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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.RecordWriter;
import org.apache.hive.hcatalog.streaming.SerializationError;
import org.apache.hive.hcatalog.streaming.StreamingConnection;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.hive.hcatalog.streaming.TransactionBatch;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.hive.HiveOptions;
import org.apache.nifi.util.hive.HiveWriter;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.apache.nifi.processors.hive.PutHiveStreaming.HIVE_STREAMING_RECORD_COUNT_ATTR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for PutHiveStreaming processor.
 */
public class TestPutHiveStreaming {

    private TestRunner runner;
    private MockPutHiveStreaming processor;

    private KerberosProperties kerberosPropsWithFile;
    private KerberosProperties kerberosPropsWithoutFile;

    @Before
    public void setUp() throws Exception {

        // needed for calls to UserGroupInformation.setConfiguration() to work when passing in
        // config with Kerberos authentication enabled
        System.setProperty("java.security.krb5.realm", "nifi.com");
        System.setProperty("java.security.krb5.kdc", "nifi.kdc");

        NiFiProperties niFiPropertiesWithKerberos = mock(NiFiProperties.class);
        when(niFiPropertiesWithKerberos.getKerberosConfigurationFile()).thenReturn(new File("src/test/resources/krb5.conf"));
        kerberosPropsWithFile = KerberosProperties.create(niFiPropertiesWithKerberos);

        NiFiProperties niFiPropertiesWithoutKerberos = mock(NiFiProperties.class);
        when(niFiPropertiesWithKerberos.getKerberosConfigurationFile()).thenReturn(null);
        kerberosPropsWithoutFile = KerberosProperties.create(niFiPropertiesWithoutKerberos);

        processor = new MockPutHiveStreaming();
        processor.setKerberosProperties(kerberosPropsWithFile);
        runner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testSetup() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.assertNotValid();
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.assertNotValid();
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.assertValid();
        runner.run();
    }

    @Test
    public void testSetupBadPartitionColumns() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.assertValid();
        runner.setProperty(PutHiveStreaming.PARTITION_COLUMNS, "favorite_number,,");
        runner.setProperty(PutHiveStreaming.AUTOCREATE_PARTITIONS, "true");
        runner.assertNotValid();
    }

    @Test(expected = AssertionError.class)
    public void testSetupWithKerberosAuthFailed() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.HIVE_CONFIGURATION_RESOURCES, "src/test/resources/core-site-security.xml, src/test/resources/hive-site-security.xml");
        runner.setProperty(kerberosPropsWithFile.getKerberosPrincipal(), "test@REALM");
        runner.setProperty(kerberosPropsWithFile.getKerberosKeytab(), "src/test/resources/fake.keytab");
        runner.run();
    }

    @Test
    public void testSingleBatchInvalid() throws Exception {
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "2");
        runner.assertValid();
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "1");
        runner.assertNotValid();
    }

    @Test
    public void onTrigger() throws Exception {
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        runner.setValidateExpressionUsage(false);
        Map<String, Object> user1 = new HashMap<String, Object>() {
            {
                put("name", "Joe");
                put("favorite_number", 146);
            }
        };
        runner.enqueue(createAvroRecord(Collections.singletonList(user1)));
        runner.run();

        runner.assertTransferCount(PutHiveStreaming.REL_SUCCESS, 1);
        assertEquals("1", runner.getFlowFilesForRelationship(PutHiveStreaming.REL_SUCCESS).get(0).getAttribute(HIVE_STREAMING_RECORD_COUNT_ATTR));
    }

    @Test
    public void onTriggerBadInput() throws Exception {
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        runner.setValidateExpressionUsage(false);
        runner.enqueue("I am not an Avro record".getBytes());
        runner.run();

        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 1);
    }

    @Test
    public void onTriggerMultipleRecords() throws Exception {
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "2");
        runner.setValidateExpressionUsage(false);
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
        runner.enqueue(createAvroRecord(Arrays.asList(user1, user2, user3)));
        runner.run();

        runner.assertTransferCount(PutHiveStreaming.REL_SUCCESS, 1);
        MockFlowFile resultFlowFile = runner.getFlowFilesForRelationship(PutHiveStreaming.REL_SUCCESS).get(0);
        assertNotNull(resultFlowFile);
        assertEquals("3", resultFlowFile.getAttribute(PutHiveStreaming.HIVE_STREAMING_RECORD_COUNT_ATTR));
        final DataFileStream<GenericRecord> reader = new DataFileStream<>(
                new ByteArrayInputStream(resultFlowFile.toByteArray()),
                new GenericDatumReader<GenericRecord>());

        Schema schema = reader.getSchema();

        // Verify that the schema is preserved
        assertTrue(schema.equals(new Schema.Parser().parse(new File("src/test/resources/user.avsc"))));

        // Verify the records are intact. We can't guarantee order so check the total number and non-null fields
        assertTrue(reader.hasNext());
        GenericRecord record = reader.next(null);
        assertNotNull(record.get("name"));
        assertNotNull(record.get("favorite_number"));
        assertNull(record.get("favorite_color"));
        assertNull(record.get("scale"));
        assertTrue(reader.hasNext());
        record = reader.next(record);
        assertTrue(reader.hasNext());
        reader.next(record);
        assertFalse(reader.hasNext());
    }

    @Test
    public void onTriggerWithPartitionColumns() throws Exception {
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        runner.setProperty(PutHiveStreaming.PARTITION_COLUMNS, "favorite_number, favorite_color");
        runner.setProperty(PutHiveStreaming.AUTOCREATE_PARTITIONS, "true");
        runner.setValidateExpressionUsage(false);
        Map<String, Object> user1 = new HashMap<String, Object>() {
            {
                put("name", "Joe");
                put("favorite_number", 146);
                put("favorite_color", "blue");
            }
        };

        runner.enqueue(createAvroRecord(Collections.singletonList(user1)));
        runner.run();

        runner.assertTransferCount(PutHiveStreaming.REL_SUCCESS, 1);
        assertEquals("1", runner.getFlowFilesForRelationship(PutHiveStreaming.REL_SUCCESS).get(0).getAttribute(HIVE_STREAMING_RECORD_COUNT_ATTR));
        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_RETRY, 0);
    }

    @Test
    public void onTriggerWithPartitionColumnsNotInRecord() throws Exception {
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        runner.setProperty(PutHiveStreaming.PARTITION_COLUMNS, "favorite_food");
        runner.setProperty(PutHiveStreaming.AUTOCREATE_PARTITIONS, "false");
        runner.setValidateExpressionUsage(false);
        Map<String, Object> user1 = new HashMap<String, Object>() {
            {
                put("name", "Joe");
                put("favorite_number", 146);
                put("favorite_color", "blue");
            }
        };

        runner.enqueue(createAvroRecord(Collections.singletonList(user1)));
        runner.run();

        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 1);
        runner.assertTransferCount(PutHiveStreaming.REL_SUCCESS, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_RETRY, 0);
    }

    @Test
    public void onTriggerWithRetireWriters() throws Exception {
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "2");
        runner.setValidateExpressionUsage(false);
        Map<String, Object> user1 = new HashMap<String, Object>() {
            {
                put("name", "Joe");
                put("favorite_number", 146);
            }
        };
        for (int i = 0; i < 10; i++) {
            runner.enqueue(createAvroRecord(Collections.singletonList(user1)));
        }
        runner.run(10);

        runner.assertTransferCount(PutHiveStreaming.REL_SUCCESS, 10);
        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_RETRY, 0);
    }

    @Test
    public void onTriggerWithHeartbeat() throws Exception {
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        runner.setProperty(PutHiveStreaming.HEARTBEAT_INTERVAL, "1");
        runner.setValidateExpressionUsage(false);
        Map<String, Object> user1 = new HashMap<String, Object>() {
            {
                put("name", "Joe");
                put("favorite_number", 146);
            }
        };
        runner.enqueue(createAvroRecord(Collections.singletonList(user1)));
        runner.run(1, false);
        // Wait for a heartbeat
        Thread.sleep(1000);

        runner.enqueue(createAvroRecord(Collections.singletonList(user1)));
        runner.run(1, true);
        runner.assertTransferCount(PutHiveStreaming.REL_SUCCESS, 2);
        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_RETRY, 0);
    }

    @Test
    public void onTriggerWithConnectFailure() throws Exception {
        processor.setGenerateConnectFailure(true);
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        runner.setValidateExpressionUsage(false);
        Map<String, Object> user1 = new HashMap<String, Object>() {
            {
                put("name", "Joe");
                put("favorite_number", 146);
            }
        };
        runner.enqueue(createAvroRecord(Collections.singletonList(user1)));
        runner.run();

        runner.assertTransferCount(PutHiveStreaming.REL_RETRY, 1);
        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_SUCCESS, 0);
    }

    @Test
    public void onTriggerWithInterruptedException() throws Exception {
        processor.setGenerateInterruptedExceptionOnCreateWriter(true);
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        runner.setValidateExpressionUsage(false);
        Map<String, Object> user1 = new HashMap<String, Object>() {
            {
                put("name", "Joe");
                put("favorite_number", 146);
            }
        };
        runner.enqueue(createAvroRecord(Collections.singletonList(user1)));
        runner.run();

        runner.assertTransferCount(PutHiveStreaming.REL_RETRY, 1);
    }

    @Test
    public void onTriggerWithWriteFailure() throws Exception {
        processor.setGenerateWriteFailure(true);
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        runner.setValidateExpressionUsage(false);
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
        runner.run();

        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 1);
        assertEquals("2", runner.getFlowFilesForRelationship(PutHiveStreaming.REL_FAILURE).get(0).getAttribute(HIVE_STREAMING_RECORD_COUNT_ATTR));
    }

    @Test
    public void onTriggerWithSerializationError() throws Exception {
        processor.setGenerateSerializationError(true);
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        runner.setValidateExpressionUsage(false);
        Map<String, Object> user1 = new HashMap<String, Object>() {
            {
                put("name", "Joe");
                put("favorite_number", 146);
            }
        };
        runner.enqueue(createAvroRecord(Collections.singletonList(user1)));
        runner.run();

        runner.assertTransferCount(PutHiveStreaming.REL_SUCCESS, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 1);
    }

    @Test
    public void onTriggerWithCommitFailure() throws Exception {
        processor.setGenerateCommitFailure(true);
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        runner.setValidateExpressionUsage(false);
        Map<String, Object> user1 = new HashMap<String, Object>() {
            {
                put("name", "Joe");
                put("favorite_number", 146);
            }
        };
        runner.enqueue(createAvroRecord(Collections.singletonList(user1)));
        runner.run();

        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 1);
        runner.assertTransferCount(PutHiveStreaming.REL_SUCCESS, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_RETRY, 0);
    }

    @Test
    public void onTriggerWithTransactionFailure() throws Exception {
        processor.setGenerateTransactionFailure(true);
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        runner.setValidateExpressionUsage(false);
        Map<String, Object> user1 = new HashMap<String, Object>() {
            {
                put("name", "Joe");
                put("favorite_number", 146);
            }
        };
        runner.enqueue(createAvroRecord(Collections.singletonList(user1)));
        runner.run();

        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 1);
        runner.assertTransferCount(PutHiveStreaming.REL_SUCCESS, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_RETRY, 0);
    }

    @Test
    public void onTriggerWithExceptionOnFlushAndClose() throws Exception {
        processor.setGenerateExceptionOnFlushAndClose(true);
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        runner.setValidateExpressionUsage(false);
        Map<String, Object> user1 = new HashMap<String, Object>() {
            {
                put("name", "Joe");
                put("favorite_number", 146);
            }
        };
        runner.enqueue(createAvroRecord(Collections.singletonList(user1)));
        runner.run();
    }

    @Test
    public void cleanup() throws Exception {

    }

    @Test
    public void flushAllWriters() throws Exception {

    }

    @Test
    public void abortAndCloseWriters() throws Exception {

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

    private class MockPutHiveStreaming extends PutHiveStreaming {
        private KerberosProperties kerberosProperties;
        private boolean generateConnectFailure = false;
        private boolean generateInterruptedExceptionOnCreateWriter = false;
        private boolean generateWriteFailure = false;
        private boolean generateSerializationError = false;
        private boolean generateCommitFailure = false;
        private boolean generateTransactionFailure = false;
        private boolean generateExceptionOnFlushAndClose = false;

        @Override
        public KerberosProperties getKerberosProperties() {
            return this.kerberosProperties;
        }

        public void setKerberosProperties(KerberosProperties kerberosProperties) {
            this.kerberosProperties = kerberosProperties;
        }

        @Override
        public HiveEndPoint makeHiveEndPoint(List<String> partitionValues, HiveOptions hiveOptions) {
            HiveEndPoint hiveEndPoint = mock(HiveEndPoint.class);
            return hiveEndPoint;
        }

        @Override
        protected HiveWriter makeHiveWriter(HiveEndPoint endPoint, ExecutorService callTimeoutPool, UserGroupInformation ugi, HiveOptions options)
                throws HiveWriter.ConnectFailure, InterruptedException {
            if (generateConnectFailure) {
                throw new HiveWriter.ConnectFailure(endPoint, new Exception());
            }
            if (generateInterruptedExceptionOnCreateWriter) {
                throw new InterruptedException();
            }
            MockHiveWriter hiveWriter = new MockHiveWriter(endPoint, options.getTxnsPerBatch(), options.getAutoCreatePartitions(), options.getCallTimeOut(), callTimeoutPool, ugi);
            hiveWriter.setGenerateWriteFailure(generateWriteFailure);
            hiveWriter.setGenerateSerializationError(generateSerializationError);
            hiveWriter.setGenerateCommitFailure(generateCommitFailure);
            hiveWriter.setGenerateTransactionFailure(generateTransactionFailure);
            hiveWriter.setGenerateExceptionOnFlushAndClose(generateExceptionOnFlushAndClose);
            return hiveWriter;
        }

        public void setGenerateConnectFailure(boolean generateConnectFailure) {
            this.generateConnectFailure = generateConnectFailure;
        }

        public void setGenerateInterruptedExceptionOnCreateWriter(boolean generateInterruptedExceptionOnCreateWriter) {
            this.generateInterruptedExceptionOnCreateWriter = generateInterruptedExceptionOnCreateWriter;
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

        public void setGenerateTransactionFailure(boolean generateTransactionFailure) {
            this.generateTransactionFailure = generateTransactionFailure;
        }

        public void setGenerateExceptionOnFlushAndClose(boolean generateExceptionOnFlushAndClose) {
            this.generateExceptionOnFlushAndClose = generateExceptionOnFlushAndClose;
        }

    }

    private class MockHiveWriter extends HiveWriter {

        private boolean generateWriteFailure = false;
        private boolean generateSerializationError = false;
        private boolean generateCommitFailure = false;
        private boolean generateTransactionFailure = false;
        private boolean generateExceptionOnFlushAndClose = false;

        private HiveEndPoint endPoint;

        public MockHiveWriter(HiveEndPoint endPoint, int txnsPerBatch, boolean autoCreatePartitions,
                              long callTimeout, ExecutorService callTimeoutPool, UserGroupInformation ugi)
                throws InterruptedException, ConnectFailure {
            super(endPoint, txnsPerBatch, autoCreatePartitions, callTimeout, callTimeoutPool, ugi);
            this.endPoint = endPoint;
        }

        @Override
        public synchronized void write(byte[] record) throws WriteFailure, SerializationError, InterruptedException {
            if (generateWriteFailure) {
                throw new HiveWriter.WriteFailure(endPoint, 1L, new Exception());
            }
            if (generateSerializationError) {
                throw new SerializationError("Test Serialization Error", new Exception());
            }
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

        public void setGenerateTransactionFailure(boolean generateTransactionFailure) {
            this.generateTransactionFailure = generateTransactionFailure;
        }

        public void setGenerateExceptionOnFlushAndClose(boolean generateExceptionOnFlushAndClose) {
            this.generateExceptionOnFlushAndClose = generateExceptionOnFlushAndClose;
        }

        @Override
        protected RecordWriter getRecordWriter(HiveEndPoint endPoint) throws StreamingException {
            return mock(RecordWriter.class);
        }

        @Override
        protected StreamingConnection newConnection(UserGroupInformation ugi) throws InterruptedException, ConnectFailure {
            StreamingConnection connection = mock(StreamingConnection.class);
            return connection;
        }

        @Override
        public void flush(boolean rollToNext) throws CommitFailure, TxnBatchFailure, TxnFailure, InterruptedException {
            if (generateCommitFailure) {
                throw new HiveWriter.CommitFailure(endPoint, 1L, new Exception());
            }
            if (generateTransactionFailure) {
                throw new HiveWriter.TxnFailure(mock(TransactionBatch.class), new Exception());
            }
        }

        @Override
        public void heartBeat() throws InterruptedException {

        }

        @Override
        public void flushAndClose() throws TxnBatchFailure, TxnFailure, CommitFailure, IOException, InterruptedException {
            if (generateExceptionOnFlushAndClose) {
                throw new IOException();
            }
        }

        @Override
        public void close() throws IOException, InterruptedException {

        }

        @Override
        public void abort() throws StreamingException, TxnBatchFailure, InterruptedException {

        }

        @Override
        protected void closeConnection() throws InterruptedException {
            // Empty
        }

        @Override
        protected void commitTxn() throws CommitFailure, InterruptedException {
            // Empty
        }

        @Override
        protected TransactionBatch nextTxnBatch(RecordWriter recordWriter) throws InterruptedException, TxnBatchFailure {
            TransactionBatch txnBatch = mock(TransactionBatch.class);
            return txnBatch;
        }

        @Override
        protected void closeTxnBatch() throws InterruptedException {
            // Empty
        }

        @Override
        protected void abortTxn() throws InterruptedException {
            // Empty
        }

        @Override
        protected void nextTxn(boolean rollToNext) throws StreamingException, InterruptedException, TxnBatchFailure {
            // Empty
        }
    }


}