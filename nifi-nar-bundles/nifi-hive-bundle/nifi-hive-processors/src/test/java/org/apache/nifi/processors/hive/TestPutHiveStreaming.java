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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.RecordWriter;
import org.apache.hive.hcatalog.streaming.SerializationError;
import org.apache.hive.hcatalog.streaming.StreamingConnection;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.hive.hcatalog.streaming.TransactionBatch;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.hive.AuthenticationFailedException;
import org.apache.nifi.util.hive.HiveConfigurator;
import org.apache.nifi.util.hive.HiveOptions;
import org.apache.nifi.util.hive.HiveWriter;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
import static org.apache.nifi.processors.hive.PutHiveStreaming.REL_SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for PutHiveStreaming processor.
 */
public class TestPutHiveStreaming {

    private TestRunner runner;
    private MockPutHiveStreaming processor;

    private KerberosProperties kerberosPropsWithFile;
    private HiveConfigurator hiveConfigurator;
    private HiveConf hiveConf;
    private UserGroupInformation ugi;

    @Before
    public void setUp() throws Exception {

        // needed for calls to UserGroupInformation.setConfiguration() to work when passing in
        // config with Kerberos authentication enabled
        System.setProperty("java.security.krb5.realm", "nifi.com");
        System.setProperty("java.security.krb5.kdc", "nifi.kdc");

        ugi = null;
        kerberosPropsWithFile = new KerberosProperties(new File("src/test/resources/krb5.conf"));

        processor = new MockPutHiveStreaming();
        hiveConfigurator = mock(HiveConfigurator.class);
        hiveConf = mock(HiveConf.class);
        when(hiveConfigurator.getConfigurationFromFiles(anyString())).thenReturn(hiveConf);
        processor.hiveConfigurator = hiveConfigurator;
        processor.setKerberosProperties(kerberosPropsWithFile);
        runner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testSetup() throws Exception {
        runner.assertNotValid();
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.assertNotValid();
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.assertValid();
        runner.run();
    }

    @Test
    public void testUgiGetsCleared() {
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        processor.ugi = mock(UserGroupInformation.class);
        runner.run();
        assertNull(processor.ugi);
    }

    @Test
    public void testUgiGetsSetIfSecure() throws AuthenticationFailedException, IOException {
        when(hiveConf.get(SecurityUtil.HADOOP_SECURITY_AUTHENTICATION)).thenReturn(SecurityUtil.KERBEROS);
        ugi = mock(UserGroupInformation.class);
        when(hiveConfigurator.authenticate(eq(hiveConf), anyString(), anyString(), anyLong(), any())).thenReturn(ugi);
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
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
    public void testSetupBadPartitionColumns() throws Exception {
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
        runner.enqueue("I am not an Avro record".getBytes());
        runner.run();

        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 1);
    }

    @Test
    public void onTriggerBadInputRollbackOnFailure() throws Exception {
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        runner.setProperty(PutHiveStreaming.ROLLBACK_ON_FAILURE, "true");
        runner.enqueue("I am not an Avro record".getBytes());
        try {
            runner.run();
            fail("ProcessException should be thrown");
        } catch (AssertionError e) {
            assertTrue(e.getCause() instanceof ProcessException);
        }

        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 0);
        // Assert incoming FlowFile stays in input queue.
        assertEquals(1, runner.getQueueSize().getObjectCount());
    }


    @Test
    public void onTriggerMultipleRecordsSingleTransaction() throws Exception {
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.RECORDS_PER_TXN, "100");
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

        runner.assertTransferCount(PutHiveStreaming.REL_SUCCESS, 1);
        MockFlowFile resultFlowFile = runner.getFlowFilesForRelationship(PutHiveStreaming.REL_SUCCESS).get(0);
        assertOutputAvroRecords(users, resultFlowFile);
    }

    @Test
    public void onTriggerMultipleRecordsMultipleTransaction() throws Exception {
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.RECORDS_PER_TXN, "2");
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

        runner.assertTransferCount(PutHiveStreaming.REL_SUCCESS, 1);
        MockFlowFile resultFlowFile = runner.getFlowFilesForRelationship(PutHiveStreaming.REL_SUCCESS).get(0);
        assertOutputAvroRecords(users, resultFlowFile);
    }

    @Test
    public void onTriggerMultipleRecordsFailInMiddle() throws Exception {
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.RECORDS_PER_TXN, "2");
        processor.setGenerateWriteFailure(true, 1);
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
        Map<String, Object> user4 = new HashMap<String, Object>() {
            {
                put("name", "Mike");
                put("favorite_number", 345);
            }
        };
        final List<Map<String, Object>> users = Arrays.asList(user1, user2, user3, user4);
        runner.enqueue(createAvroRecord(users));
        runner.run();

        runner.assertTransferCount(PutHiveStreaming.REL_SUCCESS, 1);
        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 1);
        runner.assertTransferCount(PutHiveStreaming.REL_RETRY, 0);

        MockFlowFile resultFlowFile = runner.getFlowFilesForRelationship(PutHiveStreaming.REL_SUCCESS).get(0);
        assertOutputAvroRecords(Arrays.asList(user1, user3, user4), resultFlowFile);

        final MockFlowFile failedFlowFile = runner.getFlowFilesForRelationship(PutHiveStreaming.REL_FAILURE).get(0);
        assertOutputAvroRecords(Arrays.asList(user2), failedFlowFile);
    }

    @Test
    public void onTriggerMultipleRecordsFailInMiddleRollbackOnFailure() throws Exception {
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.RECORDS_PER_TXN, "2");
        runner.setProperty(PutHiveStreaming.ROLLBACK_ON_FAILURE, "true");
        processor.setGenerateWriteFailure(true, 1);
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
        try {
            runner.run();
            fail("ProcessException should be thrown, because any Hive Transaction is committed yet.");
        } catch (AssertionError e) {
            assertTrue(e.getCause() instanceof ProcessException);
        }

        runner.assertTransferCount(PutHiveStreaming.REL_SUCCESS, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_RETRY, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 0);
        // Assert incoming FlowFile stays in input queue.
        assertEquals(1, runner.getQueueSize().getObjectCount());
    }

    @Test
    public void onTriggerMultipleRecordsFailInMiddleRollbackOnFailureCommitted() throws Exception {
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.RECORDS_PER_TXN, "2");
        runner.setProperty(PutHiveStreaming.ROLLBACK_ON_FAILURE, "true");
        // The first two records are committed, then an issue will happen at the 3rd record.
        processor.setGenerateWriteFailure(true, 2);
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
        Map<String, Object> user4 = new HashMap<String, Object>() {
            {
                put("name", "Mike");
                put("favorite_number", 345);
            }
        };
        runner.enqueue(createAvroRecord(Arrays.asList(user1, user2, user3, user4)));
        // ProcessException should NOT be thrown, because a Hive Transaction is already committed.
        runner.run();

        runner.assertTransferCount(PutHiveStreaming.REL_SUCCESS, 1);
        runner.assertTransferCount(PutHiveStreaming.REL_RETRY, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 0);

        // Assert transferred FlowFile.
        assertOutputAvroRecords(Arrays.asList(user1, user2), runner.getFlowFilesForRelationship(REL_SUCCESS).get(0));

        // Assert incoming FlowFile stays in input queue.
        assertEquals(1, runner.getQueueSize().getObjectCount());

    }

    private void assertOutputAvroRecords(List<Map<String, Object>> expectedRecords, MockFlowFile resultFlowFile) throws IOException {
        assertEquals(String.valueOf(expectedRecords.size()), resultFlowFile.getAttribute(PutHiveStreaming.HIVE_STREAMING_RECORD_COUNT_ATTR));

        final DataFileStream<GenericRecord> reader = new DataFileStream<>(
                new ByteArrayInputStream(resultFlowFile.toByteArray()),
                new GenericDatumReader<GenericRecord>());

        Schema schema = reader.getSchema();

        // Verify that the schema is preserved
        assertTrue(schema.equals(new Schema.Parser().parse(new File("src/test/resources/user.avsc"))));

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
    public void onTriggerWithPartitionColumns() throws Exception {
        runner.setVariable("metastore", "thrift://localhost:9083");
        runner.setVariable("database", "default");
        runner.setVariable("table", "users");
        runner.setVariable("partitions", "favorite_number, favorite_color");

        runner.setProperty(PutHiveStreaming.METASTORE_URI, "${metastore}");
        runner.setProperty(PutHiveStreaming.DB_NAME, "${database}");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "${table}");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        runner.setProperty(PutHiveStreaming.PARTITION_COLUMNS, "${partitions}");
        runner.setProperty(PutHiveStreaming.AUTOCREATE_PARTITIONS, "true");

        runner.assertValid();

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
    public void onTriggerWithPartitionColumnsNotInRecordRollbackOnFailure() throws Exception {
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        runner.setProperty(PutHiveStreaming.PARTITION_COLUMNS, "favorite_food");
        runner.setProperty(PutHiveStreaming.AUTOCREATE_PARTITIONS, "false");
        runner.setProperty(PutHiveStreaming.ROLLBACK_ON_FAILURE, "true");
        Map<String, Object> user1 = new HashMap<String, Object>() {
            {
                put("name", "Joe");
                put("favorite_number", 146);
                put("favorite_color", "blue");
            }
        };

        runner.enqueue(createAvroRecord(Collections.singletonList(user1)));
        try {
            runner.run();
            fail("ProcessException should be thrown");
        } catch (AssertionError e) {
            assertTrue(e.getCause() instanceof ProcessException);
        }

        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_SUCCESS, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_RETRY, 0);
        // Assert incoming FlowFile stays in input queue.
        assertEquals(1, runner.getQueueSize().getObjectCount());
    }

    @Test
    public void onTriggerWithRetireWriters() throws Exception {
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "2");
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
    public void onTriggerWithConnectFailureRollbackOnFailure() throws Exception {
        processor.setGenerateConnectFailure(true);
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        runner.setProperty(PutHiveStreaming.ROLLBACK_ON_FAILURE, "true");
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

        runner.assertTransferCount(PutHiveStreaming.REL_RETRY, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_SUCCESS, 0);
        // Assert incoming FlowFile stays in input queue.
        assertEquals(1, runner.getQueueSize().getObjectCount());
    }

    @Test
    public void onTriggerWithInterruptedException() throws Exception {
        processor.setGenerateInterruptedExceptionOnCreateWriter(true);
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
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
    public void onTriggerWithInterruptedExceptionRollbackOnFailure() throws Exception {
        processor.setGenerateInterruptedExceptionOnCreateWriter(true);
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        runner.setProperty(PutHiveStreaming.ROLLBACK_ON_FAILURE, "true");
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

        runner.assertTransferCount(PutHiveStreaming.REL_RETRY, 0);
    }

    @Test
    public void onTriggerWithWriteFailure() throws Exception {
        processor.setGenerateWriteFailure(true);
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
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
    public void onTriggerWithWriteFailureRollbackOnFailure() throws Exception {
        processor.setGenerateWriteFailure(true);
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        runner.setProperty(PutHiveStreaming.ROLLBACK_ON_FAILURE, "true");
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

        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 0);
        // Assert incoming FlowFile stays in input queue.
        assertEquals(1, runner.getQueueSize().getObjectCount());
    }

    @Test
    public void onTriggerWithSerializationError() throws Exception {
        processor.setGenerateSerializationError(true);
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
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
    public void onTriggerWithSerializationErrorRollbackOnFailure() throws Exception {
        processor.setGenerateSerializationError(true);
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        runner.setProperty(PutHiveStreaming.ROLLBACK_ON_FAILURE, "true");
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

        runner.assertTransferCount(PutHiveStreaming.REL_SUCCESS, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 0);
        // Assert incoming FlowFile stays in input queue.
        assertEquals(1, runner.getQueueSize().getObjectCount());
    }

    @Test
    public void onTriggerWithCommitFailure() throws Exception {
        processor.setGenerateCommitFailure(true);
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        Map<String, Object> user1 = new HashMap<String, Object>() {
            {
                put("name", "Joe");
                put("favorite_number", 146);
            }
        };
        runner.enqueue(createAvroRecord(Collections.singletonList(user1)));
        runner.run();

        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_SUCCESS, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_RETRY, 1);
    }

    @Test
    public void onTriggerWithCommitFailureRollbackOnFailure() throws Exception {
        processor.setGenerateCommitFailure(true);
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        runner.setProperty(PutHiveStreaming.ROLLBACK_ON_FAILURE, "true");
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

        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_SUCCESS, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_RETRY, 0);
        // Assert incoming FlowFile stays in input queue.
        assertEquals(1, runner.getQueueSize().getObjectCount());
    }

    @Test
    public void onTriggerWithTransactionFailure() throws Exception {
        processor.setGenerateTransactionFailure(true);
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        Map<String, Object> user1 = new HashMap<String, Object>() {
            {
                put("name", "Joe");
                put("favorite_number", 146);
            }
        };
        runner.enqueue(createAvroRecord(Collections.singletonList(user1)));
        runner.run();

        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_SUCCESS, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_RETRY, 1);
    }

    @Test
    public void onTriggerWithTransactionFailureRollbackOnFailure() throws Exception {
        processor.setGenerateTransactionFailure(true);
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
        runner.setProperty(PutHiveStreaming.ROLLBACK_ON_FAILURE, "true");
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

        runner.assertTransferCount(PutHiveStreaming.REL_FAILURE, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_SUCCESS, 0);
        runner.assertTransferCount(PutHiveStreaming.REL_RETRY, 0);
        // Assert incoming FlowFile stays in input queue.
        assertEquals(1, runner.getQueueSize().getObjectCount());
    }

    @Test
    public void onTriggerWithExceptionOnFlushAndClose() throws Exception {
        processor.setGenerateExceptionOnFlushAndClose(true);
        runner.setProperty(PutHiveStreaming.METASTORE_URI, "thrift://localhost:9083");
        runner.setProperty(PutHiveStreaming.DB_NAME, "default");
        runner.setProperty(PutHiveStreaming.TABLE_NAME, "users");
        runner.setProperty(PutHiveStreaming.TXNS_PER_BATCH, "100");
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
        processor.cleanup();
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
        private Integer generateWriteFailureRecordIndex;
        private boolean generateSerializationError = false;
        private boolean generateCommitFailure = false;
        private boolean generateTransactionFailure = false;
        private boolean generateExceptionOnFlushAndClose = false;
        private HiveEndPoint hiveEndPoint = mock(HiveEndPoint.class);

        @Override
        public KerberosProperties getKerberosProperties() {
            return this.kerberosProperties;
        }

        public void setKerberosProperties(KerberosProperties kerberosProperties) {
            this.kerberosProperties = kerberosProperties;
        }

        @Override
        public HiveEndPoint makeHiveEndPoint(List<String> partitionValues, HiveOptions hiveOptions) {
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
            MockHiveWriter hiveWriter = new MockHiveWriter(endPoint, options.getTxnsPerBatch(), options.getAutoCreatePartitions(), options.getCallTimeOut(), callTimeoutPool, ugi, hiveConfig);
            hiveWriter.setGenerateWriteFailure(generateWriteFailure, generateWriteFailureRecordIndex);
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

        public void setGenerateWriteFailure(boolean generateWriteFailure, int generateWriteFailureRecordIndex) {
            this.generateWriteFailure = generateWriteFailure;
            this.generateWriteFailureRecordIndex = generateWriteFailureRecordIndex;
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
        private Integer generateWriteFailureRecordIndex;
        private boolean generateSerializationError = false;
        private boolean generateCommitFailure = false;
        private boolean generateTransactionFailure = false;
        private boolean generateExceptionOnFlushAndClose = false;
        private int writeAttemptCount = 0;
        private int totalRecords = 0;

        private HiveEndPoint endPoint;

        public MockHiveWriter(HiveEndPoint endPoint, int txnsPerBatch, boolean autoCreatePartitions,
                long callTimeout, ExecutorService callTimeoutPool, UserGroupInformation ugi, HiveConf hiveConf)
                throws InterruptedException, ConnectFailure {
            super(endPoint, txnsPerBatch, autoCreatePartitions, callTimeout, callTimeoutPool, ugi, hiveConf);
            assertEquals(TestPutHiveStreaming.this.ugi, ugi);
            this.endPoint = endPoint;
        }

        @Override
        public synchronized void write(byte[] record) throws WriteFailure, SerializationError, InterruptedException {
            try {
                if (generateWriteFailure
                        && (generateWriteFailureRecordIndex == null || writeAttemptCount == generateWriteFailureRecordIndex)) {
                    throw new WriteFailure(endPoint, 1L, new Exception());
                }
                if (generateSerializationError) {
                    throw new SerializationError("Test Serialization Error", new Exception());
                }
                totalRecords++;
            } finally {
                writeAttemptCount++;
            }
        }

        public void setGenerateWriteFailure(boolean generateWriteFailure, Integer generateWriteFailureRecordIndex) {
            this.generateWriteFailure = generateWriteFailure;
            this.generateWriteFailureRecordIndex = generateWriteFailureRecordIndex;
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
        protected RecordWriter getRecordWriter(HiveEndPoint endPoint, UserGroupInformation ugi, HiveConf conf) throws StreamingException {
            assertEquals(hiveConf, conf);
            return mock(RecordWriter.class);
        }

        @Override
        protected StreamingConnection newConnection(HiveEndPoint endPoint, boolean autoCreatePartitions, HiveConf conf, UserGroupInformation ugi) throws InterruptedException, ConnectFailure {
            StreamingConnection connection = mock(StreamingConnection.class);
            assertEquals(hiveConf, conf);
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

        @Override
        public int getTotalRecords() {
            return totalRecords;
        }
    }

}
