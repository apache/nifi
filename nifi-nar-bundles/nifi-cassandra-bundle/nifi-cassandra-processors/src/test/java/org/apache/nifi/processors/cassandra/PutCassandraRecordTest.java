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
package org.apache.nifi.processors.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PutCassandraRecordTest {

    private TestRunner testRunner;
    private MockRecordParser recordReader;

    @Before
    public void setUp() throws Exception {
        MockPutCassandraRecord processor = new MockPutCassandraRecord();
        recordReader = new MockRecordParser();
        testRunner = TestRunners.newTestRunner(processor);
        testRunner.setProperty(PutCassandraRecord.RECORD_READER_FACTORY, "reader");
    }

    @Test
    public void testProcessorConfigValidity() throws InitializationException {
        testRunner.setProperty(PutCassandraRecord.CONTACT_POINTS, "localhost:9042");
        testRunner.assertNotValid();

        testRunner.setProperty(PutCassandraRecord.PASSWORD, "password");
        testRunner.assertNotValid();

        testRunner.setProperty(PutCassandraRecord.USERNAME, "username");
        testRunner.assertNotValid();

        testRunner.setProperty(PutCassandraRecord.CONSISTENCY_LEVEL, "SERIAL");
        testRunner.assertNotValid();

        testRunner.setProperty(PutCassandraRecord.BATCH_STATEMENT_TYPE, "LOGGED");
        testRunner.assertNotValid();

        testRunner.setProperty(PutCassandraRecord.KEYSPACE, "sampleks");
        testRunner.assertNotValid();

        testRunner.setProperty(PutCassandraRecord.TABLE, "sampletbl");
        testRunner.assertNotValid();

        testRunner.addControllerService("reader", recordReader);
        testRunner.enableControllerService(recordReader);
        testRunner.assertValid();
    }

    private void setUpStandardTestConfig() throws InitializationException {
        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, "localhost:9042");
        testRunner.setProperty(AbstractCassandraProcessor.PASSWORD, "password");
        testRunner.setProperty(AbstractCassandraProcessor.USERNAME, "username");
        testRunner.setProperty(PutCassandraRecord.CONSISTENCY_LEVEL, "SERIAL");
        testRunner.setProperty(PutCassandraRecord.BATCH_STATEMENT_TYPE, "LOGGED");
        testRunner.setProperty(PutCassandraRecord.TABLE, "sampleks.sampletbl");
        testRunner.addControllerService("reader", recordReader);
        testRunner.enableControllerService(recordReader);
    }

    @Test
    public void testSimplePut() throws InitializationException {
        setUpStandardTestConfig();

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);

        recordReader.addRecord("John Doe", 48, "Soccer");
        recordReader.addRecord("Jane Doe", 47, "Tennis");
        recordReader.addRecord("Sally Doe", 47, "Curling");
        recordReader.addRecord("Jimmy Doe", 14, null);
        recordReader.addRecord("Pizza Doe", 14, null);

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutCassandraRecord.REL_SUCCESS, 1);
    }

    @Test
    public void testStringArrayPut() throws InitializationException {
        setUpStandardTestConfig();

        recordReader.addSchemaField(new RecordField("names", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())));
        recordReader.addSchemaField("age", RecordFieldType.INT);

        recordReader.addRecord(new Object[]{"John", "Doe"}, 1);
        recordReader.addRecord(new Object[]{"John", "Doe"}, 2);
        recordReader.addRecord(new Object[]{"John", "Doe"}, 3);

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutCassandraRecord.REL_SUCCESS, 1);
    }

    @Test
    public void testSimpleUpdate() throws InitializationException {
        setUpStandardTestConfig();
        testRunner.setProperty(PutCassandraRecord.STATEMENT_TYPE, PutCassandraRecord.UPDATE_TYPE);
        testRunner.setProperty(PutCassandraRecord.UPDATE_METHOD, PutCassandraRecord.SET_TYPE);
        testRunner.setProperty(PutCassandraRecord.UPDATE_KEYS, "name,age");
        testRunner.setProperty(PutCassandraRecord.BATCH_STATEMENT_TYPE, PutCassandraRecord.COUNTER_TYPE);

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("goals", RecordFieldType.INT);

        recordReader.addRecord("John Doe", 48, 1L);
        recordReader.addRecord("Jane Doe", 47, 2L);
        recordReader.addRecord("Sally Doe", 47, 0);

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutCassandraRecord.REL_SUCCESS, 1);
    }

    @Test
    public void testUpdateInvalidFieldType() throws InitializationException {
        setUpStandardTestConfig();
        testRunner.setProperty(PutCassandraRecord.STATEMENT_TYPE, PutCassandraRecord.UPDATE_TYPE);
        testRunner.setProperty(PutCassandraRecord.UPDATE_METHOD, PutCassandraRecord.INCR_TYPE);
        testRunner.setProperty(PutCassandraRecord.UPDATE_KEYS, "name,age");
        testRunner.setProperty(PutCassandraRecord.BATCH_STATEMENT_TYPE, PutCassandraRecord.COUNTER_TYPE);

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("goals", RecordFieldType.STRING);

        recordReader.addRecord("John Doe", 48,"1");
        recordReader.addRecord("Jane Doe", 47, "1");
        recordReader.addRecord("Sally Doe", 47, "1");

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertTransferCount(PutCassandraRecord.REL_FAILURE, 1);
        testRunner.assertTransferCount(PutCassandraRecord.REL_SUCCESS, 0);
        testRunner.assertTransferCount(PutCassandraRecord.REL_RETRY, 0);
    }

    @Test
    public void testUpdateEmptyUpdateKeys() throws InitializationException {
        setUpStandardTestConfig();
        testRunner.setProperty(PutCassandraRecord.STATEMENT_TYPE, PutCassandraRecord.UPDATE_TYPE);
        testRunner.setProperty(PutCassandraRecord.UPDATE_METHOD, PutCassandraRecord.INCR_TYPE);
        testRunner.setProperty(PutCassandraRecord.UPDATE_KEYS, "");
        testRunner.setProperty(PutCassandraRecord.BATCH_STATEMENT_TYPE, PutCassandraRecord.COUNTER_TYPE);

        testRunner.assertNotValid();
    }

    @Test
    public void testUpdateNullUpdateKeys() throws InitializationException {
        setUpStandardTestConfig();
        testRunner.setProperty(PutCassandraRecord.STATEMENT_TYPE, PutCassandraRecord.UPDATE_TYPE);
        testRunner.setProperty(PutCassandraRecord.UPDATE_METHOD, PutCassandraRecord.SET_TYPE);
        testRunner.setProperty(PutCassandraRecord.BATCH_STATEMENT_TYPE, PutCassandraRecord.COUNTER_TYPE);

        testRunner.assertNotValid();
    }

    @Test
    public void testUpdateSetLoggedBatch() throws InitializationException {
        setUpStandardTestConfig();
        testRunner.setProperty(PutCassandraRecord.STATEMENT_TYPE, PutCassandraRecord.UPDATE_TYPE);
        testRunner.setProperty(PutCassandraRecord.UPDATE_METHOD, PutCassandraRecord.SET_TYPE);
        testRunner.setProperty(PutCassandraRecord.UPDATE_KEYS, "name,age");
        testRunner.setProperty(PutCassandraRecord.BATCH_STATEMENT_TYPE, PutCassandraRecord.LOGGED_TYPE);

        testRunner.assertValid();
    }

    @Test
    public void testUpdateCounterWrongBatchStatementType() throws InitializationException {
        setUpStandardTestConfig();
        testRunner.setProperty(PutCassandraRecord.STATEMENT_TYPE, PutCassandraRecord.UPDATE_TYPE);
        testRunner.setProperty(PutCassandraRecord.UPDATE_METHOD, PutCassandraRecord.INCR_TYPE);
        testRunner.setProperty(PutCassandraRecord.UPDATE_KEYS, "name,age");
        testRunner.setProperty(PutCassandraRecord.BATCH_STATEMENT_TYPE, PutCassandraRecord.LOGGED_TYPE);

        testRunner.assertNotValid();
    }

    @Test
    public void testUpdateWithUpdateMethodAndKeyAttributes() throws InitializationException {
        setUpStandardTestConfig();
        testRunner.setProperty(PutCassandraRecord.STATEMENT_TYPE, PutCassandraRecord.UPDATE_TYPE);
        testRunner.setProperty(PutCassandraRecord.UPDATE_METHOD, PutCassandraRecord.UPDATE_METHOD_USE_ATTR_TYPE);
        testRunner.setProperty(PutCassandraRecord.UPDATE_KEYS, "${cql.update.keys}");
        testRunner.setProperty(PutCassandraRecord.BATCH_STATEMENT_TYPE, PutCassandraRecord.COUNTER_TYPE);

        testRunner.assertValid();

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("goals", RecordFieldType.LONG);

        recordReader.addRecord("John Doe", 48, 1L);
        recordReader.addRecord("Jane Doe", 47, 1L);
        recordReader.addRecord("Sally Doe", 47, 1L);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("cql.update.method", "Increment");
        attributes.put("cql.update.keys", "name,age");
        testRunner.enqueue("", attributes);
        testRunner.run();

        testRunner.assertTransferCount(PutCassandraRecord.REL_FAILURE, 0);
        testRunner.assertTransferCount(PutCassandraRecord.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutCassandraRecord.REL_RETRY, 0);
    }

    @Test
    public void testInsertWithStatementAttribute() throws InitializationException {
        setUpStandardTestConfig();
        testRunner.setProperty(PutCassandraRecord.STATEMENT_TYPE, PutCassandraRecord.STATEMENT_TYPE_USE_ATTR_TYPE);

        testRunner.assertValid();

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("goals", RecordFieldType.LONG);

        recordReader.addRecord("John Doe", 48, 1L);
        recordReader.addRecord("Jane Doe", 47, 1L);
        recordReader.addRecord("Sally Doe", 47, 1L);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("cql.statement.type", "Insert");
        testRunner.enqueue("", attributes);
        testRunner.run();

        testRunner.assertTransferCount(PutCassandraRecord.REL_FAILURE, 0);
        testRunner.assertTransferCount(PutCassandraRecord.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutCassandraRecord.REL_RETRY, 0);
    }

    @Test
    public void testInsertWithStatementAttributeInvalid() throws InitializationException {
        setUpStandardTestConfig();
        testRunner.setProperty(PutCassandraRecord.STATEMENT_TYPE, PutCassandraRecord.STATEMENT_TYPE_USE_ATTR_TYPE);

        testRunner.assertValid();

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("goals", RecordFieldType.LONG);

        recordReader.addRecord("John Doe", 48, 1L);
        recordReader.addRecord("Jane Doe", 47, 1L);
        recordReader.addRecord("Sally Doe", 47, 1L);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("cql.statement.type", "invalid-type");
        testRunner.enqueue("", attributes);
        testRunner.run();

        testRunner.assertTransferCount(PutCassandraRecord.REL_FAILURE, 1);
        testRunner.assertTransferCount(PutCassandraRecord.REL_SUCCESS, 0);
        testRunner.assertTransferCount(PutCassandraRecord.REL_RETRY, 0);
    }

    @Test
    public void testInsertWithBatchStatementAttribute() throws InitializationException {
        setUpStandardTestConfig();
        testRunner.setProperty(PutCassandraRecord.STATEMENT_TYPE, PutCassandraRecord.INSERT_TYPE);
        testRunner.setProperty(PutCassandraRecord.BATCH_STATEMENT_TYPE, PutCassandraRecord.BATCH_STATEMENT_TYPE_USE_ATTR_TYPE);

        testRunner.assertValid();

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("goals", RecordFieldType.LONG);

        recordReader.addRecord("John Doe", 48, 1L);
        recordReader.addRecord("Jane Doe", 47, 1L);
        recordReader.addRecord("Sally Doe", 47, 1L);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("cql.batch.statement.type", "counter");
        testRunner.enqueue("", attributes);
        testRunner.run();

        testRunner.assertTransferCount(PutCassandraRecord.REL_FAILURE, 0);
        testRunner.assertTransferCount(PutCassandraRecord.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutCassandraRecord.REL_RETRY, 0);
    }

    @Test
    public void testInsertWithBatchStatementAttributeInvalid() throws InitializationException {
        setUpStandardTestConfig();
        testRunner.setProperty(PutCassandraRecord.STATEMENT_TYPE, PutCassandraRecord.INSERT_TYPE);
        testRunner.setProperty(PutCassandraRecord.BATCH_STATEMENT_TYPE, PutCassandraRecord.BATCH_STATEMENT_TYPE_USE_ATTR_TYPE);

        testRunner.assertValid();

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("goals", RecordFieldType.LONG);

        recordReader.addRecord("John Doe", 48, 1L);
        recordReader.addRecord("Jane Doe", 47, 1L);
        recordReader.addRecord("Sally Doe", 47, 1L);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("cql.batch.statement.type", "invalid-type");
        testRunner.enqueue("", attributes);
        testRunner.run();

        testRunner.assertTransferCount(PutCassandraRecord.REL_FAILURE, 1);
        testRunner.assertTransferCount(PutCassandraRecord.REL_SUCCESS, 0);
        testRunner.assertTransferCount(PutCassandraRecord.REL_RETRY, 0);
    }

    @Test
    public void testUpdateWithAttributesInvalidUpdateMethod() throws InitializationException {
        setUpStandardTestConfig();
        testRunner.setProperty(PutCassandraRecord.STATEMENT_TYPE, PutCassandraRecord.UPDATE_TYPE);
        testRunner.setProperty(PutCassandraRecord.UPDATE_METHOD, PutCassandraRecord.UPDATE_METHOD_USE_ATTR_TYPE);
        testRunner.setProperty(PutCassandraRecord.UPDATE_KEYS, "${cql.update.keys}");
        testRunner.setProperty(PutCassandraRecord.BATCH_STATEMENT_TYPE, PutCassandraRecord.COUNTER_TYPE);

        testRunner.assertValid();

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("goals", RecordFieldType.INT);

        recordReader.addRecord("John Doe", 48, 1L);
        recordReader.addRecord("Jane Doe", 47, 1L);
        recordReader.addRecord("Sally Doe", 47, 1L);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("cql.update.method", "invalid-method");
        attributes.put("cql.update.keys", "name,age");
        testRunner.enqueue("", attributes);
        testRunner.run();

        testRunner.assertTransferCount(PutCassandraRecord.REL_FAILURE, 1);
        testRunner.assertTransferCount(PutCassandraRecord.REL_SUCCESS, 0);
        testRunner.assertTransferCount(PutCassandraRecord.REL_RETRY, 0);
    }

    @Test
    public void testUpdateWithAttributesIncompatibleBatchStatementType() throws InitializationException {
        setUpStandardTestConfig();
        testRunner.setProperty(PutCassandraRecord.STATEMENT_TYPE, PutCassandraRecord.UPDATE_TYPE);
        testRunner.setProperty(PutCassandraRecord.UPDATE_METHOD, PutCassandraRecord.INCR_TYPE);
        testRunner.setProperty(PutCassandraRecord.UPDATE_KEYS, "name,age");
        testRunner.setProperty(PutCassandraRecord.BATCH_STATEMENT_TYPE, PutCassandraRecord.BATCH_STATEMENT_TYPE_USE_ATTR_TYPE);

        testRunner.assertValid();

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("goals", RecordFieldType.INT);

        recordReader.addRecord("John Doe", 48, 1L);
        recordReader.addRecord("Jane Doe", 47, 1L);
        recordReader.addRecord("Sally Doe", 47, 1L);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("cql.batch.statement.type", "LOGGED");
        testRunner.enqueue("", attributes);
        testRunner.run();

        testRunner.assertTransferCount(PutCassandraRecord.REL_FAILURE, 1);
        testRunner.assertTransferCount(PutCassandraRecord.REL_SUCCESS, 0);
        testRunner.assertTransferCount(PutCassandraRecord.REL_RETRY, 0);
    }

    @Test
    public void testUpdateWithAttributesEmptyUpdateKeysAttribute() throws InitializationException {
        setUpStandardTestConfig();
        testRunner.setProperty(PutCassandraRecord.STATEMENT_TYPE, PutCassandraRecord.UPDATE_TYPE);
        testRunner.setProperty(PutCassandraRecord.UPDATE_METHOD, PutCassandraRecord.UPDATE_METHOD_USE_ATTR_TYPE);
        testRunner.setProperty(PutCassandraRecord.UPDATE_KEYS, "${cql.update.keys}");
        testRunner.setProperty(PutCassandraRecord.BATCH_STATEMENT_TYPE, PutCassandraRecord.COUNTER_TYPE);

        testRunner.assertValid();

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("goals", RecordFieldType.LONG);

        recordReader.addRecord("John Doe", 48, 1L);
        recordReader.addRecord("Jane Doe", 47, 1L);
        recordReader.addRecord("Sally Doe", 47, 1L);

        HashMap<String, String> attributes = new HashMap<>();
        attributes.put("cql.update.method", "Increment");
        attributes.put("cql.update.keys", "");
        testRunner.enqueue("", attributes);
        testRunner.run();

        testRunner.assertTransferCount(PutCassandraRecord.REL_FAILURE, 1);
        testRunner.assertTransferCount(PutCassandraRecord.REL_SUCCESS, 0);
        testRunner.assertTransferCount(PutCassandraRecord.REL_RETRY, 0);
    }

    @Test
    public void testUpdateWithAttributesEmptyUpdateMethodAttribute() throws InitializationException {
        setUpStandardTestConfig();
        testRunner.setProperty(PutCassandraRecord.STATEMENT_TYPE, PutCassandraRecord.UPDATE_TYPE);
        testRunner.setProperty(PutCassandraRecord.UPDATE_METHOD, PutCassandraRecord.UPDATE_METHOD_USE_ATTR_TYPE);
        testRunner.setProperty(PutCassandraRecord.UPDATE_KEYS, "name,age");
        testRunner.setProperty(PutCassandraRecord.BATCH_STATEMENT_TYPE, PutCassandraRecord.COUNTER_TYPE);

        testRunner.assertValid();

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("goals", RecordFieldType.LONG);

        recordReader.addRecord("John Doe", 48, 1L);
        recordReader.addRecord("Jane Doe", 47, 1L);
        recordReader.addRecord("Sally Doe", 47, 1L);

        HashMap<String, String> attributes = new HashMap<>();
        attributes.put("cql.update.method", "");
        testRunner.enqueue("", attributes);
        testRunner.run();

        testRunner.assertTransferCount(PutCassandraRecord.REL_FAILURE, 1);
        testRunner.assertTransferCount(PutCassandraRecord.REL_SUCCESS, 0);
        testRunner.assertTransferCount(PutCassandraRecord.REL_RETRY, 0);
    }

    @Test
    public void testEL() throws InitializationException {
        testRunner.setProperty(PutCassandraRecord.CONTACT_POINTS, "${contact.points}");
        testRunner.setProperty(PutCassandraRecord.PASSWORD, "${pass}");
        testRunner.setProperty(PutCassandraRecord.USERNAME, "${user}");
        testRunner.setProperty(PutCassandraRecord.CONSISTENCY_LEVEL, "SERIAL");
        testRunner.setProperty(PutCassandraRecord.BATCH_STATEMENT_TYPE, "LOGGED");
        testRunner.setProperty(PutCassandraRecord.TABLE, "sampleks.sampletbl");
        testRunner.addControllerService("reader", recordReader);
        testRunner.enableControllerService(recordReader);

        testRunner.assertValid();

        testRunner.setVariable("contact.points", "localhost:9042");
        testRunner.setVariable("user", "username");
        testRunner.setVariable("pass", "password");

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);

        recordReader.addRecord("John Doe", 48, "Soccer");
        recordReader.addRecord("Jane Doe", 47, "Tennis");
        recordReader.addRecord("Sally Doe", 47, "Curling");
        recordReader.addRecord("Jimmy Doe", 14, null);
        recordReader.addRecord("Pizza Doe", 14, null);

        testRunner.enqueue("");
        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(PutCassandraRecord.REL_SUCCESS, 1);
    }

    private static class MockPutCassandraRecord extends PutCassandraRecord {
        private Exception exceptionToThrow = null;
        private Session mockSession = mock(Session.class);

        @Override
        protected Cluster createCluster(List<InetSocketAddress> contactPoints, SSLContext sslContext,
                                        String username, String password, String compressionType) {
            Cluster mockCluster = mock(Cluster.class);
            try {
                Metadata mockMetadata = mock(Metadata.class);
                when(mockMetadata.getClusterName()).thenReturn("cluster1");
                when(mockCluster.getMetadata()).thenReturn(mockMetadata);
                when(mockCluster.connect()).thenReturn(mockSession);
                when(mockCluster.connect(anyString())).thenReturn(mockSession);
                Configuration config = Configuration.builder().build();
                when(mockCluster.getConfiguration()).thenReturn(config);
                ResultSetFuture future = mock(ResultSetFuture.class);
                ResultSet rs = CassandraQueryTestUtil.createMockResultSet();
                PreparedStatement ps = mock(PreparedStatement.class);
                when(mockSession.prepare(anyString())).thenReturn(ps);
                BoundStatement bs = mock(BoundStatement.class);
                when(ps.bind()).thenReturn(bs);
                when(future.getUninterruptibly()).thenReturn(rs);
                try {
                    doReturn(rs).when(future).getUninterruptibly(anyLong(), any(TimeUnit.class));
                } catch (TimeoutException te) {
                    throw new IllegalArgumentException("Mocked cluster doesn't time out");
                }
                if (exceptionToThrow != null) {
                    doThrow(exceptionToThrow).when(mockSession).executeAsync(anyString());
                    doThrow(exceptionToThrow).when(mockSession).executeAsync(any(Statement.class));

                } else {
                    when(mockSession.executeAsync(anyString())).thenReturn(future);
                    when(mockSession.executeAsync(any(Statement.class))).thenReturn(future);
                }
                when(mockSession.getCluster()).thenReturn(mockCluster);
            } catch (Exception e) {
                fail(e.getMessage());
            }
            return mockCluster;
        }
    }
}
