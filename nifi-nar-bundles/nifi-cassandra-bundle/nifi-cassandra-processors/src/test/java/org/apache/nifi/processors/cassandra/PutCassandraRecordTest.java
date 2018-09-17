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
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
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
                                        String username, String password) {
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
