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
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.UnavailableException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.util.HashMap;
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

/**
 * Unit tests for the PutCassandraQL processor
 */
public class PutCassandraQLTest {

    private TestRunner testRunner;
    private MockPutCassandraQL processor;

    @Before
    public void setUp() {
        processor = new MockPutCassandraQL();
        testRunner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testProcessorConfigValidity() {
        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, "localhost:9042");
        testRunner.assertValid();
        testRunner.setProperty(AbstractCassandraProcessor.PASSWORD, "password");
        testRunner.assertNotValid();
        testRunner.setProperty(AbstractCassandraProcessor.USERNAME, "username");
        testRunner.setProperty(AbstractCassandraProcessor.CONSISTENCY_LEVEL, "ONE");
        testRunner.assertValid();
    }

    @Test
    public void testProcessorELConfigValidity() {
        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, "${hosts}");
        testRunner.setProperty(AbstractCassandraProcessor.PASSWORD, "${pass}");
        testRunner.setProperty(AbstractCassandraProcessor.USERNAME, "${user}");
        testRunner.setProperty(AbstractCassandraProcessor.CHARSET, "${charset}");
        testRunner.setProperty(PutCassandraQL.STATEMENT_TIMEOUT, "${timeout}");

        testRunner.assertValid();
    }

    @Test
    public void testProcessorHappyPath() {
        setUpStandardTestConfig();

        testRunner.enqueue("INSERT INTO users (user_id, first_name, last_name, properties, bits, scaleset, largenum, scale, byteobject, ts) VALUES ?, ?, ?, ?, ?, ?, ?, ?, ?, ?",
                new HashMap<String, String>() {
                    {
                        put("cql.args.1.type", "int");
                        put("cql.args.1.value", "1");
                        put("cql.args.2.type", "text");
                        put("cql.args.2.value", "Joe");
                        put("cql.args.3.type", "text");
                        // No value for arg 3 to test setNull
                        put("cql.args.4.type", "map<text,text>");
                        put("cql.args.4.value", "{'a':'Hello', 'b':'World'}");
                        put("cql.args.5.type", "list<boolean>");
                        put("cql.args.5.value", "[true,false,true]");
                        put("cql.args.6.type", "set<double>");
                        put("cql.args.6.value", "{1.0, 2.0}");
                        put("cql.args.7.type", "bigint");
                        put("cql.args.7.value", "20000000");
                        put("cql.args.8.type", "float");
                        put("cql.args.8.value", "1.0");
                        put("cql.args.9.type", "blob");
                        put("cql.args.9.value", "0xDEADBEEF");
                        put("cql.args.10.type", "timestamp");
                        put("cql.args.10.value", "2016-07-01T15:21:05Z");

                    }
                });

        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(PutCassandraQL.REL_SUCCESS, 1);
        testRunner.clearTransferState();
    }

    @Test
    public void testProcessorHappyPathELConfig() {
        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, "${hosts}");
        testRunner.setProperty(AbstractCassandraProcessor.PASSWORD, "${pass}");
        testRunner.setProperty(AbstractCassandraProcessor.USERNAME, "${user}");
        testRunner.setProperty(AbstractCassandraProcessor.CONSISTENCY_LEVEL, "ONE");
        testRunner.setProperty(AbstractCassandraProcessor.CHARSET, "${charset}");
        testRunner.setProperty(PutCassandraQL.STATEMENT_TIMEOUT, "${timeout}");
        testRunner.assertValid();

        testRunner.setVariable("hosts", "localhost:9042");
        testRunner.setVariable("user", "username");
        testRunner.setVariable("pass", "password");
        testRunner.setVariable("charset", "UTF-8");
        testRunner.setVariable("timeout", "30 sec");

        testRunner.enqueue("INSERT INTO users (user_id, first_name, last_name, properties, bits, scaleset, largenum, scale, byteobject, ts) VALUES ?, ?, ?, ?, ?, ?, ?, ?, ?, ?",
                new HashMap<String, String>() {
                    {
                        put("cql.args.1.type", "int");
                        put("cql.args.1.value", "1");
                        put("cql.args.2.type", "text");
                        put("cql.args.2.value", "Joe");
                        put("cql.args.3.type", "text");
                        // No value for arg 3 to test setNull
                        put("cql.args.4.type", "map<text,text>");
                        put("cql.args.4.value", "{'a':'Hello', 'b':'World'}");
                        put("cql.args.5.type", "list<boolean>");
                        put("cql.args.5.value", "[true,false,true]");
                        put("cql.args.6.type", "set<double>");
                        put("cql.args.6.value", "{1.0, 2.0}");
                        put("cql.args.7.type", "bigint");
                        put("cql.args.7.value", "20000000");
                        put("cql.args.8.type", "float");
                        put("cql.args.8.value", "1.0");
                        put("cql.args.9.type", "blob");
                        put("cql.args.9.value", "0xDEADBEEF");
                        put("cql.args.10.type", "timestamp");
                        put("cql.args.10.value", "2016-07-01T15:21:05Z");

                    }
                });

        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(PutCassandraQL.REL_SUCCESS, 1);
        testRunner.clearTransferState();
    }

    @Test
    public void testMultipleQuery() {
        setUpStandardTestConfig();
        testRunner.setProperty(PutCassandraQL.STATEMENT_CACHE_SIZE, "1");

        HashMap<String, String> testData = new HashMap<>();
        testData.put("cql.args.1.type", "int");
        testData.put("cql.args.1.value", "1");
        testData.put("cql.args.2.type", "text");
        testData.put("cql.args.2.value", "Joe");
        testData.put("cql.args.3.type", "text");
        // No value for arg 3 to test setNull
        testData.put("cql.args.4.type", "map<text,text>");
        testData.put("cql.args.4.value", "{'a':'Hello', 'b':'World'}");
        testData.put("cql.args.5.type", "list<boolean>");
        testData.put("cql.args.5.value", "[true,false,true]");
        testData.put("cql.args.6.type", "set<double>");
        testData.put("cql.args.6.value", "{1.0, 2.0}");
        testData.put("cql.args.7.type", "bigint");
        testData.put("cql.args.7.value", "20000000");
        testData.put("cql.args.8.type", "float");
        testData.put("cql.args.8.value", "1.0");
        testData.put("cql.args.9.type", "blob");
        testData.put("cql.args.9.value", "0xDEADBEEF");
        testData.put("cql.args.10.type", "timestamp");
        testData.put("cql.args.10.value", "2016-07-01T15:21:05Z");

        testRunner.enqueue("INSERT INTO users (user_id, first_name, last_name, properties, bits, scaleset, largenum, scale, byteobject, ts) VALUES ?, ?, ?, ?, ?, ?, ?, ?, ?, ?",
                testData);

        testRunner.enqueue("INSERT INTO newusers (user_id, first_name, last_name, properties, bits, scaleset, largenum, scale, byteobject, ts) VALUES ?, ?, ?, ?, ?, ?, ?, ?, ?, ?",
                testData);

        // Change it up a bit, the same statement is executed with different data
        testData.put("cql.args.1.value", "2");
        testRunner.enqueue("INSERT INTO users (user_id, first_name, last_name, properties, bits, scaleset, largenum, scale, byteobject, ts) VALUES ?, ?, ?, ?, ?, ?, ?, ?, ?, ?",
                testData);

        testRunner.enqueue("INSERT INTO users (user_id) VALUES ('user_id data');");

        testRunner.run(4, true, true);
        testRunner.assertAllFlowFilesTransferred(PutCassandraQL.REL_SUCCESS, 4);
    }

    @Test
    public void testProcessorBadTimestamp() {
        setUpStandardTestConfig();
        processor.setExceptionToThrow(
                new InvalidQueryException(new InetSocketAddress("localhost", 9042), "invalid timestamp"));
        testRunner.enqueue("INSERT INTO users (user_id, first_name, last_name, properties, bits, scaleset, largenum, scale, byteobject, ts) VALUES ?, ?, ?, ?, ?, ?, ?, ?, ?, ?",
                new HashMap<String, String>() {
                    {
                        put("cql.args.1.type", "int");
                        put("cql.args.1.value", "1");
                        put("cql.args.2.type", "text");
                        put("cql.args.2.value", "Joe");
                        put("cql.args.3.type", "text");
                        // No value for arg 3 to test setNull
                        put("cql.args.4.type", "map<text,text>");
                        put("cql.args.4.value", "{'a':'Hello', 'b':'World'}");
                        put("cql.args.5.type", "list<boolean>");
                        put("cql.args.5.value", "[true,false,true]");
                        put("cql.args.6.type", "set<double>");
                        put("cql.args.6.value", "{1.0, 2.0}");
                        put("cql.args.7.type", "bigint");
                        put("cql.args.7.value", "20000000");
                        put("cql.args.8.type", "float");
                        put("cql.args.8.value", "1.0");
                        put("cql.args.9.type", "blob");
                        put("cql.args.9.value", "0xDEADBEEF");
                        put("cql.args.10.type", "timestamp");
                        put("cql.args.10.value", "not a timestamp");

                    }
                });

        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(PutCassandraQL.REL_FAILURE, 1);
        testRunner.clearTransferState();
    }

    @Test
    public void testProcessorInvalidQueryException() {
        setUpStandardTestConfig();

        // Test exceptions
        processor.setExceptionToThrow(
                new InvalidQueryException(new InetSocketAddress("localhost", 9042), "invalid query"));
        testRunner.enqueue("UPDATE users SET cities = [ 'New York', 'Los Angeles' ] WHERE user_id = 'coast2coast';");
        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(PutCassandraQL.REL_FAILURE, 1);
        testRunner.clearTransferState();
    }

    @Test
    public void testProcessorUnavailableException() {
        setUpStandardTestConfig();

        processor.setExceptionToThrow(
                new UnavailableException(new InetSocketAddress("localhost", 9042), ConsistencyLevel.ALL, 5, 2));
        testRunner.enqueue("UPDATE users SET cities = [ 'New York', 'Los Angeles' ] WHERE user_id = 'coast2coast';");
        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(PutCassandraQL.REL_RETRY, 1);
    }

    @Test
    public void testProcessorNoHostAvailableException() {
        setUpStandardTestConfig();

        processor.setExceptionToThrow(new NoHostAvailableException(new HashMap<>()));
        testRunner.enqueue("UPDATE users SET cities = [ 'New York', 'Los Angeles' ] WHERE user_id = 'coast2coast';");
        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(PutCassandraQL.REL_RETRY, 1);
    }

    @Test
    public void testProcessorProcessException() {
        setUpStandardTestConfig();

        processor.setExceptionToThrow(new ProcessException());
        testRunner.enqueue("UPDATE users SET cities = [ 'New York', 'Los Angeles' ] WHERE user_id = 'coast2coast';");
        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(PutCassandraQL.REL_FAILURE, 1);
    }

    private void setUpStandardTestConfig() {
        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, "localhost:9042");
        testRunner.setProperty(AbstractCassandraProcessor.PASSWORD, "password");
        testRunner.setProperty(AbstractCassandraProcessor.USERNAME, "username");
        testRunner.setProperty(AbstractCassandraProcessor.CONSISTENCY_LEVEL, "ONE");
        testRunner.assertValid();
    }

    /**
     * Provides a stubbed processor instance for testing
     */
    private static class MockPutCassandraQL extends PutCassandraQL {

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

        void setExceptionToThrow(Exception e) {
            exceptionToThrow = e;
            doThrow(exceptionToThrow).when(mockSession).executeAsync(anyString());
            doThrow(exceptionToThrow).when(mockSession).executeAsync(any(Statement.class));
        }

    }
}
