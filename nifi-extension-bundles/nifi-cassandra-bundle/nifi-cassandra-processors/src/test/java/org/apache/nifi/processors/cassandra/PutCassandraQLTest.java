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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Unit tests for the PutCassandraQL processor
 */
public class PutCassandraQLTest {

    private TestRunner testRunner;
    private MockPutCassandraQL processor;

    @BeforeEach
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
        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, "127.0.0.1:9042");
        testRunner.setProperty(AbstractCassandraProcessor.PASSWORD, "cassandra");
        testRunner.setProperty(AbstractCassandraProcessor.USERNAME, "cassandra");
        testRunner.setProperty(AbstractCassandraProcessor.CHARSET, "UTF-8");
        testRunner.setProperty(PutCassandraQL.STATEMENT_TIMEOUT, "30 sec");
        testRunner.assertValid();
    }

    @Test
    public void testProcessorHappyPath() {
        setUpStandardTestConfig();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("cql.args.1.type", "int");
        attributes.put("cql.args.1.value", "1");
        attributes.put("cql.args.2.type", "text");
        attributes.put("cql.args.2.value", "Joe");
        attributes.put("cql.args.3.type", "text");
        // No value for arg 3 to test setNull
        attributes.put("cql.args.4.type", "map<text,text>");
        attributes.put("cql.args.4.value", "{\"a\":\"Hello\",\"b\":\"World\"}");
        attributes.put("cql.args.5.type", "list<boolean>");
        attributes.put("cql.args.5.value", "[true,false,true]");
        attributes.put("cql.args.6.type", "set<double>");
        attributes.put("cql.args.6.value", "[1.0, 2.0]");
        attributes.put("cql.args.7.type", "bigint");
        attributes.put("cql.args.7.value", "20000000");
        attributes.put("cql.args.8.type", "float");
        attributes.put("cql.args.8.value", "1.0");
        attributes.put("cql.args.9.type", "blob");
        attributes.put("cql.args.9.value", "0xDEADBEEF");
        attributes.put("cql.args.10.type", "timestamp");
        attributes.put("cql.args.10.value", "2016-07-01T15:21:05Z");

        testRunner.enqueue("INSERT INTO users (user_id, first_name, last_name, properties, bits, scaleset, largenum, scale, byteobject, ts) VALUES ?, ?, ?, ?, ?, ?, ?, ?, ?, ?",
                attributes);

        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(PutCassandraQL.REL_SUCCESS, 1);
        testRunner.clearTransferState();
    }

    @Test
    public void testProcessorHappyPathELConfig() {
        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, "localhost:9042");
        testRunner.setProperty(AbstractCassandraProcessor.PASSWORD, "password");
        testRunner.setProperty(AbstractCassandraProcessor.USERNAME, "username");
        testRunner.setProperty(AbstractCassandraProcessor.CONSISTENCY_LEVEL, "ONE");
        testRunner.setProperty(AbstractCassandraProcessor.CHARSET, "UTF-8");
        testRunner.setProperty(PutCassandraQL.STATEMENT_TIMEOUT, "30 sec");
        testRunner.assertValid();

        String cql = "INSERT INTO users (user_id, first_name, last_name, properties, bits, scaleset, "
                + "largenum, scale, byteobject, ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        Map<String, String> attributes = new HashMap<>();
        attributes.put("cql.args.1.type", "int");
        attributes.put("cql.args.1.value", "1");
        attributes.put("cql.args.2.type", "text");
        attributes.put("cql.args.2.value", "Joe");
        attributes.put("cql.args.3.type", "text");
        attributes.put("cql.args.4.type", "map<text,text>");
        attributes.put("cql.args.4.value", "{\"a\":\"Hello\",\"b\":\"World\"}");
        attributes.put("cql.args.5.type", "list<boolean>");
        attributes.put("cql.args.5.value", "[true,false,true]");
        attributes.put("cql.args.6.type", "set<double>");
        attributes.put("cql.args.6.value", "[1.0,2.0]");
        attributes.put("cql.args.7.type", "bigint");
        attributes.put("cql.args.7.value", "20000000");
        attributes.put("cql.args.8.type", "float");
        attributes.put("cql.args.8.value", "1.0");
        attributes.put("cql.args.9.type", "blob");
        attributes.put("cql.args.9.value", "0xDEADBEEF");
        attributes.put("cql.args.10.type", "timestamp");
        attributes.put("cql.args.10.value", "2016-07-01T15:21:05Z");

        testRunner.enqueue(cql, attributes);
        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(PutCassandraQL.REL_SUCCESS, 1);
        testRunner.clearTransferState();
    }

    @Test
    public void testMultipleQuery() {
        setUpStandardTestConfig();
        testRunner.setProperty(PutCassandraQL.STATEMENT_CACHE_SIZE, "1");

        Map<String, String> testData = new HashMap<>();
        testData.put("cql.args.1.type", "int");
        testData.put("cql.args.1.value", "1");
        testData.put("cql.args.2.type", "text");
        testData.put("cql.args.2.value", "Joe");
        testData.put("cql.args.3.type", "text");
        // No value for arg 3 to test setNull
        testData.put("cql.args.4.type", "map<text,text>");
        testData.put("cql.args.4.value", "{\"a\":\"Hello\",\"b\":\"World\"}");
        testData.put("cql.args.5.type", "list<boolean>");
        testData.put("cql.args.5.value", "[true,false,true]");
        testData.put("cql.args.6.type", "set<double>");
        testData.put("cql.args.6.value", "[1.0, 2.0]");
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

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("cql.args.1.type", "int");
        attributes.put("cql.args.1.value", "1");
        attributes.put("cql.args.2.type", "text");
        attributes.put("cql.args.2.value", "Joe");
        attributes.put("cql.args.3.type", "text");
        // intentionally null
        attributes.put("cql.args.4.type", "map<text,text>");
        attributes.put("cql.args.4.value", "{\"a\":\"Hello\",\"b\":\"World\"}");
        attributes.put("cql.args.5.type", "list<boolean>");
        attributes.put("cql.args.5.value", "[true,false,true]");
        attributes.put("cql.args.6.type", "set<double>");
        attributes.put("cql.args.6.value", "[1.0,2.0]");
        attributes.put("cql.args.7.type", "bigint");
        attributes.put("cql.args.7.value", "20000000");
        attributes.put("cql.args.8.type", "float");
        attributes.put("cql.args.8.value", "1.0");
        attributes.put("cql.args.9.type", "blob");
        attributes.put("cql.args.9.value", "0xDEADBEEF");
        attributes.put("cql.args.10.type", "timestamp");
        attributes.put("cql.args.10.value", "not a timestamp");

        testRunner.enqueue(
                "INSERT INTO users (user_id, first_name, last_name, properties, bits, scaleset, largenum, scale, byteobject, ts) VALUES ?, ?, ?, ?, ?, ?, ?, ?, ?, ?",
                attributes
        );

        testRunner.run(1, false);
        testRunner.assertAllFlowFilesTransferred(PutCassandraQL.REL_FAILURE, 1);
    }

    @Test
    public void testProcessorUuid() {
        setUpStandardTestConfig();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("cql.args.1.type", "int");
        attributes.put("cql.args.1.value", "1");
        attributes.put("cql.args.2.type", "text");
        attributes.put("cql.args.2.value", "Joe");
        attributes.put("cql.args.3.type", "text");
        // No value for arg 3 to test setNull
        attributes.put("cql.args.4.type", "map<text,text>");
        attributes.put("cql.args.4.value", "{\"a\":\"Hello\", \"b\":\"World\"}\n");
        attributes.put("cql.args.5.type", "list<boolean>");
        attributes.put("cql.args.5.value", "[true,false,true]");
        attributes.put("cql.args.6.type", "set<double>");
        attributes.put("cql.args.6.value", "[1.0, 2.0]");
        attributes.put("cql.args.7.type", "bigint");
        attributes.put("cql.args.7.value", "20000000");
        attributes.put("cql.args.8.type", "float");
        attributes.put("cql.args.8.value", "1.0");
        attributes.put("cql.args.9.type", "blob");
        attributes.put("cql.args.9.value", "0xDEADBEEF");
        attributes.put("cql.args.10.type", "uuid");
        attributes.put("cql.args.10.value", "5442b1f6-4c16-11ea-87f5-45a32dbc5199");

        testRunner.enqueue("INSERT INTO users (user_id, first_name, last_name, properties, bits, scaleset, largenum, scale, byteobject, ts) VALUES ?, ?, ?, ?, ?, ?, ?, ?, ?, ?",
                attributes);

        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(PutCassandraQL.REL_SUCCESS, 1);
        testRunner.clearTransferState();
    }

    @Test
    public void testProcessorBadUuid() {
        setUpStandardTestConfig();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("cql.args.1.type", "int");
        attributes.put("cql.args.1.value", "1");
        attributes.put("cql.args.2.type", "text");
        attributes.put("cql.args.2.value", "Joe");
        attributes.put("cql.args.3.type", "text");
        // No value for arg 3 to test setNull
        attributes.put("cql.args.4.type", "map<text,text>");
        attributes.put("cql.args.4.value", "{\"a\":\"Hello\",\"b\":\"World\"}");
        attributes.put("cql.args.5.type", "list<boolean>");
        attributes.put("cql.args.5.value", "[true,false,true]");
        attributes.put("cql.args.6.type", "set<double>");
        attributes.put("cql.args.6.value", "[1.0, 2.0]");
        attributes.put("cql.args.7.type", "bigint");
        attributes.put("cql.args.7.value", "20000000");
        attributes.put("cql.args.8.type", "float");
        attributes.put("cql.args.8.value", "1.0");
        attributes.put("cql.args.9.type", "blob");
        attributes.put("cql.args.9.value", "0xDEADBEEF");
        attributes.put("cql.args.10.type", "uuid");
        attributes.put("cql.args.10.value", "bad-uuid");

        testRunner.enqueue("INSERT INTO users (user_id, first_name, last_name, properties, bits, scaleset, largenum, scale, byteobject, ts) VALUES ?, ?, ?, ?, ?, ?, ?, ?, ?, ?",
                attributes);

        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(PutCassandraQL.REL_FAILURE, 1);
        testRunner.clearTransferState();
    }
    @Test
    public void testProcessorInvalidQueryException() {
        setUpStandardTestConfig();
        processor.setExceptionToThrow(new InvalidQueryException(mock(Node.class), "invalid query"));
        testRunner.enqueue("UPDATE users SET cities = [ 'New York', 'Los Angeles' ] WHERE user_id = 'coast2coast';");
        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(PutCassandraQL.REL_FAILURE, 1);
        testRunner.clearTransferState();
    }

    @Test
    public void testProcessorUnavailableException() {
        setUpStandardTestConfig();
        processor.setExceptionToThrow(new UnavailableException(mock(Node.class), ConsistencyLevel.ALL, 5, 2));
        testRunner.enqueue("UPDATE users SET cities = [ 'New York', 'Los Angeles' ] WHERE user_id = 'coast2coast';");
        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(PutCassandraQL.REL_RETRY, 1);
    }

    @Test
    public void testProcessorSuccess() {
        setUpStandardTestConfig();

        testRunner.enqueue(
                "UPDATE users SET cities = [ 'New York', 'Los Angeles' ] WHERE user_id = 'coast2coast';"
        );
        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(PutCassandraQL.REL_SUCCESS, 1);
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
        protected AsyncResultSet mockResultSet;
        private CqlSession mockSession; // Keep reference for exception mocking

        @Override
        protected void connectToCassandra(ProcessContext context) {
            mockSession = Mockito.mock(CqlSession.class);
            PreparedStatement mockPrepared = Mockito.mock(PreparedStatement.class);
            BoundStatementBuilder mockBuilder = Mockito.mock(BoundStatementBuilder.class);
            BoundStatement mockBound = Mockito.mock(BoundStatement.class);

            try {
                // Mock AsyncResultSet
                mockResultSet = CassandraQueryTestUtil.createMockAsyncResultSet(false);
                CompletableFuture<AsyncResultSet> future;
                if (exceptionToThrow != null) {
                    future = CompletableFuture.failedFuture(exceptionToThrow);
                } else {
                    future = CompletableFuture.completedFuture(mockResultSet);
                }

                Metadata metadata = Mockito.mock(Metadata.class);
                when(metadata.getNodes()).thenReturn(Collections.emptyMap());
                when(mockSession.getMetadata()).thenReturn(metadata);

                // Mock session and prepared statement
                when(mockSession.prepare(anyString())).thenReturn(mockPrepared);
                when(mockPrepared.boundStatementBuilder()).thenReturn(mockBuilder);
                when(mockBuilder.build()).thenReturn(mockBound);

                if (exceptionToThrow != null) {
                    doThrow(exceptionToThrow).when(mockSession).executeAsync(any(BoundStatement.class));
                    doThrow(exceptionToThrow).when(mockSession).executeAsync(anyString());
                } else {
                    when(mockSession.executeAsync(any(BoundStatement.class))).thenReturn(future);
                    when(mockSession.executeAsync(anyString())).thenReturn(future);
                }

                // Assign mocked session and initialize cache
                this.cassandraSession.set(mockSession);
                this.statementCache = new ConcurrentHashMap<>();

            } catch (Exception e) {
                fail("Failed to setup mock session: " + e.getMessage());
            }
        }

        void setExceptionToThrow(Exception e) {
            this.exceptionToThrow = e;

            if (mockSession != null) {
                try {
                    doThrow(exceptionToThrow).when(mockSession).executeAsync(any(BoundStatement.class));
                    doThrow(exceptionToThrow).when(mockSession).executeAsync(anyString());
                } catch (Exception ex) {
                    fail("Failed to setup exception mocking: " + ex.getMessage());
                }
            }
        }
    }
}
