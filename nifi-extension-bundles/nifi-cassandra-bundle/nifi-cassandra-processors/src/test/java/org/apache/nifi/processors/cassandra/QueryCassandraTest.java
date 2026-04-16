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

import org.apache.nifi.cassandra.CassandraClient;
import org.apache.nifi.cassandra.CassandraConnectionService;
import org.apache.nifi.cassandra.exception.CassandraException;
import org.apache.nifi.cassandra.exception.CassandraExceptionCategory;
import org.apache.nifi.cassandra.models.CassandraQueryRequest;
import org.apache.nifi.cassandra.models.CassandraQueryResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class QueryCassandraTest {

    private static final String CASSANDRA_SERVICE_ID = "cassandra";
    private static final String WRITER_ID = "writer";

    private TestRunner testRunner;
    private QueryCassandra processor;
    private CassandraClient mockClient;

    @BeforeEach
    void setUp() throws Exception {
        processor = new QueryCassandra();
        testRunner = TestRunners.newTestRunner(processor);

        mockClient = mock(CassandraClient.class);
        final CassandraConnectionService connectionService = new TestCassandraConnectionService(mockClient, "localhost:9042/test_keyspace");
        testRunner.addControllerService(CASSANDRA_SERVICE_ID, connectionService);
        testRunner.enableControllerService(connectionService);

        final MockRecordWriter writerService = new MockRecordWriter(null, false);
        testRunner.addControllerService(WRITER_ID, writerService);
        testRunner.enableControllerService(writerService);
        testRunner.setProperty(QueryCassandra.RECORD_WRITER, WRITER_ID);
    }

    @Test
    void testProcessorConfigValid() {
        testRunner.setProperty(AbstractCassandraProcessor.CASSANDRA_CONNECTION_PROVIDER, CASSANDRA_SERVICE_ID);
        testRunner.assertNotValid();
        testRunner.setProperty(QueryCassandra.CQL_SELECT_QUERY, "select * from test");
        testRunner.assertValid();
    }

    @Test
    void testProcessorELConfigValid() {
        testRunner.setProperty(AbstractCassandraProcessor.CASSANDRA_CONNECTION_PROVIDER, CASSANDRA_SERVICE_ID);
        testRunner.setProperty(QueryCassandra.CQL_SELECT_QUERY, "SELECT * FROM test");
        testRunner.assertValid();
    }

    @Test
    void testProcessorNoInputFlowFileAndException() {
        setUpStandardProcessorConfig();
        testRunner.setValidateExpressionUsage(false);
        testRunner.setIncomingConnection(false);

        setSessionException(new CassandraException("All nodes failed", CassandraExceptionCategory.RETRY, null));
        testRunner.run(1, true, true);
        testRunner.assertTransferCount(QueryCassandra.REL_SUCCESS, 0);
        testRunner.assertTransferCount(QueryCassandra.REL_RETRY, 0);
        testRunner.assertTransferCount(QueryCassandra.REL_FAILURE, 0);
    }

    @Test
     void testProcessorSuccessOutput() throws Exception {
        setUpStandardProcessorConfig();
        setMockResultSet(false);
        testRunner.setIncomingConnection(false);
        testRunner.setValidateExpressionUsage(false);
        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(QueryCassandra.REL_SUCCESS, 1);

        final MockFlowFile output = testRunner.getFlowFilesForRelationship(QueryCassandra.REL_SUCCESS).get(0);
        output.assertAttributeEquals(QueryCassandra.RESULT_ROW_COUNT, "2");

        final String content = new String(output.toByteArray(), StandardCharsets.UTF_8);
        assertTrue(content.contains("user1"));
        assertTrue(content.contains("Joe"));
        assertTrue(content.contains("jsmith@notareal.com"));
        assertTrue(content.contains("user2"));
        assertTrue(content.contains("Mary"));
        assertTrue(content.contains("mjones@notareal.com"));
    }

    @Test
     void testProcessorJsonOutputFragmentAttributes() throws Exception {
        processor = new QueryCassandra();
        testRunner = TestRunners.newTestRunner(processor);

        mockClient = mock(CassandraClient.class);
        final CassandraConnectionService connectionService = new TestCassandraConnectionService(mockClient, "localhost:9042/test_keyspace");
        testRunner.addControllerService(CASSANDRA_SERVICE_ID, connectionService);
        testRunner.enableControllerService(connectionService);

        final MockRecordWriter writerService = new MockRecordWriter(null, false);
        testRunner.addControllerService(WRITER_ID, writerService);
        testRunner.enableControllerService(writerService);
        testRunner.setProperty(QueryCassandra.RECORD_WRITER, WRITER_ID);

        setUpStandardProcessorConfig();
        setMockResultSet(true);
        testRunner.setIncomingConnection(false);
        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(QueryCassandra.MAX_ROWS_PER_FLOW_FILE, "1");
        testRunner.run(1, true, true);

        testRunner.assertAllFlowFilesTransferred(QueryCassandra.REL_SUCCESS, 2);
        List<MockFlowFile> files = testRunner.getFlowFilesForRelationship(QueryCassandra.REL_SUCCESS);
        assertNotNull(files);
        assertEquals(2, files.size());
        String fragmentId = null;
        Set<String> fragmentIndexes = new HashSet<>();

        for (MockFlowFile flowFile : files) {
            fragmentIndexes.add(flowFile.getAttribute(QueryCassandra.FRAGMENT_INDEX));

            if (fragmentId == null) {
                fragmentId = flowFile.getAttribute(QueryCassandra.FRAGMENT_ID);
            } else {
                flowFile.assertAttributeEquals(QueryCassandra.FRAGMENT_ID, fragmentId);
            }

            flowFile.assertAttributeEquals(
                    QueryCassandra.FRAGMENT_COUNT,
                    String.valueOf(files.size())
            );
        }

        assertEquals(Set.of("0", "1"), fragmentIndexes);
    }

    @Test
     void testProcessorConfigOutput() throws Exception {
        setUpStandardProcessorConfig();
        setMockResultSet(false);

        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(QueryCassandra.CQL_SELECT_QUERY, "select * from test");
        testRunner.setProperty(QueryCassandra.QUERY_TIMEOUT, "30 sec");
        testRunner.setProperty(QueryCassandra.FETCH_SIZE, "0");
        testRunner.setProperty(QueryCassandra.MAX_ROWS_PER_FLOW_FILE, "0");
        testRunner.setIncomingConnection(false);

        testRunner.assertValid();
        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(QueryCassandra.REL_SUCCESS, 1);

        final MockFlowFile output = testRunner.getFlowFilesForRelationship(QueryCassandra.REL_SUCCESS).get(0);
        output.assertAttributeEquals(QueryCassandra.RESULT_ROW_COUNT, "2");

        final String content = output.getContent();
        assertTrue(content.contains("user1"));
        assertTrue(content.contains("user2"));
    }

    @Test
     void testQueryTimeoutConfigured() throws Exception {
        setUpStandardProcessorConfig();
        setMockResultSet(false);

        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(QueryCassandra.QUERY_TIMEOUT, "5 sec");
        testRunner.setIncomingConnection(false);
        testRunner.assertValid();
        testRunner.run(1, true, true);

        testRunner.assertAllFlowFilesTransferred(QueryCassandra.REL_SUCCESS, 1);
        final MockFlowFile output = testRunner.getFlowFilesForRelationship(QueryCassandra.REL_SUCCESS).get(0);
        output.assertAttributeEquals(QueryCassandra.RESULT_ROW_COUNT, "2");
    }

    @Test
     void testProcessorEmptyFlowFile() {
        setUpStandardProcessorConfig();
        setMockResultSet(false);
        testRunner.setValidateExpressionUsage(false);
        testRunner.setIncomingConnection(true);
        testRunner.enqueue(new byte[0]);
        testRunner.run(1, true, true);
        testRunner.assertTransferCount(QueryCassandra.REL_SUCCESS, 1);

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(QueryCassandra.REL_SUCCESS).get(0);
        assertTrue(flowFile.getSize() > 0);
        testRunner.clearTransferState();
    }

    @Test
    public void testProcessorEmptyFlowFileAndRetryException() {
        setUpStandardProcessorConfig();
        setSessionException(new CassandraException("All nodes failed", CassandraExceptionCategory.RETRY, null));
        testRunner.setValidateExpressionUsage(false);
        testRunner.enqueue(new byte[0]);
        testRunner.run(1, true, true);
        testRunner.assertTransferCount(QueryCassandra.REL_RETRY, 1);
    }

    @Test
     void testProcessorEmptyFlowFileAndFailureException() {
        setUpStandardProcessorConfig();
        setSessionException(new CassandraException("Invalid query", CassandraExceptionCategory.FAILURE, null));
        testRunner.setValidateExpressionUsage(false);
        testRunner.enqueue(new byte[0]);
        testRunner.run(1, true, true);
        testRunner.assertTransferCount(QueryCassandra.REL_FAILURE, 1);
    }

    private void setUpStandardProcessorConfig() {
        testRunner.setProperty(AbstractCassandraProcessor.CASSANDRA_CONNECTION_PROVIDER, CASSANDRA_SERVICE_ID);
        testRunner.setProperty(QueryCassandra.CQL_SELECT_QUERY, "select * from test");
        testRunner.setProperty(QueryCassandra.MAX_ROWS_PER_FLOW_FILE, "0");
    }

    private void setMockResultSet(final boolean twoPages) {
        try {
            final CassandraQueryResult mockResultSet = CassandraQueryTestUtil.createMockQueryResult(twoPages);
            when(mockClient.executeQuery(any(CassandraQueryRequest.class))).thenReturn(mockResultSet);
        } catch (final Exception e) {
            fail("Failed to setup mock result set: " + e.getMessage());
        }
    }

    private void setSessionException(final CassandraException exception) {
        try {
            when(mockClient.executeQuery(any(CassandraQueryRequest.class))).thenThrow(exception);
        } catch (final Exception e) {
            fail("Failed to setup session exception: " + e.getMessage());
        }
    }

    private static class TestCassandraConnectionService extends AbstractControllerService implements CassandraConnectionService {
        private final CassandraClient client;
        private final String clusterAddress;

        TestCassandraConnectionService(final CassandraClient client, final String clusterAddress) {
            this.client = client;
            this.clusterAddress = clusterAddress;
        }

        @Override
        public CassandraClient getClient() {
            return client;
        }

        @Override
        public String getDatabaseLocation() {
            return clusterAddress;
        }
    }

}
