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

import com.datastax.driver.core.SniEndPoint;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryCassandraTest {

    private TestRunner testRunner;
    private MockQueryCassandra processor;

    @BeforeEach
    public void setUp() throws Exception {
        processor = new MockQueryCassandra();
        testRunner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testProcessorConfigValid() {
        testRunner.setProperty(AbstractCassandraProcessor.CONSISTENCY_LEVEL, "ONE");
        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, "localhost:9042");
        testRunner.assertNotValid();
        testRunner.setProperty(QueryCassandra.CQL_SELECT_QUERY, "select * from test");
        testRunner.assertValid();
        testRunner.setProperty(AbstractCassandraProcessor.PASSWORD, "password");
        testRunner.assertNotValid();
        testRunner.setProperty(AbstractCassandraProcessor.USERNAME, "username");
        testRunner.assertValid();

        testRunner.setProperty(QueryCassandra.TIMESTAMP_FORMAT_PATTERN, "invalid format");
        testRunner.assertNotValid();
        testRunner.setProperty(QueryCassandra.TIMESTAMP_FORMAT_PATTERN, "yyyy-MM-dd HH:mm:ss.SSSZ");
        testRunner.assertValid();
    }

    @Test
    public void testProcessorELConfigValid() {
        testRunner.setProperty(AbstractCassandraProcessor.CONSISTENCY_LEVEL, "ONE");
        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, "127.0.0.1:9042"); // must be valid
        testRunner.setProperty(QueryCassandra.CQL_SELECT_QUERY, "SELECT * FROM test");
        testRunner.setProperty(AbstractCassandraProcessor.PASSWORD, "cassandra");
        testRunner.setProperty(AbstractCassandraProcessor.USERNAME, "cassandra");
        testRunner.assertValid();
    }

    @Test
    public void testProcessorNoInputFlowFileAndExceptions()  {
        setUpStandardProcessorConfig();
        testRunner.setValidateExpressionUsage(false);

        testRunner.setIncomingConnection(false);
        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(QueryCassandra.REL_SUCCESS, 1);
        testRunner.clearTransferState();

        processor.setExceptionToThrow(new ExecutionException(AllNodesFailedException.fromErrors(Collections.emptyMap())));
        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(QueryCassandra.REL_RETRY, 1);
        testRunner.clearTransferState();

        processor.setExceptionToThrow(new ExecutionException(new RuntimeException("All nodes failed")));
        testRunner.run(1, true, true);
        testRunner.assertTransferCount(QueryCassandra.REL_RETRY, 0);
        testRunner.assertTransferCount(QueryCassandra.REL_FAILURE, 0);
        testRunner.clearTransferState();

        processor.setExceptionToThrow(new ExecutionException(new DriverTimeoutException("read timeout")));
        testRunner.run(1, true, true);
        testRunner.assertTransferCount(QueryCassandra.REL_RETRY, 0);
        testRunner.assertTransferCount(QueryCassandra.REL_FAILURE, 0);
        testRunner.clearTransferState();

        processor.setExceptionToThrow(new ExecutionException(new InvalidQueryException("invalid")));
        testRunner.run(1, true, true);
        testRunner.assertTransferCount(QueryCassandra.REL_FAILURE, 0);
        testRunner.clearTransferState();

        processor.setExceptionToThrow(new ProcessException("Process Exception"));
        testRunner.run(1, true, true);
        testRunner.assertTransferCount(QueryCassandra.REL_FAILURE, 0);
        testRunner.clearTransferState();

        processor.setExceptionToThrow(null);
    }


    @Test
    public void testProcessorJsonOutput() throws Exception {
        setUpStandardProcessorConfig();
        testRunner.setIncomingConnection(false);
        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(QueryCassandra.OUTPUT_FORMAT, QueryCassandra.JSON_FORMAT);
        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(QueryCassandra.REL_SUCCESS, 1);

        List<MockFlowFile> files = testRunner.getFlowFilesForRelationship(QueryCassandra.REL_SUCCESS);

        assertNotNull(files);
        assertEquals(1, files.size(), "One file should be transferred to success");

        String json = new String(files.get(0).toByteArray(), StandardCharsets.UTF_8);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(json);

        assertTrue(root.has("results"));
        JsonNode results = root.get("results");
        assertEquals(2, results.size());

        JsonNode row1 = results.get(0);

        assertEquals("user1", row1.get("user_id").asText());
        assertEquals("Joe", row1.get("first_name").asText());
        assertEquals("Smith", row1.get("last_name").asText());
        assertFalse(row1.get("registered").asBoolean());
        assertEquals(1.0, row1.get("scale").asDouble());
        assertEquals(2.0, row1.get("metric").asDouble());
        assertEquals(1, row1.get("emails").size());
        assertEquals("jsmith@notareal.com", row1.get("emails").get(0).asText());
        assertEquals(2, row1.get("top_places").size());
        assertEquals("New York, NY", row1.get("top_places").get(0).asText());
        assertEquals("Santa Clara, CA", row1.get("top_places").get(1).asText());

        JsonNode todo1 = row1.get("todo");
        assertEquals(1, todo1.size());
        assertEquals("Set my alarm \"for\" a month from now", todo1.get("2016-01-03 05:00:00+0000").asText());

        JsonNode row2 = results.get(1);

        assertEquals("user2", row2.get("user_id").asText());
        assertEquals("Mary", row2.get("first_name").asText());
        assertEquals("Jones", row2.get("last_name").asText());
        assertTrue(row2.get("registered").asBoolean());
        assertEquals(3.0, row2.get("scale").asDouble());
        assertEquals(4.0, row2.get("metric").asDouble());
        assertEquals(1, row2.get("emails").size());
        assertEquals("mjones@notareal.com", row2.get("emails").get(0).asText());
        assertEquals(1, row2.get("top_places").size());
        assertEquals("Orlando, FL", row2.get("top_places").get(0).asText());

        JsonNode todo2 = row2.get("todo");
        assertEquals(1, todo2.size());
        assertEquals("Get milk and bread", todo2.get("2016-02-03 05:00:00+0000").asText());
    }

    @Test
    public void testProcessorJsonOutputFragmentAttributes() {
        processor = new MockQueryCassandraTwoRounds();
        testRunner = TestRunners.newTestRunner(processor);
        setUpStandardProcessorConfig();
        testRunner.setIncomingConnection(false);
        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(QueryCassandra.MAX_ROWS_PER_FLOW_FILE, "1");
        testRunner.setProperty(QueryCassandra.OUTPUT_FORMAT, QueryCassandra.JSON_FORMAT);
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
    public void testProcessorConfigJsonOutput() throws Exception {
        setUpStandardProcessorConfig();

        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, "localhost:9042");
        testRunner.setProperty(QueryCassandra.CQL_SELECT_QUERY, "select * from test");
        testRunner.setProperty(AbstractCassandraProcessor.USERNAME, "username");
        testRunner.setProperty(AbstractCassandraProcessor.PASSWORD, "password");
        testRunner.setProperty(AbstractCassandraProcessor.CHARSET, "UTF-8");
        testRunner.setProperty(QueryCassandra.QUERY_TIMEOUT, "30 sec");
        testRunner.setProperty(QueryCassandra.FETCH_SIZE, "0");
        testRunner.setProperty(QueryCassandra.MAX_ROWS_PER_FLOW_FILE, "0");
        testRunner.setIncomingConnection(false);

        testRunner.assertValid();
        testRunner.setProperty(QueryCassandra.OUTPUT_FORMAT, QueryCassandra.JSON_FORMAT);
        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(QueryCassandra.REL_SUCCESS, 1);
        List<MockFlowFile> files = testRunner.getFlowFilesForRelationship(QueryCassandra.REL_SUCCESS);

        assertNotNull(files);
        assertEquals(1, files.size());
        String json = files.get(0).getContent();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(json);

        assertTrue(root.has("results"));
        JsonNode results = root.get("results");
        assertEquals(2, results.size());

        JsonNode row1 = results.get(0);
        assertEquals("user1", row1.get("user_id").asText());
        assertEquals("Joe", row1.get("first_name").asText());
        assertEquals("Smith", row1.get("last_name").asText());
        assertFalse(row1.get("registered").asBoolean());
        assertEquals(1.0, row1.get("scale").asDouble());
        assertEquals(2.0, row1.get("metric").asDouble());

        JsonNode row2 = results.get(1);
        assertEquals("user2", row2.get("user_id").asText());
        assertEquals("Mary", row2.get("first_name").asText());
        assertEquals("Jones", row2.get("last_name").asText());
        assertTrue(row2.get("registered").asBoolean());
        assertEquals(3.0, row2.get("scale").asDouble());
        assertEquals(4.0, row2.get("metric").asDouble());
    }

    @Test
    public void testJsonOutputWithQueryTimeoutConfigured() throws Exception {
        setUpStandardProcessorConfig();

        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(QueryCassandra.QUERY_TIMEOUT, "5 sec");
        testRunner.setProperty(QueryCassandra.OUTPUT_FORMAT, QueryCassandra.JSON_FORMAT);
        testRunner.setIncomingConnection(false);
        testRunner.assertValid();
        testRunner.run(1, true, true);

        testRunner.assertAllFlowFilesTransferred(QueryCassandra.REL_SUCCESS, 1);
        List<MockFlowFile> files = testRunner.getFlowFilesForRelationship(QueryCassandra.REL_SUCCESS);
        assertNotNull(files);
        assertEquals(1, files.size());

        String json = files.get(0).getContent();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(json);
        assertTrue(root.has("results"));
        assertTrue(root.get("results").isArray());
    }

    @Test
    public void testProcessorEmptyFlowFile() {
        setUpStandardProcessorConfig();
        testRunner.setValidateExpressionUsage(false);
        testRunner.setIncomingConnection(true);
        processor.setExceptionToThrow(null);
        testRunner.enqueue(new byte[0]);
        testRunner.run(1, true, true);
        testRunner.assertTransferCount(QueryCassandra.REL_SUCCESS, 1);

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(QueryCassandra.REL_SUCCESS).get(0);
        assertTrue(flowFile.getSize() > 0);
        testRunner.clearTransferState();
    }

    @Test
    public void testProcessorEmptyFlowFileMaxRowsPerFlowFileEqOne() {
        processor = new MockQueryCassandraTwoRounds();
        testRunner = TestRunners.newTestRunner(processor);
        testRunner.setValidateExpressionUsage(false);
        setUpStandardProcessorConfig();
        testRunner.setIncomingConnection(true);
        testRunner.setProperty(QueryCassandra.MAX_ROWS_PER_FLOW_FILE, "1");
        processor.setExceptionToThrow(null);
        testRunner.enqueue(new byte[0]);
        testRunner.run(1, true, true);
        testRunner.assertTransferCount(QueryCassandra.REL_SUCCESS, 2);
        testRunner.clearTransferState();
    }

    @Test
    public void testProcessorEmptyFlowFileAndAllNodesFailedException() {
        setUpStandardProcessorConfig();

        AllNodesFailedException allNodesFailed = AllNodesFailedException.fromErrors(Collections.emptyMap());

        ExecutionException executionException = new ExecutionException(allNodesFailed);

        processor.setExceptionToThrow(executionException);
        testRunner.setValidateExpressionUsage(false);
        testRunner.enqueue("".getBytes());
        testRunner.run(1, true, true);
        testRunner.assertTransferCount(QueryCassandra.REL_RETRY, 1);
    }

    @Test
    public void testProcessorEmptyFlowFileAndReadTimeout() {
        setUpStandardProcessorConfig();

        processor.setExceptionToThrow(
                new ExecutionException(new DriverTimeoutException("read timeout"))
        );

        testRunner.setValidateExpressionUsage(false);
        testRunner.enqueue("".getBytes(StandardCharsets.UTF_8));
        testRunner.run(1, true, true);

        // Processor routes DriverTimeoutException to FAILURE, not RETRY
        testRunner.assertTransferCount(QueryCassandra.REL_FAILURE, 1);

        testRunner.clearTransferState();
    }

    @Test
    public void testProcessorEmptyFlowFileAndInvalidQueryException() {
        setUpStandardProcessorConfig();
        processor.setExceptionToThrow(new InvalidQueryException(new SniEndPoint(new InetSocketAddress("localhost", 9042), ""), "invalid query"));
        testRunner.setValidateExpressionUsage(false);
        testRunner.enqueue("".getBytes(StandardCharsets.UTF_8));
        testRunner.run(1, true, true);
        testRunner.assertTransferCount(QueryCassandra.REL_FAILURE, 1);
        testRunner.clearTransferState();
    }

    @Test
    public void testProcessorEmptyFlowFileAndProcessException() {
        setUpStandardProcessorConfig();
        processor.setExceptionToThrow(new ProcessException());
        testRunner.setValidateExpressionUsage(false);
        testRunner.enqueue("".getBytes(StandardCharsets.UTF_8));
        testRunner.run(1, true, true);
        testRunner.assertTransferCount(QueryCassandra.REL_FAILURE, 1);
        testRunner.clearTransferState();
    }

    @Test
    public void testCreateSchemaOneColumn() throws Exception {
        AsyncResultSet resultSet = CassandraQueryTestUtil.createMockAsyncResultSetOneColumn();

        Row row = StreamSupport.stream(resultSet.currentPage().spliterator(), false)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No rows in mock result set"));

        Schema schema = QueryCassandra.createSchema(row);
        assertNotNull(schema);
        assertEquals("NiFi_Cassandra_Query_Record", schema.getName());
        assertEquals(1, schema.getFields().size());
        assertEquals("user_id", schema.getFields().get(0).name());
    }

    @Test
    public void testCreateSchema() throws Exception {
        AsyncResultSet resultSet = CassandraQueryTestUtil.createMockAsyncResultSet(false);

        Row row = StreamSupport.stream(resultSet.currentPage().spliterator(), false)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No rows in mock result set"));

        Schema schema = QueryCassandra.createSchema(row);

        assertNotNull(schema);
        assertEquals(Schema.Type.RECORD, schema.getType());

        Schema.Field field = schema.getField("user_id");
        assertNotNull(field);
        Schema fieldSchema = field.schema();
        assertEquals(Schema.Type.UNION, fieldSchema.getType());
        assertEquals(Schema.Type.NULL, fieldSchema.getTypes().get(0).getType());
        assertEquals(Schema.Type.STRING, fieldSchema.getTypes().get(1).getType());

        field = schema.getField("first_name");
        assertNotNull(field);
        fieldSchema = field.schema();
        assertEquals(Schema.Type.UNION, fieldSchema.getType());
        assertEquals(Schema.Type.NULL, fieldSchema.getTypes().get(0).getType());
        assertEquals(Schema.Type.STRING, fieldSchema.getTypes().get(1).getType());

        field = schema.getField("last_name");
        assertNotNull(field);
        fieldSchema = field.schema();
        assertEquals(Schema.Type.UNION, fieldSchema.getType());
        assertEquals(Schema.Type.NULL, fieldSchema.getTypes().get(0).getType());
        assertEquals(Schema.Type.STRING, fieldSchema.getTypes().get(1).getType());

        field = schema.getField("emails");
        assertNotNull(field);
        fieldSchema = field.schema();
        assertEquals(Schema.Type.UNION, fieldSchema.getType());
        assertEquals(Schema.Type.NULL, fieldSchema.getTypes().get(0).getType());
        assertEquals(Schema.Type.ARRAY, fieldSchema.getTypes().get(1).getType());
        Schema arraySchema = fieldSchema.getTypes().get(1);
        Schema elementSchema = arraySchema.getElementType();
        assertEquals(Schema.Type.UNION, elementSchema.getType());
        assertEquals(Schema.Type.NULL, elementSchema.getTypes().get(0).getType());
        assertEquals(Schema.Type.STRING, elementSchema.getTypes().get(1).getType());

        field = schema.getField("top_places");
        assertNotNull(field);
        fieldSchema = field.schema();
        assertEquals(Schema.Type.UNION, fieldSchema.getType());
        assertEquals(Schema.Type.ARRAY, fieldSchema.getTypes().get(1).getType());
        arraySchema = fieldSchema.getTypes().get(1);
        elementSchema = arraySchema.getElementType();
        assertEquals(Schema.Type.UNION, elementSchema.getType());
        assertEquals(Schema.Type.NULL, elementSchema.getTypes().get(0).getType());
        assertEquals(Schema.Type.STRING, elementSchema.getTypes().get(1).getType());

        field = schema.getField("todo");
        assertNotNull(field);
        fieldSchema = field.schema();
        assertEquals(Schema.Type.UNION, fieldSchema.getType());
        assertEquals(Schema.Type.MAP, fieldSchema.getTypes().get(1).getType());
        Schema mapSchema = fieldSchema.getTypes().get(1);
        Schema valueSchema = mapSchema.getValueType();
        assertEquals(Schema.Type.NULL, valueSchema.getTypes().get(0).getType());
        assertEquals(Schema.Type.STRING, valueSchema.getTypes().get(1).getType());

        field = schema.getField("registered");
        assertNotNull(field);
        fieldSchema = field.schema();
        assertEquals(Schema.Type.UNION, fieldSchema.getType());
        assertEquals(Schema.Type.NULL, fieldSchema.getTypes().get(0).getType());
        assertEquals(Schema.Type.BOOLEAN, fieldSchema.getTypes().get(1).getType());

        field = schema.getField("scale");
        assertNotNull(field);
        fieldSchema = field.schema();
        assertEquals(Schema.Type.UNION, fieldSchema.getType());
        assertEquals(Schema.Type.NULL, fieldSchema.getTypes().get(0).getType());
        assertEquals(Schema.Type.FLOAT, fieldSchema.getTypes().get(1).getType());

        field = schema.getField("metric");
        assertNotNull(field);
        fieldSchema = field.schema();
        assertEquals(Schema.Type.UNION, fieldSchema.getType());
        assertEquals(Schema.Type.NULL, fieldSchema.getTypes().get(0).getType());
        assertEquals(Schema.Type.DOUBLE, fieldSchema.getTypes().get(1).getType());
    }

    @Test
    public void testConvertToAvroStream() throws Exception {
        processor = new MockQueryCassandraTwoRounds();
        testRunner = TestRunners.newTestRunner(processor);
        setUpStandardProcessorConfig();
        AsyncResultSet resultSet = CassandraQueryTestUtil.createMockAsyncResultSet(false);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        long numberOfRows = QueryCassandra.convertToAvroStream(resultSet.currentPage(), 0, baos);

        assertEquals(2, numberOfRows);

        try (DataFileReader<GenericRecord> reader = new DataFileReader<>(
                new SeekableByteArrayInput(baos.toByteArray()),
                new GenericDatumReader<>())) {
            List<GenericRecord> records = new ArrayList<>();
            while (reader.hasNext()) {
                records.add(reader.next());
            }
            assertEquals(2, records.size());
            assertEquals("user1", records.get(0).get("user_id").toString());
            assertEquals("user2", records.get(1).get("user_id").toString());
        }
    }

    @Test
    public void testConvertToJSONStream() throws Exception {
        setUpStandardProcessorConfig();
        AsyncResultSet resultSet = CassandraQueryTestUtil.createMockAsyncResultSet(false);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        long numberOfRows = QueryCassandra.convertToJsonStream(Optional.empty(), resultSet.currentPage(), 0, baos, StandardCharsets.UTF_8);
        assertEquals(2, numberOfRows);
        String jsonOutput = baos.toString(StandardCharsets.UTF_8);
        assertTrue(jsonOutput.contains("\"user_id\":\"user1\""));
        assertTrue(jsonOutput.contains("\"user_id\":\"user2\""));
    }

    @Test
    public void testDefaultDateFormatInConvertToJSONStream() throws Exception {
        Iterable<Row> rows = CassandraQueryTestUtil.createMockDateRows();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        DateFormat df = new SimpleDateFormat(QueryCassandra.TIMESTAMP_FORMAT_PATTERN.getDefaultValue());
        df.setTimeZone(TimeZone.getTimeZone("UTC"));

        long numberOfRows = QueryCassandra.convertToJsonStream(Optional.of(testRunner.getProcessContext()), rows, 0, baos, StandardCharsets.UTF_8);
        assertEquals(1, numberOfRows);
        Map<String, List<Map<String, String>>> map = new ObjectMapper().readValue(baos.toByteArray(), HashMap.class);

        String date = map.get("results").get(0).get("date");
        Instant expected = CassandraQueryTestUtil.TEST_DATE.toInstant();
        assertEquals(expected.toString(), date);
    }

    @Test
    public void testCustomDateFormatInConvertToJSONStream() throws Exception {
        MockProcessContext context = (MockProcessContext) testRunner.getProcessContext();
        Iterable<Row> rows = CassandraQueryTestUtil.createMockDateRows();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final String customDateFormat = "yyyy-MM-dd HH:mm:ss.SSSZ";
        context.setProperty(QueryCassandra.TIMESTAMP_FORMAT_PATTERN, customDateFormat);

        DateFormat df = new SimpleDateFormat(customDateFormat);
        df.setTimeZone(TimeZone.getTimeZone("UTC"));

        long numberOfRows = QueryCassandra.convertToJsonStream(Optional.of(context), rows, 0, baos, StandardCharsets.UTF_8);
        assertEquals(1, numberOfRows);

        Map<String, List<Map<String, String>>> map = new ObjectMapper().readValue(baos.toByteArray(), HashMap.class);

        String date = map.get("results").get(0).get("date");
        Instant expected = CassandraQueryTestUtil.TEST_DATE.toInstant();

        assertEquals(expected.toString(), date);
    }

    private void setUpStandardProcessorConfig() {
        testRunner.setProperty(AbstractCassandraProcessor.CONSISTENCY_LEVEL, "ONE");
        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, "localhost:9042");
        testRunner.setProperty(QueryCassandra.CQL_SELECT_QUERY, "select * from test");
        testRunner.setProperty(AbstractCassandraProcessor.PASSWORD, "password");
        testRunner.setProperty(AbstractCassandraProcessor.USERNAME, "username");
        testRunner.setProperty(QueryCassandra.MAX_ROWS_PER_FLOW_FILE, "0");
    }

    private static class MockQueryCassandra extends QueryCassandra {

        private Exception exceptionToThrow;
        protected AsyncResultSet mockResultSet;

        @Override
        protected void connectToCassandra(ProcessContext context) {
            CqlSession mockSession = mock(CqlSession.class);
            try {
                mockResultSet = CassandraQueryTestUtil.createMockAsyncResultSet(false);

                if (exceptionToThrow != null) {
                    when(mockSession.executeAsync(any(Statement.class)))
                            .thenReturn(CompletableFuture.failedFuture(exceptionToThrow));
                } else {
                    when(mockSession.executeAsync(any(Statement.class)))
                            .thenReturn(CompletableFuture.completedFuture(mockResultSet));
                }
                this.cassandraSession.set(mockSession);
            } catch (Exception e) {
                fail("Failed to setup mock session: " + e.getMessage());
            }
        }

        public void setExceptionToThrow(Exception e) {
            this.exceptionToThrow = e;
        }
    }

    private static class MockQueryCassandraTwoRounds extends MockQueryCassandra {

        private Exception exceptionToThrow;

        @Override
        protected void connectToCassandra(ProcessContext context) {
            CqlSession mockSession = mock(CqlSession.class);
            try {
                AsyncResultSet firstPage = CassandraQueryTestUtil.createMockAsyncResultSet(true);

                if (exceptionToThrow != null) {
                    when(mockSession.executeAsync(any(Statement.class)))
                            .thenReturn(CompletableFuture.failedFuture(exceptionToThrow));
                } else {
                    when(mockSession.executeAsync(any(Statement.class)))
                            .thenReturn(CompletableFuture.completedFuture(firstPage));
                }

                this.cassandraSession.set(mockSession);
            } catch (Exception e) {
                fail("Failed to setup mock session: " + e.getMessage());
            }
        }

        @Override
        public void setExceptionToThrow(Exception e) {
            this.exceptionToThrow = e;
        }
    }
}


