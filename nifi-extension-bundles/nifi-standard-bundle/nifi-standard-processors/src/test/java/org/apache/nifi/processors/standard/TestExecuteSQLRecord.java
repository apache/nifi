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
package org.apache.nifi.processors.standard;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.standard.sql.RecordSqlWriter;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MockCsvRecordWriter;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.db.JdbcCommon.AvroConversionOptions;
import org.apache.nifi.util.db.JdbcProperties;
import org.apache.nifi.util.db.SimpleCommerceDataSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestExecuteSQLRecord extends AbstractDatabaseConnectionServiceTest {

    static final String QUERY_WITHOUT_EL = """
            select
              PER.ID as PersonId, PER.NAME as PersonName,
              PER.CODE as PersonCode,
              PRD.ID as ProductId,
              PRD.NAME as ProductName,
              PRD.CODE as ProductCode,
              REL.ID as RelId,
              REL.NAME as RelName,
              REL.CODE as RelCode,
              ROW_NUMBER() OVER () as rownr
            from
              persons PER,
              products PRD,
              relationships REL
            where
              PER.ID = 10
            """;

    private static final int BATCH_SIZE = 250;

    private static final int FRAGMENT_SIZE = 50;

    private static final int LAST_BATCH_RECORD_INDEX = 49;

    private TestRunner runner;

    @BeforeEach
    public void setup() throws InitializationException {
        runner = newTestRunner(ExecuteSQLRecord.class);
    }

    @AfterEach
    void dropTables() {
        final List<String> tables = List.of(
                "TEST_DROP_TABLE",
                "TEST_TRUNCATE_TABLE",
                "TEST_NULL_INT"
        );

        for (final String table : tables) {
            try (
                    Connection connection = getConnection();
                    Statement statement = connection.createStatement()
            ) {
                statement.execute("DROP TABLE %s".formatted(table));

            } catch (final SQLException ignored) {

            }
        }
    }

    @Test
    public void testIncomingConnectionWithNoFlowFile() throws InitializationException {
        runner.setIncomingConnection(true);
        runner.setProperty(AbstractExecuteSQL.SQL_QUERY, "SELECT * FROM persons");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.run();
        runner.assertTransferCount(AbstractExecuteSQL.REL_SUCCESS, 0);
        runner.assertTransferCount(AbstractExecuteSQL.REL_FAILURE, 0);
    }

    @Test
    public void testIncomingConnectionWithNoFlowFileAndNoQuery() throws InitializationException {
        runner.setIncomingConnection(true);
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.run();
        runner.assertTransferCount(AbstractExecuteSQL.REL_SUCCESS, 0);
        runner.assertTransferCount(AbstractExecuteSQL.REL_FAILURE, 0);
    }

    @Test
    public void testNoIncomingConnectionAndNoQuery() {
        runner.setIncomingConnection(false);
        assertThrows(AssertionError.class, () -> runner.run());
    }

    @Test
    public void testNoIncomingConnection() throws SQLException, InitializationException {
        runner.setIncomingConnection(false);
        invokeOnTriggerRecords(null, QUERY_WITHOUT_EL, false, null, true);
        assertEquals(ProvenanceEventType.RECEIVE, runner.getProvenanceEvents().getFirst().getEventType());
    }

    @Test
    public void testSelectQueryInFlowFile() throws InitializationException, SQLException {
        invokeOnTriggerRecords(null, QUERY_WITHOUT_EL, true, null, false);
        assertEquals(ProvenanceEventType.FORK, runner.getProvenanceEvents().get(0).getEventType());
        assertEquals(ProvenanceEventType.FETCH, runner.getProvenanceEvents().get(1).getEventType());
    }

    @Test
    public void testAutoCommitFalse() throws InitializationException, SQLException {
        runner.setProperty(ExecuteSQL.AUTO_COMMIT, "false");
        invokeOnTriggerRecords(null, QUERY_WITHOUT_EL, true, null, false);
    }

    @Test
    public void testAutoCommitTrue() throws InitializationException, SQLException {
        runner.setProperty(ExecuteSQL.AUTO_COMMIT, "true");
        invokeOnTriggerRecords(null, QUERY_WITHOUT_EL, true, null, false);
    }

    @Test
    public void testFlowFileAttributeResolution() throws InitializationException, SQLException {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        for (int i = 0; i < 2; i++) {
            executeSql("insert into TEST_NULL_INT (id, val1, val2) VALUES (" + i + ", 1, 1)");
        }

        MockRecordWriter recordWriter = MockCsvRecordWriter.builder()
                .withHeader("foo|bar|baz")
                .quoteValues(false)
                .withSeparator(attr -> attr.getOrDefault("csv.separator", ","))
                .build();

        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQLRecord.SQL_QUERY, "SELECT * FROM TEST_NULL_INT");
        runner.enqueue("", Map.of("csv.separator", "|"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQLRecord.REL_SUCCESS, 1);
        MockFlowFile out = runner.getFlowFilesForRelationship(ExecuteSQLRecord.REL_SUCCESS).getFirst();
        out.assertContentEquals("""
                foo|bar|baz
                0|1|1
                1|1|1
                """);
    }

    @Test
    public void testWithOutputBatching() throws InitializationException, SQLException {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");
        insertRecords();

        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);

        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteSQLRecord.MAX_ROWS_PER_FLOW_FILE, "5");
        runner.setProperty(ExecuteSQLRecord.OUTPUT_BATCH_SIZE, "5");
        runner.setProperty(ExecuteSQLRecord.SQL_QUERY, "SELECT * FROM TEST_NULL_INT");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQLRecord.REL_SUCCESS, FRAGMENT_SIZE);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQLRecord.REL_SUCCESS, FragmentAttributes.FRAGMENT_INDEX.key());
        runner.assertAllFlowFilesContainAttribute(ExecuteSQLRecord.REL_SUCCESS, FragmentAttributes.FRAGMENT_ID.key());

        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQLRecord.REL_SUCCESS).getFirst();

        firstFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULT_ROW_COUNT, "5");
        firstFlowFile.assertAttributeNotExists(FragmentAttributes.FRAGMENT_COUNT.key());
        firstFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "0");
        firstFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULTSET_INDEX, "0");

        MockFlowFile lastFlowFile = runner.getFlowFilesForRelationship(ExecuteSQLRecord.REL_SUCCESS).get(LAST_BATCH_RECORD_INDEX);

        lastFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULT_ROW_COUNT, "5");
        lastFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), Integer.toString(LAST_BATCH_RECORD_INDEX));
        lastFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULTSET_INDEX, "0");
    }

    @Test
    public void testWithOutputBatchingAndIncomingFlowFile() throws InitializationException, SQLException {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");
        insertRecords();

        Map<String, String> attrMap = new HashMap<>();
        String testAttrName = "attr1";
        String testAttrValue = "value1";
        attrMap.put(testAttrName, testAttrValue);

        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQLRecord.MAX_ROWS_PER_FLOW_FILE, "5");
        runner.setProperty(ExecuteSQLRecord.OUTPUT_BATCH_SIZE, "1");
        MockFlowFile inputFlowFile = runner.enqueue("SELECT * FROM TEST_NULL_INT", attrMap);
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQLRecord.REL_SUCCESS, FRAGMENT_SIZE);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQLRecord.REL_SUCCESS, FragmentAttributes.FRAGMENT_INDEX.key());
        runner.assertAllFlowFilesContainAttribute(ExecuteSQLRecord.REL_SUCCESS, FragmentAttributes.FRAGMENT_ID.key());

        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQLRecord.REL_SUCCESS).getFirst();

        firstFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULT_ROW_COUNT, "5");
        firstFlowFile.assertAttributeNotExists(FragmentAttributes.FRAGMENT_COUNT.key());
        firstFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "0");
        firstFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULTSET_INDEX, "0");

        MockFlowFile lastFlowFile = runner.getFlowFilesForRelationship(ExecuteSQLRecord.REL_SUCCESS).get(LAST_BATCH_RECORD_INDEX);

        lastFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULT_ROW_COUNT, "5");
        lastFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), Integer.toString(LAST_BATCH_RECORD_INDEX));
        lastFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULTSET_INDEX, "0");
        lastFlowFile.assertAttributeEquals(testAttrName, testAttrValue);
        lastFlowFile.assertAttributeEquals(AbstractExecuteSQL.INPUT_FLOWFILE_UUID, inputFlowFile.getAttribute(CoreAttributes.UUID.key()));
    }

    @Test
    public void testWithOutputBatchingLastBatchFails() throws InitializationException, SQLException {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 varchar(50), constraint my_pk primary key (id))");

        // Insert some valid numeric values (for TO_NUMBER call later)
        for (int i = 0; i < 11; i++) {
            executeSql("insert into TEST_NULL_INT (id, val1) VALUES (" + i + ", '" + i + "')");
        }
        // Insert invalid numeric value
        executeSql("insert into TEST_NULL_INT (id, val1) VALUES (100, 'abc')");

        Map<String, String> attrMap = new HashMap<>();
        String testAttrName = "attr1";
        String testAttrValue = "value1";
        attrMap.put(testAttrName, testAttrValue);

        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQLRecord.MAX_ROWS_PER_FLOW_FILE, "5");
        runner.enqueue("SELECT ID, CAST(VAL1 AS INTEGER) AS TN FROM TEST_NULL_INT", attrMap);
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQLRecord.REL_FAILURE, 1);
        runner.assertTransferCount(ExecuteSQLRecord.REL_SUCCESS, 0);
    }

    @Test
    public void testMaxRowsPerFlowFile() throws Exception {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");
        insertRecords();

        runner.setIncomingConnection(false);
        runner.setProperty(AbstractExecuteSQL.MAX_ROWS_PER_FLOW_FILE, "5");
        runner.setProperty(AbstractExecuteSQL.OUTPUT_BATCH_SIZE, "0");
        runner.setProperty(AbstractExecuteSQL.SQL_QUERY, "SELECT * FROM TEST_NULL_INT");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExecuteSQL.REL_SUCCESS, FRAGMENT_SIZE);
        runner.assertTransferCount(AbstractExecuteSQL.REL_FAILURE, 0);
        runner.assertAllFlowFilesContainAttribute(AbstractExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_INDEX.key());
        runner.assertAllFlowFilesContainAttribute(AbstractExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_ID.key());
        runner.assertAllFlowFilesContainAttribute(AbstractExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_COUNT.key());

        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(AbstractExecuteSQL.REL_SUCCESS).getFirst();

        firstFlowFile.assertAttributeEquals(AbstractExecuteSQL.RESULT_ROW_COUNT, "5");
        firstFlowFile.assertAttributeEquals("record.count", "5");
        firstFlowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain"); // MockRecordWriter has text/plain MIME type
        firstFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "0");
        firstFlowFile.assertAttributeEquals(AbstractExecuteSQL.RESULTSET_INDEX, "0");

        MockFlowFile lastFlowFile = runner.getFlowFilesForRelationship(AbstractExecuteSQL.REL_SUCCESS).get(LAST_BATCH_RECORD_INDEX);

        lastFlowFile.assertAttributeEquals(AbstractExecuteSQL.RESULT_ROW_COUNT, "5");
        lastFlowFile.assertAttributeEquals("record.count", "5");
        lastFlowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain"); // MockRecordWriter has text/plain MIME type
        lastFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), Integer.toString(LAST_BATCH_RECORD_INDEX));
        lastFlowFile.assertAttributeEquals(AbstractExecuteSQL.RESULTSET_INDEX, "0");
    }

    @Test
    public void testInsertStatementCreatesFlowFile() throws Exception {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        runner.setIncomingConnection(false);
        runner.setProperty(AbstractExecuteSQL.SQL_QUERY, "insert into TEST_NULL_INT (id, val1, val2) VALUES (0, NULL, 1)");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExecuteSQL.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(AbstractExecuteSQL.REL_SUCCESS).getFirst().assertAttributeEquals(AbstractExecuteSQL.RESULT_ROW_COUNT, "0");
    }

    @Test
    public void testNoRowsStatementCreatesEmptyFlowFile() throws Exception {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQLRecord.SQL_QUERY, "select * from TEST_NULL_INT");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.enqueue("Hello".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQLRecord.REL_SUCCESS, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQLRecord.REL_SUCCESS).getFirst();
        firstFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULT_ROW_COUNT, "0");
        firstFlowFile.assertContentEquals("");
    }

    @Test
    public void testNoResultCreatesEmptyFlowFile() throws Exception {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQLRecord.SQL_QUERY, "drop table TEST_NULL_INT");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.enqueue("Hello".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQLRecord.REL_SUCCESS, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQLRecord.REL_SUCCESS).getFirst();
        firstFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULT_ROW_COUNT, "0");
        firstFlowFile.assertContentEquals("");
    }

    @Test
    public void testWithSqlException() throws Exception {
        executeSql("create table TEST_NO_ROWS (id integer)");

        runner.setIncomingConnection(false);
        // Try a valid SQL statement that will generate an error (val1 does not exist, e.g.)
        runner.setProperty(AbstractExecuteSQL.SQL_QUERY, "SELECT val1 FROM TEST_NO_ROWS");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.run();

        //No incoming flow file containing a query, and an exception causes no outbound flowfile.
        // There should be no flow files on either relationship
        runner.assertAllFlowFilesTransferred(AbstractExecuteSQL.REL_FAILURE, 0);
        runner.assertAllFlowFilesTransferred(AbstractExecuteSQL.REL_SUCCESS, 0);
    }

    @Test
    public void testStreamingExceptionWhileOutputBatchingRoutesOriginalFlowFileToFailure() throws Exception {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");
        for (int i = 0; i < 5; i++) {
            executeSql("insert into TEST_NULL_INT (id, val1, val2) VALUES (" + i + ", 1, 1)");
        }

        runner.setIncomingConnection(true);
        runner.setProperty(AbstractExecuteSQL.SQL_QUERY, "SELECT * FROM TEST_NULL_INT");
        runner.setProperty(AbstractExecuteSQL.MAX_ROWS_PER_FLOW_FILE, "1");
        runner.setProperty(AbstractExecuteSQL.OUTPUT_BATCH_SIZE, "1");

        FailAfterNRecordSetWriterFactory recordWriter = new FailAfterNRecordSetWriterFactory(2);
        runner.addControllerService("fail-writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "fail-writer");
        runner.enableControllerService(recordWriter);

        runner.enqueue("SELECT * FROM TEST_NULL_INT", Map.of("retain.attr", "retain-value"));
        runner.run();

        runner.assertTransferCount(AbstractExecuteSQL.REL_SUCCESS, 2);
        runner.assertTransferCount(AbstractExecuteSQL.REL_FAILURE, 1);

        MockFlowFile failedFlowFile = runner.getFlowFilesForRelationship(AbstractExecuteSQL.REL_FAILURE).getFirst();
        final String errorMessage = failedFlowFile.getAttribute(AbstractExecuteSQL.RESULT_ERROR_MESSAGE);
        assertTrue(errorMessage.endsWith("Simulated failure after 2 successful writes"), () -> "Unexpected error message: " + errorMessage);
        failedFlowFile.assertAttributeEquals("retain.attr", "retain-value");
    }

    public void invokeOnTriggerRecords(final Integer queryTimeout, final String query, final boolean incomingFlowFile, final Map<String, String> attrs, final boolean setQueryProperty)
            throws InitializationException, SQLException {

        if (queryTimeout != null) {
            runner.setProperty(AbstractExecuteSQL.QUERY_TIMEOUT, queryTimeout + " secs");
        }

        // load test data to database
        try (Connection con = getConnection()) {
            SimpleCommerceDataSet.loadTestData2Database(con, 100, 200, 100);
        }

        // ResultSet size will be 1x200x100 = 20 000 rows
        // because of where PER.ID = ${person.id}

        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);

        if (incomingFlowFile) {
            // incoming FlowFile content is not used, but attributes are used
            final Map<String, String> attributes = (attrs == null) ? new HashMap<>() : attrs;
            attributes.put("person.id", "10");
            if (!setQueryProperty) {
                runner.enqueue(query.getBytes(), attributes);
            } else {
                runner.enqueue("Hello".getBytes(), attributes);
            }
        }

        if (setQueryProperty) {
            runner.setProperty(AbstractExecuteSQL.SQL_QUERY, query);
        }

        runner.run();
        runner.assertAllFlowFilesTransferred(AbstractExecuteSQL.REL_SUCCESS, 1);
        runner.assertAllFlowFilesContainAttribute(AbstractExecuteSQL.REL_SUCCESS, AbstractExecuteSQL.RESULT_QUERY_DURATION);
        runner.assertAllFlowFilesContainAttribute(AbstractExecuteSQL.REL_SUCCESS, AbstractExecuteSQL.RESULT_QUERY_EXECUTION_TIME);
        runner.assertAllFlowFilesContainAttribute(AbstractExecuteSQL.REL_SUCCESS, AbstractExecuteSQL.RESULT_QUERY_FETCH_TIME);
        runner.assertAllFlowFilesContainAttribute(AbstractExecuteSQL.REL_SUCCESS, AbstractExecuteSQL.RESULT_ROW_COUNT);

        final List<MockFlowFile> flowfiles = runner.getFlowFilesForRelationship(AbstractExecuteSQL.REL_SUCCESS);
        final long executionTime = Long.parseLong(flowfiles.getFirst().getAttribute(AbstractExecuteSQL.RESULT_QUERY_EXECUTION_TIME));
        final long fetchTime = Long.parseLong(flowfiles.getFirst().getAttribute(AbstractExecuteSQL.RESULT_QUERY_FETCH_TIME));
        final long durationTime = Long.parseLong(flowfiles.getFirst().getAttribute(AbstractExecuteSQL.RESULT_QUERY_DURATION));
        assertEquals(durationTime, fetchTime + executionTime);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWithSqlExceptionErrorProcessingResultSet() throws Exception {
        DBCPService dbcp = mock(DBCPService.class);
        Connection conn = mock(Connection.class);
        when(dbcp.getConnection(any(Map.class))).thenReturn(conn);
        when(dbcp.getIdentifier()).thenReturn("mockdbcp");
        PreparedStatement statement = mock(PreparedStatement.class);
        when(conn.prepareStatement(anyString())).thenReturn(statement);
        when(statement.execute()).thenReturn(true);
        ResultSet rs = mock(ResultSet.class);
        when(statement.getResultSet()).thenReturn(rs);
        // Throw an exception the first time you access the ResultSet, this is after the flow file to hold the results has been created.
        when(rs.getMetaData()).thenThrow(new SQLException("test execute statement failed"));

        runner.addControllerService("mockdbcp", dbcp, new HashMap<>());
        runner.enableControllerService(dbcp);
        runner.setProperty(AbstractExecuteSQL.DBCP_SERVICE, "mockdbcp");

        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);

        runner.setIncomingConnection(true);
        runner.enqueue("SELECT 1");
        runner.run();

        runner.assertTransferCount(AbstractExecuteSQL.REL_FAILURE, 1);
        runner.assertTransferCount(AbstractExecuteSQL.REL_SUCCESS, 0);

        // Assert exception message has been put to flow file attribute
        MockFlowFile failedFlowFile = runner.getFlowFilesForRelationship(AbstractExecuteSQL.REL_FAILURE).getFirst();
        assertEquals("java.sql.SQLException: test execute statement failed", failedFlowFile.getAttribute(AbstractExecuteSQL.RESULT_ERROR_MESSAGE));
    }

    @Test
    public void testPreQuery() throws Exception {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");
        executeSql("insert into TEST_NULL_INT values(1,2,3)");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQLRecord.SQL_PRE_QUERY, "VALUES (1)");
        runner.setProperty(ExecuteSQLRecord.SQL_QUERY, "select * from TEST_NULL_INT");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.enqueue("test".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQLRecord.REL_SUCCESS, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQLRecord.REL_SUCCESS).getFirst();
        firstFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULT_ROW_COUNT, "1");
    }

    @Test
    public void testPostQuery() throws Exception {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");
        executeSql("insert into TEST_NULL_INT values(1,2,3)");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQLRecord.SQL_PRE_QUERY, "VALUES (1)");
        runner.setProperty(ExecuteSQLRecord.SQL_QUERY, "select * from TEST_NULL_INT");
        runner.setProperty(ExecuteSQLRecord.SQL_POST_QUERY, "VALUES (2)");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.enqueue("test".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQLRecord.REL_SUCCESS, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQLRecord.REL_SUCCESS).getFirst();
        firstFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULT_ROW_COUNT, "1");
    }

    @Test
    public void testPreQueryFail() throws Exception {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        runner.setIncomingConnection(true);
        // Simulate failure by not provide parameter
        runner.setProperty(ExecuteSQLRecord.SQL_PRE_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS()");
        runner.setProperty(ExecuteSQLRecord.SQL_QUERY, "select * from TEST_NULL_INT");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.enqueue("test".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQLRecord.REL_FAILURE, 1);
    }

    @Test
    public void testPostQueryFail() throws Exception {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQLRecord.SQL_PRE_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)");
        runner.setProperty(ExecuteSQLRecord.SQL_QUERY, "select * from TEST_NULL_INT");
        // Simulate failure by not provide parameter
        runner.setProperty(ExecuteSQLRecord.SQL_POST_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS()");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.enqueue("test".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQLRecord.REL_FAILURE, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQLRecord.REL_FAILURE).getFirst();
        firstFlowFile.assertContentEquals("test");
    }

    @Test
    public void testArrayOfStringsInference() throws Exception {
        final ResultSetMetaData meta = mock(ResultSetMetaData.class);
        when(meta.getColumnCount()).thenReturn(1);
        when(meta.getColumnLabel(1)).thenReturn("test");
        when(meta.getColumnName(1)).thenReturn("test");
        when(meta.getColumnType(1)).thenReturn(Types.ARRAY);
        when(meta.getTableName(1)).thenReturn("");

        final ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(meta);
        when(rs.next()).thenReturn(true, false);

        final Array array = mock(Array.class);
        when(array.getArray()).thenReturn(new String[] {"test"});
        when(rs.getArray(1)).thenReturn(array);
        when(rs.getObject(1)).thenReturn(array);
        when(rs.getObject("test")).thenReturn(array);

        final TestRunner localRunner = TestRunners.newTestRunner(ExecuteSQLRecord.class);
        final RecordSetWriterFactory writerFactory = new JsonRecordSetWriter();
        localRunner.addControllerService("writer", writerFactory);
        localRunner.enableControllerService(writerFactory);

        final RecordSqlWriter sqlWriter = new RecordSqlWriter(writerFactory, AvroConversionOptions.builder().useLogicalTypes(false).build(), 0, Map.of());
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ComponentLog log = new MockComponentLog("test", sqlWriter);
        sqlWriter.writeResultSet(rs, out, log, null);
        final String json = out.toString();
        assertTrue(json.contains("\"test\":[\"test\"]"), "Expected JSON to contain array of strings: " + json);
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("sql-pre-query", AbstractExecuteSQL.SQL_PRE_QUERY.getName()),
                Map.entry("SQL select query", AbstractExecuteSQL.SQL_QUERY.getName()),
                Map.entry("sql-post-query", AbstractExecuteSQL.SQL_POST_QUERY.getName()),
                Map.entry("esql-max-rows", AbstractExecuteSQL.MAX_ROWS_PER_FLOW_FILE.getName()),
                Map.entry("Max Rows Per Flow File", AbstractExecuteSQL.MAX_ROWS_PER_FLOW_FILE.getName()),
                Map.entry("esql-output-batch-size", AbstractExecuteSQL.OUTPUT_BATCH_SIZE.getName()),
                Map.entry("esql-fetch-size", AbstractExecuteSQL.FETCH_SIZE.getName()),
                Map.entry("esql-auto-commit", AbstractExecuteSQL.AUTO_COMMIT.getName()),
                Map.entry("esqlrecord-record-writer", ExecuteSQLRecord.RECORD_WRITER_FACTORY.getName()),
                Map.entry("esqlrecord-normalize", ExecuteSQLRecord.NORMALIZE_NAMES.getName()),
                Map.entry(JdbcProperties.OLD_USE_AVRO_LOGICAL_TYPES_PROPERTY_NAME, JdbcProperties.USE_AVRO_LOGICAL_TYPES.getName()),
                Map.entry(JdbcProperties.OLD_DEFAULT_PRECISION_PROPERTY_NAME, JdbcProperties.DEFAULT_PRECISION.getName()),
                Map.entry(JdbcProperties.OLD_DEFAULT_SCALE_PROPERTY_NAME, JdbcProperties.DEFAULT_SCALE.getName())
        );

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
    }

    private void insertRecords() throws SQLException {
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement()
        ) {
            for (int i = 0; i < BATCH_SIZE; i++) {
                statement.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (%d, 1, 1)".formatted(i));
            }
        }
    }

    private static final class FailAfterNRecordSetWriterFactory extends AbstractControllerService implements RecordSetWriterFactory {
        private final AtomicInteger successfulWrites = new AtomicInteger(0);
        private final int failAfter;

        FailAfterNRecordSetWriterFactory(final int failAfter) {
            this.failAfter = failAfter;
        }

        @Override
        public RecordSchema getSchema(final Map<String, String> variables, final RecordSchema readSchema) {
            return readSchema;
        }

        @Override
        public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out, final Map<String, String> variables) {
            return new RecordSetWriter() {
                private long currentRecordCount;

                @Override
                public WriteResult write(final RecordSet recordSet) throws IOException {
                    if (successfulWrites.get() >= failAfter) {
                        throw new IOException("Simulated failure after " + failAfter + " successful writes");
                    }

                    long count = 0;
                    while (recordSet.next() != null) {
                        count++;
                    }
                    currentRecordCount = count;
                    successfulWrites.incrementAndGet();
                    return WriteResult.of(Math.toIntExact(count), Collections.emptyMap());
                }

                @Override
                public WriteResult write(final Record record) {
                    throw new UnsupportedOperationException("Record-by-record writes not supported");
                }

                @Override
                public String getMimeType() {
                    return "text/plain";
                }

                @Override
                public void flush() {
                }

                @Override
                public void close() {
                }

                @Override
                public void beginRecordSet() {
                    currentRecordCount = 0;
                }

                @Override
                public WriteResult finishRecordSet() {
                    return WriteResult.of(Math.toIntExact(currentRecordCount), Collections.emptyMap());
                }
            };
        }
    }
}
