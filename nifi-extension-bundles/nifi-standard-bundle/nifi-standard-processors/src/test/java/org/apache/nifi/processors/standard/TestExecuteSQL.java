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

import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.db.AvroUtil;
import org.apache.nifi.util.db.JdbcProperties;
import org.apache.nifi.util.db.SimpleCommerceDataSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.util.db.JdbcProperties.DEFAULT_PRECISION;
import static org.apache.nifi.util.db.JdbcProperties.DEFAULT_SCALE;
import static org.apache.nifi.util.db.JdbcProperties.NORMALIZE_NAMES_FOR_AVRO;
import static org.apache.nifi.util.db.JdbcProperties.USE_AVRO_LOGICAL_TYPES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestExecuteSQL extends AbstractDatabaseConnectionServiceTest {

    static final String QUERY_WITH_EL = "select "
        + "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
        + ", PRD.ID as ProductId,PRD.NAME as ProductName,PRD.CODE as ProductCode"
        + ", REL.ID as RelId,    REL.NAME as RelName,    REL.CODE as RelCode"
        + ", ROW_NUMBER() OVER () as rownr "
        + " from persons PER, products PRD, relationships REL"
        + " where PER.ID = ${person.id}";

    static final String QUERY_WITHOUT_EL = "select "
        + "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
        + ", PRD.ID as ProductId,PRD.NAME as ProductName,PRD.CODE as ProductCode"
        + ", REL.ID as RelId,    REL.NAME as RelName,    REL.CODE as RelCode"
        + ", ROW_NUMBER() OVER () as rownr "
        + " from persons PER, products PRD, relationships REL"
        + " where PER.ID = 10";

    static final String QUERY_WITHOUT_EL_WITH_PARAMS = "select "
            + "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
            + ", PRD.ID as ProductId,PRD.NAME as ProductName,PRD.CODE as ProductCode"
            + ", REL.ID as RelId,    REL.NAME as RelName,    REL.CODE as RelCode"
            + ", ROW_NUMBER() OVER () as rownr "
            + " from persons PER, products PRD, relationships REL"
            + " where PER.ID < ? AND REL.ID < ?";

    private static final int BATCH_SIZE = 250;

    private static final int FRAGMENT_SIZE = 50;

    private static final int LAST_BATCH_RECORD_INDEX = 49;

    TestRunner runner;

    @BeforeEach
    void setup() throws InitializationException {
        runner = newTestRunner(ExecuteSQL.class);
    }

    @AfterEach
    void dropTables() {
        final List<String> tables = List.of(
                "TEST_DROP_TABLE",
                "TEST_TRUNCATE_TABLE",
                "TEST_NULL_INT",
                "host1",
                "host2"
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
    public void testIncomingConnectionWithNoFlowFile() {
        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQL.SQL_QUERY, "SELECT * FROM persons");
        runner.run();
        runner.assertTransferCount(ExecuteSQL.REL_SUCCESS, 0);
        runner.assertTransferCount(ExecuteSQL.REL_FAILURE, 0);
    }

    @Test
    public void testIncomingConnectionWithNoFlowFileAndNoQuery() {
        runner.setIncomingConnection(true);
        runner.run();
        runner.assertTransferCount(ExecuteSQL.REL_SUCCESS, 0);
        runner.assertTransferCount(ExecuteSQL.REL_FAILURE, 0);
    }

    @Test
    public void testNoIncomingConnectionAndNoQuery() {
        runner.setIncomingConnection(false);
        assertThrows(AssertionError.class, () -> runner.run());
    }

    @Test
    public void testNoIncomingConnection() throws SQLException, IOException {
        runner.setIncomingConnection(false);
        invokeOnTrigger(null, QUERY_WITHOUT_EL, false, null, true);
    }

    @Test
    public void testNoTimeLimit() throws SQLException, IOException {
        invokeOnTrigger(null, QUERY_WITH_EL, true, null, true);
    }

    @Test
    public void testSelectQueryInFlowFile() throws SQLException, IOException {
        invokeOnTrigger(null, QUERY_WITHOUT_EL, true, null, false);
    }

    @Test
    public void testSelectQueryInFlowFileWithParameters() throws SQLException, IOException {
        Map<String, String> sqlParams = new HashMap<>();
        sqlParams.put("sql.args.1.type", "4");
        sqlParams.put("sql.args.1.value", "20");
        sqlParams.put("sql.args.2.type", "4");
        sqlParams.put("sql.args.2.value", "5");

        invokeOnTrigger(null, QUERY_WITHOUT_EL_WITH_PARAMS, true, sqlParams, false);
    }

    @Test
    public void testQueryTimeout() throws SQLException, IOException {
        // Does to seem to have any effect when using embedded Derby
        invokeOnTrigger(1, QUERY_WITH_EL, true, null, true); // 1 second max time
    }

    @Test
    public void testAutoCommitFalse() throws SQLException, IOException {
        runner.setProperty(ExecuteSQL.AUTO_COMMIT, "false");
        invokeOnTrigger(null, QUERY_WITHOUT_EL, true, null, false);
    }

    @Test
    public void testAutoCommitTrue() throws SQLException, IOException {
        runner.setProperty(ExecuteSQL.AUTO_COMMIT, "true");
        invokeOnTrigger(null, QUERY_WITHOUT_EL, true, null, false);
    }

    @Test
    public void testWithNullIntColumn() throws SQLException {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");
        executeSql("insert into TEST_NULL_INT (id, val1, val2) VALUES (0, NULL, 1)");
        executeSql("insert into TEST_NULL_INT (id, val1, val2) VALUES (1, 1, 1)");

        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteSQL.SQL_QUERY, "SELECT * FROM TEST_NULL_INT");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).getFirst().assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "2");
        runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).getFirst().assertAttributeEquals(ExecuteSQL.RESULTSET_INDEX, "0");
    }

    @Test
    public void testDropTableWithOverwrite() throws SQLException, IOException {
        executeSql("create table TEST_DROP_TABLE (id integer not null, val1 integer, val2 integer)");
        executeSql("insert into TEST_DROP_TABLE (id, val1, val2) VALUES (0, NULL, 1)");
        executeSql("insert into TEST_DROP_TABLE (id, val1, val2) VALUES (1, 1, 1)");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQL.SQL_QUERY, "DROP TABLE TEST_DROP_TABLE");
        runner.enqueue("some data");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);

        final List<MockFlowFile> flowfiles = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS);
        final InputStream in = new ByteArrayInputStream(flowfiles.getFirst().toByteArray());
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(in, datumReader)) {
            assertFalse(dataFileReader.hasNext());
        }
    }

    @Test
    public void testDropTableNoOverwrite() throws SQLException {
        executeSql("create table TEST_TRUNCATE_TABLE (id integer not null, val1 integer, val2 integer)");
        executeSql("insert into TEST_TRUNCATE_TABLE (id, val1, val2) VALUES (0, NULL, 1)");
        executeSql("insert into TEST_TRUNCATE_TABLE (id, val1, val2) VALUES (1, 1, 1)");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQL.CONTENT_OUTPUT_STRATEGY, AbstractExecuteSQL.ContentOutputStrategy.ORIGINAL);
        runner.setProperty(ExecuteSQL.SQL_QUERY, "TRUNCATE TABLE TEST_TRUNCATE_TABLE");
        runner.enqueue("some data");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        runner.assertContents(ExecuteSQL.REL_SUCCESS, List.of("some data"));
    }

    @Test
    public void testCompression() throws SQLException, IOException {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");
        executeSql("insert into TEST_NULL_INT (id, val1, val2) VALUES (0, NULL, 1)");
        executeSql("insert into TEST_NULL_INT (id, val1, val2) VALUES (1, 1, 1)");

        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteSQL.COMPRESSION_FORMAT, AvroUtil.CodecType.BZIP2.name());
        runner.setProperty(ExecuteSQL.SQL_QUERY, "SELECT * FROM TEST_NULL_INT");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).getFirst();

        try (DataFileStream<GenericRecord> dfs = new DataFileStream<>(new ByteArrayInputStream(flowFile.toByteArray()), new GenericDatumReader<>())) {
            assertEquals(AvroUtil.CodecType.BZIP2.name().toLowerCase(), dfs.getMetaString(DataFileConstants.CODEC).toLowerCase());
        }
    }

    @Test
    public void testWithOutputBatching() throws SQLException {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");
        insertRecords();

        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteSQL.MAX_ROWS_PER_FLOW_FILE, "5");
        runner.setProperty(ExecuteSQL.OUTPUT_BATCH_SIZE, "5");
        runner.setProperty(ExecuteSQL.SQL_QUERY, "SELECT * FROM TEST_NULL_INT");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, FRAGMENT_SIZE);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_INDEX.key());
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_ID.key());

        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).getFirst();

        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "5");
        firstFlowFile.assertAttributeNotExists(FragmentAttributes.FRAGMENT_COUNT.key());
        firstFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "0");
        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULTSET_INDEX, "0");

        MockFlowFile lastFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).get(LAST_BATCH_RECORD_INDEX);

        lastFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "5");
        lastFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), Integer.toString(LAST_BATCH_RECORD_INDEX));
        lastFlowFile.assertAttributeEquals(ExecuteSQL.RESULTSET_INDEX, "0");
    }

    @Test
    public void testWithOutputBatchingAndIncomingFlowFile() throws SQLException {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");
        insertRecords();

        Map<String, String> attrMap = new HashMap<>();
        String testAttrName = "attr1";
        String testAttrValue = "value1";
        attrMap.put(testAttrName, testAttrValue);
        attrMap.put("max.rows", "5");
        attrMap.put("batch.size", "1");
        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQL.MAX_ROWS_PER_FLOW_FILE, "${max.rows}");
        runner.setProperty(ExecuteSQL.OUTPUT_BATCH_SIZE, "${batch.size}");
        MockFlowFile inputFlowFile = runner.enqueue("SELECT * FROM TEST_NULL_INT", attrMap);
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, FRAGMENT_SIZE);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_INDEX.key());
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_ID.key());

        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).getFirst();

        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "5");
        firstFlowFile.assertAttributeNotExists(FragmentAttributes.FRAGMENT_COUNT.key());
        firstFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "0");
        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULTSET_INDEX, "0");

        MockFlowFile lastFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).get(LAST_BATCH_RECORD_INDEX);

        lastFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "5");
        lastFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), Integer.toString(LAST_BATCH_RECORD_INDEX));
        lastFlowFile.assertAttributeEquals(testAttrName, testAttrValue);
        lastFlowFile.assertAttributeEquals(AbstractExecuteSQL.INPUT_FLOWFILE_UUID, inputFlowFile.getAttribute(CoreAttributes.UUID.key()));
    }

    @Test
    public void testMaxRowsPerFlowFile() throws SQLException {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");
        insertRecords();

        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteSQL.MAX_ROWS_PER_FLOW_FILE, "5");
        runner.setProperty(AbstractExecuteSQL.FETCH_SIZE, "5");
        runner.setProperty(ExecuteSQL.OUTPUT_BATCH_SIZE, "0");
        runner.setProperty(ExecuteSQL.SQL_QUERY, "SELECT * FROM TEST_NULL_INT");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, FRAGMENT_SIZE);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_INDEX.key());
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_ID.key());
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_COUNT.key());

        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).getFirst();

        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "5");
        firstFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "0");
        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULTSET_INDEX, "0");

        MockFlowFile lastFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).get(LAST_BATCH_RECORD_INDEX);

        lastFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "5");
        lastFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), Integer.toString(LAST_BATCH_RECORD_INDEX));
        lastFlowFile.assertAttributeEquals(ExecuteSQL.RESULTSET_INDEX, "0");
    }

    @Test
    public void testInsertStatementCreatesFlowFile() throws SQLException {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteSQL.SQL_QUERY, "insert into TEST_NULL_INT (id, val1, val2) VALUES (0, NULL, 1)");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).getFirst().assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "0");
    }

    @Test
    public void testNoRowsStatementCreatesEmptyFlowFile() throws Exception {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQL.SQL_QUERY, "select * from TEST_NULL_INT");
        runner.enqueue("Hello".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).getFirst();
        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "0");
        final InputStream in = new ByteArrayInputStream(firstFlowFile.toByteArray());
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(in, datumReader)) {
            GenericRecord record = null;
            long recordsFromStream = 0;
            while (dataFileReader.hasNext()) {
                // Reuse record object by passing it to next(). This saves us from
                // allocating and garbage collecting many objects for files with
                // many items.
                record = dataFileReader.next(record);
                recordsFromStream += 1;
            }

            assertEquals(0, recordsFromStream);
        }
    }

    @Test
    public void testWithDuplicateColumns() throws SQLException {
        executeSql("create table host1 (id integer not null, host varchar(45))");
        executeSql("create table host2 (id integer not null, host varchar(45))");
        executeSql("insert into host1 values(1,'host1')");
        executeSql("insert into host2 values(1,'host2')");
        executeSql("select a.host as hostA,b.host as hostB from host1 a join host2 b on b.id=a.id");
        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteSQL.SQL_QUERY, "select a.host as hostA,b.host as hostB from host1 a join host2 b on b.id=a.id");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).getFirst().assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "1");
    }

    @Test
    public void testWithSqlException() throws SQLException {
        executeSql("create table TEST_NO_ROWS (id integer)");

        runner.setIncomingConnection(false);
        // Try a valid SQL statement that will generate an error (val1 does not exist, e.g.)
        runner.setProperty(ExecuteSQL.SQL_QUERY, "SELECT val1 FROM TEST_NO_ROWS");
        runner.run();

        //No incoming flow file containing a query, and an exception causes no outbound flowfile.
        // There should be no flow files on either relationship
        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_FAILURE, 0);
        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 0);
    }

    @Test
    @SuppressWarnings("unchecked")
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
        runner.setProperty(ExecuteSQL.DBCP_SERVICE, "mockdbcp");

        runner.setIncomingConnection(true);
        runner.enqueue("SELECT 1");
        runner.run();

        runner.assertTransferCount(ExecuteSQL.REL_FAILURE, 1);
        runner.assertTransferCount(ExecuteSQL.REL_SUCCESS, 0);

        // Assert exception message has been put to flow file attribute
        MockFlowFile failedFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_FAILURE).getFirst();
        assertEquals("java.sql.SQLException: test execute statement failed", failedFlowFile.getAttribute(ExecuteSQL.RESULT_ERROR_MESSAGE));
    }

    public void invokeOnTrigger(final Integer queryTimeout, final String query, final boolean incomingFlowFile, final Map<String, String> attrs, final boolean setQueryProperty)
        throws SQLException, IOException {

        if (queryTimeout != null) {
            runner.setProperty(ExecuteSQL.QUERY_TIMEOUT, queryTimeout + " secs");
        }

        // load test data to database
        final Connection con = getConnection();
        SimpleCommerceDataSet.loadTestData2Database(con, 100, 200, 100);

        //commit loaded data if auto-commit is disabled
        if (!con.getAutoCommit()) {
            con.commit();
        }

        // ResultSet size will be 1x200x100 = 20 000 rows
        // because of where PER.ID = ${person.id}
        final int nrOfRows = 20000;

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
            runner.setProperty(ExecuteSQL.SQL_QUERY, query);
        }

        runner.run();
        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, ExecuteSQL.RESULT_QUERY_DURATION);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, ExecuteSQL.RESULT_QUERY_EXECUTION_TIME);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, ExecuteSQL.RESULT_QUERY_FETCH_TIME);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, ExecuteSQL.RESULT_ROW_COUNT);

        final List<MockFlowFile> flowfiles = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS);
        final long executionTime = Long.parseLong(flowfiles.getFirst().getAttribute(ExecuteSQL.RESULT_QUERY_EXECUTION_TIME));
        final long fetchTime = Long.parseLong(flowfiles.getFirst().getAttribute(ExecuteSQL.RESULT_QUERY_FETCH_TIME));
        final long durationTime = Long.parseLong(flowfiles.getFirst().getAttribute(ExecuteSQL.RESULT_QUERY_DURATION));

        assertEquals(durationTime, fetchTime + executionTime);

        final InputStream in = new ByteArrayInputStream(flowfiles.getFirst().toByteArray());
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(in, datumReader)) {
            GenericRecord record = null;
            long recordsFromStream = 0;
            while (dataFileReader.hasNext()) {
                // Reuse record object by passing it to next(). This saves us from
                // allocating and garbage collecting many objects for files with
                // many items.
                record = dataFileReader.next(record);
                recordsFromStream += 1;
            }

            assertEquals(nrOfRows, recordsFromStream);
        }
    }

    @Test
    public void testPreQuery() throws Exception {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");
        executeSql("insert into TEST_NULL_INT values(1,2,3)");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQL.SQL_PRE_QUERY, "VALUES (1)");
        runner.setProperty(ExecuteSQL.SQL_QUERY, "select * from TEST_NULL_INT");
        runner.enqueue("test".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).getFirst();
        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "1");

        final InputStream in = new ByteArrayInputStream(firstFlowFile.toByteArray());
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(in, datumReader)) {
            GenericRecord record = null;
            long recordsFromStream = 0;
            while (dataFileReader.hasNext()) {
                // Reuse record object by passing it to next(). This saves us from
                // allocating and garbage collecting many objects for files with
                // many items.
                record = dataFileReader.next(record);
                recordsFromStream += 1;
            }

            assertEquals(1, recordsFromStream);
        }
    }

    @Test
    public void testPostQuery() throws Exception {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");
        executeSql("insert into TEST_NULL_INT values(1,2,3)");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQL.SQL_PRE_QUERY, "VALUES (1)");
        runner.setProperty(ExecuteSQL.SQL_QUERY, "select * from TEST_NULL_INT");
        runner.setProperty(ExecuteSQL.SQL_POST_QUERY, "VALUES (2)");
        runner.enqueue("test".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).getFirst();
        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "1");

        final InputStream in = new ByteArrayInputStream(firstFlowFile.toByteArray());
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(in, datumReader)) {
            GenericRecord record = null;
            long recordsFromStream = 0;
            while (dataFileReader.hasNext()) {
                // Reuse record object by passing it to next(). This saves us from
                // allocating and garbage collecting many objects for files with
                // many items.
                record = dataFileReader.next(record);
                recordsFromStream += 1;
            }

            assertEquals(1, recordsFromStream);
        }
    }

    @Test
    public void testPreQueryFail() throws Exception {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        runner.setIncomingConnection(true);
        // Simulate failure by not provide parameter
        runner.setProperty(ExecuteSQL.SQL_PRE_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS()");
        runner.setProperty(ExecuteSQL.SQL_QUERY, "select * from TEST_NULL_INT");
        runner.enqueue("test".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_FAILURE, 1);
    }

    @Test
    public void testPostQueryFail() throws Exception {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQL.SQL_PRE_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)");
        runner.setProperty(ExecuteSQL.SQL_QUERY, "select * from TEST_NULL_INT");
        // Simulate failure by not provide parameter
        runner.setProperty(ExecuteSQL.SQL_POST_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS()");
        runner.enqueue("test".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_FAILURE, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_FAILURE).getFirst();
        firstFlowFile.assertContentEquals("test");
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
                Map.entry("compression-format", ExecuteSQL.COMPRESSION_FORMAT.getName()),
                Map.entry(JdbcProperties.OLD_NORMALIZE_NAMES_FOR_AVRO_PROPERTY_NAME, NORMALIZE_NAMES_FOR_AVRO.getName()),
                Map.entry(JdbcProperties.OLD_USE_AVRO_LOGICAL_TYPES_PROPERTY_NAME, USE_AVRO_LOGICAL_TYPES.getName()),
                Map.entry(JdbcProperties.OLD_DEFAULT_PRECISION_PROPERTY_NAME, DEFAULT_PRECISION.getName()),
                Map.entry(JdbcProperties.OLD_DEFAULT_SCALE_PROPERTY_NAME, DEFAULT_SCALE.getName())
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
}
