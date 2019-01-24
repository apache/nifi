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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.util.AvroUtil;
import org.apache.nifi.processors.standard.util.TestJdbcHugeStream;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestExecuteSQL {

    private static final Logger LOGGER;

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.io.nio", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.ExecuteSQL", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.TestExecuteSQL", "debug");
        LOGGER = LoggerFactory.getLogger(TestExecuteSQL.class);
    }

    final static String DB_LOCATION = "target/db";

    final static String QUERY_WITH_EL = "select "
        + "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
        + ", PRD.ID as ProductId,PRD.NAME as ProductName,PRD.CODE as ProductCode"
        + ", REL.ID as RelId,    REL.NAME as RelName,    REL.CODE as RelCode"
        + ", ROW_NUMBER() OVER () as rownr "
        + " from persons PER, products PRD, relationships REL"
        + " where PER.ID = ${person.id}";

    final static String QUERY_WITHOUT_EL = "select "
        + "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
        + ", PRD.ID as ProductId,PRD.NAME as ProductName,PRD.CODE as ProductCode"
        + ", REL.ID as RelId,    REL.NAME as RelName,    REL.CODE as RelCode"
        + ", ROW_NUMBER() OVER () as rownr "
        + " from persons PER, products PRD, relationships REL"
        + " where PER.ID = 10";

    final static String QUERY_WITHOUT_EL_WITH_PARAMS = "select "
            + "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
            + ", PRD.ID as ProductId,PRD.NAME as ProductName,PRD.CODE as ProductCode"
            + ", REL.ID as RelId,    REL.NAME as RelName,    REL.CODE as RelCode"
            + ", ROW_NUMBER() OVER () as rownr "
            + " from persons PER, products PRD, relationships REL"
            + " where PER.ID < ? AND REL.ID < ?";


    @BeforeClass
    public static void setupClass() {
        System.setProperty("derby.stream.error.file", "target/derby.log");
    }

    private TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        final DBCPService dbcp = new DBCPServiceSimpleImpl();
        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(ExecuteSQL.class);
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(ExecuteSQL.DBCP_SERVICE, "dbcp");
    }

    @Test
    public void testIncomingConnectionWithNoFlowFile() throws InitializationException {
        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "SELECT * FROM persons");
        runner.run();
        runner.assertTransferCount(ExecuteSQL.REL_SUCCESS, 0);
        runner.assertTransferCount(ExecuteSQL.REL_FAILURE, 0);
    }

    @Test
    public void testIncomingConnectionWithNoFlowFileAndNoQuery() throws InitializationException {
        runner.setIncomingConnection(true);
        runner.run();
        runner.assertTransferCount(ExecuteSQL.REL_SUCCESS, 0);
        runner.assertTransferCount(ExecuteSQL.REL_FAILURE, 0);
    }

    @Test(expected = AssertionError.class)
    public void testNoIncomingConnectionAndNoQuery() throws InitializationException {
        runner.setIncomingConnection(false);
        runner.run();
    }

    @Test
    public void testNoIncomingConnection() throws ClassNotFoundException, SQLException, InitializationException, IOException {
        runner.setIncomingConnection(false);
        invokeOnTrigger(null, QUERY_WITHOUT_EL, false, null, true);
    }

    @Test
    public void testNoTimeLimit() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        invokeOnTrigger(null, QUERY_WITH_EL, true, null, true);
    }

    @Test
    public void testSelectQueryInFlowFile() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        invokeOnTrigger(null, QUERY_WITHOUT_EL, true, null, false);
    }

    @Test
    public void testSelectQueryInFlowFileWithParameters() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        Map<String, String> sqlParams = new HashMap<String, String>() {{
            put("sql.args.1.type", "4");
            put("sql.args.1.value", "20");
            put("sql.args.2.type", "4");
            put("sql.args.2.value", "5");
        }};

        invokeOnTrigger(null, QUERY_WITHOUT_EL_WITH_PARAMS, true, sqlParams, false);
    }

    @Test
    public void testQueryTimeout() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        // Does to seem to have any effect when using embedded Derby
        invokeOnTrigger(1, QUERY_WITH_EL, true, null, true); // 1 second max time
    }

    @Test
    public void testWithNullIntColumn() throws SQLException {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        stmt.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (0, NULL, 1)");
        stmt.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (1, 1, 1)");

        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "SELECT * FROM TEST_NULL_INT");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).get(0).assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "2");
        runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).get(0).assertAttributeEquals(ExecuteSQL.RESULTSET_INDEX, "0");
    }

    @Test
    public void testCompression() throws SQLException, CompressorException, IOException {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        stmt.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (0, NULL, 1)");
        stmt.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (1, 1, 1)");

        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteSQL.COMPRESSION_FORMAT, AvroUtil.CodecType.BZIP2.name());
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "SELECT * FROM TEST_NULL_INT");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).get(0);

        try (DataFileStream<GenericRecord> dfs = new DataFileStream<>(new ByteArrayInputStream(flowFile.toByteArray()), new GenericDatumReader<GenericRecord>())) {
            assertEquals(AvroUtil.CodecType.BZIP2.name().toLowerCase(), dfs.getMetaString(DataFileConstants.CODEC).toLowerCase());
        }
    }

    @Test
    public void testWithOutputBatching() throws SQLException {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        for (int i = 0; i < 1000; i++) {
            stmt.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (" + i + ", 1, 1)");
        }

        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteSQL.MAX_ROWS_PER_FLOW_FILE, "5");
        runner.setProperty(ExecuteSQL.OUTPUT_BATCH_SIZE, "5");
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "SELECT * FROM TEST_NULL_INT");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 200);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_INDEX.key());
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_ID.key());

        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).get(0);

        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "5");
        firstFlowFile.assertAttributeNotExists(FragmentAttributes.FRAGMENT_COUNT.key());
        firstFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "0");
        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULTSET_INDEX, "0");

        MockFlowFile lastFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).get(199);

        lastFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "5");
        lastFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "199");
        lastFlowFile.assertAttributeEquals(ExecuteSQL.RESULTSET_INDEX, "0");
    }

    @Test
    public void testMaxRowsPerFlowFile() throws SQLException {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        for (int i = 0; i < 1000; i++) {
            stmt.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (" + i + ", 1, 1)");
        }

        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteSQL.MAX_ROWS_PER_FLOW_FILE, "5");
        runner.setProperty(ExecuteSQL.OUTPUT_BATCH_SIZE, "0");
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "SELECT * FROM TEST_NULL_INT");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 200);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_INDEX.key());
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_ID.key());
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_COUNT.key());

        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).get(0);

        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "5");
        firstFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "0");
        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULTSET_INDEX, "0");

        MockFlowFile lastFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).get(199);

        lastFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "5");
        lastFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "199");
        lastFlowFile.assertAttributeEquals(ExecuteSQL.RESULTSET_INDEX, "0");
    }

    @Test
    public void testInsertStatementCreatesFlowFile() throws SQLException {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "insert into TEST_NULL_INT (id, val1, val2) VALUES (0, NULL, 1)");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).get(0).assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "0");
    }

    @Test
    public void testNoRowsStatementCreatesEmptyFlowFile() throws Exception {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "select * from TEST_NULL_INT");
        runner.enqueue("Hello".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).get(0);
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
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table host1");
            stmt.execute("drop table host2");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table host1 (id integer not null, host varchar(45))");
        stmt.execute("create table host2 (id integer not null, host varchar(45))");
        stmt.execute("insert into host1 values(1,'host1')");
        stmt.execute("insert into host2 values(1,'host2')");
        stmt.execute("select a.host as hostA,b.host as hostB from host1 a join host2 b on b.id=a.id");
        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "select a.host as hostA,b.host as hostB from host1 a join host2 b on b.id=a.id");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).get(0).assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "1");
    }

    @Test
    public void testWithSqlException() throws SQLException {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NO_ROWS");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_NO_ROWS (id integer)");

        runner.setIncomingConnection(false);
        // Try a valid SQL statement that will generate an error (val1 does not exist, e.g.)
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "SELECT val1 FROM TEST_NO_ROWS");
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
        MockFlowFile failedFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_FAILURE).get(0);
        Assert.assertEquals("java.sql.SQLException: test execute statement failed",failedFlowFile.getAttribute(ExecuteSQL.RESULT_ERROR_MESSAGE));
    }

    public void invokeOnTrigger(final Integer queryTimeout, final String query, final boolean incomingFlowFile, final Map<String,String> attrs, final boolean setQueryProperty)
        throws InitializationException, ClassNotFoundException, SQLException, IOException {

        if (queryTimeout != null) {
            runner.setProperty(ExecuteSQL.QUERY_TIMEOUT, queryTimeout.toString() + " secs");
        }

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        TestJdbcHugeStream.loadTestData2Database(con, 100, 200, 100);
        LOGGER.info("test data loaded");

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

        if(setQueryProperty) {
            runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, query);
        }

        runner.run();
        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, ExecuteSQL.RESULT_QUERY_DURATION);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, ExecuteSQL.RESULT_QUERY_EXECUTION_TIME);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, ExecuteSQL.RESULT_QUERY_FETCH_TIME);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, ExecuteSQL.RESULT_ROW_COUNT);

        final List<MockFlowFile> flowfiles = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS);
        final long executionTime = Long.parseLong(flowfiles.get(0).getAttribute(ExecuteSQL.RESULT_QUERY_EXECUTION_TIME));
        final long fetchTime = Long.parseLong(flowfiles.get(0).getAttribute(ExecuteSQL.RESULT_QUERY_FETCH_TIME));
        final long durationTime = Long.parseLong(flowfiles.get(0).getAttribute(ExecuteSQL.RESULT_QUERY_DURATION));

        assertEquals(durationTime, fetchTime + executionTime);

        final InputStream in = new ByteArrayInputStream(flowfiles.get(0).toByteArray());
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

            LOGGER.info("total nr of records from stream: " + recordsFromStream);
            assertEquals(nrOfRows, recordsFromStream);
        }
    }

    @Test
    public void testPreQuery() throws Exception {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");
        stmt.execute("insert into TEST_NULL_INT values(1,2,3)");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQL.SQL_PRE_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)");
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "select * from TEST_NULL_INT");
        runner.enqueue("test".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).get(0);
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
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");
        stmt.execute("insert into TEST_NULL_INT values(1,2,3)");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQL.SQL_PRE_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)");
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "select * from TEST_NULL_INT");
        runner.setProperty(ExecuteSQL.SQL_POST_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0);CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(0)");
        runner.enqueue("test".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).get(0);
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
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        runner.setIncomingConnection(true);
        // Simulate failure by not provide parameter
        runner.setProperty(ExecuteSQL.SQL_PRE_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS()");
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "select * from TEST_NULL_INT");
        runner.enqueue("test".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_FAILURE, 1);
    }

    @Test
    public void testPostQueryFail() throws Exception {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQL.SQL_PRE_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)");
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "select * from TEST_NULL_INT");
        // Simulate failure by not provide parameter
        runner.setProperty(ExecuteSQL.SQL_POST_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS()");
        runner.enqueue("test".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_FAILURE, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_FAILURE).get(0);
        firstFlowFile.assertContentEquals("test");
    }

    /**
     * Simple implementation only for ExecuteSQL processor testing.
     *
     */
    class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {

        @Override
        public String getIdentifier() {
            return "dbcp";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
                final Connection con = DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";create=true");
                return con;
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }

}
