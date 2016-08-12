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

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.db.DatabaseAdapter;
import org.apache.nifi.processors.standard.db.impl.GenericDatabaseAdapter;
import org.apache.nifi.processors.standard.db.impl.OracleDatabaseAdapter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.file.FileUtils;
import org.fusesource.hawtbuf.ByteArrayInputStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the QueryDatabaseTable processor
 */
public class QueryDatabaseTableTest {

    MockQueryDatabaseTable processor;
    private TestRunner runner;
    private final static String DB_LOCATION = "target/db_qdt";
    private DatabaseAdapter dbAdapter;


    @BeforeClass
    public static void setupBeforeClass() throws IOException {
        System.setProperty("derby.stream.error.file", "target/derby.log");

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        try {
            FileUtils.deleteFile(dbLocation, true);
        } catch (IOException ioe) {
            // Do nothing, may not have existed
        }
    }

    @AfterClass
    public static void cleanUpAfterClass() throws Exception {
        try {
            DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";shutdown=true");
        } catch (SQLNonTransientConnectionException e) {
            // Do nothing, this is what happens at Derby shutdown
        }
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        try {
            FileUtils.deleteFile(dbLocation, true);
        } catch (IOException ioe) {
            // Do nothing, may not have existed
        }
    }


    @Before
    public void setup() throws InitializationException, IOException {
        final DBCPService dbcp = new DBCPServiceSimpleImpl();
        final Map<String, String> dbcpProperties = new HashMap<>();
        dbAdapter = new GenericDatabaseAdapter();

        processor = new MockQueryDatabaseTable();
        runner = TestRunners.newTestRunner(processor);
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(QueryDatabaseTable.DBCP_SERVICE, "dbcp");
        runner.setProperty(QueryDatabaseTable.DB_TYPE, dbAdapter.getName());
        runner.getStateManager().clear(Scope.CLUSTER);
    }

    @After
    public void teardown() {
        runner = null;
    }

    @Test
    public void testGetQuery() throws Exception {
        String query = processor.getQuery(dbAdapter, "myTable", null, null, null);
        assertEquals("SELECT * FROM myTable", query);
        query = processor.getQuery(dbAdapter, "myTable", "col1,col2", null, null);
        assertEquals("SELECT col1,col2 FROM myTable", query);

        query = processor.getQuery(dbAdapter, "myTable", null, Collections.singletonList("id"), null);
        assertEquals("SELECT * FROM myTable", query);

        Map<String, String> maxValues = new HashMap<>();
        maxValues.put("id", "509");
        StateManager stateManager = runner.getStateManager();
        stateManager.setState(maxValues, Scope.CLUSTER);
        processor.putColumnType("id", Types.INTEGER);
        query = processor.getQuery(dbAdapter, "myTable", null, Collections.singletonList("id"), stateManager.getState(Scope.CLUSTER));
        assertEquals("SELECT * FROM myTable WHERE id > 509", query);

        maxValues.put("date_created", "2016-03-07 12:34:56");
        stateManager.setState(maxValues, Scope.CLUSTER);
        processor.putColumnType("date_created", Types.TIMESTAMP);
        query = processor.getQuery(dbAdapter, "myTable", null, Arrays.asList("id", "DATE_CREATED"), stateManager.getState(Scope.CLUSTER));
        assertEquals("SELECT * FROM myTable WHERE id > 509 AND DATE_CREATED > '2016-03-07 12:34:56'", query);

        // Test Oracle strategy
        dbAdapter = new OracleDatabaseAdapter();
        query = processor.getQuery(dbAdapter, "myTable", null, Arrays.asList("id", "DATE_CREATED"), stateManager.getState(Scope.CLUSTER));
        assertEquals("SELECT * FROM myTable WHERE id > 509 AND DATE_CREATED > to_date('2016-03-07 12:34:56', 'yyyy-mm-dd HH24:MI:SS')", query);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetQueryNoTable() throws Exception {
        processor.getQuery(dbAdapter, null, null, null, null);
    }

    @Test
    public void testAddedRows() throws ClassNotFoundException, SQLException, InitializationException, IOException {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (0, 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (1, 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (2, NULL, 2.0, '2010-01-01 00:00:00')");

        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "ID");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);

        InputStream in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(0).toByteArray());
        runner.setProperty(QueryDatabaseTable.FETCH_SIZE, "2");
        assertEquals(3, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add a new row with a higher ID and run, one flowfile with one new row should be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (3, 'Mary West', 15.0, '2000-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(0).toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));

        // Sanity check - run again, this time no flowfiles/rows should be transferred
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add timestamp as a max value column name
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "id, created_on");

        // Add a new row with a higher ID and run, one flow file will be transferred because no max value for the timestamp has been stored
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (4, 'Marty Johnson', 15.0, '2011-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(0).toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Add a new row with a higher ID but lower timestamp and run, no flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (5, 'NO NAME', 15.0, '2001-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add a new row with a higher ID and run, one flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (6, 'Mr. NiFi', 1.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(0).toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Set name as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "name");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(0).toByteArray());
        assertEquals(7, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Add a new row with a "higher" name than the max but lower than "NULL" (to test that null values are skipped), one flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (7, 'NULK', 1.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(0).toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Set scale as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "scale");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(0).toByteArray());
        assertEquals(8, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Add a new row with a higher value for scale than the max, one flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (8, 'NULK', 100.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(0).toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Set scale as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "bignum");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(0).toByteArray());
        assertEquals(9, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Add a new row with a higher value for scale than the max, one flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on, bignum) VALUES (9, 'Alice Bob', 100.0, '2012-01-01 03:23:34.234', 1)");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(0).toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();
    }

    @Test
    public void testTimestampNanos() throws ClassNotFoundException, SQLException, InitializationException, IOException {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (1, 'Carrie Jones', 5.0, '2000-01-01 03:23:34.000123456')");

        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "created_on");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        InputStream in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(0).toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add a new row with a lower timestamp (but same millisecond value), no flow file should be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (3, 'Mary West', 15.0, '2000-01-01 03:23:34.000')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add a new row with a higher timestamp, one flow file should be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (3, 'Mary West', 15.0, '2000-01-01 03:23:34.0003')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(0).toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();
    }

        @Test
    public void testWithNullIntColumn() throws SQLException {
        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException sqle) {
            // Ignore, usually due to Derby not having DROP TABLE IF EXISTS
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        stmt.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (0, NULL, 1)");
        stmt.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (1, 1, 1)");

        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "TEST_NULL_INT");
        runner.run();

        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(0).assertAttributeEquals(QueryDatabaseTable.RESULT_ROW_COUNT, "2");
    }

    @Test
    public void testWithSqlException() throws SQLException {
        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NO_ROWS");
        } catch (final SQLException sqle) {
            // Ignore, usually due to Derby not having DROP TABLE IF EXISTS
        }

        stmt.execute("create table TEST_NO_ROWS (id integer)");

        runner.setIncomingConnection(false);
        // Try a valid SQL statement that will generate an error (val1 does not exist, e.g.)
        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "TEST_NO_ROWS");
        runner.setProperty(QueryDatabaseTable.COLUMN_NAMES, "val1");
        runner.run();

        assertTrue(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).isEmpty());
    }

    private long getNumberOfRecordsFromStream(InputStream in) throws IOException {
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

            return recordsFromStream;
        }
    }

    /**
     * Simple implementation only for QueryDatabaseTable processor testing.
     */
    private class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {

        @Override
        public String getIdentifier() {
            return "dbcp";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
                return DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";create=true");
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }

    @Stateful(scopes = Scope.CLUSTER, description = "Mock for QueryDatabaseTable processor")
    private static class MockQueryDatabaseTable extends QueryDatabaseTable {
        void putColumnType(String colName, Integer colType) {
            columnTypeMap.put(colName, colType);
        }
    }
}