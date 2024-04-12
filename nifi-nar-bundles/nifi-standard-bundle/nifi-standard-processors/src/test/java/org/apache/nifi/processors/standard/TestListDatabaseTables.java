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
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.file.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for ListDatabaseTables processor.
 */
public class TestListDatabaseTables {

    TestRunner runner;
    ListDatabaseTables processor;

    private final static String DB_LOCATION = "target/db_ldt";

    @BeforeAll
    public static void setupBeforeClass() {
        System.setProperty("derby.stream.error.file", "target/derby.log");

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        try {
            FileUtils.deleteFile(dbLocation, true);
        } catch (IOException ioe) {
            // Do nothing, may not have existed
        }
    }

    @AfterAll
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
        System.clearProperty("derby.stream.error.file");
    }

    @BeforeEach
    public void setUp() throws Exception {
        processor = new ListDatabaseTables();

        runner = TestRunners.newTestRunner(ListDatabaseTables.class);

        final DBCPService dbcp = new DBCPServiceSimpleImpl();
        runner.addControllerService("dbcp", dbcp, new HashMap<>());
        runner.enableControllerService(dbcp);

        final DBCPService dbcpFailure = new DBCPServiceFailureImpl();
        runner.addControllerService("dbcpFailure", dbcpFailure, new HashMap<>());
        runner.enableControllerService(dbcpFailure);

        runner.setIncomingConnection(false);
        runner.setNonLoopConnection(false);
        runner.setProperty(ListDatabaseTables.DBCP_SERVICE, "dbcp");
    }

    @Test
    public void testListTablesNoCount() throws Exception {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_TABLE1");
            stmt.execute("drop table TEST_TABLE2");
        } catch (final SQLException sqle) {
            // Do nothing, may not have existed
        }

        stmt.execute("create table TEST_TABLE1 (id integer not null, val1 integer, val2 integer, constraint my_pk1 primary key (id))");
        stmt.execute("create table TEST_TABLE2 (id integer not null, val1 integer, val2 integer, constraint my_pk2 primary key (id))");

        runner.run();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 2);
        // Already got these tables, shouldn't get them again
        runner.clearTransferState();
        runner.run();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 0);
    }

    @Test
    public void testListTablesWithCount() throws Exception {
        runner.setProperty(ListDatabaseTables.INCLUDE_COUNT, "true");

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_TABLE1");
            stmt.execute("drop table TEST_TABLE2");
        } catch (final SQLException sqle) {
            // Do nothing, may not have existed
        }

        stmt.execute("create table TEST_TABLE1 (id integer not null, val1 integer, val2 integer, constraint my_pk1 primary key (id))");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (0, NULL, 1)");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (1, 1, 1)");
        stmt.execute("create table TEST_TABLE2 (id integer not null, val1 integer, val2 integer, constraint my_pk2 primary key (id))");

        runner.run();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 2);
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ListDatabaseTables.REL_SUCCESS);
        assertEquals("2", results.get(0).getAttribute(ListDatabaseTables.DB_TABLE_COUNT));
        assertEquals("0", results.get(1).getAttribute(ListDatabaseTables.DB_TABLE_COUNT));
    }

    @Test
    public void testListTablesWithCountAsRecord() throws Exception {
        runner.setProperty(ListDatabaseTables.INCLUDE_COUNT, "true");

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_TABLE1");
            stmt.execute("drop table TEST_TABLE2");
        } catch (final SQLException sqle) {
            // Do nothing, may not have existed
        }

        stmt.execute("create table TEST_TABLE1 (id integer not null, val1 integer, val2 integer, constraint my_pk1 primary key (id))");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (0, NULL, 1)");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (1, 1, 1)");
        stmt.execute("create table TEST_TABLE2 (id integer not null, val1 integer, val2 integer, constraint my_pk2 primary key (id))");

        final MockRecordWriter recordWriter = new MockRecordWriter(null, false);
        runner.addControllerService("record-writer", recordWriter);
        runner.setProperty(ListDatabaseTables.RECORD_WRITER, "record-writer");
        runner.enableControllerService(recordWriter);

        runner.run();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ListDatabaseTables.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(
            "TEST_TABLE1,,APP,APP.TEST_TABLE1,TABLE,,2\n" +
                "TEST_TABLE2,,APP,APP.TEST_TABLE2,TABLE,,0\n");
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testListTablesAfterRefresh() throws Exception {
        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_TABLE1");
            stmt.execute("drop table TEST_TABLE2");
        } catch (final SQLException sqle) {
            // Do nothing, may not have existed
        }

        stmt.execute("create table TEST_TABLE1 (id integer not null, val1 integer, val2 integer, constraint my_pk1 primary key (id))");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (0, NULL, 1)");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (1, 1, 1)");
        stmt.execute("create table TEST_TABLE2 (id integer not null, val1 integer, val2 integer, constraint my_pk2 primary key (id))");
        stmt.close();

        runner.setProperty(ListDatabaseTables.INCLUDE_COUNT, "true");
        runner.setProperty(ListDatabaseTables.REFRESH_INTERVAL, "100 millis");
        runner.run();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 2);
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ListDatabaseTables.REL_SUCCESS);
        assertEquals("2", results.get(0).getAttribute(ListDatabaseTables.DB_TABLE_COUNT));
        assertEquals("0", results.get(1).getAttribute(ListDatabaseTables.DB_TABLE_COUNT));
        runner.clearTransferState();
        runner.run();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 0);

        // Now wait longer than 100 millis and assert the refresh has happened (the two tables are re-listed)
        Thread.sleep(200);
        runner.run();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 2);
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testListTablesMultipleRefresh() throws Exception {
        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_TABLE1");
            stmt.execute("drop table TEST_TABLE2");
        } catch (final SQLException sqle) {
            // Do nothing, may not have existed
        }

        stmt.execute("create table TEST_TABLE1 (id integer not null, val1 integer, val2 integer, constraint my_pk1 primary key (id))");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (0, NULL, 1)");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (1, 1, 1)");

        runner.setProperty(ListDatabaseTables.INCLUDE_COUNT, "true");
        runner.setProperty(ListDatabaseTables.REFRESH_INTERVAL, "200 millis");
        runner.run();
        long startTimer = System.currentTimeMillis();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 1);
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ListDatabaseTables.REL_SUCCESS);
        assertEquals("2", results.get(0).getAttribute(ListDatabaseTables.DB_TABLE_COUNT));
        runner.clearTransferState();

        // Add another table immediately, the first table should not be listed again but the second should
        stmt.execute("create table TEST_TABLE2 (id integer not null, val1 integer, val2 integer, constraint my_pk2 primary key (id))");
        stmt.close();
        runner.run();
        long endTimer = System.currentTimeMillis();
        // Expect 1 or 2 tables (whether execution has taken longer than the refresh time)
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, (endTimer - startTimer > 200) ? 2 : 1);
        results = runner.getFlowFilesForRelationship(ListDatabaseTables.REL_SUCCESS);
        assertEquals((endTimer - startTimer > 200) ? "2": "0", results.get(0).getAttribute(ListDatabaseTables.DB_TABLE_COUNT));
        runner.clearTransferState();

        // Now wait longer than the refresh interval and assert the refresh has happened (i.e. the two tables are re-listed)
        Thread.sleep(500);
        runner.run();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 2);
    }

    @Test
    public void testIncomingFlowFileSuccess() throws ClassNotFoundException, SQLException, InitializationException, IOException {
        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_TABLE1");
            stmt.execute("drop table TEST_TABLE2");
        } catch (final SQLException sqle) {
            // Do nothing, may not have existed
        }

        stmt.execute("create table TEST_TABLE1 (id integer not null, val1 integer, val2 integer, constraint my_pk1 primary key (id))");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (0, NULL, 1)");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (1, 1, 1)");
        stmt.execute("create table TEST_TABLE2 (id integer not null, val1 integer, val2 integer, constraint my_pk2 primary key (id))");

        runner.setProperty(ListDatabaseTables.INCLUDE_COUNT, "true");
        runner.setProperty(ListDatabaseTables.REFRESH_INTERVAL, "0 sec");
        runner.setIncomingConnection(true);
        runner.setNonLoopConnection(true);
        runner.addConnection(ListDatabaseTables.REL_FAILURE);

        runner.enqueue("");

        runner.run(1);
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 2);
        runner.assertTransferCount(ListDatabaseTables.REL_FAILURE, 0);

        // One flow file for table 1
        assertEquals(runner.getFlowFilesForRelationship(ListDatabaseTables.REL_SUCCESS).stream().filter(
                        (ff) -> "TEST_TABLE1".equals(ff.getAttribute("db.table.name"))).count(),
                1);

        // One flow file for table 2
        assertEquals(runner.getFlowFilesForRelationship(ListDatabaseTables.REL_SUCCESS).stream().filter(
                        (ff) -> "TEST_TABLE2".equals(ff.getAttribute("db.table.name"))).count(),
                1);
    }

    @Test
    public void testIncomingFlowFileFailure() throws ClassNotFoundException, SQLException, InitializationException, IOException {
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();
        stmt.close();

        runner.setProperty(ListDatabaseTables.DBCP_SERVICE, "dbcpFailure");

        runner.setProperty(ListDatabaseTables.INCLUDE_COUNT, "true");
        runner.setProperty(ListDatabaseTables.REFRESH_INTERVAL, "0 sec");
        runner.setIncomingConnection(true);
        runner.setNonLoopConnection(true);
        runner.addConnection(ListDatabaseTables.REL_FAILURE);

        runner.enqueue("");

        runner.run(1);
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 0);
        runner.assertTransferCount(ListDatabaseTables.REL_FAILURE, 1);

        assertEquals(runner.getFlowFilesForRelationship(ListDatabaseTables.REL_FAILURE).stream().filter(
                        (ff) -> ff.getAttribute("listdatabasetables.error") != null).count(),
                1);
    }

    /**
     * Simple implementation only for ListDatabaseTables processor testing.
     */
    private static class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {
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

    private static class DBCPServiceFailureImpl extends AbstractControllerService implements DBCPService {
        @Override
        public String getIdentifier() {
            return "dbcpFailure";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            return null;  // Intentionally cause exception in calling code
        }
    }

}