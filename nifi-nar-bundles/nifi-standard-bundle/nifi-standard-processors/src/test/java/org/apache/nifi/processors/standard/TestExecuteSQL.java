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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.util.TestJdbcHugeStream;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.fusesource.hawtbuf.ByteArrayInputStream;
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
        invokeOnTrigger(null, QUERY_WITHOUT_EL, false, true);
    }

    @Test
    public void testNoTimeLimit() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        invokeOnTrigger(null, QUERY_WITH_EL, true, true);
    }

    @Test
    public void testSelectQueryInFlowFile() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        invokeOnTrigger(null, QUERY_WITHOUT_EL, true, false);
    }

    @Test
    public void testQueryTimeout() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        // Does to seem to have any effect when using embedded Derby
        invokeOnTrigger(1, QUERY_WITH_EL, true, true); // 1 second max time
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
    }

    @Test
    public void testWithduplicateColumns() throws SQLException {
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

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_FAILURE, 1);
    }

    public void invokeOnTrigger(final Integer queryTimeout, final String query, final boolean incomingFlowFile, final boolean setQueryProperty)
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
            final Map<String, String> attributes = new HashMap<>();
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
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, ExecuteSQL.RESULT_ROW_COUNT);

        final List<MockFlowFile> flowfiles = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS);
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
