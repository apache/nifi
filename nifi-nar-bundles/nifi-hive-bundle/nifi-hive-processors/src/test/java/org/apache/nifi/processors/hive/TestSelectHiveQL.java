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
package org.apache.nifi.processors.hive;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.dbcp.hive.HiveDBCPService;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.nifi.processors.hive.SelectHiveQL.HIVEQL_OUTPUT_FORMAT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSelectHiveQL {

    private static final Logger LOGGER;

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.io.nio", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.hive.SelectHiveQL", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.hive.TestSelectHiveQL", "debug");
        LOGGER = LoggerFactory.getLogger(TestSelectHiveQL.class);
    }

    final static String DB_LOCATION = "target/db";

    final static String QUERY_WITH_EL = "select "
            + "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
            + " from persons PER"
            + " where PER.ID > ${person.id}";

    final static String QUERY_WITHOUT_EL = "select "
            + "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
            + " from persons PER"
            + " where PER.ID > 10";


    @BeforeClass
    public static void setupClass() {
        System.setProperty("derby.stream.error.file", "target/derby.log");
    }

    private TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        final DBCPService dbcp = new DBCPServiceSimpleImpl();
        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(SelectHiveQL.class);
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(SelectHiveQL.HIVE_DBCP_SERVICE, "dbcp");
    }

    @Test
    public void testIncomingConnectionWithNoFlowFile() throws InitializationException {
        runner.setIncomingConnection(true);
        runner.setProperty(SelectHiveQL.HIVEQL_SELECT_QUERY, "SELECT * FROM persons");
        runner.run();
        runner.assertTransferCount(SelectHiveQL.REL_SUCCESS, 0);
        runner.assertTransferCount(SelectHiveQL.REL_FAILURE, 0);
    }

    @Test
    public void testNoIncomingConnection() throws ClassNotFoundException, SQLException, InitializationException, IOException {
        runner.setIncomingConnection(false);
        invokeOnTrigger(QUERY_WITHOUT_EL, false, "Avro");
    }

    @Test
    public void testNoTimeLimit() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        invokeOnTrigger(QUERY_WITH_EL, true, "Avro");
    }


    @Test
    public void testWithNullIntColumn() throws SQLException {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((HiveDBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        stmt.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (0, NULL, 1)");
        stmt.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (1, 1, 1)");

        runner.setIncomingConnection(false);
        runner.setProperty(SelectHiveQL.HIVEQL_SELECT_QUERY, "SELECT * FROM TEST_NULL_INT");
        runner.run();

        runner.assertAllFlowFilesTransferred(SelectHiveQL.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(SelectHiveQL.REL_SUCCESS).get(0).assertAttributeEquals(SelectHiveQL.RESULT_ROW_COUNT, "2");
    }

    @Test
    public void testWithSqlException() throws SQLException {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((HiveDBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NO_ROWS");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_NO_ROWS (id integer)");

        runner.setIncomingConnection(false);
        // Try a valid SQL statment that will generate an error (val1 does not exist, e.g.)
        runner.setProperty(SelectHiveQL.HIVEQL_SELECT_QUERY, "SELECT val1 FROM TEST_NO_ROWS");
        runner.run();

        runner.assertAllFlowFilesTransferred(SelectHiveQL.REL_FAILURE, 1);
    }


    @Test
    public void invokeOnTriggerWithCsv()
            throws InitializationException, ClassNotFoundException, SQLException, IOException {
        invokeOnTrigger(QUERY_WITHOUT_EL, false, SelectHiveQL.CSV);
    }

    @Test
    public void invokeOnTriggerWithAvro()
            throws InitializationException, ClassNotFoundException, SQLException, IOException {
        invokeOnTrigger(QUERY_WITHOUT_EL, false, SelectHiveQL.AVRO);
    }

    public void invokeOnTrigger(final String query, final boolean incomingFlowFile, String outputFormat)
            throws InitializationException, ClassNotFoundException, SQLException, IOException {

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((HiveDBCPService) runner.getControllerService("dbcp")).getConnection();
        final Statement stmt = con.createStatement();
        try {
            stmt.execute("drop table persons");
        } catch (final SQLException sqle) {
            // Nothing to do here, the table didn't exist
        }

        stmt.execute("create table persons (id integer, name varchar(100), code integer)");
        Random rng = new Random(53496);
        final int nrOfRows = 100;
        stmt.executeUpdate("insert into persons values (1, 'Joe Smith', " + rng.nextInt(469947) + ")");
        for (int i = 2; i < nrOfRows; i++) {
            stmt.executeUpdate("insert into persons values (" + i + ", 'Someone Else', " + rng.nextInt(469947) + ")");
        }
        stmt.executeUpdate("insert into persons values (" + nrOfRows + ", 'Last Person', NULL)");

        LOGGER.info("test data loaded");

        runner.setProperty(SelectHiveQL.HIVEQL_SELECT_QUERY, query);
        runner.setProperty(HIVEQL_OUTPUT_FORMAT, outputFormat);

        if (incomingFlowFile) {
            // incoming FlowFile content is not used, but attributes are used
            final Map<String, String> attributes = new HashMap<>();
            attributes.put("person.id", "10");
            runner.enqueue("Hello".getBytes(), attributes);
        }

        runner.setIncomingConnection(incomingFlowFile);
        runner.run();
        runner.assertAllFlowFilesTransferred(SelectHiveQL.REL_SUCCESS, 1);

        final List<MockFlowFile> flowfiles = runner.getFlowFilesForRelationship(SelectHiveQL.REL_SUCCESS);
        MockFlowFile flowFile = flowfiles.get(0);
        final InputStream in = new ByteArrayInputStream(flowFile.toByteArray());
        long recordsFromStream = 0;
        if (SelectHiveQL.AVRO.equals(outputFormat)) {
            assertEquals(SelectHiveQL.AVRO_MIME_TYPE, flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));
            final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            try (DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(in, datumReader)) {
                GenericRecord record = null;
                while (dataFileReader.hasNext()) {
                    // Reuse record object by passing it to next(). This saves us from
                    // allocating and garbage collecting many objects for files with
                    // many items.
                    record = dataFileReader.next(record);
                    recordsFromStream++;
                }
            }
        } else {
            assertEquals(SelectHiveQL.CSV_MIME_TYPE, flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));
            BufferedReader br = new BufferedReader(new InputStreamReader(in));

            String headerRow = br.readLine();
            // Derby capitalizes column names
            assertEquals("PERSONID,PERSONNAME,PERSONCODE", headerRow);

            // Validate rows
            String line;
            while ((line = br.readLine()) != null) {
                recordsFromStream++;
                String[] values = line.split(",");
                if(recordsFromStream < (nrOfRows - 10)) {
                    assertEquals(3, values.length);
                    assertTrue(values[1].startsWith("\""));
                    assertTrue(values[1].endsWith("\""));
                } else {
                    assertEquals(2, values.length); // Middle value is null
                }
            }
        }
        assertEquals(nrOfRows - 10, recordsFromStream);
        assertEquals(recordsFromStream, Integer.parseInt(flowFile.getAttribute(SelectHiveQL.RESULT_ROW_COUNT)));
    }

    /**
     * Simple implementation only for SelectHiveQL processor testing.
     */
    private class DBCPServiceSimpleImpl extends AbstractControllerService implements HiveDBCPService {

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

        @Override
        public String getConnectionURL() {
            return "jdbc:derby:" + DB_LOCATION + ";create=true";
        }
    }

}
