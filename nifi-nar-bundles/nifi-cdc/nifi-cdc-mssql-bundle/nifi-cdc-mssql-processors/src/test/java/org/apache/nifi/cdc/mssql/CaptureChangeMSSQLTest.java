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
package org.apache.nifi.cdc.mssql;

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.cdc.event.ColumnDefinition;
import org.apache.nifi.cdc.mssql.event.MSSQLColumnDefinition;
import org.apache.nifi.cdc.mssql.event.MSSQLTableInfo;
import org.apache.nifi.cdc.mssql.processors.CaptureChangeMSSQL;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.file.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CaptureChangeMSSQLTest {

    private TestRunner runner;
    private MockCaptureChangeMSSQL processor;
    private final static String DB_LOCATION = "target/db_qdt";


    @BeforeClass
    public static void setupBeforeClass() throws IOException, SQLException {
        System.setProperty("derby.stream.error.file", "target/derby.log");

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        try {
            FileUtils.deleteFile(dbLocation, true);
        } catch (IOException ioe) {
            // Do nothing, may not have existed
        }

        // load CDC schema to database
        final DBCPService dbcp = new DBCPServiceSimpleImpl();
        final Connection con = dbcp.getConnection();
        Statement stmt = con.createStatement();

        stmt.execute("CREATE TABLE cdc.change_tables(\n" +
                "object_id int,\n" +
                //These four columns are computed from object_id/source_object_id in MS SQL, but for testing we put them as strings
                "schemaName varchar(128),\n" +
                "tableName varchar(128),\n" +
                "sourceSchemaName varchar(128),\n" +
                "sourceTableName varchar(128),\n" +

                "version int,\n" +
                "capture_instance varchar(128),\n" +
                "start_lsn int,\n" +
                "end_lsn int,\n" +
                "supports_net_changes BOOLEAN,\n" +
                "has_drop_pending BOOLEAN,\n" +
                "role_name varchar(128),\n" +
                "index_name varchar(128),\n" +
                "filegroup_name varchar(128),\n" +
                "create_date TIMESTAMP,\n" +
                "partition_switch BOOLEAN)");

        stmt.execute("CREATE TABLE cdc.lsn_time_mapping(\n" +
                "start_lsn int,\n" +
                "tran_begin_time TIMESTAMP,\n" +
                "tran_end_time TIMESTAMP,\n" +
                "tran_id int,\n" +
                "tran_begin_lsn int)");

        stmt.execute("CREATE TABLE cdc.index_columns(\n" +
                "object_id int,\n" +
                "column_name varchar(128),\n" +
                "index_ordinal int,\n" +
                "column_id int)");

        stmt.execute("CREATE TABLE cdc.captured_columns(\n" +
                "object_id int,\n" +
                "column_name varchar(128),\n" +
                "column_id int,\n" +
                "column_type varchar(128),\n" +
                "column_ordinal int,\n" +
                "is_computed BOOLEAN)");
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
    public void setup() throws InitializationException, IOException, SQLException {
        final DBCPService dbcp = new DBCPServiceSimpleImpl();
        final Map<String, String> dbcpProperties = new HashMap<>();

        processor = new MockCaptureChangeMSSQL();
        runner = TestRunners.newTestRunner(processor);

        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(CaptureChangeMSSQL.DBCP_SERVICE, "dbcp");

        final MockRecordWriter writerService = new MockRecordWriter(null, false);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);
        runner.setProperty(CaptureChangeMSSQL.RECORD_WRITER, "writer");

        runner.getStateManager().clear(Scope.CLUSTER);
    }

    @After
    public void teardown() throws IOException {
        runner.getStateManager().clear(Scope.CLUSTER);
        runner = null;
    }

    @Test
    public void testSelectGenerator(){
        MSSQLCDCUtils mssqlcdcUtils = new MSSQLCDCUtils();

        List<ColumnDefinition> columns = new ArrayList<>();
        columns.add(new MSSQLColumnDefinition(Types.INTEGER, "ID", 1, true));
        columns.add(new MSSQLColumnDefinition(Types.VARCHAR, "LastName", 2, false));
        columns.add(new MSSQLColumnDefinition(Types.VARCHAR, "FirstName", 3, false));

        MSSQLTableInfo ti = new MSSQLTableInfo("NiFi", "cdc", "Names",
                "dbo", "dbo_Names_CT", 1000L, columns);

        String noMaxTime = mssqlcdcUtils.getCDCSelectStatement(ti, null);

        Assert.assertEquals("SELECT t.tran_begin_time\n" +
                ",t.tran_end_time \"tran_end_time\"\n" +
                ",CAST(t.tran_id AS bigint) trans_id\n" +
                ",CAST(\"o\".\"__$start_lsn\" AS bigint) start_lsn\n" +
                ",CAST(\"o\".\"__$seqval\" AS bigint) seqval\n" +
                ",\"o\".\"__$operation\" operation\n" +
                ",CAST(\"o\".\"__$update_mask\" AS bigint) update_mask\n" +
                ",\"o\".\"ID\"\n" +
                ",\"o\".\"LastName\"\n" +
                ",\"o\".\"FirstName\"\n" +
                ",CURRENT_TIMESTAMP EXTRACT_TIME\n" +
                "FROM cdc.\"Names\" \"o\"\n" +
                "INNER JOIN cdc.lsn_time_mapping t ON (t.start_lsn = \"o\".\"__$start_lsn\")\n" +
                "ORDER BY CAST(\"o\".\"__$start_lsn\" AS bigint), \"o\".\"__$seqval\", \"o\".\"__$operation\"", noMaxTime);

        String withMaxTime = mssqlcdcUtils.getCDCSelectStatement(ti, new Timestamp(0));

        Assert.assertEquals("SELECT t.tran_begin_time\n" +
                ",t.tran_end_time \"tran_end_time\"\n" +
                ",CAST(t.tran_id AS bigint) trans_id\n" +
                ",CAST(\"o\".\"__$start_lsn\" AS bigint) start_lsn\n" +
                ",CAST(\"o\".\"__$seqval\" AS bigint) seqval\n" +
                ",\"o\".\"__$operation\" operation\n" +
                ",CAST(\"o\".\"__$update_mask\" AS bigint) update_mask\n" +
                ",\"o\".\"ID\"\n" +
                ",\"o\".\"LastName\"\n" +
                ",\"o\".\"FirstName\"\n" +
                ",CURRENT_TIMESTAMP EXTRACT_TIME\n" +
                "FROM cdc.\"Names\" \"o\"\n" +
                "INNER JOIN cdc.lsn_time_mapping t ON (t.start_lsn = \"o\".\"__$start_lsn\")\n" +
                "WHERE t.tran_end_time > ?\n" +
                "ORDER BY CAST(\"o\".\"__$start_lsn\" AS bigint), \"o\".\"__$seqval\", \"o\".\"__$operation\"", withMaxTime);
    }

    @Test
    public void testRetrieveAllChanges() throws SQLException, IOException {
        setupNamesTable();
        
        runner.setIncomingConnection(false);

        runner.setProperty(CaptureChangeMSSQL.CDC_TABLES, "Names");

        runner.run();

        runner.assertAllFlowFilesTransferred(CaptureChangeMSSQL.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CaptureChangeMSSQL.REL_SUCCESS).get(0);

        Map<String,String> attributes = flowFile.getAttributes();

        Assert.assertTrue("Tablename attribute", attributes.containsKey("tablename"));
        Assert.assertTrue("CDC row count attribute", attributes.containsKey("mssqlcdc.row.count"));
        Assert.assertTrue("Maximum Transacation End Time attribute", attributes.containsKey("maxvalue.tran_end_time"));

        Assert.assertEquals("Names", attributes.get("tablename"));
        Assert.assertEquals("4", attributes.get("mssqlcdc.row.count"));
        Assert.assertEquals("2017-01-01 02:03:06.567", attributes.get("maxvalue.tran_end_time"));
        Assert.assertEquals("false", attributes.get("fullsnapshot"));

        StateMap stateMap = runner.getStateManager().getState(Scope.CLUSTER);
        Assert.assertEquals("2017-01-01 02:03:06.567", stateMap.get("names"));

        //Add rows, check again
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        stmt.execute("insert into cdc.lsn_time_mapping (start_lsn, tran_begin_time, tran_end_time, tran_id, tran_begin_lsn) VALUES (12350, '2017-01-01 03:04:05.123', "
                + "'2017-01-01 03:04:06.123', 10030, 12350)");
        stmt.execute("insert into dbo.\"Names\" (\"ID\", \"FirstName\", \"LastName\") VALUES (2000, 'Chris', 'Stone')");
        stmt.execute("insert into cdc.\"dbo_Names_CT\" (\"__$start_lsn\", \"__$end_lsn\", \"__$seqval\", \"__$operation\", \"__$update_mask\", "
                + "\"ID\", \"FirstName\", \"LastName\") VALUES (12350, 12350, 1, 1, 0, 2000, 'Chris', 'Stone')");

        runner.clearTransferState();

        runner.run();

        runner.assertAllFlowFilesTransferred(CaptureChangeMSSQL.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(CaptureChangeMSSQL.REL_SUCCESS).get(0);

        attributes = flowFile.getAttributes();

        Assert.assertTrue("Tablename attribute", attributes.containsKey("tablename"));
        Assert.assertTrue("CDC row count attribute", attributes.containsKey("mssqlcdc.row.count"));
        Assert.assertTrue("Maximum Transacation End Time attribute", attributes.containsKey("maxvalue.tran_end_time"));

        Assert.assertEquals("Names", attributes.get("tablename"));
        Assert.assertEquals("1", attributes.get("mssqlcdc.row.count"));
        Assert.assertEquals("2017-01-01 03:04:06.123", attributes.get("maxvalue.tran_end_time"));
        Assert.assertEquals("false", attributes.get("fullsnapshot"));

        stateMap = runner.getStateManager().getState(Scope.CLUSTER);
        Assert.assertEquals("2017-01-01 03:04:06.123", stateMap.get("names"));
    }

    @Test
    public void testInitialTimestamp() throws SQLException, IOException {
        setupNamesTable();

        runner.setIncomingConnection(false);

        runner.setProperty(CaptureChangeMSSQL.CDC_TABLES, "Names");
        runner.setProperty("initial.timestamp.names", "2017-01-01 02:03:04.123");

        runner.run();

        runner.assertAllFlowFilesTransferred(CaptureChangeMSSQL.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CaptureChangeMSSQL.REL_SUCCESS).get(0);

        Map<String,String> attributes = flowFile.getAttributes();

        Assert.assertTrue("Tablename attribute", attributes.containsKey("tablename"));
        Assert.assertTrue("CDC row count attribute", attributes.containsKey("mssqlcdc.row.count"));
        Assert.assertTrue("Maximum Transacation End Time attribute", attributes.containsKey("maxvalue.tran_end_time"));

        Assert.assertEquals("Names", attributes.get("tablename"));
        Assert.assertEquals("3", attributes.get("mssqlcdc.row.count"));
        Assert.assertEquals("2017-01-01 02:03:06.567", attributes.get("maxvalue.tran_end_time"));
        Assert.assertEquals("false", attributes.get("fullsnapshot"));

        StateMap stateMap = runner.getStateManager().getState(Scope.CLUSTER);
        Assert.assertEquals("2017-01-01 02:03:06.567", stateMap.get("names"));

        runner.clearTransferState();
        runner.run();

        runner.assertTransferCount(CaptureChangeMSSQL.REL_SUCCESS, 0);

        //Add rows, check again
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        stmt.execute("insert into cdc.lsn_time_mapping (start_lsn, tran_begin_time, tran_end_time, tran_id, tran_begin_lsn) VALUES (12350, '2017-01-01 03:04:05.123', "
                + "'2017-01-01 03:04:06.123', 10030, 12350)");
        stmt.execute("insert into dbo.\"Names\" (\"ID\", \"FirstName\", \"LastName\") VALUES (2000, 'Chris', 'Stone')");
        stmt.execute("insert into cdc.\"dbo_Names_CT\" (\"__$start_lsn\", \"__$end_lsn\", \"__$seqval\", \"__$operation\", \"__$update_mask\", "
                + "\"ID\", \"FirstName\", \"LastName\") VALUES (12350, 12350, 1, 1, 0, 2000, 'Chris', 'Stone')");

        runner.clearTransferState();
        runner.run();

        runner.assertAllFlowFilesTransferred(CaptureChangeMSSQL.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(CaptureChangeMSSQL.REL_SUCCESS).get(0);

        attributes = flowFile.getAttributes();

        Assert.assertTrue("Tablename attribute", attributes.containsKey("tablename"));
        Assert.assertTrue("CDC row count attribute", attributes.containsKey("mssqlcdc.row.count"));
        Assert.assertTrue("Maximum Transacation End Time attribute", attributes.containsKey("maxvalue.tran_end_time"));

        Assert.assertEquals("Names", attributes.get("tablename"));
        Assert.assertEquals("1", attributes.get("mssqlcdc.row.count"));
        Assert.assertEquals("2017-01-01 03:04:06.123", attributes.get("maxvalue.tran_end_time"));
        Assert.assertEquals("false", attributes.get("fullsnapshot"));

        stateMap = runner.getStateManager().getState(Scope.CLUSTER);
        Assert.assertEquals("2017-01-01 03:04:06.123", stateMap.get("names"));
    }

    @Test
    public void testRowLimit() throws SQLException, IOException {
        setupNamesTable();

        runner.setIncomingConnection(false);

        runner.setProperty(CaptureChangeMSSQL.CDC_TABLES, "Names");
        runner.setProperty(CaptureChangeMSSQL.FULL_SNAPSHOT_ROW_LIMIT, "2");

        runner.run();

        runner.assertAllFlowFilesTransferred(CaptureChangeMSSQL.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CaptureChangeMSSQL.REL_SUCCESS).get(0);

        Map<String,String> attributes = flowFile.getAttributes();

        Assert.assertTrue("Tablename attribute", attributes.containsKey("tablename"));
        Assert.assertTrue("CDC row count attribute", attributes.containsKey("mssqlcdc.row.count"));
        Assert.assertTrue("Maximum Transacation End Time attribute", attributes.containsKey("maxvalue.tran_end_time"));

        Assert.assertEquals("Names", attributes.get("tablename"));
        Assert.assertEquals("6", attributes.get("mssqlcdc.row.count"));

        //since a full data snapshot was taken, the CURRENT_TIMESTAMP from the database is used
        Assert.assertEquals("2017-03-01 01:01:01.123", attributes.get("maxvalue.tran_end_time"));
        Assert.assertEquals("true", attributes.get("fullsnapshot"));

        StateMap stateMap = runner.getStateManager().getState(Scope.CLUSTER);
        Assert.assertEquals("2017-03-01 01:01:01.123", stateMap.get("names"));

        runner.clearTransferState();
        runner.run();

        runner.assertTransferCount(CaptureChangeMSSQL.REL_SUCCESS, 0);

        //These rows should be skipped. The timestamp coming back from the full table snapshot is greater then the CDC update timestamp
        // resulting in no rows returned.
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        stmt.execute("insert into cdc.lsn_time_mapping (start_lsn, tran_begin_time, tran_end_time, tran_id, tran_begin_lsn) VALUES (12350, '2017-01-01 03:04:05.123', "
                + "'2017-01-01 03:04:06.123', 10030, 12350)");
        stmt.execute("insert into dbo.\"Names\" (\"ID\", \"FirstName\", \"LastName\") VALUES (2000, 'Chris', 'Stone')");
        stmt.execute("insert into cdc.\"dbo_Names_CT\" (\"__$start_lsn\", \"__$end_lsn\", \"__$seqval\", \"__$operation\", \"__$update_mask\", "
                + "\"ID\", \"FirstName\", \"LastName\") VALUES (12350, 12350, 1, 1, 0, 2000, 'Chris', 'Stone')");

        runner.clearTransferState();
        runner.run();

        runner.assertAllFlowFilesTransferred(CaptureChangeMSSQL.REL_SUCCESS, 0);

        stateMap = runner.getStateManager().getState(Scope.CLUSTER);
        Assert.assertEquals("2017-03-01 01:01:01.123", stateMap.get("names"));

        //Add one row, past current max timestamp, make sure we get a CDC only output
        stmt.execute("insert into cdc.lsn_time_mapping (start_lsn, tran_begin_time, tran_end_time, tran_id, tran_begin_lsn) VALUES (12360, '2017-03-01 23:59:58.123', "
                + "'2017-03-02 00:00:00.001', 10440, 12360)");
        stmt.execute("insert into dbo.\"Names\" (\"ID\", \"FirstName\", \"LastName\") VALUES (1900, 'Chris', 'Stone')");
        stmt.execute("insert into cdc.\"dbo_Names_CT\" (\"__$start_lsn\", \"__$end_lsn\", \"__$seqval\", \"__$operation\", \"__$update_mask\", "
                + "\"ID\", \"FirstName\", \"LastName\") VALUES (12360, 12360, 1, 1, 0, 1900, 'Juan', 'Stone')");

        runner.clearTransferState();
        runner.run();

        runner.assertAllFlowFilesTransferred(CaptureChangeMSSQL.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(CaptureChangeMSSQL.REL_SUCCESS).get(0);

        attributes = flowFile.getAttributes();

        Assert.assertTrue("Tablename attribute", attributes.containsKey("tablename"));
        Assert.assertTrue("CDC row count attribute", attributes.containsKey("mssqlcdc.row.count"));
        Assert.assertTrue("Maximum Transacation End Time attribute", attributes.containsKey("maxvalue.tran_end_time"));

        Assert.assertEquals("Names", attributes.get("tablename"));
        Assert.assertEquals("1", attributes.get("mssqlcdc.row.count"));

        Assert.assertEquals("2017-03-02 00:00:00.001", attributes.get("maxvalue.tran_end_time"));
        Assert.assertEquals("false", attributes.get("fullsnapshot"));

        stateMap = runner.getStateManager().getState(Scope.CLUSTER);
        Assert.assertEquals("2017-03-02 00:00:00.001", stateMap.get("names"));


        //Add two rows so we go over the row limit and get a full snapshot
        // This snapshot will include the row we inserted above but which got skipped previously
        stmt.execute("insert into cdc.lsn_time_mapping (start_lsn, tran_begin_time, tran_end_time, tran_id, tran_begin_lsn) VALUES (12360, '2017-02-28 23:59:01.123', "
                + "'2017-03-02 00:01:01.123', 10040, 12360)");
        stmt.execute("insert into dbo.\"Names\" (\"ID\", \"FirstName\", \"LastName\") VALUES (2100, 'James', 'Stone')");
        stmt.execute("insert into cdc.\"dbo_Names_CT\" (\"__$start_lsn\", \"__$end_lsn\", \"__$seqval\", \"__$operation\", \"__$update_mask\", "
                + "\"ID\", \"FirstName\", \"LastName\") VALUES (12360, 12360, 1, 1, 0, 2100, 'James', 'Stone')");
        stmt.execute("insert into dbo.\"Names\" (\"ID\", \"FirstName\", \"LastName\") VALUES (2200, 'Clark', 'Stone')");
        stmt.execute("insert into cdc.\"dbo_Names_CT\" (\"__$start_lsn\", \"__$end_lsn\", \"__$seqval\", \"__$operation\", \"__$update_mask\", "
                + "\"ID\", \"FirstName\", \"LastName\") VALUES (12360, 12360, 1, 1, 0, 2200, 'Clark', 'Stone')");

        //Update the "CURRENT_TIMESTAMP" value, which gets used
        // as the current tran_end_time marker for full snapshots
        processor.db_timestamp = "2017-03-03 01:01:01.123";

        runner.clearTransferState();
        runner.run();

        runner.assertAllFlowFilesTransferred(CaptureChangeMSSQL.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(CaptureChangeMSSQL.REL_SUCCESS).get(0);

        attributes = flowFile.getAttributes();

        Assert.assertTrue("Tablename attribute", attributes.containsKey("tablename"));
        Assert.assertTrue("CDC row count attribute", attributes.containsKey("mssqlcdc.row.count"));
        Assert.assertTrue("Maximum Transacation End Time attribute", attributes.containsKey("maxvalue.tran_end_time"));

        Assert.assertEquals("Names", attributes.get("tablename"));
        Assert.assertEquals("10", attributes.get("mssqlcdc.row.count"));
        Assert.assertEquals("true", attributes.get("fullsnapshot"));
        Assert.assertEquals("2017-03-03 01:01:01.123", attributes.get("maxvalue.tran_end_time"));

        stateMap = runner.getStateManager().getState(Scope.CLUSTER);
        Assert.assertEquals("2017-03-03 01:01:01.123", stateMap.get("names"));
    }

    @Test
    public void testBaseline() throws SQLException, IOException {
        setupNamesTable();

        runner.setIncomingConnection(false);

        runner.setProperty(CaptureChangeMSSQL.CDC_TABLES, "Names");
        runner.setProperty(CaptureChangeMSSQL.TAKE_INITIAL_SNAPSHOT, "true");

        runner.run();

        runner.assertAllFlowFilesTransferred(CaptureChangeMSSQL.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CaptureChangeMSSQL.REL_SUCCESS).get(0);

        Map<String,String> attributes = flowFile.getAttributes();

        Assert.assertTrue("Tablename attribute", attributes.containsKey("tablename"));
        Assert.assertTrue("CDC row count attribute", attributes.containsKey("mssqlcdc.row.count"));
        Assert.assertTrue("Maximum Transacation End Time attribute", attributes.containsKey("maxvalue.tran_end_time"));

        Assert.assertEquals("Names", attributes.get("tablename"));
        Assert.assertEquals("6", attributes.get("mssqlcdc.row.count"));

        //since a full data snapshot was taken, the CURRENT_TIMESTAMP from the database is used
        Assert.assertEquals("2017-03-01 01:01:01.123", attributes.get("maxvalue.tran_end_time"));
        Assert.assertEquals("true", attributes.get("fullsnapshot"));

        StateMap stateMap = runner.getStateManager().getState(Scope.CLUSTER);
        Assert.assertEquals("2017-03-01 01:01:01.123", stateMap.get("names"));

        runner.clearTransferState();
        runner.run();

        runner.assertTransferCount(CaptureChangeMSSQL.REL_SUCCESS, 0);

        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        //Add one row, past current max timestamp, make sure we get a CDC only output
        stmt.execute("insert into cdc.lsn_time_mapping (start_lsn, tran_begin_time, tran_end_time, tran_id, tran_begin_lsn) VALUES (12360, '2017-03-01 23:59:58.123', "
                + "'2017-03-02 00:00:00.001', 10440, 12360)");
        stmt.execute("insert into dbo.\"Names\" (\"ID\", \"FirstName\", \"LastName\") VALUES (1900, 'Chris', 'Stone')");
        stmt.execute("insert into cdc.\"dbo_Names_CT\" (\"__$start_lsn\", \"__$end_lsn\", \"__$seqval\", \"__$operation\", \"__$update_mask\", "
                + "\"ID\", \"FirstName\", \"LastName\") VALUES (12360, 12360, 1, 1, 0, 1900, 'Juan', 'Stone')");

        runner.clearTransferState();
        runner.run();

        runner.assertAllFlowFilesTransferred(CaptureChangeMSSQL.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(CaptureChangeMSSQL.REL_SUCCESS).get(0);

        attributes = flowFile.getAttributes();

        Assert.assertTrue("Tablename attribute", attributes.containsKey("tablename"));
        Assert.assertTrue("CDC row count attribute", attributes.containsKey("mssqlcdc.row.count"));
        Assert.assertTrue("Maximum Transacation End Time attribute", attributes.containsKey("maxvalue.tran_end_time"));

        Assert.assertEquals("Names", attributes.get("tablename"));
        Assert.assertEquals("1", attributes.get("mssqlcdc.row.count"));

        Assert.assertEquals("2017-03-02 00:00:00.001", attributes.get("maxvalue.tran_end_time"));
        Assert.assertEquals("false", attributes.get("fullsnapshot"));

        stateMap = runner.getStateManager().getState(Scope.CLUSTER);
        Assert.assertEquals("2017-03-02 00:00:00.001", stateMap.get("names"));
    }

    @Test
    public void testBaselineRowCount() throws SQLException, IOException {
        setupNamesTable();

        runner.setIncomingConnection(false);

        runner.setProperty(CaptureChangeMSSQL.CDC_TABLES, "Names");
        runner.setProperty(CaptureChangeMSSQL.TAKE_INITIAL_SNAPSHOT, "true");
        runner.setProperty(CaptureChangeMSSQL.FULL_SNAPSHOT_ROW_LIMIT, "2");

        runner.run();

        runner.assertAllFlowFilesTransferred(CaptureChangeMSSQL.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CaptureChangeMSSQL.REL_SUCCESS).get(0);

        Map<String,String> attributes = flowFile.getAttributes();

        Assert.assertTrue("Tablename attribute", attributes.containsKey("tablename"));
        Assert.assertTrue("CDC row count attribute", attributes.containsKey("mssqlcdc.row.count"));
        Assert.assertTrue("Maximum Transacation End Time attribute", attributes.containsKey("maxvalue.tran_end_time"));

        Assert.assertEquals("Names", attributes.get("tablename"));
        Assert.assertEquals("6", attributes.get("mssqlcdc.row.count"));

        //since a full data snapshot was taken, the CURRENT_TIMESTAMP from the database is used
        Assert.assertEquals("2017-03-01 01:01:01.123", attributes.get("maxvalue.tran_end_time"));
        Assert.assertEquals("true", attributes.get("fullsnapshot"));

        StateMap stateMap = runner.getStateManager().getState(Scope.CLUSTER);
        Assert.assertEquals("2017-03-01 01:01:01.123", stateMap.get("names"));

        runner.clearTransferState();
        runner.run();

        runner.assertTransferCount(CaptureChangeMSSQL.REL_SUCCESS, 0);

        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        //Add one row, past current max timestamp, make sure we get a CDC only output
        stmt.execute("insert into cdc.lsn_time_mapping (start_lsn, tran_begin_time, tran_end_time, tran_id, tran_begin_lsn) VALUES (12360, '2017-03-01 23:59:58.123', "
                + "'2017-03-02 00:00:00.001', 10440, 12360)");
        stmt.execute("insert into dbo.\"Names\" (\"ID\", \"FirstName\", \"LastName\") VALUES (1900, 'Chris', 'Stone')");
        stmt.execute("insert into cdc.\"dbo_Names_CT\" (\"__$start_lsn\", \"__$end_lsn\", \"__$seqval\", \"__$operation\", \"__$update_mask\", "
                + "\"ID\", \"FirstName\", \"LastName\") VALUES (12360, 12360, 1, 1, 0, 1900, 'Juan', 'Stone')");

        runner.clearTransferState();
        runner.run();

        runner.assertAllFlowFilesTransferred(CaptureChangeMSSQL.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(CaptureChangeMSSQL.REL_SUCCESS).get(0);

        attributes = flowFile.getAttributes();

        Assert.assertTrue("Tablename attribute", attributes.containsKey("tablename"));
        Assert.assertTrue("CDC row count attribute", attributes.containsKey("mssqlcdc.row.count"));
        Assert.assertTrue("Maximum Transacation End Time attribute", attributes.containsKey("maxvalue.tran_end_time"));

        Assert.assertEquals("Names", attributes.get("tablename"));
        Assert.assertEquals("1", attributes.get("mssqlcdc.row.count"));

        Assert.assertEquals("2017-03-02 00:00:00.001", attributes.get("maxvalue.tran_end_time"));
        Assert.assertEquals("false", attributes.get("fullsnapshot"));

        stateMap = runner.getStateManager().getState(Scope.CLUSTER);
        Assert.assertEquals("2017-03-02 00:00:00.001", stateMap.get("names"));


        //Add two rows so we go over the row limit and get a full snapshot
        stmt.execute("insert into cdc.lsn_time_mapping (start_lsn, tran_begin_time, tran_end_time, tran_id, tran_begin_lsn) VALUES (12360, '2017-02-28 23:59:01.123', "
                + "'2017-03-02 00:01:01.123', 10040, 12360)");
        stmt.execute("insert into dbo.\"Names\" (\"ID\", \"FirstName\", \"LastName\") VALUES (2100, 'James', 'Stone')");
        stmt.execute("insert into cdc.\"dbo_Names_CT\" (\"__$start_lsn\", \"__$end_lsn\", \"__$seqval\", \"__$operation\", \"__$update_mask\", "
                + "\"ID\", \"FirstName\", \"LastName\") VALUES (12360, 12360, 1, 1, 0, 2100, 'James', 'Stone')");
        stmt.execute("insert into dbo.\"Names\" (\"ID\", \"FirstName\", \"LastName\") VALUES (2200, 'Clark', 'Stone')");
        stmt.execute("insert into cdc.\"dbo_Names_CT\" (\"__$start_lsn\", \"__$end_lsn\", \"__$seqval\", \"__$operation\", \"__$update_mask\", "
                + "\"ID\", \"FirstName\", \"LastName\") VALUES (12360, 12360, 1, 1, 0, 2200, 'Clark', 'Stone')");

        //Update the "CURRENT_TIMESTAMP" value, which gets used
        // as the current tran_end_time marker for full snapshots
        processor.db_timestamp = "2017-03-03 01:01:01.123";

        runner.clearTransferState();
        runner.run();

        runner.assertAllFlowFilesTransferred(CaptureChangeMSSQL.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(CaptureChangeMSSQL.REL_SUCCESS).get(0);

        attributes = flowFile.getAttributes();

        Assert.assertTrue("Tablename attribute", attributes.containsKey("tablename"));
        Assert.assertTrue("CDC row count attribute", attributes.containsKey("mssqlcdc.row.count"));
        Assert.assertTrue("Maximum Transacation End Time attribute", attributes.containsKey("maxvalue.tran_end_time"));

        Assert.assertEquals("Names", attributes.get("tablename"));
        Assert.assertEquals("9", attributes.get("mssqlcdc.row.count"));
        Assert.assertEquals("true", attributes.get("fullsnapshot"));
        Assert.assertEquals("2017-03-03 01:01:01.123", attributes.get("maxvalue.tran_end_time"));

        stateMap = runner.getStateManager().getState(Scope.CLUSTER);
        Assert.assertEquals("2017-03-03 01:01:01.123", stateMap.get("names"));
    }


    private void setupNamesTable() throws SQLException {
        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try{
            stmt.execute("DROP TABLE cdc.\"dbo_Names_CT\"");
        } catch(SQLException e){

        }

        try{
            stmt.execute("DROP TABLE dbo.\"Names\"");
        } catch(SQLException e){

        }

        try{
            stmt.execute("DROP TABLE cdc.\"dbo_Names2_CT\"");
        } catch(SQLException e){

        }

        try{
            stmt.execute("DROP TABLE dbo.\"Names2\"");
        } catch(SQLException e){

        }

        stmt.execute("CREATE TABLE cdc.\"dbo_Names_CT\"(\n" +
                "\"__$start_lsn\" int,\n" +
                "\"__$end_lsn\" int,\n" +
                "\"__$seqval\" int,\n" +
                "\"__$operation\" int,\n" +
                "\"__$update_mask\" int," +
                "\"ID\" int,\n" +
                "\"FirstName\" varchar(128),\n" +
                "\"LastName\" varchar(128))");

        stmt.execute("CREATE TABLE dbo.\"Names\"(\n" +
                "\"ID\" int,\n" +
                "\"FirstName\" varchar(128),\n" +
                "\"LastName\" varchar(128))");

        stmt.execute("CREATE TABLE cdc.\"dbo_Names2_CT\"(\n" +
                "\"__$start_lsn\" int,\n" +
                "\"__$end_lsn\" int,\n" +
                "\"__$seqval\" int,\n" +
                "\"__$operation\" int,\n" +
                "\"__$update_mask\" int," +
                "\"ID\" int,\n" +
                "\"Kanji\" varchar(128))");

        stmt.execute("CREATE TABLE dbo.\"Names2\"(\n" +
                "\"ID\" int,\n" +
                "\"Kanji\" varchar(128))");

        //Empty CDC tables
        stmt.execute("DELETE FROM cdc.change_tables");
        stmt.execute("DELETE FROM cdc.lsn_time_mapping");
        stmt.execute("DELETE FROM cdc.index_columns");
        stmt.execute("DELETE FROM cdc.captured_columns");

        stmt.execute("insert into cdc.change_tables (object_id, schemaName, tableName, sourceSchemaName, sourceTableName) VALUES (1, 'cdc', 'dbo_Names_CT', 'dbo', 'Names')");
        stmt.execute("insert into cdc.change_tables (object_id, schemaName, tableName, sourceSchemaName, sourceTableName) VALUES (2, 'cdc', 'dbo_Names2_CT', 'dbo', 'Names2')");

        stmt.execute("insert into cdc.captured_columns (object_id, column_name, column_id, column_type, column_ordinal) VALUES (1, 'ID', 1, 'int', 1)");
        stmt.execute("insert into cdc.captured_columns (object_id, column_name, column_id, column_type, column_ordinal) VALUES (1, 'FirstName', 2, 'nvarchar', 2)");
        stmt.execute("insert into cdc.captured_columns (object_id, column_name, column_id, column_type, column_ordinal) VALUES (1, 'LastName', 3, 'nvarchar', 3)");

        stmt.execute("insert into cdc.captured_columns (object_id, column_name, column_id, column_type, column_ordinal) VALUES (2, 'ID', 1, 'int', 1)");
        stmt.execute("insert into cdc.captured_columns (object_id, column_name, column_id, column_type, column_ordinal) VALUES (2, 'Kanji', 2, 'nvarchar', 2)");

        stmt.execute("insert into cdc.index_columns (object_id, column_name, column_id, index_ordinal) VALUES (1, 'ID', 1, 1)");

        stmt.execute("insert into cdc.index_columns (object_id, column_name, column_id, index_ordinal) VALUES (2, 'ID', 1, 1)");




        //Load in sample data, both into "data tables" and into "cdc tables"

        //Load in some data that predates CDC data
        stmt.execute("insert into dbo.\"Names\" (\"ID\", \"FirstName\", \"LastName\") VALUES (900, 'Jim', 'Chen')");
        stmt.execute("insert into dbo.\"Names\" (\"ID\", \"FirstName\", \"LastName\") VALUES (901, 'Audrey', 'Evans')");
        stmt.execute("insert into dbo.\"Names\" (\"ID\", \"FirstName\", \"LastName\") VALUES (902, 'Hao', 'Chen')");
        stmt.execute("insert into dbo.\"Names\" (\"ID\", \"FirstName\", \"LastName\") VALUES (903, 'Greg', 'Phillips')");

        //Load in data that is in both data tables and in CDC
        stmt.execute("insert into cdc.lsn_time_mapping (start_lsn, tran_begin_time, tran_end_time, tran_id, tran_begin_lsn) VALUES (12345, '2017-01-01 02:03:03.123', "
                + "'2017-01-01 02:03:04.123', 10000, 12345)");
        stmt.execute("insert into dbo.\"Names\" (\"ID\", \"FirstName\", \"LastName\") VALUES (1000, 'John', 'Smith')");
        stmt.execute("insert into cdc.\"dbo_Names_CT\" (\"__$start_lsn\", \"__$end_lsn\", \"__$seqval\", \"__$operation\", \"__$update_mask\", "
                + "\"ID\", \"FirstName\", \"LastName\") VALUES (12345, 12345, 1, 1, 0, 1000, 'John', 'Smith')");

        stmt.execute("insert into cdc.lsn_time_mapping (start_lsn, tran_begin_time, tran_end_time, tran_id, tran_begin_lsn) VALUES (12346, '2017-01-01 02:01:05.123', "
                + "'2017-01-01 02:03:06.567', 10001, 12346)");
        stmt.execute("insert into dbo.\"Names\" (\"ID\", \"FirstName\", \"LastName\") VALUES (1010, 'Ami', 'Smith')");
        stmt.execute("insert into cdc.\"dbo_Names_CT\" (\"__$start_lsn\", \"__$end_lsn\", \"__$seqval\", \"__$operation\", \"__$update_mask\", "
                + "\"ID\", \"FirstName\", \"LastName\") VALUES (12346, 12346, 1, 1, 0, 1010, 'Ami', 'Smith')");

        stmt.execute("insert into dbo.\"Names2\" (\"ID\", \"Kanji\") VALUES (1010, '亜美')");
        stmt.execute("insert into cdc.\"dbo_Names2_CT\" (\"__$start_lsn\", \"__$end_lsn\", \"__$seqval\", \"__$operation\", \"__$update_mask\", "
                + "\"ID\", \"Kanji\") VALUES (12346, 12346, 1, 1, 0, 1010, '亜美')");

        stmt.execute("update dbo.\"Names\" SET \"LastName\"='Smithson' where ID=1010");
        //Pre-update values
        stmt.execute("insert into cdc.\"dbo_Names_CT\" (\"__$start_lsn\", \"__$end_lsn\", \"__$seqval\", \"__$operation\", \"__$update_mask\", "
                + "\"ID\", \"FirstName\", \"LastName\") VALUES (12346, 12346, 2, 3, 0, 1010, 'Ami', 'Smith')");
        //Post-update values
        stmt.execute("insert into cdc.\"dbo_Names_CT\" (\"__$start_lsn\", \"__$end_lsn\", \"__$seqval\", \"__$operation\", \"__$update_mask\", "
                + "\"ID\", \"FirstName\", \"LastName\") VALUES (12346, 12346, 2, 4, 0, 1010, 'Ami', 'Smithson')");
    }

    /**
     * Simple implementation only for CaptureChangeMSSQL processor testing.
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

    @Stateful(scopes = Scope.CLUSTER, description = "Mock for CaptureChangeMSSQL processor")
    public static class MockCaptureChangeMSSQL extends CaptureChangeMSSQL {
        public String db_timestamp = "2017-03-01 01:01:01.123";

        MSSQLCDCUtils mssqlcdcUtils = new MSSQLCDCUtils() {
            @Override
            public String getLIST_CHANGE_TRACKING_TABLES_SQL() {
                return "SELECT object_id,\n" +
                        "  'NiFi_TEST' AS databaseName, \n" +
                        "  schemaName, \n" +
                        "  tableName, \n" +
                        "  sourceSchemaName,\n" +
                        "  sourceTableName\n" +
                        "FROM cdc.change_tables";
            }

            @Override
            public String getLIST_TABLE_COLUMNS() {
                return super.getLIST_TABLE_COLUMNS();
            }

            @Override
            public String getCURRENT_TIMESTAMP() {
                return "TIMESTAMP('" + db_timestamp + "')";
            }
        };

        @Override
        public MSSQLCDCUtils getMssqlcdcUtils() {
            return mssqlcdcUtils;
        }
    }
}
