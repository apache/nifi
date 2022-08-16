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

import org.apache.commons.dbcp2.DelegatingConnection;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.pattern.RollbackOnFailure;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordFailureType;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.file.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PutDatabaseRecordTest {

    private static final String CONNECTION_FAILED = "Connection Failed";

    private static final String PARSER_ID = MockRecordParser.class.getSimpleName();

    private static final String TABLE_NAME = "PERSONS";

    private static final String createPersons = "CREATE TABLE PERSONS (id integer primary key, name varchar(100)," +
            " code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000), dt date)";
    private static final String createPersonsSchema1 = "CREATE TABLE SCHEMA1.PERSONS (id integer primary key, name varchar(100)," +
            " code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000), dt date)";
    private static final String createPersonsSchema2 = "CREATE TABLE SCHEMA2.PERSONS (id2 integer primary key, name varchar(100)," +
            " code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000), dt date)";

    private final static String DB_LOCATION = "target/db_pdr";

    TestRunner runner;
    PutDatabaseRecord processor;
    DBCPServiceSimpleImpl dbcp;

    @BeforeAll
    public static void setDatabaseLocation() {
        System.setProperty("derby.stream.error.file", "target/derby.log");

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        try {
            FileUtils.deleteFile(dbLocation, true);
        } catch (IOException ignore) {
            // Do nothing, may not have existed
        }
    }

    @AfterAll
    public static void shutdownDatabase() throws Exception {
        try {
            DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";shutdown=true");
        } catch (SQLNonTransientConnectionException ignore) {
            // Do nothing, this is what happens at Derby shutdown
        }
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        try {
            FileUtils.deleteFile(dbLocation, true);
        } catch (IOException ignore) {
            // Do nothing, may not have existed
        }
        System.clearProperty("derby.stream.error.file");
    }

    @BeforeEach
    public void setRunner() throws Exception {
        processor = new PutDatabaseRecord();
        //Mock the DBCP Controller Service so we can control the Results
        dbcp = spy(new DBCPServiceSimpleImpl(DB_LOCATION));

        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(processor);
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(PutDatabaseRecord.DBCP_SERVICE, "dbcp");
    }

    @Test
    public void testGetConnectionFailure() throws InitializationException {
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService(PARSER_ID, parser);
        runner.enableControllerService(parser);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, PARSER_ID);
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, TABLE_NAME);

        when(dbcp.getConnection(anyMap())).thenThrow(new ProcessException(CONNECTION_FAILED));

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_FAILURE);
    }

    @Test
    public void testInsertNonRequiredColumnsUnmatchedField() throws InitializationException, ProcessException {
        // Need to override the @Before method with a new processor that behaves badly
        processor = new PutDatabaseRecordUnmatchedField();
        //Mock the DBCP Controller Service so we can control the Results
        dbcp = spy(new DBCPServiceSimpleImpl(DB_LOCATION));

        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(processor);
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(PutDatabaseRecord.DBCP_SERVICE, "dbcp");

        recreateTable();
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService(PARSER_ID, parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("extra", RecordFieldType.STRING);
        parser.addSchemaField("dt", RecordFieldType.DATE);

        LocalDate testDate1 = LocalDate.of(2021, 1, 26);
        Date nifiDate1 = new Date(testDate1.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());
        LocalDate testDate2 = LocalDate.of(2021, 7, 26);
        Date nifiDate2 = new Date(testDate2.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());

        parser.addRecord(1, "rec1", "test", nifiDate1);
        parser.addRecord(2, "rec2", "test", nifiDate2);
        parser.addRecord(3, "rec3", "test", null);
        parser.addRecord(4, "rec4", "test", null);
        parser.addRecord(5, null, null, null);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, PARSER_ID);
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, TABLE_NAME);
        runner.setProperty(PutDatabaseRecord.UNMATCHED_FIELD_BEHAVIOR, PutDatabaseRecord.FAIL_UNMATCHED_FIELD);

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(PutDatabaseRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 1);
    }

    @Test
    void testGeneratePreparedStatements() throws SQLException, MalformedRecordException {

        final List<RecordField> fields = Arrays.asList(new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("name", RecordFieldType.STRING.getDataType()),
                new RecordField("code", RecordFieldType.INT.getDataType()),
                new RecordField("non_existing", RecordFieldType.BOOLEAN.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final PutDatabaseRecord.TableSchema tableSchema = new PutDatabaseRecord.TableSchema(
                Arrays.asList(
                        new PutDatabaseRecord.ColumnDescription("id", 4, true, 2, false),
                        new PutDatabaseRecord.ColumnDescription("name", 12, true, 255, true),
                        new PutDatabaseRecord.ColumnDescription("code", 4, true, 10, true)
                ),
                false,
                new HashSet<String>(Arrays.asList("id")),
                ""
        );

        runner.setProperty(PutDatabaseRecord.TRANSLATE_FIELD_NAMES, "false");
        runner.setProperty(PutDatabaseRecord.UNMATCHED_FIELD_BEHAVIOR, PutDatabaseRecord.IGNORE_UNMATCHED_FIELD);
        runner.setProperty(PutDatabaseRecord.UNMATCHED_COLUMN_BEHAVIOR, PutDatabaseRecord.IGNORE_UNMATCHED_COLUMN);
        runner.setProperty(PutDatabaseRecord.QUOTE_IDENTIFIERS, "false");
        runner.setProperty(PutDatabaseRecord.QUOTE_TABLE_IDENTIFIER, "false");
        final PutDatabaseRecord.DMLSettings settings = new PutDatabaseRecord.DMLSettings(runner.getProcessContext());

        assertEquals("INSERT INTO PERSONS (id, name, code) VALUES (?,?,?)",
                processor.generateInsert(schema, "PERSONS", tableSchema, settings).getSql());
        assertEquals("UPDATE PERSONS SET name = ?, code = ? WHERE id = ?",
                processor.generateUpdate(schema, "PERSONS", null, tableSchema, settings).getSql());
        assertEquals("DELETE FROM PERSONS WHERE (id = ?) AND (name = ? OR (name is null AND ? is null)) AND (code = ? OR (code is null AND ? is null))",
                processor.generateDelete(schema, "PERSONS", tableSchema, settings).getSql());
    }

    @Test
    void testGeneratePreparedStatementsFailUnmatchedField() throws SQLException, MalformedRecordException {

        final List<RecordField> fields = Arrays.asList(new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("name", RecordFieldType.STRING.getDataType()),
                new RecordField("code", RecordFieldType.INT.getDataType()),
                new RecordField("non_existing", RecordFieldType.BOOLEAN.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final PutDatabaseRecord.TableSchema tableSchema = new PutDatabaseRecord.TableSchema(
                Arrays.asList(
                        new PutDatabaseRecord.ColumnDescription("id", 4, true, 2, false),
                        new PutDatabaseRecord.ColumnDescription("name", 12, true, 255, true),
                        new PutDatabaseRecord.ColumnDescription("code", 4, true, 10, true)
                ),
                false,
                new HashSet<String>(Arrays.asList("id")),
                ""
        );

        runner.setProperty(PutDatabaseRecord.TRANSLATE_FIELD_NAMES, "false");
        runner.setProperty(PutDatabaseRecord.UNMATCHED_FIELD_BEHAVIOR, PutDatabaseRecord.FAIL_UNMATCHED_FIELD);
        runner.setProperty(PutDatabaseRecord.UNMATCHED_COLUMN_BEHAVIOR, PutDatabaseRecord.IGNORE_UNMATCHED_COLUMN);
        runner.setProperty(PutDatabaseRecord.QUOTE_IDENTIFIERS, "false");
        runner.setProperty(PutDatabaseRecord.QUOTE_TABLE_IDENTIFIER, "false");
        final PutDatabaseRecord.DMLSettings settings = new PutDatabaseRecord.DMLSettings(runner.getProcessContext());

        SQLDataException e = assertThrows(SQLDataException.class,
                () -> processor.generateInsert(schema, "PERSONS", tableSchema, settings),
                "generateInsert should fail with unmatched fields");
        assertEquals("Cannot map field 'non_existing' to any column in the database\nColumns: id,name,code", e.getMessage());

        e = assertThrows(SQLDataException.class,
                () -> processor.generateUpdate(schema, "PERSONS", null, tableSchema, settings),
                "generateUpdate should fail with unmatched fields");
        assertEquals("Cannot map field 'non_existing' to any column in the database\nColumns: id,name,code", e.getMessage());

        e = assertThrows(SQLDataException.class,
                () -> processor.generateDelete(schema, "PERSONS", tableSchema, settings),
                "generateDelete should fail with unmatched fields");
        assertEquals("Cannot map field 'non_existing' to any column in the database\nColumns: id,name,code", e.getMessage());
    }

    @Test
    void testInsert() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);
        parser.addSchemaField("dt", RecordFieldType.DATE);

        LocalDate testDate1 = LocalDate.of(2021, 1, 26);
        Date jdbcDate1 = Date.valueOf(testDate1); // in local TZ
        LocalDate testDate2 = LocalDate.of(2021, 7, 26);
        Date jdbcDate2 = Date.valueOf(testDate2); // in local TZ

        parser.addRecord(1, "rec1", 101, jdbcDate1);
        parser.addRecord(2, "rec2", 102, jdbcDate2);
        parser.addRecord(3, "rec3", 103, null);
        parser.addRecord(4, "rec4", 104, null);
        parser.addRecord(5, null, 105, null);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("rec1", rs.getString(2));
        assertEquals(101, rs.getInt(3));
        assertEquals(jdbcDate1.toString(), rs.getDate(4).toString());
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("rec2", rs.getString(2));
        assertEquals(102, rs.getInt(3));
        assertEquals(jdbcDate2.toString(), rs.getDate(4).toString());
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals("rec3", rs.getString(2));
        assertEquals(103, rs.getInt(3));
        assertNull(rs.getDate(4));
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertEquals("rec4", rs.getString(2));
        assertEquals(104, rs.getInt(3));
        assertNull(rs.getDate(4));
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
        assertNull(rs.getString(2));
        assertEquals(105, rs.getInt(3));
        assertNull(rs.getDate(4));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    void testInsertNonRequiredColumns() throws InitializationException, ProcessException, SQLException {
        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("dt", RecordFieldType.DATE);

        LocalDate testDate1 = LocalDate.of(2021, 1, 26);
        Date jdbcDate1 = Date.valueOf(testDate1); // in local TZ
        LocalDate testDate2 = LocalDate.of(2021, 7, 26);
        Date jdbcDate2 = Date.valueOf(testDate2); // in local TZ

        parser.addRecord(1, "rec1", jdbcDate1);
        parser.addRecord(2, "rec2", jdbcDate2);
        parser.addRecord(3, "rec3", null);
        parser.addRecord(4, "rec4", null);
        parser.addRecord(5, null, null);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("rec1", rs.getString(2));
        // Zero value because of the constraint
        assertEquals(0, rs.getInt(3));
        assertEquals(jdbcDate1.toString(), rs.getDate(4).toString());
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("rec2", rs.getString(2));
        assertEquals(0, rs.getInt(3));
        assertEquals(jdbcDate2.toString(), rs.getDate(4).toString());
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals("rec3", rs.getString(2));
        assertEquals(0, rs.getInt(3));
        assertNull(rs.getDate(4));
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertEquals("rec4", rs.getString(2));
        assertEquals(0, rs.getInt(3));
        assertNull(rs.getDate(4));
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
        assertNull(rs.getString(2));
        assertEquals(0, rs.getInt(3));
        assertNull(rs.getDate(4));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    void testInsertBatchUpdateException() throws InitializationException, ProcessException, SQLException {
        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord(1, "rec1", 101);
        parser.addRecord(2, "rec2", 102);
        parser.addRecord(3, "rec3", 1000);   // This record violates the constraint on the "code" column so should result in FlowFile being routed to failure
        parser.addRecord(4, "rec4", 104);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_FAILURE, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        // Transaction should be rolled back and table should remain empty.
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    void testInsertBatchUpdateExceptionRollbackOnFailure() throws InitializationException, ProcessException, SQLException {
        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord(1, "rec1", 101);
        parser.addRecord(2, "rec2", 102);
        parser.addRecord(3, "rec3", 1000);
        parser.addRecord(4, "rec4", 104);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, "true");

        runner.enqueue(new byte[0]);
        runner.run();

        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        // Transaction should be rolled back and table should remain empty.
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    void testInsertNoTableSpecified() throws InitializationException, ProcessException, SQLException {
        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord(1, "rec1", 101);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "${not.a.real.attr}");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 1);
    }

    @Test
    void testInsertNoTableExists() throws InitializationException, ProcessException, SQLException {
        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord(1, "rec1", 101);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS2");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutDatabaseRecord.REL_FAILURE).get(0);
        final String errorMessage = flowFile.getAttribute("putdatabaserecord.error");
        assertTrue(errorMessage.contains("PERSONS2"));
    }

    @Test
    void testInsertViaSqlStatementType() throws InitializationException, ProcessException, SQLException {
        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("sql", RecordFieldType.STRING);

        parser.addRecord("INSERT INTO PERSONS (id, name, code) VALUES (1, 'rec1',101)");
        parser.addRecord("INSERT INTO PERSONS (id, name, code) VALUES (2, 'rec2',102)");

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.USE_ATTR_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");
        runner.setProperty(PutDatabaseRecord.FIELD_CONTAINING_SQL, "sql");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(PutDatabaseRecord.STATEMENT_TYPE_ATTRIBUTE, "sql");
        runner.enqueue(new byte[0], attrs);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("rec1", rs.getString(2));
        assertEquals(101, rs.getInt(3));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("rec2", rs.getString(2));
        assertEquals(102, rs.getInt(3));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    void testMultipleInsertsViaSqlStatementType() throws InitializationException, ProcessException, SQLException {
        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("sql", RecordFieldType.STRING);

        parser.addRecord("INSERT INTO PERSONS (id, name, code) VALUES (1, 'rec1',101);INSERT INTO PERSONS (id, name, code) VALUES (2, 'rec2',102)");

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.USE_ATTR_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");
        runner.setProperty(PutDatabaseRecord.FIELD_CONTAINING_SQL, "sql");
        runner.setProperty(PutDatabaseRecord.ALLOW_MULTIPLE_STATEMENTS, "true");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(PutDatabaseRecord.STATEMENT_TYPE_ATTRIBUTE, "sql");
        runner.enqueue(new byte[0], attrs);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("rec1", rs.getString(2));
        assertEquals(101, rs.getInt(3));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("rec2", rs.getString(2));
        assertEquals(102, rs.getInt(3));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    void testMultipleInsertsViaSqlStatementTypeBadSQL() throws InitializationException, ProcessException, SQLException {
        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("sql", RecordFieldType.STRING);

        parser.addRecord("INSERT INTO PERSONS (id, name, code) VALUES (1, 'rec1',101);" +
                        "INSERT INTO PERSONS (id, name, code) VALUES (2, 'rec2',102);" +
                        "INSERT INTO PERSONS2 (id, name, code) VALUES (2, 'rec2',102);");

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.USE_ATTR_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");
        runner.setProperty(PutDatabaseRecord.FIELD_CONTAINING_SQL, "sql");
        runner.setProperty(PutDatabaseRecord.ALLOW_MULTIPLE_STATEMENTS, "true");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(PutDatabaseRecord.STATEMENT_TYPE_ATTRIBUTE, "sql");
        runner.enqueue(new byte[0], attrs);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        // The first two legitimate statements should have been rolled back
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    void testInvalidData() throws InitializationException, ProcessException, SQLException {
        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord(1, "rec1", 101);
        parser.addRecord(2, "rec2", 102);
        parser.addRecord(3, "rec3", 104);

        parser.failAfter(1);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_FAILURE, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        // Transaction should be rolled back and table should remain empty.
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    void testIOExceptionOnReadData() throws InitializationException, ProcessException, SQLException {
        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord(1, "rec1", 101);
        parser.addRecord(2, "rec2", 102);
        parser.addRecord(3, "rec3", 104);

        parser.failAfter(1, MockRecordFailureType.IO_EXCEPTION);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_FAILURE, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        // Transaction should be rolled back and table should remain empty.
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    void testSqlStatementTypeNoValue() throws InitializationException, ProcessException, SQLException {
        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("sql", RecordFieldType.STRING);

        parser.addRecord("");

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.USE_ATTR_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");
        runner.setProperty(PutDatabaseRecord.FIELD_CONTAINING_SQL, "sql");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(PutDatabaseRecord.STATEMENT_TYPE_ATTRIBUTE, "sql");
        runner.enqueue(new byte[0], attrs);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 1);
    }

    @Test
    void testSqlStatementTypeNoValueRollbackOnFailure() throws InitializationException, ProcessException, SQLException {
        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("sql", RecordFieldType.STRING);

        parser.addRecord("");

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.USE_ATTR_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");
        runner.setProperty(PutDatabaseRecord.FIELD_CONTAINING_SQL, "sql");
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, "true");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(PutDatabaseRecord.STATEMENT_TYPE_ATTRIBUTE, "sql");
        runner.enqueue(new byte[0], attrs);

        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 0);
    }

    @Test
    void testUpdate() throws InitializationException, ProcessException, SQLException {
        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord(1, "rec1", 201);
        parser.addRecord(2, "rec2", 202);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.UPDATE_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        // Set some existing records with different values for name and code
        final Connection conn = dbcp.getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("INSERT INTO PERSONS VALUES (1,'x1',101, null)");
        stmt.execute("INSERT INTO PERSONS VALUES (2,'x2',102, null)");
        stmt.close();

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("rec1", rs.getString(2));
        assertEquals(201, rs.getInt(3));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("rec2", rs.getString(2));
        assertEquals(202, rs.getInt(3));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    void testUpdatePkNotFirst() throws InitializationException, ProcessException, SQLException {
        recreateTable("CREATE TABLE PERSONS (name varchar(100), id integer primary key, code integer)");
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord("rec1", 1, 201);
        parser.addRecord("rec2", 2, 202);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.UPDATE_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        // Set some existing records with different values for name and code
        final Connection conn = dbcp.getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("INSERT INTO PERSONS VALUES ('x1', 1, 101)");
        stmt.execute("INSERT INTO PERSONS VALUES ('x2', 2, 102)");
        stmt.close();

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals("rec1", rs.getString(1));
        assertEquals(1, rs.getInt(2));
        assertEquals(201, rs.getInt(3));
        assertTrue(rs.next());
        assertEquals("rec2", rs.getString(1));
        assertEquals(2, rs.getInt(2));
        assertEquals(202, rs.getInt(3));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    void testUpdateMultipleSchemas() throws InitializationException, ProcessException, SQLException {
        // Manually create and drop the tables and schemas
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        stmt.execute("create schema SCHEMA1");
        stmt.execute("create schema SCHEMA2");
        stmt.execute(createPersonsSchema1);
        stmt.execute(createPersonsSchema2);

        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord(1, "rec1", 201);
        parser.addRecord(2, "rec2", 202);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.UPDATE_TYPE);
        runner.setProperty(PutDatabaseRecord.SCHEMA_NAME, "SCHEMA1");
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        // Set some existing records with different values for name and code
        Exception e;
        ResultSet rs;

        stmt.execute("INSERT INTO SCHEMA1.PERSONS VALUES (1,'x1',101,null)");
        stmt.execute("INSERT INTO SCHEMA2.PERSONS VALUES (2,'x2',102,null)");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        rs = stmt.executeQuery("SELECT * FROM SCHEMA1.PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("rec1", rs.getString(2));
        assertEquals(201, rs.getInt(3));
        assertFalse(rs.next());
        rs = stmt.executeQuery("SELECT * FROM SCHEMA2.PERSONS");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        // Values should not have been updated
        assertEquals("x2", rs.getString(2));
        assertEquals(102, rs.getInt(3));
        assertFalse(rs.next());

        // Drop the schemas here so as not to interfere with other tests
        stmt.execute("drop table SCHEMA1.PERSONS");
        stmt.execute("drop table SCHEMA2.PERSONS");
        stmt.execute("drop schema SCHEMA1 RESTRICT");
        stmt.execute("drop schema SCHEMA2 RESTRICT");
        stmt.close();

        // Don't proceed if there was a problem with the asserts
        rs = conn.getMetaData().getSchemas();
        List<String> schemas = new ArrayList<>();
        while(rs.next()) {
            schemas.add(rs.getString(1));
        }
        assertFalse(schemas.contains("SCHEMA1"));
        assertFalse(schemas.contains("SCHEMA2"));
        conn.close();
    }

    @Test
    void testUpdateAfterInsert() throws InitializationException, ProcessException, SQLException {
        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord(1, "rec1", 101);
        parser.addRecord(2, "rec2", 102);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        final Connection conn = dbcp.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("rec1", rs.getString(2));
        assertEquals(101, rs.getInt(3));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("rec2", rs.getString(2));
        assertEquals(102, rs.getInt(3));
        assertFalse(rs.next());
        stmt.close();
        runner.clearTransferState();

        parser.addRecord(1, "rec1", 201);
        parser.addRecord(2, "rec2", 202);
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.UPDATE_TYPE);
        runner.enqueue(new byte[0]);
        runner.run(1, true, false);

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        stmt = conn.createStatement();
        rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("rec1", rs.getString(2));
        assertEquals(201, rs.getInt(3));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("rec2", rs.getString(2));
        assertEquals(202, rs.getInt(3));
        assertFalse(rs.next());
        stmt.close();
        conn.close();
    }

    @Test
    void testUpdateNoPrimaryKeys() throws InitializationException, ProcessException, SQLException {
        recreateTable("CREATE TABLE PERSONS (id integer, name varchar(100), code integer)");
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.UPDATE_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        parser.addRecord(1, "rec1", 201);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutDatabaseRecord.REL_FAILURE).get(0);
        assertEquals("Table 'PERSONS' not found or does not have a Primary Key and no Update Keys were specified", flowFile.getAttribute(PutDatabaseRecord.PUT_DATABASE_RECORD_ERROR));
    }

    @Test
    void testUpdateSpecifyUpdateKeys() throws InitializationException, ProcessException, SQLException {
        recreateTable("CREATE TABLE PERSONS (id integer, name varchar(100), code integer)");
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord(1, "rec1", 201);
        parser.addRecord(2, "rec2", 202);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.UPDATE_TYPE);
        runner.setProperty(PutDatabaseRecord.UPDATE_KEYS, "id");
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        // Set some existing records with different values for name and code
        final Connection conn = dbcp.getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("INSERT INTO PERSONS VALUES (1,'x1',101)");
        stmt.execute("INSERT INTO PERSONS VALUES (2,'x2',102)");
        stmt.close();

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("rec1", rs.getString(2));
        assertEquals(201, rs.getInt(3));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("rec2", rs.getString(2));
        assertEquals(202, rs.getInt(3));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    void testUpdateSpecifyUpdateKeysNotFirst() throws InitializationException, ProcessException, SQLException {
        recreateTable("CREATE TABLE PERSONS (id integer, name varchar(100), code integer)");
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord(1, "rec1", 201);
        parser.addRecord(2, "rec2", 202);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.UPDATE_TYPE);
        runner.setProperty(PutDatabaseRecord.UPDATE_KEYS, "code");
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        // Set some existing records with different values for name and code
        final Connection conn = dbcp.getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("INSERT INTO PERSONS VALUES (10,'x1',201)");
        stmt.execute("INSERT INTO PERSONS VALUES (12,'x2',202)");
        stmt.close();

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("rec1", rs.getString(2));
        assertEquals(201, rs.getInt(3));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("rec2", rs.getString(2));
        assertEquals(202, rs.getInt(3));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    void testUpdateSpecifyQuotedUpdateKeys() throws InitializationException, ProcessException, SQLException {
        recreateTable("CREATE TABLE PERSONS (\"id\" integer, name varchar(100), code integer)");
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord(1, "rec1", 201);
        parser.addRecord(2, "rec2", 202);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.UPDATE_TYPE);
        runner.setProperty(PutDatabaseRecord.UPDATE_KEYS, "${updateKey}");
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");
        runner.setProperty(PutDatabaseRecord.QUOTE_IDENTIFIERS, "true");

        // Set some existing records with different values for name and code
        final Connection conn = dbcp.getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("INSERT INTO PERSONS VALUES (1,'x1',101)");
        stmt.execute("INSERT INTO PERSONS VALUES (2,'x2',102)");
        stmt.close();

        runner.enqueue(new byte[0], Collections.singletonMap("updateKey", "id"));
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("rec1", rs.getString(2));
        assertEquals(201, rs.getInt(3));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("rec2", rs.getString(2));
        assertEquals(202, rs.getInt(3));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    void testDelete() throws InitializationException, ProcessException, SQLException {
        recreateTable(createPersons);
        Connection conn = dbcp.getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("INSERT INTO PERSONS VALUES (1, 'rec1', 101, null)");
        stmt.execute("INSERT INTO PERSONS VALUES (2, 'rec2', 102, null)");
        stmt.execute("INSERT INTO PERSONS VALUES (3, 'rec3', 103, null)");
        stmt.close();

        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord(2, "rec2", 102);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.DELETE_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("rec1", rs.getString(2));
        assertEquals(101, rs.getInt(3));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals("rec3", rs.getString(2));
        assertEquals(103, rs.getInt(3));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    void testDeleteWithNulls() throws InitializationException, ProcessException, SQLException {
        recreateTable(createPersons);
        Connection conn = dbcp.getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("INSERT INTO PERSONS VALUES (1, 'rec1', 101, null)");
        stmt.execute("INSERT INTO PERSONS VALUES (2, 'rec2', null, null)");
        stmt.execute("INSERT INTO PERSONS VALUES (3, 'rec3', 103, null)");
        stmt.close();

        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord(2, "rec2", null);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.DELETE_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("rec1", rs.getString(2));
        assertEquals(101, rs.getInt(3));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals("rec3", rs.getString(2));
        assertEquals(103, rs.getInt(3));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    void testRecordPathOptions() throws InitializationException, SQLException {
        recreateTable("CREATE TABLE PERSONS (id integer, name varchar(100), code integer)");
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        final List<RecordField> dataFields = new ArrayList<>();
        dataFields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        dataFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        dataFields.add(new RecordField("code", RecordFieldType.INT.getDataType()));

        final RecordSchema dataSchema = new SimpleRecordSchema(dataFields);
        parser.addSchemaField("operation", RecordFieldType.STRING);
        parser.addSchemaField(new RecordField("data", RecordFieldType.RECORD.getRecordDataType(dataSchema)));

        // CREATE, CREATE, CREATE, DELETE, UPDATE
        parser.addRecord("INSERT", new MapRecord(dataSchema, createValues(1, "John Doe", 55)));
        parser.addRecord("INSERT", new MapRecord(dataSchema, createValues(2, "Jane Doe", 44)));
        parser.addRecord("INSERT", new MapRecord(dataSchema, createValues(3, "Jim Doe", 2)));
        parser.addRecord("DELETE", new MapRecord(dataSchema, createValues(2, "Jane Doe", 44)));
        parser.addRecord("UPDATE", new MapRecord(dataSchema, createValues(1, "John Doe", 201)));

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.USE_RECORD_PATH);
        runner.setProperty(PutDatabaseRecord.DATA_RECORD_PATH, "/data");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE_RECORD_PATH, "/operation");
        runner.setProperty(PutDatabaseRecord.UPDATE_KEYS, "id");
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_SUCCESS, 1);

        Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("John Doe", rs.getString(2));
        assertEquals(201, rs.getInt(3));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals("Jim Doe", rs.getString(2));
        assertEquals(2, rs.getInt(3));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    void testInsertWithMaxBatchSize() throws InitializationException, ProcessException, SQLException {
        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        for (int i = 1; i < 12; i++) {
            parser.addRecord(i, String.format("rec%s", i), 100 + i);
        }

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");
        runner.setProperty(PutDatabaseRecord.MAX_BATCH_SIZE, "5");

        Supplier<PreparedStatement> spyStmt = createPreparedStatementSpy();

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);

        assertEquals(11, getTableSize());

        assertNotNull(spyStmt.get());
        verify(spyStmt.get(), times(3)).executeBatch();
    }

    @Test
    void testInsertWithDefaultMaxBatchSize() throws InitializationException, ProcessException, SQLException {
        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        for (int i = 1; i < 12; i++) {
            parser.addRecord(i, String.format("rec%s", i), 100 + i);
        }

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        Supplier<PreparedStatement> spyStmt = createPreparedStatementSpy();

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);

        assertEquals(11, getTableSize());

        assertNotNull(spyStmt.get());
        verify(spyStmt.get(), times(1)).executeBatch();
    }

    @Test
    void testGenerateTableName() throws Exception {
        final List<RecordField> fields = Arrays.asList(new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("name", RecordFieldType.STRING.getDataType()),
                new RecordField("code", RecordFieldType.INT.getDataType()),
                new RecordField("non_existing", RecordFieldType.BOOLEAN.getDataType())
        );

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final PutDatabaseRecord.TableSchema tableSchema = new PutDatabaseRecord.TableSchema(
                Arrays.asList(
                        new PutDatabaseRecord.ColumnDescription("id", 4, true, 2, false),
                        new PutDatabaseRecord.ColumnDescription("name", 12, true, 255, true),
                        new PutDatabaseRecord.ColumnDescription("code", 4, true, 10, true)
                ),
                false,
                new HashSet<>(Arrays.asList("id")),
                ""
        );

        runner.setProperty(PutDatabaseRecord.TRANSLATE_FIELD_NAMES, "false");
        runner.setProperty(PutDatabaseRecord.UNMATCHED_FIELD_BEHAVIOR, PutDatabaseRecord.IGNORE_UNMATCHED_FIELD);
        runner.setProperty(PutDatabaseRecord.UNMATCHED_COLUMN_BEHAVIOR, PutDatabaseRecord.IGNORE_UNMATCHED_COLUMN);
        runner.setProperty(PutDatabaseRecord.QUOTE_IDENTIFIERS, "true");
        runner.setProperty(PutDatabaseRecord.QUOTE_TABLE_IDENTIFIER, "true");
        final PutDatabaseRecord.DMLSettings settings = new PutDatabaseRecord.DMLSettings(runner.getProcessContext());


        assertEquals("test_catalog.test_schema.test_table",
                processor.generateTableName(settings, "test_catalog", "test_schema", "test_table", tableSchema));
    }

    @Test
    void testInsertMismatchedCompatibleDataTypes() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);
        parser.addSchemaField("dt", RecordFieldType.BIGINT);

        LocalDate testDate1 = LocalDate.of(2021, 1, 26);
        Date jdbcDate1 = Date.valueOf(testDate1); // in local TZ
        BigInteger nifiDate1 = BigInteger.valueOf(jdbcDate1.getTime()); // in local TZ

        LocalDate testDate2 = LocalDate.of(2021, 7, 26);
        Date jdbcDate2 = Date.valueOf(testDate2); // in local TZ
        BigInteger nifiDate2 = BigInteger.valueOf(jdbcDate2.getTime()); // in local TZ

        parser.addRecord(1, "rec1", 101, nifiDate1);
        parser.addRecord(2, "rec2", 102, nifiDate2);
        parser.addRecord(3, "rec3", 103, null);
        parser.addRecord(4, "rec4", 104, null);
        parser.addRecord(5, null, 105, null);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("rec1", rs.getString(2));
        assertEquals(101, rs.getInt(3));
        assertEquals(jdbcDate1.toString(), rs.getDate(4).toString());
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("rec2", rs.getString(2));
        assertEquals(102, rs.getInt(3));
        assertEquals(jdbcDate2.toString(), rs.getDate(4).toString());
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals("rec3", rs.getString(2));
        assertEquals(103, rs.getInt(3));
        assertNull(rs.getDate(4));
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertEquals("rec4", rs.getString(2));
        assertEquals(104, rs.getInt(3));
        assertNull(rs.getDate(4));
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
        assertNull(rs.getString(2));
        assertEquals(105, rs.getInt(3));
        assertNull(rs.getDate(4));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    void testInsertMismatchedNotCompatibleDataTypes() throws InitializationException, ProcessException, SQLException {
        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.STRING);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);
        parser.addSchemaField("dt", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.FLOAT.getDataType()).getFieldType());

        LocalDate testDate1 = LocalDate.of(2021, 1, 26);
        BigInteger nifiDate1 = BigInteger.valueOf(testDate1.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli()); // in UTC
        Date jdbcDate1 = Date.valueOf(testDate1); // in local TZ
        LocalDate testDate2 = LocalDate.of(2021, 7, 26);
        BigInteger nifiDate2 = BigInteger.valueOf(testDate2.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli()); // in UTC
        Date jdbcDate2 = Date.valueOf(testDate2); // in local TZ

        parser.addRecord("1", "rec1", 101, Arrays.asList(1.0, 2.0));
        parser.addRecord("2", "rec2", 102, Arrays.asList(3.0, 4.0));
        parser.addRecord("3", "rec3", 103, null);
        parser.addRecord("4", "rec4", 104, null);
        parser.addRecord("5", null, 105, null);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        // A SQLFeatureNotSupportedException exception is expected from Derby when you try to put the data as an ARRAY
        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 1);
    }

    @Test
    void testLongVarchar() throws InitializationException, ProcessException, SQLException {
        // Manually create and drop the tables and schemas
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        try {
            stmt.execute("DROP TABLE TEMP");
        } catch(final Exception e) {
            // Do nothing, table may not exist
        }
        stmt.execute("CREATE TABLE TEMP (id integer primary key, name long varchar)");

        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);

        parser.addRecord(1, "rec1");
        parser.addRecord(2, "rec2");

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "TEMP");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        ResultSet rs = stmt.executeQuery("SELECT * FROM TEMP");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("rec1", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("rec2", rs.getString(2));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    void testInsertWithDifferentColumnOrdering() throws InitializationException, ProcessException, SQLException, IOException {
        // Manually create and drop the tables and schemas
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        try {
            stmt.execute("DROP TABLE TEMP");
        } catch(final Exception e) {
            // Do nothing, table may not exist
        }
        stmt.execute("CREATE TABLE TEMP (id integer primary key, code integer, name long varchar)");

        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("code", RecordFieldType.INT);

        // change order of columns
        parser.addRecord("rec1", 1, 101);
        parser.addRecord("rec2", 2, 102);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "TEMP");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        ResultSet rs = stmt.executeQuery("SELECT * FROM TEMP");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(101, rs.getInt(2));
        assertEquals("rec1", rs.getString(3));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals(102, rs.getInt(2));
        assertEquals("rec2", rs.getString(3));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    void testInsertWithBlobClob() throws Exception {
        String createTableWithBlob = "CREATE TABLE PERSONS (id integer primary key, name clob," +
                "content blob, code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000))";

        recreateTable(createTableWithBlob);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        byte[] bytes = "BLOB".getBytes();
        Byte[] blobRecordValue = new Byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            blobRecordValue[i] = Byte.valueOf(bytes[i]);
        }

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);
        parser.addSchemaField("content", RecordFieldType.ARRAY);

        parser.addRecord(1, "rec1", 101, blobRecordValue);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        Clob clob = rs.getClob(2);
        assertNotNull(clob);
        char[] clobText = new char[5];
        int numBytes = clob.getCharacterStream().read(clobText);
        assertEquals(4, numBytes);
        // Ignore last character, it"s meant to ensure that only 4 bytes were read even though the buffer is 5 bytes
        assertEquals("rec1", new String(clobText).substring(0, 4));
        Blob blob = rs.getBlob(3);
        assertEquals("BLOB", new String(blob.getBytes(1, (int) blob.length())));
        assertEquals(101, rs.getInt(4));

        stmt.close();
        conn.close();
    }

    @Test
    void testInsertWithBlobClobObjectArraySource() throws Exception {
        String createTableWithBlob = "CREATE TABLE PERSONS (id integer primary key, name clob," +
                "content blob, code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000))";

        recreateTable(createTableWithBlob);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        byte[] bytes = "BLOB".getBytes();
        Object[] blobRecordValue = new Object[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            blobRecordValue[i] = bytes[i];
        }

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);
        parser.addSchemaField("content", RecordFieldType.ARRAY);

        parser.addRecord(1, "rec1", 101, blobRecordValue);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        Clob clob = rs.getClob(2);
        assertNotNull(clob);
        char[] clobText = new char[5];
        int numBytes = clob.getCharacterStream().read(clobText);
        assertEquals(4, numBytes);
        // Ignore last character, it"s meant to ensure that only 4 bytes were read even though the buffer is 5 bytes
        assertEquals("rec1", new String(clobText).substring(0, 4));
        Blob blob = rs.getBlob(3);
        assertEquals("BLOB", new String(blob.getBytes(1, (int) blob.length())));
        assertEquals(101, rs.getInt(4));

        stmt.close();
        conn.close();
    }

    @Test
    void testInsertWithBlobStringSource() throws Exception {
        String createTableWithBlob = "CREATE TABLE PERSONS (id integer primary key, name clob," +
                "content blob, code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000))";

        recreateTable(createTableWithBlob);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);
        parser.addSchemaField("content", RecordFieldType.STRING);

        parser.addRecord(1, "rec1", 101, "BLOB");

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        Clob clob = rs.getClob(2);
        assertNotNull(clob);
        char[] clobText = new char[5];
        int numBytes = clob.getCharacterStream().read(clobText);
        assertEquals(4, numBytes);
        // Ignore last character, it"s meant to ensure that only 4 bytes were read even though the buffer is 5 bytes
        assertEquals("rec1", new String(clobText).substring(0, 4));
        Blob blob = rs.getBlob(3);
        assertEquals("BLOB", new String(blob.getBytes(1, (int) blob.length())));
        assertEquals(101, rs.getInt(4));

        stmt.close();
        conn.close();
    }

    @Test
    void testInsertWithBlobIntegerArraySource() throws Exception {
        String createTableWithBlob = "CREATE TABLE PERSONS (id integer primary key, name clob," +
                "content blob, code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000))";

        recreateTable(createTableWithBlob);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);
        parser.addSchemaField("content", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType()).getFieldType());

        parser.addRecord(1, "rec1", 101, new Integer[] {1, 2, 3});

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(PutDatabaseRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 1);
    }

    private void recreateTable() throws ProcessException {
        try (final Connection conn = dbcp.getConnection();
            final Statement stmt = conn.createStatement()) {
            stmt.execute("drop table PERSONS");
            stmt.execute(createPersons);
        } catch (SQLException ignore) {
            // Do nothing, may not have existed
        }
    }

    private int getTableSize() throws SQLException {
        try (final Connection connection = dbcp.getConnection()) {
            try (final Statement stmt = connection.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT count(*) FROM PERSONS");
                assertTrue(rs.next());
                return rs.getInt(1);
            }
        }
    }

    private void recreateTable(String createSQL) throws ProcessException, SQLException {
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        try {
            stmt.execute("drop table PERSONS");
        } catch (SQLException ignore) {
            // Do nothing, may not have existed
        }
        stmt.execute(createSQL);
        stmt.close();
        conn.close();
    }

    private Map<String, Object> createValues(final int id, final String name, final int code) {
        final Map<String, Object> values = new HashMap<>();
        values.put("id", id);
        values.put("name", name);
        values.put("code", code);
        return values;
    }

    private Supplier<PreparedStatement> createPreparedStatementSpy() {
        final PreparedStatement[] spyStmt = new PreparedStatement[1];
        final Answer<DelegatingConnection> answer = (inv) -> new DelegatingConnection((Connection) inv.callRealMethod()) {
            @Override
            public PreparedStatement prepareStatement(String sql) throws SQLException {
                spyStmt[0] = spy(getDelegate().prepareStatement(sql));
                return spyStmt[0];
            }
        };
        doAnswer(answer).when(dbcp).getConnection(ArgumentMatchers.anyMap());
        return () -> spyStmt[0];
    }



    static class PutDatabaseRecordUnmatchedField extends PutDatabaseRecord {
        @Override
        SqlAndIncludedColumns generateInsert(RecordSchema recordSchema, String tableName, TableSchema tableSchema, DMLSettings settings) throws IllegalArgumentException {
            return new SqlAndIncludedColumns("INSERT INTO PERSONS VALUES (?,?,?,?)", Arrays.asList(0,1,2,3));
        }
    }
}