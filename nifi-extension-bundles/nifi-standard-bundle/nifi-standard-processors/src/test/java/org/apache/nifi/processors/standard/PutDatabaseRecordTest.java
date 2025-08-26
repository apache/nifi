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
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.pattern.RollbackOnFailure;
import org.apache.nifi.processors.standard.db.ColumnDescription;
import org.apache.nifi.processors.standard.db.NameNormalizer;
import org.apache.nifi.processors.standard.db.TableSchema;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PutDatabaseRecordTest {

    private enum TestCaseEnum {
        // ENABLED means to use that test case in the parameterized tests.
        // DISABLED test cases are used for single-run tests which are not parameterized
        DEFAULT_0(ENABLED, new TestCase(false, false, 0)),
        DEFAULT_1(DISABLED, new TestCase(false, false, 1)),
        DEFAULT_2(DISABLED, new TestCase(null, false, 2)),
        DEFAULT_5(DISABLED, new TestCase(null, false, 5)),
        DEFAULT_1000(DISABLED, new TestCase(false, false, 1000)),

        ROLLBACK_0(DISABLED, new TestCase(false, true, 0)),
        ROLLBACK_1(ENABLED, new TestCase(null, true, 1)),
        ROLLBACK_2(DISABLED, new TestCase(false, true, 2)),
        ROLLBACK_1000(ENABLED, new TestCase(false, true, 1000)),

        // If autoCommit equals true, then rollbackOnFailure must be false AND batchSize must equal 0
        AUTO_COMMIT_0(ENABLED, new TestCase(true, false, 0));

        private final boolean enabled;
        private final TestCase testCase;

        TestCaseEnum(boolean enabled, TestCase t) {
            this.enabled = enabled;
            this.testCase = t;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public TestCase getTestCase() {
            return testCase;
        }

    }

    static Stream<Arguments> getTestCases() {
        return Arrays.stream(TestCaseEnum.values())
                .filter(TestCaseEnum::isEnabled)
                .map(TestCaseEnum::getTestCase)
                .map(Arguments::of);
    }

    private final static boolean ENABLED = true;
    private final static boolean DISABLED = false;

    private final static String DBCP_SERVICE_ID = "dbcp";

    private static final String CONNECTION_FAILED = "Connection Failed";

    private static final String PARSER_ID = MockRecordParser.class.getSimpleName();

    private static final String TABLE_NAME = "PERSONS";

    private static final String createPersons = "CREATE TABLE PERSONS (id integer primary key, name varchar(100)," +
            " code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000), dt date)";
    private static final String createPersonsSchema1 = "CREATE TABLE SCHEMA1.PERSONS (id integer primary key, name varchar(100)," +
            " code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000), dt date)";
    private static final String createPersonsSchema2 = "CREATE TABLE SCHEMA2.PERSONS (id2 integer primary key, name varchar(100)," +
            " code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000), dt date)";

    private static final String createUUIDSchema = "CREATE TABLE UUID_TEST (id integer primary key, name VARCHAR(100))";

    private static final String createLongVarBinarySchema = "CREATE TABLE LONGVARBINARY_TEST (id integer primary key, name LONG VARCHAR FOR BIT DATA)";

    private final static String DB_LOCATION = "target/db_pdr";

    TestRunner runner;
    PutDatabaseRecord processor;
    DBCPService dbcp;

    @BeforeAll
    public static void setDatabaseLocation() {
        System.setProperty("derby.stream.error.file", "target/derby.log");

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        try {
            FileUtils.deleteFile(dbLocation, true);
        } catch (IOException ignored) {
            // Do nothing, may not have existed
        }
    }

    @AfterAll
    public static void shutdownDatabase() {
        try {
            DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";shutdown=true");
        } catch (Exception ignored) {
            // Do nothing, this is what happens at Derby shutdown
        }
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        try {
            FileUtils.deleteFile(dbLocation, true);
        } catch (IOException ignored) {
            // Do nothing, may not have existed
        }
        System.clearProperty("derby.stream.error.file");
    }

    private void setRunner(TestCase testCase) throws InitializationException {
        processor = new PutDatabaseRecord();
        //Mock the DBCP Controller Service so we can control the Results
        dbcp = spy(new DBCPServiceSimpleImpl(DB_LOCATION));

        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(processor);
        runner.addControllerService(DBCP_SERVICE_ID, dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(PutDatabaseRecord.DBCP_SERVICE, DBCP_SERVICE_ID);
        if (testCase.getAutoCommitAsString() == null) {
            runner.removeProperty(PutDatabaseRecord.AUTO_COMMIT);
        } else {
            runner.setProperty(PutDatabaseRecord.AUTO_COMMIT, testCase.getAutoCommitAsString());
        }
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, testCase.getRollbackOnFailureAsString());
        runner.setProperty(PutDatabaseRecord.MAX_BATCH_SIZE, testCase.getBatchSizeAsString());
    }

    @Test
    public void testGetConnectionFailure() throws InitializationException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

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
    public void testSetAutoCommitFalseFailure() throws InitializationException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_1.getTestCase());

        dbcp = new DBCPServiceAutoCommitTest(DB_LOCATION);
        final Map<String, String> dbcpProperties = new HashMap<>();
        runner = TestRunners.newTestRunner(processor);
        runner.addControllerService(DBCP_SERVICE_ID, dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(PutDatabaseRecord.DBCP_SERVICE, DBCP_SERVICE_ID);

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

        parser.addRecord(1, "rec1", 101, jdbcDate1);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_SUCCESS);
    }

    @Test
    public void testProcessExceptionRouteRetry() throws InitializationException {
        setRunner(TestCaseEnum.DEFAULT_1.getTestCase());

        // This exception should route to REL_RETRY because its cause is SQLTransientException
        dbcp = new DBCPServiceThrowConnectionException(new SQLTransientException("connection failed"));
        final Map<String, String> dbcpProperties = new HashMap<>();
        runner = TestRunners.newTestRunner(processor);
        runner.addControllerService(DBCP_SERVICE_ID, dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(PutDatabaseRecord.DBCP_SERVICE, DBCP_SERVICE_ID);

        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_RETRY);
    }

    @Test
    public void testProcessExceptionRouteFailure() throws InitializationException {
        setRunner(TestCaseEnum.DEFAULT_1.getTestCase());

        // This exception should route to REL_FAILURE because its cause is NOT SQLTransientException
        dbcp = new DBCPServiceThrowConnectionException(new NullPointerException("connection is null"));
        final Map<String, String> dbcpProperties = new HashMap<>();
        runner = TestRunners.newTestRunner(processor);
        runner.addControllerService(DBCP_SERVICE_ID, dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(PutDatabaseRecord.DBCP_SERVICE, DBCP_SERVICE_ID);

        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_FAILURE);
    }

    @Test
    public void testInsertNonRequiredColumnsUnmatchedField() throws InitializationException, ProcessException {
        setRunner(TestCaseEnum.DEFAULT_5.getTestCase());

        // Need to override the @Before method with a new processor that behaves badly
        processor = new PutDatabaseRecordUnmatchedField();
        //Mock the DBCP Controller Service so we can control the Results
        dbcp = spy(new DBCPServiceSimpleImpl(DB_LOCATION));

        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(processor);
        runner.addControllerService(DBCP_SERVICE_ID, dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(PutDatabaseRecord.DBCP_SERVICE, DBCP_SERVICE_ID);

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
    public void testGeneratePreparedStatements() throws InitializationException, SQLException, MalformedRecordException {
        setRunner(TestCaseEnum.DEFAULT_1000.getTestCase());

        final List<RecordField> fields = Arrays.asList(new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("name", RecordFieldType.STRING.getDataType()),
                new RecordField("code", RecordFieldType.INT.getDataType()),
                new RecordField("non_existing", RecordFieldType.BOOLEAN.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final TableSchema tableSchema = new TableSchema(
                null,
                null,
                "PERSONS",
                Arrays.asList(
                        new ColumnDescription("id", 4, true, 2, false),
                        new ColumnDescription("name", 12, true, 255, true),
                        new ColumnDescription("code", 4, true, 10, true)
                ),
                false, null,
                new HashSet<>(List.of("id")),
                ""
        );

        runner.setProperty(PutDatabaseRecord.TRANSLATE_FIELD_NAMES, "false");
        runner.setProperty(PutDatabaseRecord.UNMATCHED_FIELD_BEHAVIOR, PutDatabaseRecord.IGNORE_UNMATCHED_FIELD);
        runner.setProperty(PutDatabaseRecord.UNMATCHED_COLUMN_BEHAVIOR, PutDatabaseRecord.IGNORE_UNMATCHED_COLUMN);
        runner.setProperty(PutDatabaseRecord.QUOTE_IDENTIFIERS, "false");
        runner.setProperty(PutDatabaseRecord.QUOTE_TABLE_IDENTIFIER, "false");
        final PutDatabaseRecord.DMLSettings settings = new PutDatabaseRecord.DMLSettings(runner.getProcessContext());

        assertEquals("INSERT INTO PERSONS (id, name, code) VALUES (?,?,?)",
                processor.generateInsert(schema, "PERSONS", tableSchema, settings, null).getSql());
        assertEquals("UPDATE PERSONS SET name = ?, code = ? WHERE id = ?",
                processor.generateUpdate(schema, "PERSONS", null, tableSchema, settings, null).getSql());
        assertEquals("DELETE FROM PERSONS WHERE (id = ?) AND (name = ? OR (name is null AND ? is null)) AND (code = ? OR (code is null AND ? is null))",
                processor.generateDelete(schema, "PERSONS", null, tableSchema, settings, null).getSql());
        assertEquals("DELETE FROM PERSONS WHERE (id = ?) AND (code = ? OR (code is null AND ? is null))",
                processor.generateDelete(schema, "PERSONS", "id, code", tableSchema, settings, null).getSql());
    }

    @Test
    public void testGeneratePreparedStatementsFailUnmatchedField() throws InitializationException {
        setRunner(TestCaseEnum.ROLLBACK_0.getTestCase());

        final List<RecordField> fields = Arrays.asList(new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("name", RecordFieldType.STRING.getDataType()),
                new RecordField("code", RecordFieldType.INT.getDataType()),
                new RecordField("non_existing", RecordFieldType.BOOLEAN.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final TableSchema tableSchema = new TableSchema(
                null,
                null,
                "PERSONS",
                Arrays.asList(
                        new ColumnDescription("id", 4, true, 2, false),
                        new ColumnDescription("name", 12, true, 255, true),
                        new ColumnDescription("code", 4, true, 10, true)
                ),
                false, null,
                new HashSet<>(List.of("id")),
                ""
        );

        runner.setProperty(PutDatabaseRecord.TRANSLATE_FIELD_NAMES, "false");
        runner.setProperty(PutDatabaseRecord.UNMATCHED_FIELD_BEHAVIOR, PutDatabaseRecord.FAIL_UNMATCHED_FIELD);
        runner.setProperty(PutDatabaseRecord.UNMATCHED_COLUMN_BEHAVIOR, PutDatabaseRecord.IGNORE_UNMATCHED_COLUMN);
        runner.setProperty(PutDatabaseRecord.QUOTE_IDENTIFIERS, "false");
        runner.setProperty(PutDatabaseRecord.QUOTE_TABLE_IDENTIFIER, "false");
        final PutDatabaseRecord.DMLSettings settings = new PutDatabaseRecord.DMLSettings(runner.getProcessContext());

        SQLDataException e = assertThrows(SQLDataException.class,
                () -> processor.generateInsert(schema, "PERSONS", tableSchema, settings, null),
                "generateInsert should fail with unmatched fields");
        assertEquals("Cannot map field 'non_existing' to any column in the database\nColumns: id,name,code", e.getMessage());

        e = assertThrows(SQLDataException.class,
                () -> processor.generateUpdate(schema, "PERSONS", null, tableSchema, settings, null),
                "generateUpdate should fail with unmatched fields");
        assertEquals("Cannot map field 'non_existing' to any column in the database\nColumns: id,name,code", e.getMessage());

        e = assertThrows(SQLDataException.class,
                () -> processor.generateDelete(schema, "PERSONS", null, tableSchema, settings, null),
                "generateDelete should fail with unmatched fields");
        assertEquals("Cannot map field 'non_existing' to any column in the database\nColumns: id,name,code", e.getMessage());
    }

    @ParameterizedTest()
    @MethodSource("getTestCases")
    public void testInsert(TestCase testCase) throws InitializationException, ProcessException, SQLException {
        setRunner(testCase);

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
    public void testInsertNonRequiredColumns() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.ROLLBACK_0.getTestCase());

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
    public void testInsertBatchUpdateException() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord(1, "rec1", 101);
        parser.addRecord(2, "rec2", 102);
        // This record violates the constraint on the "code" column so should result in FlowFile routing to failure
        parser.addRecord(3, "rec3", 1000);
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
    public void testInsertBatchUpdateExceptionRollbackOnFailure() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.ROLLBACK_1000.getTestCase());

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
    public void testInsertNoTableSpecified() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

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
    public void testInsertNoTableExists() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.AUTO_COMMIT_0.getTestCase());

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
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutDatabaseRecord.REL_FAILURE).getFirst();
        final String errorMessage = flowFile.getAttribute("putdatabaserecord.error");
        assertTrue(errorMessage.contains("PERSONS2"));
        runner.enqueue();
    }

    @ParameterizedTest()
    @MethodSource("getTestCases")
    public void testInsertViaSqlTypeOneStatement(TestCase testCase) throws InitializationException, ProcessException, SQLException {
        setRunner(testCase);

        String[] sqlStatements = new String[]{
                "INSERT INTO PERSONS (id, name, code) VALUES (1, 'rec1',101)"
        };
        testInsertViaSqlTypeStatements(sqlStatements, false);
    }

    @ParameterizedTest()
    @MethodSource("getTestCases")
    public void testInsertViaSqlTypeTwoStatementsSemicolon(TestCase testCase) throws InitializationException, ProcessException, SQLException {
        setRunner(testCase);

        String[] sqlStatements = new String[]{
                "INSERT INTO PERSONS (id, name, code) VALUES (1, 'rec1',101)",
                "INSERT INTO PERSONS (id, name, code) VALUES (2, 'rec2',102);"
        };
        testInsertViaSqlTypeStatements(sqlStatements, true);
    }

    @ParameterizedTest()
    @MethodSource("getTestCases")
    public void testInsertViaSqlTypeThreeStatements(TestCase testCase) throws InitializationException, ProcessException, SQLException {
        setRunner(testCase);

        String[] sqlStatements = new String[]{
                "INSERT INTO PERSONS (id, name, code) VALUES (1, 'rec1',101)",
                "INSERT INTO PERSONS (id, name, code) VALUES (2, 'rec2',102)",
                "UPDATE PERSONS SET code = 101 WHERE id = 1"
        };
        testInsertViaSqlTypeStatements(sqlStatements, false);
    }

    void testInsertViaSqlTypeStatements(String[] sqlStatements, boolean allowMultipleStatements) throws InitializationException, ProcessException, SQLException {
        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("sql", RecordFieldType.STRING);

        for (String sqlStatement : sqlStatements) {
            parser.addRecord(sqlStatement);
        }

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.USE_ATTR_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");
        runner.setProperty(PutDatabaseRecord.FIELD_CONTAINING_SQL, "sql");
        runner.setProperty(PutDatabaseRecord.ALLOW_MULTIPLE_STATEMENTS, String.valueOf(allowMultipleStatements));

        Supplier<Statement> spyStmt = createStatementSpy();

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(PutDatabaseRecord.STATEMENT_TYPE_ATTRIBUTE, "sql");
        runner.enqueue(new byte[0], attrs);
        runner.run();

        final int maxBatchSize = runner.getProcessContext().getProperty(PutDatabaseRecord.MAX_BATCH_SIZE).evaluateAttributeExpressions((FlowFile) null).asInteger();
        assertNotNull(spyStmt.get());
        if (sqlStatements.length <= 1) {
            // When there is only 1 sql statement, then never use batching
            verify(spyStmt.get(), times(0)).executeBatch();
        } else if (maxBatchSize == 0) {
            // When maxBatchSize is 0, verify that all statements were executed in a single executeBatch call
            verify(spyStmt.get(), times(1)).executeBatch();
        } else if (maxBatchSize == 1) {
            // When maxBatchSize is 1, verify that executeBatch was never called
            verify(spyStmt.get(), times(0)).executeBatch();
        } else {
            // When maxBatchSize > 1, verify that executeBatch was called at least once
            verify(spyStmt.get(), atLeastOnce()).executeBatch();
        }

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        if (sqlStatements.length >= 1) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("rec1", rs.getString(2));
            assertEquals(101, rs.getInt(3));
        }
        if (sqlStatements.length >= 2) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("rec2", rs.getString(2));
            assertEquals(102, rs.getInt(3));
        }
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @ParameterizedTest()
    @MethodSource("getTestCases")
    public void testMultipleInsertsForOneStatementViaSqlStatementType(TestCase testCase) throws InitializationException, ProcessException, SQLException {
        setRunner(testCase);

        String[] sqlStatements = new String[]{
                "INSERT INTO PERSONS (id, name, code) VALUES (1, 'rec1',101)"
        };
        testMultipleStatementsViaSqlStatementType(sqlStatements);
    }

    @ParameterizedTest()
    @MethodSource("getTestCases")
    public void testMultipleInsertsForTwoStatementsViaSqlStatementType(TestCase testCase) throws InitializationException, ProcessException, SQLException {
        setRunner(testCase);

        String[] sqlStatements = new String[]{
                "INSERT INTO PERSONS (id, name, code) VALUES (1, 'rec1',101)",
                "INSERT INTO PERSONS (id, name, code) VALUES (2, 'rec2',102);"
        };
        testMultipleStatementsViaSqlStatementType(sqlStatements);
    }

    void testMultipleStatementsViaSqlStatementType(String[] sqlStatements) throws InitializationException, ProcessException, SQLException {
        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("sql", RecordFieldType.STRING);

        parser.addRecord(String.join(";", sqlStatements));

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
        if (sqlStatements.length >= 1) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("rec1", rs.getString(2));
            assertEquals(101, rs.getInt(3));
        }
        if (sqlStatements.length >= 2) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("rec2", rs.getString(2));
            assertEquals(102, rs.getInt(3));
        }
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    public void testMultipleInsertsViaSqlStatementTypeBadSQL() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

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
    public void testInvalidData() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

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
        try (conn; stmt) {
            // Transaction should be rolled back and table should remain empty.
            assertFalse(rs.next());
        }
    }

    @Test
    public void testIOExceptionOnReadData() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

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
        try (conn; stmt) {
            // Transaction should be rolled back and table should remain empty.
            assertFalse(rs.next());
        }
    }

    @Test
    public void testIOExceptionOnReadDataAutoCommit() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.AUTO_COMMIT_0.getTestCase());

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
        try (conn; stmt) {
            // Transaction should be rolled back and table should remain empty.
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSqlStatementTypeNoValue() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

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
    public void testSqlStatementTypeNoValueRollbackOnFailure() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.ROLLBACK_0.getTestCase());

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
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 0);
    }

    @ParameterizedTest()
    @MethodSource("getTestCases")
    public void testUpdate(TestCase testCase) throws InitializationException, ProcessException, SQLException {
        setRunner(testCase);

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
    public void testUpdatePkNotFirst() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

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

    @ParameterizedTest()
    @MethodSource("getTestCases")
    public void testUpdateMultipleSchemas(TestCase testCase) throws InitializationException, ProcessException, SQLException {
        setRunner(testCase);

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
        while (rs.next()) {
            schemas.add(rs.getString(1));
        }
        assertFalse(schemas.contains("SCHEMA1"));
        assertFalse(schemas.contains("SCHEMA2"));
        conn.close();
    }

    @ParameterizedTest()
    @MethodSource("getTestCases")
    public void testUpdateAfterInsert(TestCase testCase) throws InitializationException, ProcessException, SQLException {
        setRunner(testCase);

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
    public void testUpdateNoPrimaryKeys() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

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
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutDatabaseRecord.REL_FAILURE).getFirst();
        assertEquals("Table 'PERSONS' not found or does not have a Primary Key and no Update Keys were specified", flowFile.getAttribute(PutDatabaseRecord.PUT_DATABASE_RECORD_ERROR));
    }

    @Test
    public void testUpdateSpecifyUpdateKeys() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

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
    public void testUpdateSpecifyUpdateKeysNotFirst() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_1.getTestCase());

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
    public void testUpdateSpecifyQuotedUpdateKeys() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_1000.getTestCase());

        recreateTable("CREATE TABLE PERSONS (\"id\" integer, \"name\" varchar(100), \"code\" integer)");
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

    @ParameterizedTest()
    @MethodSource("getTestCases")
    public void testDelete(TestCase testCase) throws InitializationException, ProcessException, SQLException {
        setRunner(testCase);

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
    public void testDeleteWithNulls() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_2.getTestCase());

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
    public void testRecordPathOptions() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

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
        parser.addRecord("c", new MapRecord(dataSchema, createValues(3, "Jim Doe", 2)));
        parser.addRecord("DELETE", new MapRecord(dataSchema, createValues(2, "Jane Doe", 44)));
        parser.addRecord("UPDATE", new MapRecord(dataSchema, createValues(1, "John Doe", 201)));
        parser.addRecord("u", new MapRecord(dataSchema, createValues(3, "Jim Doe", 20)));

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
        assertEquals(20, rs.getInt(3));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    public void testInsertWithMaxBatchSize() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

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
    public void testInsertWithDefaultMaxBatchSize() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_1000.getTestCase());

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
        runner.setProperty(PutDatabaseRecord.MAX_BATCH_SIZE, PutDatabaseRecord.MAX_BATCH_SIZE.getDefaultValue());

        Supplier<PreparedStatement> spyStmt = createPreparedStatementSpy();

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);

        assertEquals(11, getTableSize());

        assertNotNull(spyStmt.get());
        verify(spyStmt.get(), times(1)).executeBatch();
    }

    @Test
    public void testGenerateTableName() throws InitializationException, ProcessException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        final TableSchema tableSchema = new TableSchema(
                null,
                null,
                "PERSONS",
                Arrays.asList(
                        new ColumnDescription("id", 4, true, 2, false),
                        new ColumnDescription("name", 12, true, 255, true),
                        new ColumnDescription("code", 4, true, 10, true)
                ),
                false, null,
                new HashSet<>(List.of("id")),
                ""
        );

        runner.setProperty(PutDatabaseRecord.TRANSLATE_FIELD_NAMES, "false");
        runner.setProperty(PutDatabaseRecord.UNMATCHED_FIELD_BEHAVIOR, PutDatabaseRecord.IGNORE_UNMATCHED_FIELD);
        runner.setProperty(PutDatabaseRecord.UNMATCHED_COLUMN_BEHAVIOR, PutDatabaseRecord.IGNORE_UNMATCHED_COLUMN);
        runner.setProperty(PutDatabaseRecord.QUOTE_IDENTIFIERS, "true");
        runner.setProperty(PutDatabaseRecord.QUOTE_TABLE_IDENTIFIER, "true");
        final PutDatabaseRecord.DMLSettings settings = new PutDatabaseRecord.DMLSettings(runner.getProcessContext());

        assertEquals("test_catalog.test_schema.test_table", processor.generateTableName(settings, "test_catalog", "test_schema", "test_table", tableSchema));
    }

    @Test
    public void testInsertMismatchedCompatibleDataTypes() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

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
    public void testInsertMismatchedNotCompatibleDataTypes() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.STRING);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);
        parser.addSchemaField("dt", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.FLOAT.getDataType()).getFieldType());

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
    public void testLongVarchar() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        // Manually create and drop the tables and schemas
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        try {
            stmt.execute("DROP TABLE TEMP");
        } catch (final Exception ignored) {
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
    public void testInsertWithDifferentColumnOrdering() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        // Manually create and drop the tables and schemas
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        try {
            stmt.execute("DROP TABLE TEMP");
        } catch (final Exception ignored) {
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
    public void testInsertWithBlobClob() throws InitializationException, ProcessException, SQLException, IOException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        String createTableWithBlob = "CREATE TABLE PERSONS (id integer primary key, name clob," +
                "content blob, code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000))";

        recreateTable(createTableWithBlob);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        byte[] bytes = "BLOB".getBytes();
        Byte[] blobRecordValue = new Byte[bytes.length];
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
    public void testInsertHexStringIntoBinary() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        runner.setProperty(PutDatabaseRecord.BINARY_STRING_FORMAT, PutDatabaseRecord.BINARY_STRING_FORMAT_HEXADECIMAL);

        String tableName = "HEX_STRING_TEST";
        String createTable = "CREATE TABLE " + tableName + " (id integer primary key, binary_data blob)";
        String hexStringData = "abCDef";

        recreateTable(tableName, createTable);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("binaryData", RecordFieldType.STRING);

        parser.addRecord(1, hexStringData);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, tableName);

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();

        final ResultSet resultSet = stmt.executeQuery("SELECT * FROM " + tableName);
        assertTrue(resultSet.next());

        assertEquals(1, resultSet.getInt(1));

        Blob blob = resultSet.getBlob(2);
        assertArrayEquals(new byte[]{(byte) 171, (byte) 205, (byte) 239}, blob.getBytes(1, (int) blob.length()));

        stmt.close();
        conn.close();
    }

    @Test
    public void testInsertBase64StringIntoBinary() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        runner.setProperty(PutDatabaseRecord.BINARY_STRING_FORMAT, PutDatabaseRecord.BINARY_STRING_FORMAT_BASE64);

        String tableName = "BASE64_STRING_TEST";
        String createTable = "CREATE TABLE " + tableName + " (id integer primary key, binary_data blob)";
        byte[] binaryData = {(byte) 10, (byte) 103, (byte) 234};

        recreateTable(tableName, createTable);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("binaryData", RecordFieldType.STRING);

        parser.addRecord(1, Base64.getEncoder().encodeToString(binaryData));

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, tableName);

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();

        final ResultSet resultSet = stmt.executeQuery("SELECT * FROM " + tableName);
        assertTrue(resultSet.next());

        assertEquals(1, resultSet.getInt(1));

        Blob blob = resultSet.getBlob(2);
        assertArrayEquals(binaryData, blob.getBytes(1, (int) blob.length()));

        stmt.close();
        conn.close();
    }

    @Test
    public void testInsertWithBlobClobObjectArraySource() throws InitializationException, ProcessException, SQLException, IOException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

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
    public void testInsertWithBlobStringSource() throws InitializationException, ProcessException, SQLException, IOException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

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
    public void testInsertWithBlobIntegerArraySource() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

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

        parser.addRecord(1, "rec1", 101, new Integer[]{1, 2, 3});

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(PutDatabaseRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 1);
    }

    @Test
    public void testInsertEnum() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        dbcp = spy(new DBCPServiceSimpleImpl(DB_LOCATION));
        runner = TestRunners.newTestRunner(processor);
        runner.addControllerService(DBCP_SERVICE_ID, dbcp, new HashMap<>());
        runner.enableControllerService(dbcp);
        runner.setProperty(PutDatabaseRecord.DBCP_SERVICE, DBCP_SERVICE_ID);
        recreateTable("""
            CREATE TABLE ENUM_TEST (
                id integer primary key,
                suit varchar(8) not null
            )
        """);

        try (
                Connection conn = dbcp.getConnection();
                Statement stmt = conn.createStatement()
        ) {
            // Add constraint for Apache Derby
            stmt.execute("""
                ALTER TABLE ENUM_TEST
                ADD CONSTRAINT suit
                CHECK (
                    suit IN ('clubs', 'diamonds', 'hearts', 'spades')
                )
            """
            );
        }

        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        final DataType enumDataType = RecordFieldType.ENUM.getEnumDataType(List.of("clubs", "diamonds", "hearts", "spades"));
        assertNotNull(enumDataType);
        parser.addSchemaField("suit", enumDataType.getFieldType());

        parser.addRecord(1, "diamonds");
        parser.addRecord(2, "hearts");

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "ENUM_TEST");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM ENUM_TEST");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("diamonds", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("hearts", rs.getString(2));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    public void testInsertUUIDColumn() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        // Manually create and drop the tables and schemas
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        stmt.execute(createUUIDSchema);

        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.UUID);

        parser.addRecord(1, "425085a0-03ef-11ee-be56-0242ac120002");
        parser.addRecord(2, "56a000e4-03ef-11ee-be56-0242ac120002");

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "UUID_TEST");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        ResultSet rs = stmt.executeQuery("SELECT * FROM UUID_TEST");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("425085a0-03ef-11ee-be56-0242ac120002", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("56a000e4-03ef-11ee-be56-0242ac120002", rs.getString(2));
        assertFalse(rs.next());

        // Drop the schemas here so as not to interfere with other tests
        stmt.execute("drop table UUID_TEST");
        stmt.close();
        conn.close();
    }

    @Test
    public void testInsertLongVarBinaryColumn() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        // Manually create and drop the tables and schemas
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        stmt.execute(createLongVarBinarySchema);

        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType()).getFieldType());

        byte[] longVarBinaryValue1 = new byte[]{97, 98, 99};
        byte[] longVarBinaryValue2 = new byte[]{100, 101, 102};
        parser.addRecord(1, longVarBinaryValue1);
        parser.addRecord(2, longVarBinaryValue2);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "LONGVARBINARY_TEST");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);
        ResultSet rs = stmt.executeQuery("SELECT * FROM LONGVARBINARY_TEST");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertArrayEquals(longVarBinaryValue1, rs.getBytes(2));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertArrayEquals(longVarBinaryValue2, rs.getBytes(2));
        assertFalse(rs.next());

        // Drop the schemas here so as not to interfere with other tests
        stmt.execute("drop table LONGVARBINARY_TEST");
        stmt.close();
        conn.close();
    }

    private void recreateTable() throws ProcessException {
        try (final Connection conn = dbcp.getConnection();
             final Statement stmt = conn.createStatement()) {
            stmt.execute("drop table PERSONS");
            stmt.execute(createPersons);
        } catch (SQLException ignored) {
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
        recreateTable("PERSONS", createSQL);
    }

    private void recreateTable(String tableName, String createSQL) throws ProcessException, SQLException {
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        try {
            stmt.execute("drop table " + tableName);
        } catch (SQLException ignored) {
            // Do nothing, may not have existed
        }
        try (conn; stmt) {
            stmt.execute(createSQL);
        }
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
        final Answer<DelegatingConnection<?>> answer = (inv) -> new DelegatingConnection<>((Connection) inv.callRealMethod()) {
            @Override
            public PreparedStatement prepareStatement(String sql) throws SQLException {
                spyStmt[0] = spy(getDelegate().prepareStatement(sql));
                return spyStmt[0];
            }
        };
        doAnswer(answer).when(dbcp).getConnection(ArgumentMatchers.anyMap());
        return () -> spyStmt[0];
    }

    private Supplier<Statement> createStatementSpy() {
        final Statement[] spyStmt = new Statement[1];
        final Answer<DelegatingConnection<?>> answer = (inv) -> new DelegatingConnection<>((Connection) inv.callRealMethod()) {
            @Override
            public Statement createStatement() throws SQLException {
                spyStmt[0] = spy(getDelegate().createStatement());
                return spyStmt[0];
            }
        };
        doAnswer(answer).when(dbcp).getConnection();
        return () -> spyStmt[0];
    }

    static class PutDatabaseRecordUnmatchedField extends PutDatabaseRecord {
        @Override
        SqlAndIncludedColumns generateInsert(RecordSchema recordSchema, String tableName, TableSchema tableSchema, DMLSettings settings, NameNormalizer normalizer) throws IllegalArgumentException {
            return new SqlAndIncludedColumns("INSERT INTO PERSONS VALUES (?,?,?,?)", Arrays.asList(0, 1, 2, 3));
        }
    }

    static class DBCPServiceThrowConnectionException extends AbstractControllerService implements DBCPService {
        private final Exception rootCause;

        public DBCPServiceThrowConnectionException(final Exception rootCause) {
            this.rootCause = rootCause;
        }

        @Override
        public String getIdentifier() {
            return DBCP_SERVICE_ID;
        }

        @Override
        public Connection getConnection() throws ProcessException {
            throw new ProcessException(rootCause);
        }
    }

    static class DBCPServiceAutoCommitTest extends AbstractControllerService implements DBCPService {
        private final String databaseLocation;

        public DBCPServiceAutoCommitTest(final String databaseLocation) {
            this.databaseLocation = databaseLocation;
        }

        @Override
        public String getIdentifier() {
            return DBCP_SERVICE_ID;
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Connection spyConnection = spy(DriverManager.getConnection("jdbc:derby:" + databaseLocation + ";create=true"));
                doThrow(SQLFeatureNotSupportedException.class).when(spyConnection).setAutoCommit(false);
                return spyConnection;
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }

    public static class TestCase {
        TestCase(Boolean autoCommit, Boolean rollbackOnFailure, Integer batchSize) {
            this.autoCommit = autoCommit;
            this.rollbackOnFailure = rollbackOnFailure;
            this.batchSize = batchSize;
        }

        private final Boolean autoCommit;
        private final Boolean rollbackOnFailure;
        private final Integer batchSize;

        String getAutoCommitAsString() {
            return autoCommit == null ? null : autoCommit.toString();
        }

        String getRollbackOnFailureAsString() {
            return rollbackOnFailure == null ? null : rollbackOnFailure.toString();
        }

        String getBatchSizeAsString() {
            return batchSize == null ? null : batchSize.toString();
        }

        @Override
        public String toString() {
            return "autoCommit=" + autoCommit +
                    "; rollbackOnFailure=" + rollbackOnFailure +
                    "; batchSize=" + batchSize;
        }
    }
}
