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
package org.apache.nifi.processors.standard

import org.apache.commons.dbcp2.DelegatingConnection
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.processor.util.pattern.RollbackOnFailure
import org.apache.nifi.reporting.InitializationException
import org.apache.nifi.serialization.SimpleRecordSchema
import org.apache.nifi.serialization.record.MapRecord
import org.apache.nifi.serialization.record.MockRecordFailureType
import org.apache.nifi.serialization.record.MockRecordParser
import org.apache.nifi.serialization.record.RecordField
import org.apache.nifi.serialization.record.RecordFieldType
import org.apache.nifi.serialization.record.RecordSchema
import org.apache.nifi.util.MockFlowFile
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.apache.nifi.util.file.FileUtils
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import java.sql.Blob
import java.sql.Clob
import java.sql.Connection
import java.sql.Date
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLDataException
import java.sql.SQLException
import java.sql.SQLNonTransientConnectionException
import java.sql.Statement
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.function.Supplier

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertNull
import static org.junit.Assert.assertTrue
import static org.junit.Assert.fail
import static org.mockito.ArgumentMatchers.anyMap
import static org.mockito.Mockito.doAnswer
import static org.mockito.Mockito.spy
import static org.mockito.Mockito.times
import static org.mockito.Mockito.verify

/**
 * Unit tests for the PutDatabaseRecord processor
 */
@RunWith(JUnit4.class)
class TestPutDatabaseRecord {

    private static final String createPersons = "CREATE TABLE PERSONS (id integer primary key, name varchar(100)," +
            " code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000), dt date)"
    private static final String createPersonsSchema1 = "CREATE TABLE SCHEMA1.PERSONS (id integer primary key, name varchar(100)," +
            " code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000), dt date)"
    private static final String createPersonsSchema2 = "CREATE TABLE SCHEMA2.PERSONS (id2 integer primary key, name varchar(100)," +
            " code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000), dt date)"
    private final static String DB_LOCATION = "target/db_pdr"

    TestRunner runner
    PutDatabaseRecord processor
    DBCPServiceSimpleImpl dbcp

    @BeforeClass
    static void setupBeforeClass() throws IOException {
        System.setProperty("derby.stream.error.file", "target/derby.log")

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION)
        try {
            FileUtils.deleteFile(dbLocation, true)
        } catch (IOException ignore) {
            // Do nothing, may not have existed
        }
    }

    @AfterClass
    static void cleanUpAfterClass() throws Exception {
        try {
            DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";shutdown=true")
        } catch (SQLNonTransientConnectionException ignore) {
            // Do nothing, this is what happens at Derby shutdown
        }
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION)
        try {
            FileUtils.deleteFile(dbLocation, true)
        } catch (IOException ignore) {
            // Do nothing, may not have existed
        }
    }

    @Before
    void setUp() throws Exception {
        processor = new PutDatabaseRecord()
        //Mock the DBCP Controller Service so we can control the Results
        dbcp = spy(new DBCPServiceSimpleImpl(DB_LOCATION))

        final Map<String, String> dbcpProperties = new HashMap<>()

        runner = TestRunners.newTestRunner(processor)
        runner.addControllerService("dbcp", dbcp, dbcpProperties)
        runner.enableControllerService(dbcp)
        runner.setProperty(PutDatabaseRecord.DBCP_SERVICE, "dbcp")
    }

    @Test
    void testGeneratePreparedStatements() throws Exception {

        final List<RecordField> fields = [new RecordField('id', RecordFieldType.INT.dataType),
                                          new RecordField('name', RecordFieldType.STRING.dataType),
                                          new RecordField('code', RecordFieldType.INT.dataType),
                                          new RecordField('non_existing', RecordFieldType.BOOLEAN.dataType)]

        def schema = [
                getFields    : {fields},
                getFieldCount: {fields.size()},
                getField     : {int index -> fields[index]},
                getDataTypes : {fields.collect {it.dataType}},
                getFieldNames: {fields.collect {it.fieldName}},
                getDataType  : {fieldName -> fields.find {it.fieldName == fieldName}.dataType}
        ] as RecordSchema

        def tableSchema = [
                [
                        new PutDatabaseRecord.ColumnDescription('id', 4, true, 2, false),
                        new PutDatabaseRecord.ColumnDescription('name', 12, true, 255, true),
                        new PutDatabaseRecord.ColumnDescription('code', 4, true, 10, true)
                ],
                false,
                ['id'] as Set<String>,
                ''
        ] as PutDatabaseRecord.TableSchema

        runner.setProperty(PutDatabaseRecord.TRANSLATE_FIELD_NAMES, 'false')
        runner.setProperty(PutDatabaseRecord.UNMATCHED_FIELD_BEHAVIOR, PutDatabaseRecord.IGNORE_UNMATCHED_FIELD)
        runner.setProperty(PutDatabaseRecord.UNMATCHED_COLUMN_BEHAVIOR, PutDatabaseRecord.IGNORE_UNMATCHED_COLUMN)
        runner.setProperty(PutDatabaseRecord.QUOTE_IDENTIFIERS, 'false')
        runner.setProperty(PutDatabaseRecord.QUOTE_TABLE_IDENTIFIER, 'false')
        def settings = new PutDatabaseRecord.DMLSettings(runner.getProcessContext())

        processor.with {

            assertEquals('INSERT INTO PERSONS (id, name, code) VALUES (?,?,?)',
                    generateInsert(schema, 'PERSONS', tableSchema, settings).sql)

            assertEquals('UPDATE PERSONS SET name = ?, code = ? WHERE id = ?',
                    generateUpdate(schema, 'PERSONS', null, tableSchema, settings).sql)

            assertEquals('DELETE FROM PERSONS WHERE (id = ?) AND (name = ? OR (name is null AND ? is null)) AND (code = ? OR (code is null AND ? is null))',
                    generateDelete(schema, 'PERSONS', tableSchema, settings).sql)
        }
    }

    @Test
    void testGeneratePreparedStatementsFailUnmatchedField() throws Exception {

        final List<RecordField> fields = [new RecordField('id', RecordFieldType.INT.dataType),
                      new RecordField('name', RecordFieldType.STRING.dataType),
                      new RecordField('code', RecordFieldType.INT.dataType),
                      new RecordField('non_existing', RecordFieldType.BOOLEAN.dataType)]

        def schema = [
                getFields    : {fields},
                getFieldCount: {fields.size()},
                getField     : {int index -> fields[index]},
                getDataTypes : {fields.collect {it.dataType}},
                getFieldNames: {fields.collect {it.fieldName}},
                getDataType  : {fieldName -> fields.find {it.fieldName == fieldName}.dataType}
        ] as RecordSchema

        def tableSchema = [
                [
                        new PutDatabaseRecord.ColumnDescription('id', 4, true, 2, false),
                        new PutDatabaseRecord.ColumnDescription('name', 12, true, 255, true),
                        new PutDatabaseRecord.ColumnDescription('code', 4, true, 10, true)
                ],
                false,
                ['id'] as Set<String>,
                ''

        ] as PutDatabaseRecord.TableSchema

        runner.setProperty(PutDatabaseRecord.TRANSLATE_FIELD_NAMES, 'false')
        runner.setProperty(PutDatabaseRecord.UNMATCHED_FIELD_BEHAVIOR, PutDatabaseRecord.FAIL_UNMATCHED_FIELD)
        runner.setProperty(PutDatabaseRecord.UNMATCHED_COLUMN_BEHAVIOR, PutDatabaseRecord.IGNORE_UNMATCHED_COLUMN)
        runner.setProperty(PutDatabaseRecord.QUOTE_IDENTIFIERS, 'false')
        runner.setProperty(PutDatabaseRecord.QUOTE_TABLE_IDENTIFIER, 'false')
        def settings = new PutDatabaseRecord.DMLSettings(runner.getProcessContext())

        processor.with {

            try {
                generateInsert(schema, 'PERSONS', tableSchema, settings)
                fail('generateInsert should fail with unmatched fields')
            } catch (SQLDataException e) {
                assertEquals("Cannot map field 'non_existing' to any column in the database\nColumns: id,name,code", e.getMessage())
            }

            try {
                generateUpdate(schema, 'PERSONS', null, tableSchema, settings)
                fail('generateUpdate should fail with unmatched fields')
            } catch (SQLDataException e) {
                assertEquals("Cannot map field 'non_existing' to any column in the database\nColumns: id,name,code", e.getMessage())
            }

            try {
                generateDelete(schema, 'PERSONS', tableSchema, settings)
                fail('generateDelete should fail with unmatched fields')
            } catch (SQLDataException e) {
                assertEquals("Cannot map field 'non_existing' to any column in the database\nColumns: id,name,code", e.getMessage())
            }
        }
    }

    @Test
    void testInsert() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)
        parser.addSchemaField("dt", RecordFieldType.DATE)

        LocalDate testDate1 = LocalDate.of(2021, 1, 26)
        Date nifiDate1 = new Date(testDate1.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli()) // in UTC
        Date jdbcDate1 = Date.valueOf(testDate1) // in local TZ
        LocalDate testDate2 = LocalDate.of(2021, 7, 26)
        Date nifiDate2 = new Date(testDate2.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli()) // in URC
        Date jdbcDate2 = Date.valueOf(testDate2) // in local TZ

        parser.addRecord(1, 'rec1', 101, nifiDate1)
        parser.addRecord(2, 'rec2', 102, nifiDate2)
        parser.addRecord(3, 'rec3', 103, null)
        parser.addRecord(4, 'rec4', 104, null)
        parser.addRecord(5, null, 105, null)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)
        final Connection conn = dbcp.getConnection()
        final Statement stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        assertTrue(rs.next())
        assertEquals(1, rs.getInt(1))
        assertEquals('rec1', rs.getString(2))
        assertEquals(101, rs.getInt(3))
        assertEquals(jdbcDate1, rs.getDate(4))
        assertTrue(rs.next())
        assertEquals(2, rs.getInt(1))
        assertEquals('rec2', rs.getString(2))
        assertEquals(102, rs.getInt(3))
        assertEquals(jdbcDate2, rs.getDate(4))
        assertTrue(rs.next())
        assertEquals(3, rs.getInt(1))
        assertEquals('rec3', rs.getString(2))
        assertEquals(103, rs.getInt(3))
        assertNull(rs.getDate(4))
        assertTrue(rs.next())
        assertEquals(4, rs.getInt(1))
        assertEquals('rec4', rs.getString(2))
        assertEquals(104, rs.getInt(3))
        assertNull(rs.getDate(4))
        assertTrue(rs.next())
        assertEquals(5, rs.getInt(1))
        assertNull(rs.getString(2))
        assertEquals(105, rs.getInt(3))
        assertNull(rs.getDate(4))
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }

    @Test
    void testInsertNonRequiredColumns() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("dt", RecordFieldType.DATE)

        LocalDate testDate1 = LocalDate.of(2021, 1, 26)
        Date nifiDate1 = new Date(testDate1.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli()) // in UTC
        Date jdbcDate1 = Date.valueOf(testDate1) // in local TZ
        LocalDate testDate2 = LocalDate.of(2021, 7, 26)
        Date nifiDate2 = new Date(testDate2.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli()) // in URC
        Date jdbcDate2 = Date.valueOf(testDate2) // in local TZ

        parser.addRecord(1, 'rec1', nifiDate1)
        parser.addRecord(2, 'rec2', nifiDate2)
        parser.addRecord(3, 'rec3', null)
        parser.addRecord(4, 'rec4', null)
        parser.addRecord(5, null, null)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)
        final Connection conn = dbcp.getConnection()
        final Statement stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        assertTrue(rs.next())
        assertEquals(1, rs.getInt(1))
        assertEquals('rec1', rs.getString(2))
        // Zero value because of the constraint
        assertEquals(0, rs.getInt(3))
        assertEquals(jdbcDate1, rs.getDate(4))
        assertTrue(rs.next())
        assertEquals(2, rs.getInt(1))
        assertEquals('rec2', rs.getString(2))
        assertEquals(0, rs.getInt(3))
        assertEquals(jdbcDate2, rs.getDate(4))
        assertTrue(rs.next())
        assertEquals(3, rs.getInt(1))
        assertEquals('rec3', rs.getString(2))
        assertEquals(0, rs.getInt(3))
        assertNull(rs.getDate(4))
        assertTrue(rs.next())
        assertEquals(4, rs.getInt(1))
        assertEquals('rec4', rs.getString(2))
        assertEquals(0, rs.getInt(3))
        assertNull(rs.getDate(4))
        assertTrue(rs.next())
        assertEquals(5, rs.getInt(1))
        assertNull(rs.getString(2))
        assertEquals(0, rs.getInt(3))
        assertNull(rs.getDate(4))
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }

    @Test
    void testInsertBatchUpdateException() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)

        parser.addRecord(1, 'rec1', 101)
        parser.addRecord(2, 'rec2', 102)
        parser.addRecord(3, 'rec3', 1000)   // This record violates the constraint on the 'code' column so should result in FlowFile being routed to failure
        parser.addRecord(4, 'rec4', 104)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_FAILURE, 1)
        final Connection conn = dbcp.getConnection()
        final Statement stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        // Transaction should be rolled back and table should remain empty.
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }

    @Test
    void testInsertBatchUpdateExceptionRollbackOnFailure() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)

        parser.addRecord(1, 'rec1', 101)
        parser.addRecord(2, 'rec2', 102)
        parser.addRecord(3, 'rec3', 1000)
        parser.addRecord(4, 'rec4', 104)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, 'true')

        runner.enqueue(new byte[0])
        runner.run()

        final Connection conn = dbcp.getConnection()
        final Statement stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        // Transaction should be rolled back and table should remain empty.
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }

    @Test
    void testInsertNoTableSpecified() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)

        parser.addRecord(1, 'rec1', 101)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, '${not.a.real.attr}')

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0)
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 1)
    }

    @Test
    void testInsertNoTableExists() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)

        parser.addRecord(1, 'rec1', 101)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS2')

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0)
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 1)
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutDatabaseRecord.REL_FAILURE).get(0);
        final String errorMessage = flowFile.getAttribute("putdatabaserecord.error")
        assertTrue(errorMessage.contains("PERSONS2"))
    }

    @Test
    void testInsertViaSqlStatementType() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("sql", RecordFieldType.STRING)

        parser.addRecord('''INSERT INTO PERSONS (id, name, code) VALUES (1, 'rec1',101)''')
        parser.addRecord('''INSERT INTO PERSONS (id, name, code) VALUES (2, 'rec2',102)''')

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.USE_ATTR_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')
        runner.setProperty(PutDatabaseRecord.FIELD_CONTAINING_SQL, 'sql')

        def attrs = [:]
        attrs[PutDatabaseRecord.STATEMENT_TYPE_ATTRIBUTE] = 'sql'
        runner.enqueue(new byte[0], attrs)
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)
        final Connection conn = dbcp.getConnection()
        final Statement stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        assertTrue(rs.next())
        assertEquals(1, rs.getInt(1))
        assertEquals('rec1', rs.getString(2))
        assertEquals(101, rs.getInt(3))
        assertTrue(rs.next())
        assertEquals(2, rs.getInt(1))
        assertEquals('rec2', rs.getString(2))
        assertEquals(102, rs.getInt(3))
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }

    @Test
    void testMultipleInsertsViaSqlStatementType() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("sql", RecordFieldType.STRING)

        parser.addRecord('''INSERT INTO PERSONS (id, name, code) VALUES (1, 'rec1',101);INSERT INTO PERSONS (id, name, code) VALUES (2, 'rec2',102)''')

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.USE_ATTR_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')
        runner.setProperty(PutDatabaseRecord.FIELD_CONTAINING_SQL, 'sql')
        runner.setProperty(PutDatabaseRecord.ALLOW_MULTIPLE_STATEMENTS, 'true')

        def attrs = [:]
        attrs[PutDatabaseRecord.STATEMENT_TYPE_ATTRIBUTE] = 'sql'
        runner.enqueue(new byte[0], attrs)
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)
        final Connection conn = dbcp.getConnection()
        final Statement stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        assertTrue(rs.next())
        assertEquals(1, rs.getInt(1))
        assertEquals('rec1', rs.getString(2))
        assertEquals(101, rs.getInt(3))
        assertTrue(rs.next())
        assertEquals(2, rs.getInt(1))
        assertEquals('rec2', rs.getString(2))
        assertEquals(102, rs.getInt(3))
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }

    @Test
    void testMultipleInsertsViaSqlStatementTypeBadSQL() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("sql", RecordFieldType.STRING)

        parser.addRecord('''INSERT INTO PERSONS (id, name, code) VALUES (1, 'rec1',101);
                        INSERT INTO PERSONS (id, name, code) VALUES (2, 'rec2',102);
                        INSERT INTO PERSONS2 (id, name, code) VALUES (2, 'rec2',102);''')

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.USE_ATTR_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')
        runner.setProperty(PutDatabaseRecord.FIELD_CONTAINING_SQL, 'sql')
        runner.setProperty(PutDatabaseRecord.ALLOW_MULTIPLE_STATEMENTS, 'true')

        def attrs = [:]
        attrs[PutDatabaseRecord.STATEMENT_TYPE_ATTRIBUTE] = 'sql'
        runner.enqueue(new byte[0], attrs)
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0)
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 1)
        final Connection conn = dbcp.getConnection()
        final Statement stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        // The first two legitimate statements should have been rolled back
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }

    @Test
    void testInvalidData() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)

        parser.addRecord(1, 'rec1', 101)
        parser.addRecord(2, 'rec2', 102)
        parser.addRecord(3, 'rec3', 104)

        parser.failAfter(1)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_FAILURE, 1)
        final Connection conn = dbcp.getConnection()
        final Statement stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        // Transaction should be rolled back and table should remain empty.
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }

    @Test
    void testIOExceptionOnReadData() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)

        parser.addRecord(1, 'rec1', 101)
        parser.addRecord(2, 'rec2', 102)
        parser.addRecord(3, 'rec3', 104)

        parser.failAfter(1, MockRecordFailureType.IO_EXCEPTION)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_FAILURE, 1)
        final Connection conn = dbcp.getConnection()
        final Statement stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        // Transaction should be rolled back and table should remain empty.
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }

    @Test
    void testSqlStatementTypeNoValue() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("sql", RecordFieldType.STRING)

        parser.addRecord('')

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.USE_ATTR_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')
        runner.setProperty(PutDatabaseRecord.FIELD_CONTAINING_SQL, 'sql')

        def attrs = [:]
        attrs[PutDatabaseRecord.STATEMENT_TYPE_ATTRIBUTE] = 'sql'
        runner.enqueue(new byte[0], attrs)
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0)
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 1)
    }

    @Test
    void testSqlStatementTypeNoValueRollbackOnFailure() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("sql", RecordFieldType.STRING)

        parser.addRecord('')

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.USE_ATTR_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')
        runner.setProperty(PutDatabaseRecord.FIELD_CONTAINING_SQL, 'sql')
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, 'true')

        def attrs = [:]
        attrs[PutDatabaseRecord.STATEMENT_TYPE_ATTRIBUTE] = 'sql'
        runner.enqueue(new byte[0], attrs)

        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0)
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 0)
    }

    @Test
    void testUpdate() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)

        parser.addRecord(1, 'rec1', 201)
        parser.addRecord(2, 'rec2', 202)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.UPDATE_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        // Set some existing records with different values for name and code
        final Connection conn = dbcp.getConnection()
        Statement stmt = conn.createStatement()
        stmt.execute('''INSERT INTO PERSONS VALUES (1,'x1',101, null)''')
        stmt.execute('''INSERT INTO PERSONS VALUES (2,'x2',102, null)''')
        stmt.close()

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)
        stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        assertTrue(rs.next())
        assertEquals(1, rs.getInt(1))
        assertEquals('rec1', rs.getString(2))
        assertEquals(201, rs.getInt(3))
        assertTrue(rs.next())
        assertEquals(2, rs.getInt(1))
        assertEquals('rec2', rs.getString(2))
        assertEquals(202, rs.getInt(3))
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }

    @Test
    void testUpdateMultipleSchemas() throws InitializationException, ProcessException, SQLException, IOException {
        // Manually create and drop the tables and schemas
        def conn = dbcp.connection
        def stmt = conn.createStatement()
        stmt.execute('create schema SCHEMA1')
        stmt.execute('create schema SCHEMA2')
        stmt.execute(createPersonsSchema1)
        stmt.execute(createPersonsSchema2)

        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)

        parser.addRecord(1, 'rec1', 201)
        parser.addRecord(2, 'rec2', 202)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.UPDATE_TYPE)
        runner.setProperty(PutDatabaseRecord.SCHEMA_NAME, "SCHEMA1")
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        // Set some existing records with different values for name and code
        Exception e
        ResultSet rs
        try {
            stmt.execute('''INSERT INTO SCHEMA1.PERSONS VALUES (1,'x1',101,null)''')
            stmt.execute('''INSERT INTO SCHEMA2.PERSONS VALUES (2,'x2',102,null)''')

            runner.enqueue(new byte[0])
            runner.run()

            runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)
            rs = stmt.executeQuery('SELECT * FROM SCHEMA1.PERSONS')
            assertTrue(rs.next())
            assertEquals(1, rs.getInt(1))
            assertEquals('rec1', rs.getString(2))
            assertEquals(201, rs.getInt(3))
            assertFalse(rs.next())
            rs = stmt.executeQuery('SELECT * FROM SCHEMA2.PERSONS')
            assertTrue(rs.next())
            assertEquals(2, rs.getInt(1))
            // Values should not have been updated
            assertEquals('x2', rs.getString(2))
            assertEquals(102, rs.getInt(3))
            assertFalse(rs.next())
        } catch(ex) {
            e = ex
        }

        // Drop the schemas here so as not to interfere with other tests
        stmt.execute("drop table SCHEMA1.PERSONS")
        stmt.execute("drop table SCHEMA2.PERSONS")
        stmt.execute("drop schema SCHEMA1 RESTRICT")
        stmt.execute("drop schema SCHEMA2 RESTRICT")
        stmt.close()

        // Don't proceed if there was a problem with the asserts
        if(e) throw e
        rs = conn.metaData.schemas
        List<String> schemas = new ArrayList<>()
        while(rs.next()) {
            schemas += rs.getString(1)
        }
        assertFalse(schemas.contains('SCHEMA1'))
        assertFalse(schemas.contains('SCHEMA2'))
        conn.close()
    }

    @Test
    void testUpdateAfterInsert() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)

        parser.addRecord(1, 'rec1', 101)
        parser.addRecord(2, 'rec2', 102)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)
        final Connection conn = dbcp.getConnection()
        Statement stmt = conn.createStatement()
        ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        assertTrue(rs.next())
        assertEquals(1, rs.getInt(1))
        assertEquals('rec1', rs.getString(2))
        assertEquals(101, rs.getInt(3))
        assertTrue(rs.next())
        assertEquals(2, rs.getInt(1))
        assertEquals('rec2', rs.getString(2))
        assertEquals(102, rs.getInt(3))
        assertFalse(rs.next())
        stmt.close()
        runner.clearTransferState()

        parser.addRecord(1, 'rec1', 201)
        parser.addRecord(2, 'rec2', 202)
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.UPDATE_TYPE)
        runner.enqueue(new byte[0])
        runner.run(1,true,false)

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)
        stmt = conn.createStatement()
        rs = stmt.executeQuery('SELECT * FROM PERSONS')
        assertTrue(rs.next())
        assertEquals(1, rs.getInt(1))
        assertEquals('rec1', rs.getString(2))
        assertEquals(201, rs.getInt(3))
        assertTrue(rs.next())
        assertEquals(2, rs.getInt(1))
        assertEquals('rec2', rs.getString(2))
        assertEquals(202, rs.getInt(3))
        assertFalse(rs.next())
        stmt.close()
        conn.close()
    }

    @Test
    void testUpdateNoPrimaryKeys() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable('CREATE TABLE PERSONS (id integer, name varchar(100), code integer)')
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.UPDATE_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        parser.addRecord(1, 'rec1', 201)
        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0)
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 1)
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutDatabaseRecord.REL_FAILURE).get(0)
        assertEquals('Table \'PERSONS\' not found or does not have a Primary Key and no Update Keys were specified', flowFile.getAttribute(PutDatabaseRecord.PUT_DATABASE_RECORD_ERROR))
    }

    @Test
    void testUpdateSpecifyUpdateKeys() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable('CREATE TABLE PERSONS (id integer, name varchar(100), code integer)')
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)

        parser.addRecord(1, 'rec1', 201)
        parser.addRecord(2, 'rec2', 202)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.UPDATE_TYPE)
        runner.setProperty(PutDatabaseRecord.UPDATE_KEYS, 'id')
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        // Set some existing records with different values for name and code
        final Connection conn = dbcp.getConnection()
        Statement stmt = conn.createStatement()
        stmt.execute('''INSERT INTO PERSONS VALUES (1,'x1',101)''')
        stmt.execute('''INSERT INTO PERSONS VALUES (2,'x2',102)''')
        stmt.close()

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)
        stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        assertTrue(rs.next())
        assertEquals(1, rs.getInt(1))
        assertEquals('rec1', rs.getString(2))
        assertEquals(201, rs.getInt(3))
        assertTrue(rs.next())
        assertEquals(2, rs.getInt(1))
        assertEquals('rec2', rs.getString(2))
        assertEquals(202, rs.getInt(3))
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }

    @Test
    void testDelete() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons)
        Connection conn = dbcp.getConnection()
        Statement stmt = conn.createStatement()
        stmt.execute("INSERT INTO PERSONS VALUES (1,'rec1', 101, null)")
        stmt.execute("INSERT INTO PERSONS VALUES (2,'rec2', 102, null)")
        stmt.execute("INSERT INTO PERSONS VALUES (3,'rec3', 103, null)")
        stmt.close()

        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)

        parser.addRecord(2, 'rec2', 102)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.DELETE_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)
        stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        assertTrue(rs.next())
        assertEquals(1, rs.getInt(1))
        assertEquals('rec1', rs.getString(2))
        assertEquals(101, rs.getInt(3))
        assertTrue(rs.next())
        assertEquals(3, rs.getInt(1))
        assertEquals('rec3', rs.getString(2))
        assertEquals(103, rs.getInt(3))
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }

    @Test
    void testDeleteWithNulls() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons)
        Connection conn = dbcp.getConnection()
        Statement stmt = conn.createStatement()
        stmt.execute("INSERT INTO PERSONS VALUES (1,'rec1', 101, null)")
        stmt.execute("INSERT INTO PERSONS VALUES (2,'rec2', null, null)")
        stmt.execute("INSERT INTO PERSONS VALUES (3,'rec3', 103, null)")
        stmt.close()

        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)

        parser.addRecord(2, 'rec2', null)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.DELETE_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)
        stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        assertTrue(rs.next())
        assertEquals(1, rs.getInt(1))
        assertEquals('rec1', rs.getString(2))
        assertEquals(101, rs.getInt(3))
        assertTrue(rs.next())
        assertEquals(3, rs.getInt(1))
        assertEquals('rec3', rs.getString(2))
        assertEquals(103, rs.getInt(3))
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }

    @Test
    void testRecordPathOptions() {
        recreateTable('CREATE TABLE PERSONS (id integer, name varchar(100), code integer)')
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        final List<RecordField> dataFields = new ArrayList<>();
        dataFields.add(new RecordField("id", RecordFieldType.INT.getDataType()))
        dataFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()))
        dataFields.add(new RecordField("code", RecordFieldType.INT.getDataType()))

        final RecordSchema dataSchema = new SimpleRecordSchema(dataFields)
        parser.addSchemaField("operation", RecordFieldType.STRING)
        parser.addSchemaField(new RecordField("data", RecordFieldType.RECORD.getRecordDataType(dataSchema)))

        // CREATE, CREATE, CREATE, DELETE, UPDATE
        parser.addRecord("INSERT", new MapRecord(dataSchema, ["id": 1, "name": "John Doe", "code": 55] as Map))
        parser.addRecord("INSERT", new MapRecord(dataSchema, ["id": 2, "name": "Jane Doe", "code": 44] as Map))
        parser.addRecord("INSERT", new MapRecord(dataSchema, ["id": 3, "name": "Jim Doe", "code": 2] as Map))
        parser.addRecord("DELETE", new MapRecord(dataSchema, ["id": 2, "name": "Jane Doe", "code": 44] as Map))
        parser.addRecord("UPDATE", new MapRecord(dataSchema, ["id": 1, "name": "John Doe", "code": 201] as Map))

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.USE_RECORD_PATH)
        runner.setProperty(PutDatabaseRecord.DATA_RECORD_PATH, "/data")
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE_RECORD_PATH, "/operation")
        runner.setProperty(PutDatabaseRecord.UPDATE_KEYS, 'id')
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_SUCCESS, 1)

        Connection conn = dbcp.getConnection()
        final Statement stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        assertTrue(rs.next())
        assertEquals(1, rs.getInt(1))
        assertEquals('John Doe', rs.getString(2))
        assertEquals(201, rs.getInt(3))
        assertTrue(rs.next())
        assertEquals(3, rs.getInt(1))
        assertEquals('Jim Doe', rs.getString(2))
        assertEquals(2, rs.getInt(3))
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }

    @Test
    void testInsertWithMaxBatchSize() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)

        (1..11).each {
            parser.addRecord(it, "rec$it".toString(), 100 + it)
        }

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')
        runner.setProperty(PutDatabaseRecord.MAX_BATCH_SIZE, "5")

        Supplier<PreparedStatement> spyStmt = createPreparedStatementSpy()

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)

        assertEquals(11, getTableSize())

        assertNotNull(spyStmt.get())
        verify(spyStmt.get(), times(3)).executeBatch()
    }

    @Test
    void testInsertWithDefaultMaxBatchSize() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)

        (1..11).each {
            parser.addRecord(it, "rec$it".toString(), 100 + it)
        }

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        Supplier<PreparedStatement> spyStmt = createPreparedStatementSpy()

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)

        assertEquals(11, getTableSize())

        assertNotNull(spyStmt.get())
        verify(spyStmt.get(), times(1)).executeBatch()
    }

    private Supplier<PreparedStatement> createPreparedStatementSpy() {
        PreparedStatement spyStmt
        doAnswer({ inv ->
            new DelegatingConnection((Connection)inv.callRealMethod()) {
                @Override
                PreparedStatement prepareStatement(String sql) throws SQLException {
                    spyStmt = spy(getDelegate().prepareStatement(sql))
                }
            }
        }).when(dbcp).getConnection(anyMap())
        return { spyStmt }
    }

    private int getTableSize() {
        final Connection connection = dbcp.getConnection()
        try {
            final Statement stmt = connection.createStatement()
            try {
                final ResultSet rs = stmt.executeQuery('SELECT count(*) FROM PERSONS')
                assertTrue(rs.next())
                rs.getInt(1)
            } finally {
                stmt.close()
            }
        } finally {
            connection.close()
        }
    }

    private void recreateTable(String createSQL) throws ProcessException, SQLException {
        final Connection conn = dbcp.getConnection()
        final Statement stmt = conn.createStatement()
        try {
            stmt.execute("drop table PERSONS")
        } catch (SQLException ignore) {
            // Do nothing, may not have existed
        }
        stmt.execute(createSQL)
        stmt.close()
        conn.close()
    }

    @Test
    void testGenerateTableName() throws Exception {

        final List<RecordField> fields = [new RecordField('id', RecordFieldType.INT.dataType),
                                          new RecordField('name', RecordFieldType.STRING.dataType),
                                          new RecordField('code', RecordFieldType.INT.dataType),
                                          new RecordField('non_existing', RecordFieldType.BOOLEAN.dataType)]

        def schema = [
                getFields    : {fields},
                getFieldCount: {fields.size()},
                getField     : {int index -> fields[index]},
                getDataTypes : {fields.collect {it.dataType}},
                getFieldNames: {fields.collect {it.fieldName}},
                getDataType  : {fieldName -> fields.find {it.fieldName == fieldName}.dataType}
        ] as RecordSchema

        def tableSchema = [
                [
                        new PutDatabaseRecord.ColumnDescription('id', 4, true, 2, false),
                        new PutDatabaseRecord.ColumnDescription('name', 12, true, 255, true),
                        new PutDatabaseRecord.ColumnDescription('code', 4, true, 10, true)
                ],
                false,
                ['id'] as Set<String>,
                '"'

        ] as PutDatabaseRecord.TableSchema

        runner.setProperty(PutDatabaseRecord.TRANSLATE_FIELD_NAMES, 'false')
        runner.setProperty(PutDatabaseRecord.UNMATCHED_FIELD_BEHAVIOR, PutDatabaseRecord.IGNORE_UNMATCHED_FIELD)
        runner.setProperty(PutDatabaseRecord.UNMATCHED_COLUMN_BEHAVIOR, PutDatabaseRecord.IGNORE_UNMATCHED_COLUMN)
        runner.setProperty(PutDatabaseRecord.QUOTE_IDENTIFIERS, 'true')
        runner.setProperty(PutDatabaseRecord.QUOTE_TABLE_IDENTIFIER, 'true')
        def settings = new PutDatabaseRecord.DMLSettings(runner.getProcessContext())

        processor.with {

            assertEquals('"test_catalog"."test_schema"."test_table"',
                    generateTableName(settings,"test_catalog","test_schema","test_table",tableSchema))

        }
    }

    @Test
    void testInsertMismatchedCompatibleDataTypes() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)
        parser.addSchemaField("dt", RecordFieldType.BIGINT)

        LocalDate testDate1 = LocalDate.of(2021, 1, 26)
        BigInteger nifiDate1 = testDate1.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli() // in UTC
        Date jdbcDate1 = Date.valueOf(testDate1) // in local TZ
        LocalDate testDate2 = LocalDate.of(2021, 7, 26)
        BigInteger nifiDate2 = testDate2.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli() // in UTC
        Date jdbcDate2 = Date.valueOf(testDate2) // in local TZ

        parser.addRecord(1, 'rec1', 101, nifiDate1)
        parser.addRecord(2, 'rec2', 102, nifiDate2)
        parser.addRecord(3, 'rec3', 103, null)
        parser.addRecord(4, 'rec4', 104, null)
        parser.addRecord(5, null, 105, null)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)
        final Connection conn = dbcp.getConnection()
        final Statement stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        assertTrue(rs.next())
        assertEquals(1, rs.getInt(1))
        assertEquals('rec1', rs.getString(2))
        assertEquals(101, rs.getInt(3))
        assertEquals(jdbcDate1, rs.getDate(4))
        assertTrue(rs.next())
        assertEquals(2, rs.getInt(1))
        assertEquals('rec2', rs.getString(2))
        assertEquals(102, rs.getInt(3))
        assertEquals(jdbcDate2, rs.getDate(4))
        assertTrue(rs.next())
        assertEquals(3, rs.getInt(1))
        assertEquals('rec3', rs.getString(2))
        assertEquals(103, rs.getInt(3))
        assertNull(rs.getDate(4))
        assertTrue(rs.next())
        assertEquals(4, rs.getInt(1))
        assertEquals('rec4', rs.getString(2))
        assertEquals(104, rs.getInt(3))
        assertNull(rs.getDate(4))
        assertTrue(rs.next())
        assertEquals(5, rs.getInt(1))
        assertNull(rs.getString(2))
        assertEquals(105, rs.getInt(3))
        assertNull(rs.getDate(4))
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }


    @Test
    void testInsertMismatchedNotCompatibleDataTypes() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable(createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.STRING)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)
        parser.addSchemaField("dt", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.FLOAT.getDataType()).getFieldType());

        LocalDate testDate1 = LocalDate.of(2021, 1, 26)
        BigInteger nifiDate1 = testDate1.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli() // in UTC
        Date jdbcDate1 = Date.valueOf(testDate1) // in local TZ
        LocalDate testDate2 = LocalDate.of(2021, 7, 26)
        BigInteger nifiDate2 = testDate2.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli() // in UTC
        Date jdbcDate2 = Date.valueOf(testDate2) // in local TZ

        parser.addRecord('1', 'rec1', 101, [1.0,2.0])
        parser.addRecord('2', 'rec2', 102, [3.0,4.0])
        parser.addRecord('3', 'rec3', 103, null)
        parser.addRecord('4', 'rec4', 104, null)
        parser.addRecord('5', null, 105, null)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        runner.enqueue(new byte[0])
        runner.run()

        // A SQLFeatureNotSupportedException exception is expected from Derby when you try to put the data as an ARRAY
        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0)
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 1)
    }

    @Test
    void testLongVarchar() throws InitializationException, ProcessException, SQLException, IOException {
        // Manually create and drop the tables and schemas
        def conn = dbcp.connection
        def stmt = conn.createStatement()
        try {
            stmt.execute('DROP TABLE TEMP')
        } catch(ex) {
            // Do nothing, table may not exist
        }
        stmt.execute('CREATE TABLE TEMP (id integer primary key, name long varchar)')

        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)

        parser.addRecord(1, 'rec1')
        parser.addRecord(2, 'rec2')

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'TEMP')

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)
        ResultSet rs = stmt.executeQuery('SELECT * FROM TEMP')
        assertTrue(rs.next())
        assertEquals(1, rs.getInt(1))
        assertEquals('rec1', rs.getString(2))
        assertTrue(rs.next())
        assertEquals(2, rs.getInt(1))
        assertEquals('rec2', rs.getString(2))
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }

    @Test
    void testInsertWithDifferentColumnOrdering() throws InitializationException, ProcessException, SQLException, IOException {
        // Manually create and drop the tables and schemas
        def conn = dbcp.connection
        def stmt = conn.createStatement()
        try {
            stmt.execute('DROP TABLE TEMP')
        } catch(ex) {
            // Do nothing, table may not exist
        }
        stmt.execute('CREATE TABLE TEMP (id integer primary key, code integer, name long varchar)')

        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("code", RecordFieldType.INT)

        // change order of columns
        parser.addRecord('rec1', 1, 101)
        parser.addRecord('rec2', 2, 102)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'TEMP')

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)
        ResultSet rs = stmt.executeQuery('SELECT * FROM TEMP')
        assertTrue(rs.next())
        assertEquals(1, rs.getInt(1))
        assertEquals(101, rs.getInt(2))
        assertEquals('rec1', rs.getString(3))
        assertTrue(rs.next())
        assertEquals(2, rs.getInt(1))
        assertEquals(102, rs.getInt(2))
        assertEquals('rec2', rs.getString(3))
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }

    @Test
    void testInsertWithBlobClob() throws Exception {
        String createTableWithBlob = "CREATE TABLE PERSONS (id integer primary key, name clob," +
                "content blob, code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000))"

        recreateTable(createTableWithBlob)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        byte[] bytes = "BLOB".getBytes()
        Byte[] blobRecordValue = new Byte[bytes.length]
        (0 .. (bytes.length-1)).each { i -> blobRecordValue[i] = bytes[i].longValue() }

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)
        parser.addSchemaField("content", RecordFieldType.ARRAY)

        parser.addRecord(1, 'rec1', 101, blobRecordValue)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)
        final Connection conn = dbcp.getConnection()
        final Statement stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        assertTrue(rs.next())
        assertEquals(1, rs.getInt(1))
        Clob clob = rs.getClob(2)
        assertNotNull(clob)
        char[] clobText = new char[5]
        int numBytes = clob.characterStream.read(clobText)
        assertEquals(4, numBytes)
        // Ignore last character, it's meant to ensure that only 4 bytes were read even though the buffer is 5 bytes
        assertEquals('rec1', new String(clobText).substring(0,4))
        Blob blob = rs.getBlob(3)
        assertEquals("BLOB", new String(blob.getBytes(1, blob.length() as int)))
        assertEquals(101, rs.getInt(4))

        stmt.close()
        conn.close()
    }

    @Test
    void testInsertWithBlobClobObjectArraySource() throws Exception {
        String createTableWithBlob = "CREATE TABLE PERSONS (id integer primary key, name clob," +
                "content blob, code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000))"

        recreateTable(createTableWithBlob)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        byte[] bytes = "BLOB".getBytes()
        Object[] blobRecordValue = new Object[bytes.length]
        (0 .. (bytes.length-1)).each { i -> blobRecordValue[i] = bytes[i] }

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)
        parser.addSchemaField("content", RecordFieldType.ARRAY)

        parser.addRecord(1, 'rec1', 101, blobRecordValue)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)
        final Connection conn = dbcp.getConnection()
        final Statement stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        assertTrue(rs.next())
        assertEquals(1, rs.getInt(1))
        Clob clob = rs.getClob(2)
        assertNotNull(clob)
        char[] clobText = new char[5]
        int numBytes = clob.characterStream.read(clobText)
        assertEquals(4, numBytes)
        // Ignore last character, it's meant to ensure that only 4 bytes were read even though the buffer is 5 bytes
        assertEquals('rec1', new String(clobText).substring(0,4))
        Blob blob = rs.getBlob(3)
        assertEquals("BLOB", new String(blob.getBytes(1, blob.length() as int)))
        assertEquals(101, rs.getInt(4))

        stmt.close()
        conn.close()
    }

    @Test
    void testInsertWithBlobStringSource() throws Exception {
        String createTableWithBlob = "CREATE TABLE PERSONS (id integer primary key, name clob," +
                "content blob, code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000))"

        recreateTable(createTableWithBlob)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)
        parser.addSchemaField("content", RecordFieldType.STRING)

        parser.addRecord(1, 'rec1', 101, 'BLOB')

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)
        final Connection conn = dbcp.getConnection()
        final Statement stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        assertTrue(rs.next())
        assertEquals(1, rs.getInt(1))
        Clob clob = rs.getClob(2)
        assertNotNull(clob)
        char[] clobText = new char[5]
        int numBytes = clob.characterStream.read(clobText)
        assertEquals(4, numBytes)
        // Ignore last character, it's meant to ensure that only 4 bytes were read even though the buffer is 5 bytes
        assertEquals('rec1', new String(clobText).substring(0,4))
        Blob blob = rs.getBlob(3)
        assertEquals("BLOB", new String(blob.getBytes(1, blob.length() as int)))
        assertEquals(101, rs.getInt(4))

        stmt.close()
        conn.close()
    }

    @Test
    void testInsertWithBlobIntegerArraySource() throws Exception {
        String createTableWithBlob = "CREATE TABLE PERSONS (id integer primary key, name clob," +
                "content blob, code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000))"

        recreateTable(createTableWithBlob)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)
        parser.addSchemaField("content", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType()).getFieldType())

        parser.addRecord(1, 'rec1', 101, [1,2,3] as Integer[])

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0)
        runner.assertTransferCount(PutDatabaseRecord.REL_RETRY, 0)
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 1)
    }
}
