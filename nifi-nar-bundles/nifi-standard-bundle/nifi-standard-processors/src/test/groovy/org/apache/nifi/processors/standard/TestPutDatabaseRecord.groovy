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

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLDataException
import java.sql.SQLException
import java.sql.SQLNonTransientConnectionException
import java.sql.Statement
import java.util.function.Supplier

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertTrue
import static org.junit.Assert.fail
import static org.mockito.Matchers.anyMap
import static org.mockito.Mockito.doAnswer
import static org.mockito.Mockito.only
import static org.mockito.Mockito.spy
import static org.mockito.Mockito.times
import static org.mockito.Mockito.verify

/**
 * Unit tests for the PutDatabaseRecord processor
 */
@RunWith(JUnit4.class)
class TestPutDatabaseRecord {

    private static final String createPersons = "CREATE TABLE PERSONS (id integer primary key, name varchar(100)," +
            " code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000))"
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
                        new PutDatabaseRecord.ColumnDescription('id', 4, true, 2),
                        new PutDatabaseRecord.ColumnDescription('name', 12, true, 255),
                        new PutDatabaseRecord.ColumnDescription('code', 4, true, 10)
                ],
                false,
                ['id'] as Set<String>,
                ''

        ] as PutDatabaseRecord.TableSchema

        runner.setProperty(PutDatabaseRecord.TRANSLATE_FIELD_NAMES, 'false')
        runner.setProperty(PutDatabaseRecord.UNMATCHED_FIELD_BEHAVIOR, PutDatabaseRecord.IGNORE_UNMATCHED_FIELD)
        runner.setProperty(PutDatabaseRecord.UNMATCHED_COLUMN_BEHAVIOR, PutDatabaseRecord.IGNORE_UNMATCHED_COLUMN)
        runner.setProperty(PutDatabaseRecord.QUOTED_IDENTIFIERS, 'false')
        runner.setProperty(PutDatabaseRecord.QUOTED_TABLE_IDENTIFIER, 'false')
        def settings = new PutDatabaseRecord.DMLSettings(runner.getProcessContext())

        processor.with {

            assertEquals('INSERT INTO PERSONS (id, name, code) VALUES (?,?,?)',
                    generateInsert(schema, 'PERSONS', tableSchema, settings).sql)

            assertEquals('UPDATE PERSONS SET name = ?, code = ? WHERE id = ?',
                    generateUpdate(schema, 'PERSONS', null, tableSchema, settings).sql)

            assertEquals('DELETE FROM PERSONS WHERE (id = ? OR (id is null AND ? is null)) AND (name = ? OR (name is null AND ? is null)) AND (code = ? OR (code is null AND ? is null))',
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
                        new PutDatabaseRecord.ColumnDescription('id', 4, true, 2),
                        new PutDatabaseRecord.ColumnDescription('name', 12, true, 255),
                        new PutDatabaseRecord.ColumnDescription('code', 4, true, 10)
                ],
                false,
                ['id'] as Set<String>,
                ''

        ] as PutDatabaseRecord.TableSchema

        runner.setProperty(PutDatabaseRecord.TRANSLATE_FIELD_NAMES, 'false')
        runner.setProperty(PutDatabaseRecord.UNMATCHED_FIELD_BEHAVIOR, PutDatabaseRecord.FAIL_UNMATCHED_FIELD)
        runner.setProperty(PutDatabaseRecord.UNMATCHED_COLUMN_BEHAVIOR, PutDatabaseRecord.IGNORE_UNMATCHED_COLUMN)
        runner.setProperty(PutDatabaseRecord.QUOTED_IDENTIFIERS, 'false')
        runner.setProperty(PutDatabaseRecord.QUOTED_TABLE_IDENTIFIER, 'false')
        def settings = new PutDatabaseRecord.DMLSettings(runner.getProcessContext())

        processor.with {

            try {
                generateInsert(schema, 'PERSONS', tableSchema, settings)
                fail('generateInsert should fail with unmatched fields')
            } catch (SQLDataException e) {
                assertEquals("Cannot map field 'non_existing' to any column in the database", e.getMessage())
            }

            try {
                generateUpdate(schema, 'PERSONS', null, tableSchema, settings)
                fail('generateUpdate should fail with unmatched fields')
            } catch (SQLDataException e) {
                assertEquals("Cannot map field 'non_existing' to any column in the database", e.getMessage())
            }

            try {
                generateDelete(schema, 'PERSONS', tableSchema, settings)
                fail('generateDelete should fail with unmatched fields')
            } catch (SQLDataException e) {
                assertEquals("Cannot map field 'non_existing' to any column in the database", e.getMessage())
            }
        }
    }

    @Test
    void testInsert() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable("PERSONS", createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)

        parser.addRecord(1, 'rec1', 101)
        parser.addRecord(2, 'rec2', 102)
        parser.addRecord(3, 'rec3', 103)
        parser.addRecord(4, 'rec4', 104)

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
        assertTrue(rs.next())
        assertEquals(2, rs.getInt(1))
        assertEquals('rec2', rs.getString(2))
        assertEquals(102, rs.getInt(3))
        assertTrue(rs.next())
        assertEquals(3, rs.getInt(1))
        assertEquals('rec3', rs.getString(2))
        assertEquals(103, rs.getInt(3))
        assertTrue(rs.next())
        assertEquals(4, rs.getInt(1))
        assertEquals('rec4', rs.getString(2))
        assertEquals(104, rs.getInt(3))
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }

    @Test
    void testInsertBatchUpdateException() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable("PERSONS", createPersons)
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

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0)
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 0)
        runner.assertTransferCount(PutDatabaseRecord.REL_RETRY, 1)
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
        recreateTable("PERSONS", createPersons)
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
        try {
            runner.run()
            fail("ProcessException is expected")
        } catch (AssertionError e) {
            assertTrue(e.getCause() instanceof ProcessException)
        }

        final Connection conn = dbcp.getConnection()
        final Statement stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        // Transaction should be rolled back and table should remain empty.
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }

    @Test
    void testInsertNoTable() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable("PERSONS", createPersons)
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
    void testInsertViaSqlStatementType() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable("PERSONS", createPersons)
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
    void testSqlStatementTypeNoValue() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable("PERSONS", createPersons)
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
        recreateTable("PERSONS", createPersons)
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
        try {
            runner.run()
            fail("ProcessException is expected")
        } catch (AssertionError e) {
            assertTrue(e.getCause() instanceof ProcessException)
        }

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0)
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 0)
    }

    @Test
    void testUpdate() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable("PERSONS", createPersons)
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
    void testUpdateAfterInsert() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable("PERSONS", createPersons)
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
        stmt = conn.createStatement()
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
        recreateTable("PERSONS", 'CREATE TABLE PERSONS (id integer, name varchar(100), code integer)')
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.UPDATE_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0)
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 1)
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutDatabaseRecord.REL_FAILURE).get(0)
        assertEquals('Table \'PERSONS\' does not have a Primary Key and no Update Keys were specified', flowFile.getAttribute(PutDatabaseRecord.PUT_DATABASE_RECORD_ERROR))
    }

    @Test
    void testUpdateSpecifyUpdateKeys() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable("PERSONS", 'CREATE TABLE PERSONS (id integer, name varchar(100), code integer)')
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
        recreateTable("PERSONS", createPersons)
        Connection conn = dbcp.getConnection()
        Statement stmt = conn.createStatement()
        stmt.execute("INSERT INTO PERSONS VALUES (1,'rec1', 101)")
        stmt.execute("INSERT INTO PERSONS VALUES (2,'rec2', 102)")
        stmt.execute("INSERT INTO PERSONS VALUES (3,'rec3', 103)")
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
        recreateTable("PERSONS", createPersons)
        Connection conn = dbcp.getConnection()
        Statement stmt = conn.createStatement()
        stmt.execute("INSERT INTO PERSONS VALUES (1,'rec1', 101)")
        stmt.execute("INSERT INTO PERSONS VALUES (2,'rec2', null)")
        stmt.execute("INSERT INTO PERSONS VALUES (3,'rec3', 103)")
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
    void testInsertWithMaxBatchSize() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable("PERSONS", createPersons)
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
        recreateTable("PERSONS", createPersons)
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

    private void recreateTable(String tableName, String createSQL) throws ProcessException, SQLException {
        final Connection conn = dbcp.getConnection()
        final Statement stmt = conn.createStatement()
        try {
            stmt.executeUpdate("drop table " + tableName)
        } catch (SQLException ignore) {
            // Do nothing, may not have existed
        }
        stmt.executeUpdate(createSQL)
        stmt.close()
        conn.close()
    }
}
