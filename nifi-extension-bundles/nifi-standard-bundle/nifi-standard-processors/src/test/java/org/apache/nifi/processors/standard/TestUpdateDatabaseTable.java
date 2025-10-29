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

import org.apache.nifi.processors.standard.db.impl.DerbyDatabaseAdapter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestUpdateDatabaseTable extends AbstractDatabaseConnectionServiceTest {

    private static final String createPersons = "CREATE TABLE \"persons\" (\"id\" integer primary key, \"name\" varchar(100), \"code\" integer)";

    private static final String createSchema = "CREATE SCHEMA \"testSchema\"";

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = newTestRunner(UpdateDatabaseTable.class);
    }

    @AfterEach
    void dropTables() {
        final List<String> tables = List.of(
                "\"persons\"",
                "\"newTable\""
        );

        for (final String table : tables) {
            try (
                    Connection connection = getConnection();
                    Statement statement = connection.createStatement()
            ) {
                statement.execute("DROP TABLE %s".formatted(table));
            } catch (final SQLException ignored) {

            }
        }

        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement()
        ) {
            statement.execute("DROP SCHEMA \"testSchema\"");
        } catch (final SQLException ignored) {

        }
    }

    @Test
    public void testCreateTable() throws Exception {
        MockRecordParser readerFactory = new MockRecordParser();

        readerFactory.addSchemaField(new RecordField("id", RecordFieldType.INT.getDataType(), false));
        readerFactory.addSchemaField(new RecordField("name", RecordFieldType.STRING.getDataType(), true));
        readerFactory.addSchemaField(new RecordField("code", RecordFieldType.INT.getDataType(), 0, true));
        readerFactory.addSchemaField(new RecordField("newField", RecordFieldType.STRING.getDataType(), 0, true));
        readerFactory.addRecord(1, "name1", 10);

        runner.addControllerService("mock-reader-factory", readerFactory);
        runner.enableControllerService(readerFactory);

        runner.setProperty(UpdateDatabaseTable.RECORD_READER, "mock-reader-factory");
        runner.setProperty(UpdateDatabaseTable.TABLE_NAME, "${table.name}");
        runner.setProperty(UpdateDatabaseTable.CREATE_TABLE, UpdateDatabaseTable.CREATE_IF_NOT_EXISTS);
        runner.setProperty(UpdateDatabaseTable.QUOTE_TABLE_IDENTIFIER, "false");
        runner.setProperty(UpdateDatabaseTable.QUOTE_COLUMN_IDENTIFIERS, "true");
        runner.setProperty(UpdateDatabaseTable.DB_TYPE, new DerbyDatabaseAdapter().getName());

        Map<String, String> attrs = new HashMap<>();
        attrs.put("db.name", "default");
        attrs.put("table.name", "newTable");
        runner.enqueue(new byte[0], attrs);
        runner.run();

        runner.assertTransferCount(UpdateDatabaseTable.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateDatabaseTable.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals(UpdateDatabaseTable.ATTR_OUTPUT_TABLE, "newTable");
        // Verify the table has been created with the expected fields
        try (Statement s = getConnection().createStatement()) {
            // The Derby equivalent of DESCRIBE TABLE (using a query rather than the ij tool)
            ResultSet rs = s.executeQuery("select * from sys.syscolumns where referenceid = (select tableid from sys.systables where tablename = 'NEWTABLE') order by columnnumber");
            assertTrue(rs.next());
            // Columns 2,3,4 are Column Name, Column Index, and Column Type
            assertEquals("id", rs.getString(2));
            assertEquals(1, rs.getInt(3));
            assertEquals("INTEGER NOT NULL", rs.getString(4));

            assertTrue(rs.next());
            assertEquals("name", rs.getString(2));
            assertEquals(2, rs.getInt(3));
            assertEquals("VARCHAR(100)", rs.getString(4));

            assertTrue(rs.next());
            assertEquals("code", rs.getString(2));
            assertEquals(3, rs.getInt(3));
            assertEquals("INTEGER", rs.getString(4));

            assertTrue(rs.next());
            assertEquals("newField", rs.getString(2));
            assertEquals(4, rs.getInt(3));
            assertEquals("VARCHAR(100)", rs.getString(4));

            // No more rows
            assertFalse(rs.next());
        }
    }

    @Test
    public void testAddColumnToExistingTable() throws Exception {
        try (final Connection conn = getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersons);
            }

            MockRecordParser readerFactory = new MockRecordParser();

            readerFactory.addSchemaField(new RecordField("id", RecordFieldType.INT.getDataType(), false));
            readerFactory.addSchemaField(new RecordField("name", RecordFieldType.STRING.getDataType(), true));
            readerFactory.addSchemaField(new RecordField("code", RecordFieldType.INT.getDataType(), 0, true));
            readerFactory.addSchemaField(new RecordField("newField", RecordFieldType.STRING.getDataType(), 0, true));
            readerFactory.addRecord(1, "name1", null, "test");

            runner.addControllerService("mock-reader-factory", readerFactory);
            runner.enableControllerService(readerFactory);

            runner.setProperty(UpdateDatabaseTable.RECORD_READER, "mock-reader-factory");
            runner.setProperty(UpdateDatabaseTable.TABLE_NAME, "${table.name}");
            runner.setProperty(UpdateDatabaseTable.CREATE_TABLE, UpdateDatabaseTable.FAIL_IF_NOT_EXISTS);
            runner.setProperty(UpdateDatabaseTable.QUOTE_TABLE_IDENTIFIER, "true");
            runner.setProperty(UpdateDatabaseTable.QUOTE_COLUMN_IDENTIFIERS, "false");
            runner.setProperty(UpdateDatabaseTable.DB_TYPE, new DerbyDatabaseAdapter().getName());

            Map<String, String> attrs = new HashMap<>();
            attrs.put("db.name", "default");
            attrs.put("table.name", "persons");
            runner.enqueue(new byte[0], attrs);
            runner.run();

            runner.assertTransferCount(UpdateDatabaseTable.REL_SUCCESS, 1);
            final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateDatabaseTable.REL_SUCCESS).getFirst();
            flowFile.assertAttributeEquals(UpdateDatabaseTable.ATTR_OUTPUT_TABLE, "persons");
            // Verify the table has been updated with the expected field(s)
            try (Statement s = conn.createStatement()) {
                // The Derby equivalent of DESCRIBE TABLE (using a query rather than the ij tool)
                ResultSet rs = s.executeQuery("SELECT * FROM SYS.SYSCOLUMNS WHERE referenceid = (SELECT tableid FROM SYS.SYSTABLES WHERE tablename = 'persons') ORDER BY columnnumber");
                assertTrue(rs.next());
                // Columns 2,3,4 are Column Name, Column Index, and Column Type
                assertEquals("id", rs.getString(2));
                assertEquals(1, rs.getInt(3));
                // Primary key cannot be null, Derby stores that in this column
                assertEquals("INTEGER NOT NULL", rs.getString(4));

                assertTrue(rs.next());
                assertEquals("name", rs.getString(2));
                assertEquals(2, rs.getInt(3));
                assertEquals("VARCHAR(100)", rs.getString(4));

                assertTrue(rs.next());
                assertEquals("code", rs.getString(2));
                assertEquals(3, rs.getInt(3));
                assertEquals("INTEGER", rs.getString(4));

                assertTrue(rs.next());
                assertEquals("NEWFIELD", rs.getString(2));
                assertEquals(4, rs.getInt(3));
                assertEquals("VARCHAR(100)", rs.getString(4));

                // No more rows
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testAddExistingColumnTranslateFieldNames() throws Exception {
        try (final Connection conn = getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersons);
            }

            MockRecordParser readerFactory = new MockRecordParser();

            readerFactory.addSchemaField(new RecordField("ID", RecordFieldType.INT.getDataType(), false));
            readerFactory.addSchemaField(new RecordField("NAME", RecordFieldType.STRING.getDataType(), true));
            readerFactory.addSchemaField(new RecordField("CODE", RecordFieldType.INT.getDataType(), 0, true));
            readerFactory.addRecord(1, "name1", null, "test");

            runner.addControllerService("mock-reader-factory", readerFactory);
            runner.enableControllerService(readerFactory);

            runner.setProperty(UpdateDatabaseTable.RECORD_READER, "mock-reader-factory");
            runner.setProperty(UpdateDatabaseTable.TABLE_NAME, "${table.name}");
            runner.setProperty(UpdateDatabaseTable.CREATE_TABLE, UpdateDatabaseTable.FAIL_IF_NOT_EXISTS);
            runner.setProperty(UpdateDatabaseTable.TRANSLATE_FIELD_NAMES, "true");
            runner.setProperty(UpdateDatabaseTable.QUOTE_TABLE_IDENTIFIER, "true");
            runner.setProperty(UpdateDatabaseTable.QUOTE_COLUMN_IDENTIFIERS, "false");
            runner.setProperty(UpdateDatabaseTable.DB_TYPE, new DerbyDatabaseAdapter().getName());

            Map<String, String> attrs = new HashMap<>();
            attrs.put("db.name", "default");
            attrs.put("table.name", "persons");
            runner.enqueue(new byte[0], attrs);
            runner.run();

            runner.assertTransferCount(UpdateDatabaseTable.REL_SUCCESS, 1);
            final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateDatabaseTable.REL_SUCCESS).getFirst();
            flowFile.assertAttributeEquals(UpdateDatabaseTable.ATTR_OUTPUT_TABLE, "persons");
            // Verify the table has been updated with the expected field(s)
            try (Statement s = conn.createStatement()) {
                // The Derby equivalent of DESCRIBE TABLE (using a query rather than the ij tool)
                ResultSet rs = s.executeQuery("SELECT * FROM SYS.SYSCOLUMNS WHERE referenceid = (SELECT tableid FROM SYS.SYSTABLES WHERE tablename = 'persons') ORDER BY columnnumber");
                assertTrue(rs.next());
                // Columns 2,3,4 are Column Name, Column Index, and Column Type
                assertEquals("id", rs.getString(2));
                assertEquals(1, rs.getInt(3));
                // Primary key cannot be null, Derby stores that in this column
                assertEquals("INTEGER NOT NULL", rs.getString(4));

                assertTrue(rs.next());
                assertEquals("name", rs.getString(2));
                assertEquals(2, rs.getInt(3));
                assertEquals("VARCHAR(100)", rs.getString(4));

                assertTrue(rs.next());
                assertEquals("code", rs.getString(2));
                assertEquals(3, rs.getInt(3));
                assertEquals("INTEGER", rs.getString(4));

                // No more rows
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testAddExistingColumnNoTranslateFieldNames() throws Exception {
        try (final Connection conn = getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersons);
                stmt.execute("ALTER TABLE \"persons\" ADD COLUMN \"ID\" INTEGER");
            }

            MockRecordParser readerFactory = new MockRecordParser();

            readerFactory.addSchemaField(new RecordField("ID", RecordFieldType.INT.getDataType(), false));
            readerFactory.addSchemaField(new RecordField("NAME", RecordFieldType.STRING.getDataType(), true));
            readerFactory.addSchemaField(new RecordField("code", RecordFieldType.INT.getDataType(), 0, true));
            readerFactory.addRecord(1, "name1", null, "test");

            runner.addControllerService("mock-reader-factory", readerFactory);
            runner.enableControllerService(readerFactory);

            runner.setProperty(UpdateDatabaseTable.RECORD_READER, "mock-reader-factory");
            runner.setProperty(UpdateDatabaseTable.TABLE_NAME, "${table.name}");
            runner.setProperty(UpdateDatabaseTable.CREATE_TABLE, UpdateDatabaseTable.FAIL_IF_NOT_EXISTS);
            runner.setProperty(UpdateDatabaseTable.TRANSLATE_FIELD_NAMES, "false");
            runner.setProperty(UpdateDatabaseTable.QUOTE_TABLE_IDENTIFIER, "true");
            runner.setProperty(UpdateDatabaseTable.QUOTE_COLUMN_IDENTIFIERS, "false");
            runner.setProperty(UpdateDatabaseTable.DB_TYPE, new DerbyDatabaseAdapter().getName());

            Map<String, String> attrs = new HashMap<>();
            attrs.put("db.name", "default");
            attrs.put("table.name", "persons");
            runner.enqueue(new byte[0], attrs);
            runner.run();

            runner.assertTransferCount(UpdateDatabaseTable.REL_SUCCESS, 1);
            final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateDatabaseTable.REL_SUCCESS).getFirst();
            flowFile.assertAttributeEquals(UpdateDatabaseTable.ATTR_OUTPUT_TABLE, "persons");
            // Verify the table has been updated with the expected field(s)
            try (Statement s = conn.createStatement()) {
                // The Derby equivalent of DESCRIBE TABLE (using a query rather than the ij tool)
                ResultSet rs = s.executeQuery("SELECT * FROM SYS.SYSCOLUMNS WHERE referenceid = (SELECT tableid FROM SYS.SYSTABLES WHERE tablename = 'persons') ORDER BY columnnumber");
                assertTrue(rs.next());
                // Columns 2,3,4 are Column Name, Column Index, and Column Type
                assertEquals("id", rs.getString(2));
                assertEquals(1, rs.getInt(3));
                // Primary key cannot be null, Derby stores that in this column
                assertEquals("INTEGER NOT NULL", rs.getString(4));

                assertTrue(rs.next());
                assertEquals("name", rs.getString(2));
                assertEquals(2, rs.getInt(3));
                assertEquals("VARCHAR(100)", rs.getString(4));

                assertTrue(rs.next());
                assertEquals("code", rs.getString(2));
                assertEquals(3, rs.getInt(3));
                assertEquals("INTEGER", rs.getString(4));

                assertTrue(rs.next());
                assertEquals("ID", rs.getString(2));
                assertEquals(4, rs.getInt(3));
                assertEquals("INTEGER", rs.getString(4));

                assertTrue(rs.next());
                assertEquals("NAME", rs.getString(2));
                assertEquals(5, rs.getInt(3));
                assertEquals("VARCHAR(100)", rs.getString(4));

                // No more rows
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testAddColumnToExistingTableUpdateFieldNames() throws Exception {
        try (final Connection conn = getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersons);
            }

            MockRecordParser readerFactory = new MockRecordParser();

            readerFactory.addSchemaField(new RecordField("id", RecordFieldType.INT.getDataType(), false));
            readerFactory.addSchemaField(new RecordField("name", RecordFieldType.STRING.getDataType(), true));
            readerFactory.addSchemaField(new RecordField("code", RecordFieldType.INT.getDataType(), 0, true));
            readerFactory.addSchemaField(new RecordField("newField", RecordFieldType.STRING.getDataType(), 0, true));
            readerFactory.addRecord(1, "name1", null, "test");

            runner.addControllerService("mock-reader-factory", readerFactory);
            runner.enableControllerService(readerFactory);

            runner.setProperty(UpdateDatabaseTable.RECORD_READER, "mock-reader-factory");
            runner.setProperty(UpdateDatabaseTable.TABLE_NAME, "${table.name}");
            runner.setProperty(UpdateDatabaseTable.CREATE_TABLE, UpdateDatabaseTable.FAIL_IF_NOT_EXISTS);
            runner.setProperty(UpdateDatabaseTable.QUOTE_TABLE_IDENTIFIER, "true");
            runner.setProperty(UpdateDatabaseTable.QUOTE_COLUMN_IDENTIFIERS, "false");
            runner.setProperty(UpdateDatabaseTable.UPDATE_FIELD_NAMES, "true");

            MockRecordWriter writerFactory = new MockRecordWriter();
            runner.addControllerService("mock-writer-factory", writerFactory);
            runner.enableControllerService(writerFactory);
            runner.setProperty(UpdateDatabaseTable.RECORD_WRITER_FACTORY, "mock-writer-factory");

            runner.setProperty(UpdateDatabaseTable.DB_TYPE, new DerbyDatabaseAdapter().getName());

            Map<String, String> attrs = new HashMap<>();
            attrs.put("db.name", "default");
            attrs.put("table.name", "persons");
            runner.enqueue(new byte[0], attrs);
            runner.run();

            runner.assertTransferCount(UpdateDatabaseTable.REL_SUCCESS, 1);
            final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateDatabaseTable.REL_SUCCESS).getFirst();
            // Ensure the additional field is written out to the FlowFile
            flowFile.assertContentEquals("\"1\",\"name1\",\"0\",\"test\"\n");
        }
    }

    @Test
    public void testCreateTableNonDefaultSchema() throws Exception {
        try (final Connection conn = getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createSchema);
            }
        }
        MockRecordParser readerFactory = new MockRecordParser();

        readerFactory.addSchemaField(new RecordField("id", RecordFieldType.INT.getDataType(), false));
        readerFactory.addSchemaField(new RecordField("name", RecordFieldType.STRING.getDataType(), true));
        readerFactory.addSchemaField(new RecordField("code", RecordFieldType.INT.getDataType(), 0, true));
        readerFactory.addSchemaField(new RecordField("newField", RecordFieldType.STRING.getDataType(), 0, true));
        readerFactory.addRecord(1, "name1", 10);

        runner.addControllerService("mock-reader-factory", readerFactory);
        runner.enableControllerService(readerFactory);

        runner.setProperty(UpdateDatabaseTable.RECORD_READER, "mock-reader-factory");
        runner.setProperty(UpdateDatabaseTable.SCHEMA_NAME, "testSchema");
        runner.setProperty(UpdateDatabaseTable.TABLE_NAME, "${table.name}");
        runner.setProperty(UpdateDatabaseTable.CREATE_TABLE, UpdateDatabaseTable.CREATE_IF_NOT_EXISTS);
        runner.setProperty(UpdateDatabaseTable.QUOTE_TABLE_IDENTIFIER, "false");
        runner.setProperty(UpdateDatabaseTable.QUOTE_COLUMN_IDENTIFIERS, "true");
        runner.setProperty(UpdateDatabaseTable.DB_TYPE, new DerbyDatabaseAdapter().getName());

        Map<String, String> attrs = new HashMap<>();
        attrs.put("db.name", "default");
        attrs.put("table.name", "newTable");
        runner.enqueue(new byte[0], attrs);
        runner.run();

        runner.assertTransferCount(UpdateDatabaseTable.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateDatabaseTable.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals(UpdateDatabaseTable.ATTR_OUTPUT_TABLE, "newTable");
        // Verify the table has been created with the expected fields
        try (Statement s = getConnection().createStatement()) {
            // The Derby equivalent of DESCRIBE TABLE (using a query rather than the ij tool)
            ResultSet rs = s.executeQuery("select * from sys.syscolumns where referenceid = (select tableid from sys.systables "
                    + "join sys.sysschemas on sys.systables.schemaid = sys.sysschemas.schemaid where tablename = 'NEWTABLE' and sys.sysschemas.schemaname = 'TESTSCHEMA') order by columnnumber");
            assertTrue(rs.next());
            // Columns 2,3,4 are Column Name, Column Index, and Column Type
            assertEquals("id", rs.getString(2));
            assertEquals(1, rs.getInt(3));
            assertEquals("INTEGER NOT NULL", rs.getString(4));

            assertTrue(rs.next());
            assertEquals("name", rs.getString(2));
            assertEquals(2, rs.getInt(3));
            assertEquals("VARCHAR(100)", rs.getString(4));

            assertTrue(rs.next());
            assertEquals("code", rs.getString(2));
            assertEquals(3, rs.getInt(3));
            assertEquals("INTEGER", rs.getString(4));

            assertTrue(rs.next());
            assertEquals("newField", rs.getString(2));
            assertEquals(4, rs.getInt(3));
            assertEquals("VARCHAR(100)", rs.getString(4));

            // No more rows
            assertFalse(rs.next());
        }
    }
}
