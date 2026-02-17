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

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
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

    private static final String createSchema = "CREATE SCHEMA testSchema";

    private static final String TEST_DB_TYPE = "MySQL";

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
        readerFactory.addSchemaField(new RecordField("fractional", RecordFieldType.DOUBLE.getDataType(), true));
        readerFactory.addSchemaField(new RecordField("code", RecordFieldType.INT.getDataType(), 0, true));
        readerFactory.addSchemaField(new RecordField("newField", RecordFieldType.DOUBLE.getDataType(), false));
        readerFactory.addRecord(1, 1.2345, 10);

        runner.addControllerService("mock-reader-factory", readerFactory);
        runner.enableControllerService(readerFactory);

        runner.setProperty(UpdateDatabaseTable.RECORD_READER, "mock-reader-factory");
        runner.setProperty(UpdateDatabaseTable.TABLE_NAME, "${table.name}");
        runner.setProperty(UpdateDatabaseTable.CREATE_TABLE, UpdateDatabaseTable.CREATE_IF_NOT_EXISTS);
        runner.setProperty(UpdateDatabaseTable.QUOTE_TABLE_IDENTIFIER, "false");
        runner.setProperty(UpdateDatabaseTable.QUOTE_COLUMN_IDENTIFIERS, "true");
        runner.setProperty(UpdateDatabaseTable.DB_TYPE, TEST_DB_TYPE);

        final String tableName = "NEWTABLE";
        Map<String, String> attrs = new HashMap<>();
        attrs.put("db.name", "default");
        attrs.put("table.name", tableName);
        runner.enqueue(new byte[0], attrs);
        runner.run();

        runner.assertTransferCount(UpdateDatabaseTable.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateDatabaseTable.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals(UpdateDatabaseTable.ATTR_OUTPUT_TABLE, tableName);
        try (
                Statement s = getConnection().createStatement();
                ResultSet rs = getTableColumns(s, tableName)
        ) {
            assertColumnEquals(rs, "id", 1, "INTEGER");
            assertColumnEquals(rs, "fractional", 2, "DOUBLE PRECISION");
            assertColumnEquals(rs, "code", 3, "INTEGER");
            assertColumnEquals(rs, "newField", 4, "DOUBLE PRECISION");
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
            readerFactory.addSchemaField(new RecordField("newField", RecordFieldType.DOUBLE.getDataType(), false));
            readerFactory.addRecord(1, "name1", null, "test");

            runner.addControllerService("mock-reader-factory", readerFactory);
            runner.enableControllerService(readerFactory);

            runner.setProperty(UpdateDatabaseTable.RECORD_READER, "mock-reader-factory");
            runner.setProperty(UpdateDatabaseTable.TABLE_NAME, "${table.name}");
            runner.setProperty(UpdateDatabaseTable.CREATE_TABLE, UpdateDatabaseTable.FAIL_IF_NOT_EXISTS);
            runner.setProperty(UpdateDatabaseTable.QUOTE_TABLE_IDENTIFIER, "true");
            runner.setProperty(UpdateDatabaseTable.QUOTE_COLUMN_IDENTIFIERS, "false");
            runner.setProperty(UpdateDatabaseTable.DB_TYPE, TEST_DB_TYPE);

            final String tableName = "persons";
            Map<String, String> attrs = new HashMap<>();
            attrs.put("db.name", "default");
            attrs.put("table.name", tableName);
            runner.enqueue(new byte[0], attrs);
            runner.run();

            runner.assertTransferCount(UpdateDatabaseTable.REL_SUCCESS, 1);
            final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateDatabaseTable.REL_SUCCESS).getFirst();
            flowFile.assertAttributeEquals(UpdateDatabaseTable.ATTR_OUTPUT_TABLE, tableName);

            try (
                    Statement s = getConnection().createStatement();
                    ResultSet rs = getTableColumns(s, tableName)
            ) {
                assertColumnEquals(rs, "id", 1, "INTEGER");
                assertColumnEquals(rs, "name", 2, "CHARACTER VARYING");
                assertColumnEquals(rs, "code", 3, "INTEGER");
                assertColumnEquals(rs, "NEWFIELD", 4, "DOUBLE PRECISION");
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

            final String tableName = "persons";
            Map<String, String> attrs = new HashMap<>();
            attrs.put("db.name", "default");
            attrs.put("table.name", tableName);
            runner.enqueue(new byte[0], attrs);
            runner.run();

            runner.assertTransferCount(UpdateDatabaseTable.REL_SUCCESS, 1);
            final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateDatabaseTable.REL_SUCCESS).getFirst();
            flowFile.assertAttributeEquals(UpdateDatabaseTable.ATTR_OUTPUT_TABLE, tableName);

            try (
                    Statement s = conn.createStatement();
                    ResultSet rs = getTableColumns(s, tableName)
            ) {
                assertColumnEquals(rs, "id", 1, "INTEGER");
                assertColumnEquals(rs, "name", 2, "CHARACTER VARYING");
                assertColumnEquals(rs, "code", 3, "INTEGER");
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testAddExistingColumnNoTranslateFieldNames() throws Exception {
        try (final Connection conn = getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersons);
                stmt.execute("ALTER TABLE \"persons\" ADD COLUMN \"ID\" DOUBLE");
            }

            MockRecordParser readerFactory = new MockRecordParser();

            readerFactory.addSchemaField(new RecordField("ID", RecordFieldType.INT.getDataType(), false));
            readerFactory.addSchemaField(new RecordField("FRACTIONAL", RecordFieldType.DOUBLE.getDataType(), true));
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
            runner.setProperty(UpdateDatabaseTable.DB_TYPE, TEST_DB_TYPE);

            final String tableName = "persons";
            Map<String, String> attrs = new HashMap<>();
            attrs.put("db.name", "default");
            attrs.put("table.name", tableName);
            runner.enqueue(new byte[0], attrs);
            runner.run();

            runner.assertTransferCount(UpdateDatabaseTable.REL_SUCCESS, 1);
            final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateDatabaseTable.REL_SUCCESS).getFirst();
            flowFile.assertAttributeEquals(UpdateDatabaseTable.ATTR_OUTPUT_TABLE, tableName);

            try (
                    Statement s = conn.createStatement();
                    ResultSet rs = getTableColumns(s, tableName)
            ) {
                assertColumnEquals(rs, "id", 1, "INTEGER");
                assertColumnEquals(rs, "name", 2, "CHARACTER VARYING");
                assertColumnEquals(rs, "code", 3, "INTEGER");
                assertColumnEquals(rs, "ID", 4, "DOUBLE PRECISION");
                assertColumnEquals(rs, "FRACTIONAL", 5, "DOUBLE PRECISION");
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
            readerFactory.addSchemaField(new RecordField("newField", RecordFieldType.DOUBLE.getDataType(), 0, true));
            readerFactory.addRecord(1, "name1", null, "test");

            runner.addControllerService("mock-reader-factory", readerFactory);
            runner.enableControllerService(readerFactory);

            runner.setProperty(UpdateDatabaseTable.RECORD_READER, "mock-reader-factory");
            runner.setProperty(UpdateDatabaseTable.TABLE_NAME, "${table.name}");
            runner.setProperty(UpdateDatabaseTable.CREATE_TABLE, UpdateDatabaseTable.FAIL_IF_NOT_EXISTS);
            runner.setProperty(UpdateDatabaseTable.QUOTE_TABLE_IDENTIFIER, "true");
            runner.setProperty(UpdateDatabaseTable.QUOTE_COLUMN_IDENTIFIERS, "false");
            runner.setProperty(UpdateDatabaseTable.UPDATE_FIELD_NAMES, "true");
            runner.setProperty(UpdateDatabaseTable.DB_TYPE, TEST_DB_TYPE);

            MockRecordWriter writerFactory = new MockRecordWriter();
            runner.addControllerService("mock-writer-factory", writerFactory);
            runner.enableControllerService(writerFactory);
            runner.setProperty(UpdateDatabaseTable.RECORD_WRITER_FACTORY, "mock-writer-factory");

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

        final String tableName = "otherSchemaTable";
        Map<String, String> attrs = new HashMap<>();
        attrs.put("db.name", "default");
        attrs.put("table.name", tableName);
        runner.enqueue(new byte[0], attrs);
        runner.run();

        runner.assertTransferCount(UpdateDatabaseTable.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateDatabaseTable.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals(UpdateDatabaseTable.ATTR_OUTPUT_TABLE, tableName);

        try (
                Connection conn = getConnection();
                Statement s = conn.createStatement();
                ResultSet rs = getTableColumns(s, tableName.toUpperCase())
        ) {
            assertColumnEquals(rs, "id", 1, "INTEGER");
            assertColumnEquals(rs, "name", 2, "CHARACTER VARYING");
            assertColumnEquals(rs, "code", 3, "INTEGER");
            assertColumnEquals(rs, "newField", 4, "CHARACTER VARYING");
            assertFalse(rs.next());
        }
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("record-reader", UpdateDatabaseTable.RECORD_READER.getName()),
                Map.entry("db-type", UpdateDatabaseTable.DB_TYPE.getName()),
                Map.entry("updatedatabasetable-dbcp-service", UpdateDatabaseTable.DBCP_SERVICE.getName()),
                Map.entry("updatedatabasetable-catalog-name", UpdateDatabaseTable.CATALOG_NAME.getName()),
                Map.entry("updatedatabasetable-schema-name", UpdateDatabaseTable.SCHEMA_NAME.getName()),
                Map.entry("updatedatabasetable-table-name", UpdateDatabaseTable.TABLE_NAME.getName()),
                Map.entry("updatedatabasetable-create-table", UpdateDatabaseTable.CREATE_TABLE.getName()),
                Map.entry("updatedatabasetable-primary-keys", UpdateDatabaseTable.PRIMARY_KEY_FIELDS.getName()),
                Map.entry("updatedatabasetable-translate-field-names", UpdateDatabaseTable.TRANSLATE_FIELD_NAMES.getName()),
                Map.entry("updatedatabasetable-update-field-names", UpdateDatabaseTable.UPDATE_FIELD_NAMES.getName()),
                Map.entry("updatedatabasetable-record-writer", UpdateDatabaseTable.RECORD_WRITER_FACTORY.getName()),
                Map.entry("updatedatabasetable-quoted-column-identifiers", UpdateDatabaseTable.QUOTE_COLUMN_IDENTIFIERS.getName()),
                Map.entry("updatedatabasetable-quoted-table-identifiers", UpdateDatabaseTable.QUOTE_TABLE_IDENTIFIER.getName()),
                Map.entry("updatedatabasetable-query-timeout", UpdateDatabaseTable.QUERY_TIMEOUT.getName())
        );

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
    }

    private ResultSet getTableColumns(final Statement statement, final String tableName) throws SQLException {
        return statement.executeQuery("""
                    SELECT
                      COLUMN_NAME,
                      ORDINAL_POSITION,
                      DATA_TYPE
                    FROM
                      INFORMATION_SCHEMA.COLUMNS
                    WHERE
                      TABLE_NAME = '%s'
                    ORDER BY
                      ORDINAL_POSITION
                """.formatted(tableName)
        );
    }

    private void assertColumnEquals(final ResultSet rs, final String columnName, final int position, final String dataType) throws SQLException {
        assertTrue(rs.next(), "Row not found for column [%s]".formatted(columnName));
        assertEquals(columnName, rs.getString(1));
        assertEquals(position, rs.getInt(2));
        assertEquals(dataType, rs.getString(3));
    }
}
