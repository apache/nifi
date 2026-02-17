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

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.db.JdbcProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueryDatabaseTableTest extends AbstractDatabaseConnectionServiceTest {

    private static final String TABLE_NAME_KEY = "tableName";
    private static final String MAX_ROWS_KEY = "maxRows";

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws InitializationException, IOException {
        runner = newTestRunner(QueryDatabaseTable.class);

        runner.setProperty(QueryDatabaseTable.DB_TYPE, "Generic");
        runner.getStateManager().clear(Scope.CLUSTER);
    }

    @AfterEach
    void teardown() throws IOException {
        runner.getStateManager().clear(Scope.CLUSTER);
        runner = null;

        final List<String> tables = List.of(
                "TEST_QUERY_DB_TABLE",
                "TEST_NULL_INT",
                "TYPE_LIST"
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
    }

    @Test
    public void testAddedRows() throws SQLException, IOException {
        executeSql("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (0, 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (1, 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (2, NULL, 2.0, '2010-01-01 00:00:00')");

        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty(QueryDatabaseTable.MAX_ROWS_PER_FLOW_FILE, "2");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 2);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst();
        assertEquals("TEST_QUERY_DB_TABLE", flowFile.getAttribute(QueryDatabaseTable.RESULT_TABLENAME));
        assertEquals("2", flowFile.getAttribute("maxvalue.id"));
        InputStream in = new ByteArrayInputStream(flowFile.toByteArray());
        runner.setProperty(QueryDatabaseTable.FETCH_SIZE, "2");
        assertEquals(2, getNumberOfRecordsFromStream(in));

        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(1);
        assertEquals("2", flowFile.getAttribute("maxvalue.id"));
        in = new ByteArrayInputStream(flowFile.toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();

        //Remove Max Rows Per FlowFile
        runner.setProperty(QueryDatabaseTable.MAX_ROWS_PER_FLOW_FILE, "0");

        // Add a new row with a higher ID and run, one flowfile with one new row should be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (3, 'Mary West', 15.0, '2000-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst();
        assertEquals("3", flowFile.getAttribute("maxvalue.id"));
        in = new ByteArrayInputStream(flowFile.toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));

        // Sanity check - run again, this time no flowfiles/rows should be transferred
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add timestamp as a max value column name
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "id, created_on");

        // Add a new row with a higher ID and run, one flow file will be transferred because no max value for the timestamp has been stored
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (4, 'Marty Johnson', 15.0, '2011-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst();
        assertEquals("4", flowFile.getAttribute("maxvalue.id"));
        assertEquals("2011-01-01 03:23:34.234", flowFile.getAttribute("maxvalue.created_on"));
        in = new ByteArrayInputStream(flowFile.toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Add a new row with a higher ID but lower timestamp and run, no flow file will be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (5, 'NO NAME', 15.0, '2001-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add a new row with a higher ID and run, one flow file will be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (6, 'Mr. NiFi', 1.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Set name as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "name");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(7, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Add a new row with a "higher" name than the max but lower than "NULL" (to test that null values are skipped), one flow file will be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (7, 'NULK', 1.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Set scale as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "scale");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(8, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Add a new row with a higher value for scale than the max, one flow file will be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (8, 'NULK', 100.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Set scale as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "bignum");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(9, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Add a new row with a higher value for scale than the max, one flow file will be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on, bignum) VALUES (9, 'Alice Bob', 100.0, '2012-01-01 03:23:34.234', 1)");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();
    }

    @Test
    public void testAddedRowsAutoCommitTrue() throws SQLException, IOException {
        executeSql("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (0, 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (1, 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (2, NULL, 2.0, '2010-01-01 00:00:00')");

        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty(QueryDatabaseTable.MAX_ROWS_PER_FLOW_FILE, "2");
        runner.setProperty(QueryDatabaseTable.FETCH_SIZE, "2");
        runner.setProperty(QueryDatabaseTable.AUTO_COMMIT, "true");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 2);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst();
        assertEquals("TEST_QUERY_DB_TABLE", flowFile.getAttribute(QueryDatabaseTable.RESULT_TABLENAME));
        assertEquals("2", flowFile.getAttribute("maxvalue.id"));
        InputStream in = new ByteArrayInputStream(flowFile.toByteArray());
        assertEquals(2, getNumberOfRecordsFromStream(in));

        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(1);
        assertEquals("2", flowFile.getAttribute("maxvalue.id"));
        in = new ByteArrayInputStream(flowFile.toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
    }

    @Test
    public void testAddedRowsAutoCommitFalse() throws SQLException, IOException {
        executeSql("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (0, 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (1, 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (2, NULL, 2.0, '2010-01-01 00:00:00')");

        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty(QueryDatabaseTable.MAX_ROWS_PER_FLOW_FILE, "2");
        runner.setProperty(QueryDatabaseTable.FETCH_SIZE, "2");
        runner.setProperty(QueryDatabaseTable.AUTO_COMMIT, "false");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 2);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst();
        assertEquals("TEST_QUERY_DB_TABLE", flowFile.getAttribute(QueryDatabaseTable.RESULT_TABLENAME));
        assertEquals("2", flowFile.getAttribute("maxvalue.id"));
        InputStream in = new ByteArrayInputStream(flowFile.toByteArray());
        assertEquals(2, getNumberOfRecordsFromStream(in));

        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(1);
        assertEquals("2", flowFile.getAttribute("maxvalue.id"));
        in = new ByteArrayInputStream(flowFile.toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
    }

    @Test
    public void testAddedRowsTwoTables() throws SQLException, IOException {
        executeSql("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (0, 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (1, 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (2, NULL, 2.0, '2010-01-01 00:00:00')");

        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty(QueryDatabaseTable.MAX_ROWS_PER_FLOW_FILE, "2");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 2);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst();
        assertEquals("TEST_QUERY_DB_TABLE", flowFile.getAttribute(QueryDatabaseTable.RESULT_TABLENAME));
        assertEquals("2", flowFile.getAttribute("maxvalue.id"));
        InputStream in = new ByteArrayInputStream(flowFile.toByteArray());
        runner.setProperty(QueryDatabaseTable.FETCH_SIZE, "2");
        assertEquals(2, getNumberOfRecordsFromStream(in));

        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(1);
        assertEquals("2", flowFile.getAttribute("maxvalue.id"));
        in = new ByteArrayInputStream(flowFile.toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Populate a second table and set
        executeSql("create table TEST_QUERY_DB_TABLE2 (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        executeSql("insert into TEST_QUERY_DB_TABLE2 (id, name, scale, created_on) VALUES (0, 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        executeSql("insert into TEST_QUERY_DB_TABLE2 (id, name, scale, created_on) VALUES (1, 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        executeSql("insert into TEST_QUERY_DB_TABLE2 (id, name, scale, created_on) VALUES (2, NULL, 2.0, '2010-01-01 00:00:00')");

        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "TEST_QUERY_DB_TABLE2");
        runner.setProperty(QueryDatabaseTable.MAX_ROWS_PER_FLOW_FILE, "0");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);

        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst();
        assertEquals("TEST_QUERY_DB_TABLE2", flowFile.getAttribute(QueryDatabaseTable.RESULT_TABLENAME));
        assertEquals("2", flowFile.getAttribute("maxvalue.id"));
        in = new ByteArrayInputStream(flowFile.toByteArray());
        assertEquals(3, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Add a new row with a higher ID and run, one flowfile with one new row should be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE2 (id, name, scale, created_on) VALUES (3, 'Mary West', 15.0, '2000-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst();
        assertEquals("3", flowFile.getAttribute("maxvalue.id"));
        in = new ByteArrayInputStream(flowFile.toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));

        // Sanity check - run again, this time no flowfiles/rows should be transferred
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();
    }

    @Test
    public void testMultiplePartitions() throws SQLException {
        executeSql("create table TEST_QUERY_DB_TABLE (id integer not null, bucket integer not null)");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, bucket) VALUES (0, 0)");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, bucket) VALUES (1, 0)");

        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "ID, BUCKET");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        assertEquals("2",
                runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().getAttribute(QueryDatabaseTable.RESULT_ROW_COUNT)
        );
        runner.clearTransferState();

        // Add a new row in the same bucket
        executeSql("insert into TEST_QUERY_DB_TABLE (id, bucket) VALUES (2, 0)");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        assertEquals("1",
                runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().getAttribute(QueryDatabaseTable.RESULT_ROW_COUNT)
        );
        runner.clearTransferState();

        // Add a new row in a new bucket
        executeSql("insert into TEST_QUERY_DB_TABLE (id, bucket) VALUES (3, 1)");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        assertEquals("1",
                runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().getAttribute(QueryDatabaseTable.RESULT_ROW_COUNT)
        );
        runner.clearTransferState();

        // Add a new row in an old bucket, it should not be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE (id, bucket) VALUES (4, 0)");
        runner.run();
        runner.assertTransferCount(QueryDatabaseTable.REL_SUCCESS, 0);

        // Add a new row in the second bucket, only the new row should be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE (id, bucket) VALUES (5, 1)");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        assertEquals("1",
                runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().getAttribute(QueryDatabaseTable.RESULT_ROW_COUNT)
        );
        runner.clearTransferState();
    }

    @Test
    public void testTimestampNanos() throws SQLException, IOException {
        executeSql("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (1, 'Carrie Jones', 5.0, '2000-01-01 03:23:34.000123456')");

        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "created_on");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        InputStream in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add a new row with a lower timestamp (but same millisecond value), no flow file should be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (3, 'Mary West', 15.0, '2000-01-01 03:23:34.000')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add a new row with a higher timestamp, one flow file should be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (3, 'Mary West', 15.0, '2000-01-01 03:23:34.0003')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();
    }

    @Test
    public void testWithNullIntColumn() throws SQLException {
        executeSql("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        executeSql("insert into TEST_NULL_INT (id, val1, val2) VALUES (0, NULL, 1)");
        executeSql("insert into TEST_NULL_INT (id, val1, val2) VALUES (1, 1, 1)");

        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "TEST_NULL_INT");
        runner.run();

        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().assertAttributeEquals(QueryDatabaseTable.RESULT_ROW_COUNT, "2");
    }

    @Test
    public void testWithSqlException() throws SQLException {
        executeSql("create table TEST_NO_ROWS (id integer)");

        runner.setIncomingConnection(false);
        // Try a valid SQL statement that will generate an error (val1 does not exist, e.g.)
        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "TEST_NO_ROWS");
        runner.setProperty(QueryDatabaseTable.COLUMN_NAMES, "val1");
        runner.run();

        assertTrue(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).isEmpty());
    }

    @Test
    public void testOutputBatchSize() throws SQLException, IOException {
        executeSql("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        int rowCount = 0;
        // Create larger row set
        for (int batch = 0; batch < 100; batch++) {
            executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (" + rowCount + ", 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
            rowCount++;
        }

        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty(QueryDatabaseTable.MAX_ROWS_PER_FLOW_FILE, "${" + MAX_ROWS_KEY + "}");
        runner.setEnvironmentVariableValue(MAX_ROWS_KEY, "7");
        runner.setProperty(QueryDatabaseTable.OUTPUT_BATCH_SIZE, "${outputBatchSize}");
        runner.setEnvironmentVariableValue("outputBatchSize", "4");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 15);

        InputStream in;
        MockFlowFile mff;

        // Ensure all but the last file have 7 records each
        for (int ff = 0; ff < 14; ff++) {
            mff = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(ff);
            in = new ByteArrayInputStream(mff.toByteArray());
            assertEquals(7, getNumberOfRecordsFromStream(in));

            mff.assertAttributeExists("fragment.identifier");
            assertEquals(Integer.toString(ff), mff.getAttribute("fragment.index"));
            // No fragment.count set for flow files sent when Output Batch Size is set
            assertNull(mff.getAttribute("fragment.count"));
        }

        // Last file should have 2 records
        mff = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(14);
        in = new ByteArrayInputStream(mff.toByteArray());
        assertEquals(2, getNumberOfRecordsFromStream(in));
        mff.assertAttributeExists("fragment.identifier");
        assertEquals(Integer.toString(14), mff.getAttribute("fragment.index"));
        // No fragment.count set for flow files sent when Output Batch Size is set
        assertNull(mff.getAttribute("fragment.count"));
    }

    @Test
    public void testMaxRowsPerFlowFile() throws SQLException, IOException {
        InputStream in;
        MockFlowFile mff;

        executeSql("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        int rowCount = 0;
        //create larger row set
        for (int batch = 0; batch < 100; batch++) {
            executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (" + rowCount + ", 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
            rowCount++;
        }

        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty(QueryDatabaseTable.MAX_ROWS_PER_FLOW_FILE, "${" + MAX_ROWS_KEY + "}");
        runner.setEnvironmentVariableValue(MAX_ROWS_KEY, "9");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 12);

        //ensure all but the last file have 9 records each
        for (int ff = 0; ff < 11; ff++) {
            mff = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(ff);
            in = new ByteArrayInputStream(mff.toByteArray());
            assertEquals(9, getNumberOfRecordsFromStream(in));

            mff.assertAttributeExists("fragment.identifier");
            assertEquals(Integer.toString(ff), mff.getAttribute("fragment.index"));
            assertEquals("12", mff.getAttribute("fragment.count"));
        }

        //last file should have 1 record
        mff = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(11);
        in = new ByteArrayInputStream(mff.toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        mff.assertAttributeExists("fragment.identifier");
        assertEquals(Integer.toString(11), mff.getAttribute("fragment.index"));
        assertEquals("12", mff.getAttribute("fragment.count"));
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Run again, this time should be a single partial flow file
        for (int batch = 0; batch < 5; batch++) {
            executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (" + rowCount + ", 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
            rowCount++;
        }

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        mff = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst();
        in = new ByteArrayInputStream(mff.toByteArray());
        mff.assertAttributeExists("fragment.identifier");
        assertEquals(Integer.toString(0), mff.getAttribute("fragment.index"));
        assertEquals("1", mff.getAttribute("fragment.count"));
        assertEquals(5, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Run again, this time should be a full batch and a partial
        for (int batch = 0; batch < 14; batch++) {
            executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (" + rowCount + ", 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
            rowCount++;
        }

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 2);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(9, getNumberOfRecordsFromStream(in));
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(1).toByteArray());
        assertEquals(5, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Run again with a cleaned state. Should get all rows split into batches
        int ffCount = (int) Math.ceil(rowCount / 9D);
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, ffCount);

        //ensure all but the last file have 9 records each
        for (int ff = 0; ff < ffCount - 1; ff++) {
            in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(ff).toByteArray());
            assertEquals(9, getNumberOfRecordsFromStream(in));
        }

        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(ffCount - 1).toByteArray());
        assertEquals(rowCount % 9, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();
    }

    @Test
    public void testMaxRowsPerFlowFileWithMaxFragments() throws SQLException, IOException {
        InputStream in;
        MockFlowFile mff;

        executeSql("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        int rowCount = 0;
        //create larger row set
        for (int batch = 0; batch < 100; batch++) {
            executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (" + rowCount + ", 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
            rowCount++;
        }

        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty(QueryDatabaseTable.MAX_ROWS_PER_FLOW_FILE, "9");
        int maxFragments = 3;
        runner.setProperty(QueryDatabaseTable.MAX_FRAGMENTS, Integer.toString(maxFragments));

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, maxFragments);

        for (int i = 0; i < maxFragments; i++) {
            mff = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(i);
            in = new ByteArrayInputStream(mff.toByteArray());
            assertEquals(9, getNumberOfRecordsFromStream(in));

            mff.assertAttributeExists("fragment.identifier");
            assertEquals(Integer.toString(i), mff.getAttribute("fragment.index"));
            assertEquals(Integer.toString(maxFragments), mff.getAttribute("fragment.count"));
        }

        runner.clearTransferState();
    }

    @Test
    public void testInitialMaxValue() throws SQLException, IOException {
        InputStream in;

        executeSql("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");

        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC);

        int rowCount = 0;
        //create larger row set
        for (int batch = 0; batch < 10; batch++) {
            executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (" + rowCount + ", 'Joe Smith', 1.0, '" + FORMATTER.format(dateTime) + "')");

            rowCount++;
            dateTime = dateTime.plusMinutes(1);
        }

        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "${" + TABLE_NAME_KEY + "}");
        runner.setEnvironmentVariableValue(TABLE_NAME_KEY, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "created_on");

        dateTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC).plusMinutes(5);
        runner.setProperty("initial.maxvalue.CREATED_ON", FORMATTER.format(dateTime));
        // Initial run with no previous state. Should get only last 4 records
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(4, getNumberOfRecordsFromStream(in));
        runner.getStateManager().assertStateEquals("test_query_db_table" + AbstractDatabaseFetchProcessor.NAMESPACE_DELIMITER + "created_on", "1970-01-01 00:09:00.0", Scope.CLUSTER);
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        // Validate Max Value doesn't change also
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.getStateManager().assertStateEquals("test_query_db_table" + AbstractDatabaseFetchProcessor.NAMESPACE_DELIMITER + "created_on", "1970-01-01 00:09:00.0", Scope.CLUSTER);
        runner.clearTransferState();
    }

    @Test
    public void testInitialMaxValueWithEL() throws SQLException, IOException {
        InputStream in;

        executeSql("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");

        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC);

        int rowCount = 0;
        //create larger row set
        for (int batch = 0; batch < 10; batch++) {
            executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (" + rowCount + ", 'Joe Smith', 1.0, '" + FORMATTER.format(dateTime) + "')");

            rowCount++;
            dateTime = dateTime.plusMinutes(1);
        }

        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "${" + TABLE_NAME_KEY + "}");
        runner.setEnvironmentVariableValue(TABLE_NAME_KEY, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "created_on");

        dateTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC).plusMinutes(5);
        runner.setProperty("initial.maxvalue.CREATED_ON", "${created.on}");
        runner.setEnvironmentVariableValue("created.on", FORMATTER.format(dateTime));
        // Initial run with no previous state. Should get only last 4 records
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(4, getNumberOfRecordsFromStream(in));
        runner.getStateManager().assertStateEquals("test_query_db_table" + AbstractDatabaseFetchProcessor.NAMESPACE_DELIMITER + "created_on", "1970-01-01 00:09:00.0", Scope.CLUSTER);
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        // Validate Max Value doesn't change also
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.getStateManager().assertStateEquals("test_query_db_table" + AbstractDatabaseFetchProcessor.NAMESPACE_DELIMITER + "created_on", "1970-01-01 00:09:00.0", Scope.CLUSTER);
        runner.clearTransferState();

        // Append a new row, expect 1 flowfile one row
        dateTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC).plusMinutes(rowCount);
        executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (" + rowCount + ", 'Joe Smith', 1.0, '" + FORMATTER.format(dateTime) + "')");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.getStateManager().assertStateEquals("test_query_db_table" + AbstractDatabaseFetchProcessor.NAMESPACE_DELIMITER + "created_on", "1970-01-01 00:10:00.0", Scope.CLUSTER);
        runner.clearTransferState();
    }

    @Test
    public void testInitialLoadStrategyStartAtBeginning() throws SQLException, IOException {
        InputStream in;

        executeSql("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");

        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC);

        int rowCount = 0;
        //create larger row set
        for (int batch = 0; batch < 10; batch++) {
            executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (" + rowCount + ", 'Joe Smith', 1.0, '" + FORMATTER.format(dateTime) + "')");

            rowCount++;
            dateTime = dateTime.plusMinutes(1);
        }

        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "${" + TABLE_NAME_KEY + "}");
        runner.setEnvironmentVariableValue(TABLE_NAME_KEY, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "created_on");
        runner.setProperty(QueryDatabaseTable.INITIAL_LOAD_STRATEGY, QueryDatabaseTable.INITIAL_LOAD_STRATEGY_ALL_ROWS.getValue());

        // Initial run with no previous state. Should get all 10 records
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(10, getNumberOfRecordsFromStream(in));
        runner.getStateManager().assertStateEquals("test_query_db_table" + AbstractDatabaseFetchProcessor.NAMESPACE_DELIMITER + "created_on", "1970-01-01 00:09:00.0", Scope.CLUSTER);
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        // Validate Max Value doesn't change also
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.getStateManager().assertStateEquals("test_query_db_table" + AbstractDatabaseFetchProcessor.NAMESPACE_DELIMITER + "created_on", "1970-01-01 00:09:00.0", Scope.CLUSTER);
        runner.clearTransferState();
    }

    @Test
    public void testInitialLoadStrategyStartAtCurrentMaximumValues() throws SQLException {
        executeSql("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");

        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC);

        int rowCount = 0;
        //create larger row set
        for (int batch = 0; batch < 10; batch++) {
            executeSql("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (" + rowCount + ", 'Joe Smith', 1.0, '" + FORMATTER.format(dateTime) + "')");

            rowCount++;
            dateTime = dateTime.plusMinutes(1);
        }

        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "${" + TABLE_NAME_KEY + "}");
        runner.setEnvironmentVariableValue(TABLE_NAME_KEY, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "created_on");
        runner.setProperty(QueryDatabaseTable.INITIAL_LOAD_STRATEGY, QueryDatabaseTable.INITIAL_LOAD_STRATEGY_NEW_ROWS.getValue());

        // Initial run with no previous state. Should not get any records but store Max Value in the state
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.getStateManager().assertStateEquals("test_query_db_table" + AbstractDatabaseFetchProcessor.NAMESPACE_DELIMITER + "created_on", "1970-01-01 00:09:00.0", Scope.CLUSTER);
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        // Validate Max Value doesn't change also
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.getStateManager().assertStateEquals("test_query_db_table" + AbstractDatabaseFetchProcessor.NAMESPACE_DELIMITER + "created_on", "1970-01-01 00:09:00.0", Scope.CLUSTER);
        runner.clearTransferState();
    }

    @Test
    public void testAddedRowsCustomWhereClause() throws SQLException, IOException {
        executeSql("create table TEST_QUERY_DB_TABLE (id integer not null, type varchar(20), name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (0, 'male', 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (1, 'female', 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (2, NULL, NULL, 2.0, '2010-01-01 00:00:00')");

        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setProperty(QueryDatabaseTable.WHERE_CLAUSE, "type = 'male'");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty(QueryDatabaseTable.MAX_ROWS_PER_FLOW_FILE, "2");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst();
        assertEquals("TEST_QUERY_DB_TABLE", flowFile.getAttribute(QueryDatabaseTable.RESULT_TABLENAME));
        assertEquals("0", flowFile.getAttribute("maxvalue.id"));
        InputStream in = new ByteArrayInputStream(flowFile.toByteArray());
        runner.setProperty(QueryDatabaseTable.FETCH_SIZE, "2");
        assertEquals(1, getNumberOfRecordsFromStream(in));

        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();

        //Remove Max Rows Per FlowFile
        runner.setProperty(QueryDatabaseTable.MAX_ROWS_PER_FLOW_FILE, "0");

        // Add a new row with a higher ID and run, one flowfile with one new row should be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (3, 'female', 'Mary West', 15.0, '2000-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Sanity check - run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add timestamp as a max value column name
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "id, created_on");

        // Add a new row with a higher ID and run, one flow file will be transferred because no max value for the timestamp has been stored
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (4, 'male', 'Marty Johnson', 15.0, '2011-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst();
        assertEquals("4", flowFile.getAttribute("maxvalue.id"));
        assertEquals("2011-01-01 03:23:34.234", flowFile.getAttribute("maxvalue.created_on"));
        in = new ByteArrayInputStream(flowFile.toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Add a new row with a higher ID but lower timestamp and run, no flow file will be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (5, 'male', 'NO NAME', 15.0, '2001-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add a new row with a higher ID and run, one flow file will be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (6, 'male', 'Mr. NiFi', 1.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Set name as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "name");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(4, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Add a new row with a "higher" name than the max but lower than "NULL" (to test that null values are skipped), one flow file will be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (7, 'male', 'NULK', 1.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Set scale as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "scale");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(5, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Add a new row with a higher value for scale than the max, one flow file will be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (8, 'male', 'NULK', 100.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Set scale as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "bignum");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(6, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Add a new row with a higher value for scale than the max, one flow file will be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on, bignum) VALUES (9, 'female', 'Alice Bob', 100.0, '2012-01-01 03:23:34.234', 1)");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();
    }

    @Test
    public void testCustomSQL() throws SQLException, IOException {
        executeSql("create table TEST_QUERY_DB_TABLE (id integer not null, type varchar(20), name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (0, 'male', 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (1, 'female', 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (2, NULL, NULL, 2.0, '2010-01-01 00:00:00')");

        executeSql("create table TYPE_LIST (type_id integer not null, type varchar(20), descr varchar(255))");
        executeSql("insert into TYPE_LIST (type_id, type,descr) VALUES (0, 'male', 'Man')");
        executeSql("insert into TYPE_LIST (type_id, type,descr) VALUES (1, 'female', 'Woman')");
        executeSql("insert into TYPE_LIST (type_id, type,descr) VALUES (2, '', 'Unspecified')");

        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setProperty(QueryDatabaseTable.SQL_QUERY, "SELECT id, b.type as gender, b.descr, name, scale, created_on, bignum FROM TEST_QUERY_DB_TABLE a INNER JOIN TYPE_LIST b ON (a.type=b.type)");
        runner.setProperty(QueryDatabaseTable.WHERE_CLAUSE, "gender = 'male'");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty(QueryDatabaseTable.MAX_ROWS_PER_FLOW_FILE, "2");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst();
        assertEquals("TEST_QUERY_DB_TABLE", flowFile.getAttribute(QueryDatabaseTable.RESULT_TABLENAME));
        assertEquals("0", flowFile.getAttribute("maxvalue.id"));
        InputStream in = new ByteArrayInputStream(flowFile.toByteArray());
        runner.setProperty(QueryDatabaseTable.FETCH_SIZE, "2");
        assertEquals(1, getNumberOfRecordsFromStream(in));

        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();

        //Remove Max Rows Per FlowFile
        runner.setProperty(QueryDatabaseTable.MAX_ROWS_PER_FLOW_FILE, "0");

        // Add a new row with a higher ID and run, one flowfile with one new row should be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (3, 'female', 'Mary West', 15.0, '2000-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Sanity check - run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add timestamp as a max value column name
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "id, created_on");

        // Add a new row with a higher ID and run, one flow file will be transferred because no max value for the timestamp has been stored
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (4, 'male', 'Marty Johnson', 15.0, '2011-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst();
        assertEquals("4", flowFile.getAttribute("maxvalue.id"));
        assertEquals("2011-01-01 03:23:34.234", flowFile.getAttribute("maxvalue.created_on"));
        in = new ByteArrayInputStream(flowFile.toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Add a new row with a higher ID but lower timestamp and run, no flow file will be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (5, 'male', 'NO NAME', 15.0, '2001-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add a new row with a higher ID and run, one flow file will be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (6, 'male', 'Mr. NiFi', 1.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Set name as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "name");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(4, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Add a new row with a "higher" name than the max but lower than "NULL" (to test that null values are skipped), one flow file will be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (7, 'male', 'NULK', 1.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Set scale as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "scale");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(5, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Add a new row with a higher value for scale than the max, one flow file will be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (8, 'male', 'NULK', 100.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(1, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Set scale as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "bignum");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        in = new ByteArrayInputStream(runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).getFirst().toByteArray());
        assertEquals(6, getNumberOfRecordsFromStream(in));
        runner.clearTransferState();

        // Add a new row with a higher value for scale than the max, one flow file will be transferred
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on, bignum) VALUES (9, 'female', 'Alice Bob', 100.0, '2012-01-01 03:23:34.234', 1)");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 0);
        runner.clearTransferState();
    }

    @Test
    public void testMissingColumn() throws ProcessException, SQLException {
        executeSql("create table TEST_QUERY_DB_TABLE (id integer not null, type varchar(20), name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (0, 'male', 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (1, 'female', 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        executeSql("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (2, NULL, NULL, 2.0, '2010-01-01 00:00:00')");

        executeSql("create table TYPE_LIST (type_id integer not null, type varchar(20), descr varchar(255))");
        executeSql("insert into TYPE_LIST (type_id, type,descr) VALUES (0, 'male', 'Man')");
        executeSql("insert into TYPE_LIST (type_id, type,descr) VALUES (1, 'female', 'Woman')");
        executeSql("insert into TYPE_LIST (type_id, type,descr) VALUES (2, '', 'Unspecified')");

        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "TYPE_LIST");
        runner.setProperty(QueryDatabaseTable.SQL_QUERY, "SELECT b.type, b.descr, name, scale, created_on, bignum FROM TEST_QUERY_DB_TABLE a INNER JOIN TYPE_LIST b ON (a.type=b.type)");
        runner.setProperty(QueryDatabaseTable.WHERE_CLAUSE, "type = 'male'");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTable.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty(QueryDatabaseTable.MAX_ROWS_PER_FLOW_FILE, "2");

        assertThrows(AssertionError.class, runner::run);
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("db-fetch-db-type", AbstractDatabaseFetchProcessor.DB_TYPE.getName()),
                Map.entry("db-fetch-where-clause", AbstractDatabaseFetchProcessor.WHERE_CLAUSE.getName()),
                Map.entry("db-fetch-sql-query", AbstractDatabaseFetchProcessor.SQL_QUERY.getName()),
                Map.entry("qdbt-max-rows", AbstractQueryDatabaseTable.MAX_ROWS_PER_FLOW_FILE.getName()),
                Map.entry("Max Rows Per Flow File", AbstractQueryDatabaseTable.MAX_ROWS_PER_FLOW_FILE.getName()),
                Map.entry("qdbt-output-batch-size", AbstractQueryDatabaseTable.OUTPUT_BATCH_SIZE.getName()),
                Map.entry("qdbt-max-frags", AbstractQueryDatabaseTable.MAX_FRAGMENTS.getName()),
                Map.entry("transaction-isolation-level", AbstractQueryDatabaseTable.TRANS_ISOLATION_LEVEL.getName()),
                Map.entry("initial-load-strategy", AbstractQueryDatabaseTable.INITIAL_LOAD_STRATEGY.getName()),
                Map.entry(JdbcProperties.OLD_NORMALIZE_NAMES_FOR_AVRO_PROPERTY_NAME, JdbcProperties.NORMALIZE_NAMES_FOR_AVRO.getName()),
                Map.entry(JdbcProperties.OLD_USE_AVRO_LOGICAL_TYPES_PROPERTY_NAME, JdbcProperties.USE_AVRO_LOGICAL_TYPES.getName()),
                Map.entry(JdbcProperties.OLD_DEFAULT_SCALE_PROPERTY_NAME, JdbcProperties.VARIABLE_REGISTRY_ONLY_DEFAULT_SCALE.getName()),
                Map.entry(JdbcProperties.OLD_DEFAULT_PRECISION_PROPERTY_NAME, JdbcProperties.VARIABLE_REGISTRY_ONLY_DEFAULT_PRECISION.getName())
        );

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
    }

    private long getNumberOfRecordsFromStream(InputStream in) throws IOException {
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

            return recordsFromStream;
        }
    }
}
