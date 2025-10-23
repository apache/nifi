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

import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestListDatabaseTables {

    private static final String SERVICE_ID = EmbeddedDatabaseConnectionService.class.getSimpleName();

    private static EmbeddedDatabaseConnectionService service;

    TestRunner runner;

    ListDatabaseTables processor;

    @BeforeAll
    static void setService(@TempDir final Path databaseLocation) {
        service = new EmbeddedDatabaseConnectionService(databaseLocation);
    }

    @AfterAll
    static void shutdown() {
        service.close();
    }

    @BeforeEach
    void setRunner() throws Exception {
        processor = new ListDatabaseTables();

        runner = TestRunners.newTestRunner(ListDatabaseTables.class);
        runner.addControllerService(SERVICE_ID, service);
        runner.enableControllerService(service);
        runner.setProperty(ListDatabaseTables.DBCP_SERVICE, SERVICE_ID);
    }

    @AfterEach
    void dropTables() {
        final List<String> tables = List.of(
                "TEST_TABLE1",
                "TEST_TABLE2"
        );

        for (final String table : tables) {
            try (
                    Connection connection = service.getConnection();
                    Statement statement = connection.createStatement()
            ) {
                statement.execute("DROP TABLE %s".formatted(table));
            } catch (final SQLException ignored) {

            }
        }
    }

    @Test
    void testListTablesNoCount() throws Exception {
        try (Statement stmt = createStatement()) {
            stmt.execute("create table TEST_TABLE1 (id integer not null, val1 integer, val2 integer, constraint my_pk1 primary key (id))");
            stmt.execute("create table TEST_TABLE2 (id integer not null, val1 integer, val2 integer, constraint my_pk2 primary key (id))");
        }

        runner.run();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 2);
        // Already got these tables, shouldn't get them again
        runner.clearTransferState();
        runner.run();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 0);
    }

    @Test
    void testListTablesWithCount() throws Exception {
        runner.setProperty(ListDatabaseTables.INCLUDE_COUNT, "true");

        try (Statement stmt = createStatement()) {
            stmt.execute("create table TEST_TABLE1 (id integer not null, val1 integer, val2 integer, constraint my_pk1 primary key (id))");
            stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (0, NULL, 1)");
            stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (1, 1, 1)");
            stmt.execute("create table TEST_TABLE2 (id integer not null, val1 integer, val2 integer, constraint my_pk2 primary key (id))");
        }

        runner.run();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 2);
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ListDatabaseTables.REL_SUCCESS);
        assertEquals("2", results.get(0).getAttribute(ListDatabaseTables.DB_TABLE_COUNT));
        assertEquals("0", results.get(1).getAttribute(ListDatabaseTables.DB_TABLE_COUNT));
    }

    @Test
    void testListTablesWithCountAsRecord() throws Exception {
        runner.setProperty(ListDatabaseTables.INCLUDE_COUNT, "true");

        try (Statement stmt = createStatement()) {
            stmt.execute("create table TEST_TABLE1 (id integer not null, val1 integer, val2 integer, constraint my_pk1 primary key (id))");
            stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (0, NULL, 1)");
            stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (1, 1, 1)");
            stmt.execute("create table TEST_TABLE2 (id integer not null, val1 integer, val2 integer, constraint my_pk2 primary key (id))");
        }

        final MockRecordWriter recordWriter = new MockRecordWriter(null, false);
        runner.addControllerService("record-writer", recordWriter);
        runner.setProperty(ListDatabaseTables.RECORD_WRITER, "record-writer");
        runner.enableControllerService(recordWriter);

        runner.run();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ListDatabaseTables.REL_SUCCESS).getFirst();
        flowFile.assertContentEquals(
                """
                        TEST_TABLE1,,APP,APP.TEST_TABLE1,TABLE,,2
                        TEST_TABLE2,,APP,APP.TEST_TABLE2,TABLE,,0
                        """);
    }

    @Test
    void testListTablesAfterRefresh() throws Exception {

        try (Statement stmt = createStatement()) {
            stmt.execute("create table TEST_TABLE1 (id integer not null, val1 integer, val2 integer, constraint my_pk1 primary key (id))");
            stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (0, NULL, 1)");
            stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (1, 1, 1)");
            stmt.execute("create table TEST_TABLE2 (id integer not null, val1 integer, val2 integer, constraint my_pk2 primary key (id))");
        }

        runner.setProperty(ListDatabaseTables.INCLUDE_COUNT, "true");
        runner.setProperty(ListDatabaseTables.REFRESH_INTERVAL, "100 millis");
        runner.run();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 2);
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ListDatabaseTables.REL_SUCCESS);
        assertEquals("2", results.get(0).getAttribute(ListDatabaseTables.DB_TABLE_COUNT));
        assertEquals("0", results.get(1).getAttribute(ListDatabaseTables.DB_TABLE_COUNT));
        runner.clearTransferState();
        runner.run();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 0);

        // Now wait longer than 100 millis and assert the refresh has happened (the two tables are re-listed)
        Thread.sleep(200);
        runner.run();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 2);
    }

    @Test
    void testListTablesMultipleRefresh() throws Exception {
        try (Statement stmt = createStatement()) {
            stmt.execute("create table TEST_TABLE1 (id integer not null, val1 integer, val2 integer, constraint my_pk1 primary key (id))");
            stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (0, NULL, 1)");
            stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (1, 1, 1)");
        }

        runner.setProperty(ListDatabaseTables.INCLUDE_COUNT, "true");
        runner.setProperty(ListDatabaseTables.REFRESH_INTERVAL, "200 millis");
        runner.run();
        long startTimer = System.currentTimeMillis();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 1);
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ListDatabaseTables.REL_SUCCESS);
        assertEquals("2", results.getFirst().getAttribute(ListDatabaseTables.DB_TABLE_COUNT));
        runner.clearTransferState();

        // Add another table immediately, the first table should not be listed again but the second should
        try (Statement stmt = createStatement()) {
            stmt.execute("create table TEST_TABLE2 (id integer not null, val1 integer, val2 integer, constraint my_pk2 primary key (id))");
        }

        runner.run();
        long endTimer = System.currentTimeMillis();
        // Expect 1 or 2 tables (whether execution has taken longer than the refresh time)
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, (endTimer - startTimer > 200) ? 2 : 1);
        results = runner.getFlowFilesForRelationship(ListDatabaseTables.REL_SUCCESS);
        assertEquals((endTimer - startTimer > 200) ? "2" : "0", results.getFirst().getAttribute(ListDatabaseTables.DB_TABLE_COUNT));
        runner.clearTransferState();

        // Now wait longer than the refresh interval and assert the refresh has happened (i.e. the two tables are re-listed)
        Thread.sleep(500);
        runner.run();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 2);
    }

    private Statement createStatement() throws SQLException {
        final Connection connection = service.getConnection();
        return connection.createStatement();
    }
}