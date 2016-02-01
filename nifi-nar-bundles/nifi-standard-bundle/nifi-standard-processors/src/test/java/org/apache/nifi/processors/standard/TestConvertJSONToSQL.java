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

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestConvertJSONToSQL {
    static String createPersons = "CREATE TABLE PERSONS (id integer primary key, name varchar(100), code integer)";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void setup() {
        System.setProperty("derby.stream.error.file", "target/derby.log");
    }

    @Test
    public void testInsert() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(ConvertJSONToSQL.class);
        final File tempDir = folder.getRoot();
        final File dbDir = new File(tempDir, "db");
        final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersons);
            }
        }

        runner.setProperty(ConvertJSONToSQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(ConvertJSONToSQL.TABLE_NAME, "PERSONS");
        runner.setProperty(ConvertJSONToSQL.STATEMENT_TYPE, "INSERT");
        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONToSQL/person-1.json"));
        runner.run();

        runner.assertTransferCount(ConvertJSONToSQL.REL_ORIGINAL, 1);
        runner.assertTransferCount(ConvertJSONToSQL.REL_SQL, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertJSONToSQL.REL_SQL).get(0);
        out.assertAttributeEquals("sql.args.1.type", String.valueOf(java.sql.Types.INTEGER));
        out.assertAttributeEquals("sql.args.1.value", "1");
        out.assertAttributeEquals("sql.args.2.type", String.valueOf(java.sql.Types.VARCHAR));
        out.assertAttributeEquals("sql.args.2.value", "Mark");
        out.assertAttributeEquals("sql.args.3.type", String.valueOf(java.sql.Types.INTEGER));
        out.assertAttributeEquals("sql.args.3.value", "48");

        out.assertContentEquals("INSERT INTO PERSONS (ID, NAME, CODE) VALUES (?, ?, ?)");
    }

    @Test
    public void testInsertWithNullValue() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(ConvertJSONToSQL.class);
        final File tempDir = folder.getRoot();
        final File dbDir = new File(tempDir, "db");
        final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersons);
            }
        }

        runner.setProperty(ConvertJSONToSQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(ConvertJSONToSQL.TABLE_NAME, "PERSONS");
        runner.setProperty(ConvertJSONToSQL.STATEMENT_TYPE, "INSERT");
        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONToSQL/person-with-null-code.json"));
        runner.run();

        runner.assertTransferCount(ConvertJSONToSQL.REL_ORIGINAL, 1);
        runner.assertTransferCount(ConvertJSONToSQL.REL_SQL, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertJSONToSQL.REL_SQL).get(0);
        out.assertAttributeEquals("sql.args.1.type", String.valueOf(java.sql.Types.INTEGER));
        out.assertAttributeEquals("sql.args.1.value", "1");
        out.assertAttributeEquals("sql.args.2.type", String.valueOf(java.sql.Types.VARCHAR));
        out.assertAttributeEquals("sql.args.2.value", "Mark");
        out.assertAttributeEquals("sql.args.3.type", String.valueOf(java.sql.Types.INTEGER));
        out.assertAttributeNotExists("sql.args.3.value");

        out.assertContentEquals("INSERT INTO PERSONS (ID, NAME, CODE) VALUES (?, ?, ?)");
    }


    @Test
    public void testUpdateWithNullValue() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(ConvertJSONToSQL.class);
        final File tempDir = folder.getRoot();
        final File dbDir = new File(tempDir, "db");
        final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersons);
            }
        }

        runner.setProperty(ConvertJSONToSQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(ConvertJSONToSQL.TABLE_NAME, "PERSONS");
        runner.setProperty(ConvertJSONToSQL.STATEMENT_TYPE, "UPDATE");
        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONToSQL/person-with-null-code.json"));
        runner.run();

        runner.assertTransferCount(ConvertJSONToSQL.REL_ORIGINAL, 1);
        runner.assertTransferCount(ConvertJSONToSQL.REL_SQL, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertJSONToSQL.REL_SQL).get(0);
        out.assertAttributeEquals("sql.args.1.type", String.valueOf(java.sql.Types.VARCHAR));
        out.assertAttributeEquals("sql.args.1.value", "Mark");
        out.assertAttributeEquals("sql.args.2.type", String.valueOf(java.sql.Types.INTEGER));
        out.assertAttributeNotExists("sql.args.2.value");
        out.assertAttributeEquals("sql.args.3.type", String.valueOf(java.sql.Types.INTEGER));
        out.assertAttributeEquals("sql.args.3.value", "1");

        out.assertContentEquals("UPDATE PERSONS SET NAME = ?, CODE = ? WHERE ID = ?");
    }


    @Test
    public void testMultipleInserts() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(ConvertJSONToSQL.class);
        final File tempDir = folder.getRoot();
        final File dbDir = new File(tempDir, "db");
        final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersons);
            }
        }

        runner.setProperty(ConvertJSONToSQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(ConvertJSONToSQL.TABLE_NAME, "PERSONS");
        runner.setProperty(ConvertJSONToSQL.STATEMENT_TYPE, "INSERT");
        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONToSQL/persons.json"));
        runner.run();

        runner.assertTransferCount(ConvertJSONToSQL.REL_ORIGINAL, 1);
        runner.assertTransferCount(ConvertJSONToSQL.REL_SQL, 5);
        final List<MockFlowFile> mffs = runner.getFlowFilesForRelationship(ConvertJSONToSQL.REL_SQL);
        for (final MockFlowFile mff : mffs) {
            mff.assertContentEquals("INSERT INTO PERSONS (ID, NAME, CODE) VALUES (?, ?, ?)");

            for (int i=1; i <= 3; i++) {
                mff.assertAttributeExists("sql.args." + i + ".type");
                mff.assertAttributeExists("sql.args." + i + ".value");
            }
        }
    }

    @Test
    public void testUpdateBasedOnPrimaryKey() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(ConvertJSONToSQL.class);
        final File tempDir = folder.getRoot();
        final File dbDir = new File(tempDir, "db");
        final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersons);
            }
        }

        runner.setProperty(ConvertJSONToSQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(ConvertJSONToSQL.TABLE_NAME, "PERSONS");
        runner.setProperty(ConvertJSONToSQL.STATEMENT_TYPE, "UPDATE");
        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONToSQL/person-1.json"));
        runner.run();

        runner.assertTransferCount(ConvertJSONToSQL.REL_ORIGINAL, 1);
        runner.assertTransferCount(ConvertJSONToSQL.REL_SQL, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertJSONToSQL.REL_SQL).get(0);
        out.assertAttributeEquals("sql.args.1.type", String.valueOf(java.sql.Types.VARCHAR));
        out.assertAttributeEquals("sql.args.1.value", "Mark");
        out.assertAttributeEquals("sql.args.2.type", String.valueOf(java.sql.Types.INTEGER));
        out.assertAttributeEquals("sql.args.2.value", "48");
        out.assertAttributeEquals("sql.args.3.type", String.valueOf(java.sql.Types.INTEGER));
        out.assertAttributeEquals("sql.args.3.value", "1");

        out.assertContentEquals("UPDATE PERSONS SET NAME = ?, CODE = ? WHERE ID = ?");
    }

    @Test
    public void testUnmappedFieldBehavior() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(ConvertJSONToSQL.class);
        final File tempDir = folder.getRoot();
        final File dbDir = new File(tempDir, "db");
        final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersons);
            }
        }

        runner.setProperty(ConvertJSONToSQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(ConvertJSONToSQL.TABLE_NAME, "PERSONS");
        runner.setProperty(ConvertJSONToSQL.STATEMENT_TYPE, "INSERT");
        runner.setProperty(ConvertJSONToSQL.UNMATCHED_FIELD_BEHAVIOR, ConvertJSONToSQL.IGNORE_UNMATCHED_FIELD.getValue());
        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONToSQL/person-with-extra-field.json"));
        runner.run();

        runner.assertTransferCount(ConvertJSONToSQL.REL_ORIGINAL, 1);
        runner.assertTransferCount(ConvertJSONToSQL.REL_SQL, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertJSONToSQL.REL_SQL).get(0);

        out.assertContentEquals("INSERT INTO PERSONS (ID, NAME, CODE) VALUES (?, ?, ?)");

        runner.clearTransferState();
        runner.setProperty(ConvertJSONToSQL.UNMATCHED_FIELD_BEHAVIOR, ConvertJSONToSQL.FAIL_UNMATCHED_FIELD.getValue());
        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONToSQL/person-with-extra-field.json"));
        runner.run();
        runner.assertAllFlowFilesTransferred(ConvertJSONToSQL.REL_FAILURE, 1);
    }

    @Test
    public void testUpdateBasedOnUpdateKey() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(ConvertJSONToSQL.class);
        final File tempDir = folder.getRoot();
        final File dbDir = new File(tempDir, "db");
        final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersons);
            }
        }

        runner.setProperty(ConvertJSONToSQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(ConvertJSONToSQL.TABLE_NAME, "PERSONS");
        runner.setProperty(ConvertJSONToSQL.STATEMENT_TYPE, "UPDATE");
        runner.setProperty(ConvertJSONToSQL.UPDATE_KEY, "code");
        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONToSQL/person-1.json"));
        runner.run();

        runner.assertTransferCount(ConvertJSONToSQL.REL_ORIGINAL, 1);
        runner.assertTransferCount(ConvertJSONToSQL.REL_SQL, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertJSONToSQL.REL_SQL).get(0);
        out.assertAttributeEquals("sql.args.1.type", String.valueOf(java.sql.Types.INTEGER));
        out.assertAttributeEquals("sql.args.1.value", "1");
        out.assertAttributeEquals("sql.args.2.type", String.valueOf(java.sql.Types.VARCHAR));
        out.assertAttributeEquals("sql.args.2.value", "Mark");
        out.assertAttributeEquals("sql.args.3.type", String.valueOf(java.sql.Types.INTEGER));
        out.assertAttributeEquals("sql.args.3.value", "48");

        out.assertContentEquals("UPDATE PERSONS SET ID = ?, NAME = ? WHERE CODE = ?");
    }

    @Test
    public void testUpdateBasedOnCompoundUpdateKey() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(ConvertJSONToSQL.class);
        final File tempDir = folder.getRoot();
        final File dbDir = new File(tempDir, "db");
        final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersons);
            }
        }

        runner.setProperty(ConvertJSONToSQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(ConvertJSONToSQL.TABLE_NAME, "PERSONS");
        runner.setProperty(ConvertJSONToSQL.STATEMENT_TYPE, "UPDATE");
        runner.setProperty(ConvertJSONToSQL.UPDATE_KEY, "name,  code");
        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONToSQL/person-1.json"));
        runner.run();

        runner.assertTransferCount(ConvertJSONToSQL.REL_ORIGINAL, 1);
        runner.assertTransferCount(ConvertJSONToSQL.REL_SQL, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertJSONToSQL.REL_SQL).get(0);
        out.assertAttributeEquals("sql.args.1.type", String.valueOf(java.sql.Types.INTEGER));
        out.assertAttributeEquals("sql.args.1.value", "1");
        out.assertAttributeEquals("sql.args.2.type", String.valueOf(java.sql.Types.VARCHAR));
        out.assertAttributeEquals("sql.args.2.value", "Mark");
        out.assertAttributeEquals("sql.args.3.type", String.valueOf(java.sql.Types.INTEGER));
        out.assertAttributeEquals("sql.args.3.value", "48");

        out.assertContentEquals("UPDATE PERSONS SET ID = ? WHERE NAME = ? AND CODE = ?");
    }

    @Test
    public void testUpdateWithMissingFieldBasedOnCompoundUpdateKey() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(ConvertJSONToSQL.class);
        final File tempDir = folder.getRoot();
        final File dbDir = new File(tempDir, "db");
        final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersons);
            }
        }

        runner.setProperty(ConvertJSONToSQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(ConvertJSONToSQL.TABLE_NAME, "PERSONS");
        runner.setProperty(ConvertJSONToSQL.STATEMENT_TYPE, "UPDATE");
        runner.setProperty(ConvertJSONToSQL.UPDATE_KEY, "name,  code");
        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONToSQL/person-without-code.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertJSONToSQL.REL_FAILURE, 1);
    }

    @Test
    public void testUpdateWithMalformedJson() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(ConvertJSONToSQL.class);
        final File tempDir = folder.getRoot();
        final File dbDir = new File(tempDir, "db");
        final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersons);
            }
        }

        runner.setProperty(ConvertJSONToSQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(ConvertJSONToSQL.TABLE_NAME, "PERSONS");
        runner.setProperty(ConvertJSONToSQL.STATEMENT_TYPE, "UPDATE");
        runner.setProperty(ConvertJSONToSQL.UPDATE_KEY, "name,  code");
        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONToSQL/malformed-person-extra-comma.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertJSONToSQL.REL_FAILURE, 1);
    }

    @Test
    public void testInsertWithMissingField() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(ConvertJSONToSQL.class);
        final File tempDir = folder.getRoot();
        final File dbDir = new File(tempDir, "db");
        final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersons);
            }
        }

        runner.setProperty(ConvertJSONToSQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(ConvertJSONToSQL.TABLE_NAME, "PERSONS");
        runner.setProperty(ConvertJSONToSQL.STATEMENT_TYPE, "INSERT");
        runner.setProperty(ConvertJSONToSQL.UPDATE_KEY, "name,  code");
        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONToSQL/person-without-id.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertJSONToSQL.REL_FAILURE, 1);
    }

    @Test
    public void testInsertWithMissingColumnFail() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(ConvertJSONToSQL.class);
        final File tempDir = folder.getRoot();
        final File dbDir = new File(tempDir, "db");
        final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE PERSONS (id integer, name varchar(100), code integer, generated_key integer primary key)");
            }
        }

        runner.setProperty(ConvertJSONToSQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(ConvertJSONToSQL.TABLE_NAME, "PERSONS");
        runner.setProperty(ConvertJSONToSQL.STATEMENT_TYPE, "INSERT");
        runner.setProperty(ConvertJSONToSQL.UNMATCHED_COLUMN_BEHAVIOR, ConvertJSONToSQL.FAIL_UNMATCHED_COLUMN);
        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONToSQL/person-1.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertJSONToSQL.REL_FAILURE, 1);
    } // End testInsertWithMissingColumnFail()

    @Test
    public void testInsertWithMissingColumnWarning() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(ConvertJSONToSQL.class);
        final File tempDir = folder.getRoot();
        final File dbDir = new File(tempDir, "db");
        final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE PERSONS (id integer, name varchar(100), code integer, generated_key integer primary key)");
            }
        }

        runner.setProperty(ConvertJSONToSQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(ConvertJSONToSQL.TABLE_NAME, "PERSONS");
        runner.setProperty(ConvertJSONToSQL.STATEMENT_TYPE, "INSERT");
        runner.setProperty(ConvertJSONToSQL.UNMATCHED_COLUMN_BEHAVIOR, ConvertJSONToSQL.WARNING_UNMATCHED_COLUMN);
        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONToSQL/person-1.json"));
        runner.run();

        runner.assertTransferCount(ConvertJSONToSQL.REL_ORIGINAL, 1);
        runner.assertTransferCount(ConvertJSONToSQL.REL_SQL, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertJSONToSQL.REL_SQL).get(0);
        out.assertAttributeEquals("sql.args.1.type", String.valueOf(java.sql.Types.INTEGER));
        out.assertAttributeEquals("sql.args.1.value", "1");
        out.assertAttributeEquals("sql.args.2.type", String.valueOf(java.sql.Types.VARCHAR));
        out.assertAttributeEquals("sql.args.2.value", "Mark");
        out.assertAttributeEquals("sql.args.3.type", String.valueOf(java.sql.Types.INTEGER));
        out.assertAttributeEquals("sql.args.3.value", "48");

        out.assertContentEquals("INSERT INTO PERSONS (ID, NAME, CODE) VALUES (?, ?, ?)");
    } // End testInsertWithMissingColumnWarning()

    @Test
    public void testInsertWithMissingColumnIgnore() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(ConvertJSONToSQL.class);
        final File tempDir = folder.getRoot();
        final File dbDir = new File(tempDir, "db");
        final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE PERSONS (id integer, name varchar(100), code integer, generated_key integer primary key)");
            }
        }

        runner.setProperty(ConvertJSONToSQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(ConvertJSONToSQL.TABLE_NAME, "PERSONS");
        runner.setProperty(ConvertJSONToSQL.STATEMENT_TYPE, "INSERT");
        runner.setProperty(ConvertJSONToSQL.UNMATCHED_COLUMN_BEHAVIOR, "Ignore Unmatched Columns");
        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONToSQL/person-1.json"));
        runner.run();

        runner.assertTransferCount(ConvertJSONToSQL.REL_ORIGINAL, 1);
        runner.assertTransferCount(ConvertJSONToSQL.REL_SQL, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertJSONToSQL.REL_SQL).get(0);
        out.assertAttributeEquals("sql.args.1.type", String.valueOf(java.sql.Types.INTEGER));
        out.assertAttributeEquals("sql.args.1.value", "1");
        out.assertAttributeEquals("sql.args.2.type", String.valueOf(java.sql.Types.VARCHAR));
        out.assertAttributeEquals("sql.args.2.value", "Mark");
        out.assertAttributeEquals("sql.args.3.type", String.valueOf(java.sql.Types.INTEGER));
        out.assertAttributeEquals("sql.args.3.value", "48");

        out.assertContentEquals("INSERT INTO PERSONS (ID, NAME, CODE) VALUES (?, ?, ?)");
    } // End testInsertWithMissingColumnIgnore()

    @Test
    public void testUpdateWithMissingColumnFail() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(ConvertJSONToSQL.class);
        final File tempDir = folder.getRoot();
        final File dbDir = new File(tempDir, "db");
        final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersons);
            }
        }

        runner.setProperty(ConvertJSONToSQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(ConvertJSONToSQL.TABLE_NAME, "PERSONS");
        runner.setProperty(ConvertJSONToSQL.STATEMENT_TYPE, "UPDATE");
        runner.setProperty(ConvertJSONToSQL.UPDATE_KEY, "name,  code, extra");
        runner.setProperty(ConvertJSONToSQL.UNMATCHED_COLUMN_BEHAVIOR, ConvertJSONToSQL.FAIL_UNMATCHED_COLUMN);
        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONToSQL/person-1.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertJSONToSQL.REL_FAILURE, 1);
    } // End testUpdateWithMissingColumnFail()

    @Test
    public void testUpdateWithMissingColumnWarning() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(ConvertJSONToSQL.class);
        final File tempDir = folder.getRoot();
        final File dbDir = new File(tempDir, "db");
        final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersons);
            }
        }

        runner.setProperty(ConvertJSONToSQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(ConvertJSONToSQL.TABLE_NAME, "PERSONS");
        runner.setProperty(ConvertJSONToSQL.STATEMENT_TYPE, "UPDATE");
        runner.setProperty(ConvertJSONToSQL.UPDATE_KEY, "name,  code, extra");
        runner.setProperty(ConvertJSONToSQL.UNMATCHED_COLUMN_BEHAVIOR, ConvertJSONToSQL.WARNING_UNMATCHED_COLUMN);
        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONToSQL/person-1.json"));
        runner.run();

        runner.assertTransferCount(ConvertJSONToSQL.REL_ORIGINAL, 1);
        runner.assertTransferCount(ConvertJSONToSQL.REL_SQL, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertJSONToSQL.REL_SQL).get(0);
        out.assertAttributeEquals("sql.args.1.type", String.valueOf(java.sql.Types.INTEGER));
        out.assertAttributeEquals("sql.args.1.value", "1");
        out.assertAttributeEquals("sql.args.2.type", String.valueOf(java.sql.Types.VARCHAR));
        out.assertAttributeEquals("sql.args.2.value", "Mark");
        out.assertAttributeEquals("sql.args.3.type", String.valueOf(java.sql.Types.INTEGER));
        out.assertAttributeEquals("sql.args.3.value", "48");

        out.assertContentEquals("UPDATE PERSONS SET ID = ? WHERE NAME = ? AND CODE = ?");

    } // End testUpdateWithMissingColumnWarning()

    @Test
    public void testUpdateWithMissingColumnIgnore() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(ConvertJSONToSQL.class);
        final File tempDir = folder.getRoot();
        final File dbDir = new File(tempDir, "db");
        final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersons);
            }
        }

        runner.setProperty(ConvertJSONToSQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(ConvertJSONToSQL.TABLE_NAME, "PERSONS");
        runner.setProperty(ConvertJSONToSQL.STATEMENT_TYPE, "UPDATE");
        runner.setProperty(ConvertJSONToSQL.UPDATE_KEY, "name,  code, extra");
        runner.setProperty(ConvertJSONToSQL.UNMATCHED_COLUMN_BEHAVIOR, "Ignore Unmatched Columns");
        runner.enqueue(Paths.get("src/test/resources/TestConvertJSONToSQL/person-1.json"));
        runner.run();

        runner.assertTransferCount(ConvertJSONToSQL.REL_ORIGINAL, 1);
        runner.assertTransferCount(ConvertJSONToSQL.REL_SQL, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertJSONToSQL.REL_SQL).get(0);
        out.assertAttributeEquals("sql.args.1.type", String.valueOf(java.sql.Types.INTEGER));
        out.assertAttributeEquals("sql.args.1.value", "1");
        out.assertAttributeEquals("sql.args.2.type", String.valueOf(java.sql.Types.VARCHAR));
        out.assertAttributeEquals("sql.args.2.value", "Mark");
        out.assertAttributeEquals("sql.args.3.type", String.valueOf(java.sql.Types.INTEGER));
        out.assertAttributeEquals("sql.args.3.value", "48");

        out.assertContentEquals("UPDATE PERSONS SET ID = ? WHERE NAME = ? AND CODE = ?");

    } // End testUpdateWithMissingColumnIgnore()


    /**
     * Simple implementation only for testing purposes
     */
    private static class MockDBCPService extends AbstractControllerService implements DBCPService {
        private final String dbLocation;

        public MockDBCPService(final String dbLocation) {
            this.dbLocation = dbLocation;
        }

        @Override
        public String getIdentifier() {
            return "dbcp";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
                final Connection con = DriverManager.getConnection("jdbc:derby:" + dbLocation + ";create=true");
                return con;
            } catch (final Exception e) {
                e.printStackTrace();
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }
}
