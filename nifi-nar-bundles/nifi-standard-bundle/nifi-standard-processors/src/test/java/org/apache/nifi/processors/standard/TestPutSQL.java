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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

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
import org.mockito.Mockito;

public class TestPutSQL {
    private static final String createPersons = "CREATE TABLE PERSONS (id integer primary key, name varchar(100), code integer)";
    private static final String createPersonsAutoId = "CREATE TABLE PERSONS (id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1), name VARCHAR(100), code INTEGER check(code <= 100))";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void setup() {
        System.setProperty("derby.stream.error.file", "target/derby.log");
    }

    @Test
    public void testDirectStatements() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(PutSQL.class);
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

        runner.setProperty(PutSQL.CONNECTION_POOL, "dbcp");
        runner.enqueue("INSERT INTO PERSONS (ID, NAME, CODE) VALUES (1, 'Mark', 84)".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSQL.REL_SUCCESS, 1);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("Mark", rs.getString(2));
                assertEquals(84, rs.getInt(3));
                assertFalse(rs.next());
            }
        }

        runner.enqueue("UPDATE PERSONS SET NAME='George' WHERE ID=1".getBytes());
        runner.run();

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("George", rs.getString(2));
                assertEquals(84, rs.getInt(3));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testInsertWithGeneratedKeys() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(PutSQL.class);
        final File tempDir = folder.getRoot();
        final File dbDir = new File(tempDir, "db");
        final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersonsAutoId);
            }
        }

        runner.setProperty(PutSQL.OBTAIN_GENERATED_KEYS, "true");
        runner.setProperty(PutSQL.CONNECTION_POOL, "dbcp");
        runner.enqueue("INSERT INTO PERSONS (NAME, CODE) VALUES ('Mark', 84)".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSQL.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(PutSQL.REL_SUCCESS).get(0);
        mff.assertAttributeEquals("sql.generated.key", "1");

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("Mark", rs.getString(2));
                assertEquals(84, rs.getInt(3));
                assertFalse(rs.next());
            }
        }
    }


    @Test
    public void testFailInMiddleWithBadStatement() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(PutSQL.class);
        final File tempDir = folder.getRoot();
        final File dbDir = new File(tempDir, "db");
        final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersonsAutoId);
            }
        }

        runner.setProperty(PutSQL.OBTAIN_GENERATED_KEYS, "false");
        runner.setProperty(PutSQL.CONNECTION_POOL, "dbcp");
        runner.enqueue("INSERT INTO PERSONS (NAME, CODE) VALUES ('Mark', 84)".getBytes());
        runner.enqueue("INSERT INTO PERSONS".getBytes());  // intentionally wrong syntax
        runner.enqueue("INSERT INTO PERSONS (NAME, CODE) VALUES ('Tom', 3)".getBytes());
        runner.enqueue("INSERT INTO PERSONS (NAME, CODE) VALUES ('Harry', 44)".getBytes());
        runner.run();

        runner.assertTransferCount(PutSQL.REL_FAILURE, 1);
        runner.assertTransferCount(PutSQL.REL_SUCCESS, 3);
    }


    @Test
    public void testFailInMiddleWithBadParameterType() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(PutSQL.class);
        final File tempDir = folder.getRoot();
        final File dbDir = new File(tempDir, "db");
        final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersonsAutoId);
            }
        }

        runner.setProperty(PutSQL.OBTAIN_GENERATED_KEYS, "false");
        runner.setProperty(PutSQL.CONNECTION_POOL, "dbcp");

        final Map<String, String> goodAttributes = new HashMap<>();
        goodAttributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        goodAttributes.put("sql.args.1.value", "84");

        final Map<String, String> badAttributes = new HashMap<>();
        badAttributes.put("sql.args.1.type", String.valueOf(Types.VARCHAR));
        badAttributes.put("sql.args.1.value", "hello");

        final byte[] data = "INSERT INTO PERSONS (NAME, CODE) VALUES ('Mark', ?)".getBytes();
        runner.enqueue(data, goodAttributes);
        runner.enqueue(data, badAttributes);
        runner.enqueue(data, goodAttributes);
        runner.enqueue(data, goodAttributes);
        runner.run();

        runner.assertTransferCount(PutSQL.REL_FAILURE, 1);
        runner.assertTransferCount(PutSQL.REL_SUCCESS, 3);
    }


    @Test
    public void testFailInMiddleWithBadParameterValue() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(PutSQL.class);
        final File tempDir = folder.getRoot();
        final File dbDir = new File(tempDir, "db");
        final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersonsAutoId);
            }
        }

        runner.setProperty(PutSQL.OBTAIN_GENERATED_KEYS, "false");
        runner.setProperty(PutSQL.CONNECTION_POOL, "dbcp");

        final Map<String, String> goodAttributes = new HashMap<>();
        goodAttributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        goodAttributes.put("sql.args.1.value", "84");

        final Map<String, String> badAttributes = new HashMap<>();
        badAttributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        badAttributes.put("sql.args.1.value", "9999");

        final byte[] data = "INSERT INTO PERSONS (NAME, CODE) VALUES ('Mark', ?)".getBytes();
        runner.enqueue(data, goodAttributes);
        runner.enqueue(data, badAttributes);
        runner.enqueue(data, goodAttributes);
        runner.enqueue(data, goodAttributes);
        runner.run();

        runner.assertTransferCount(PutSQL.REL_SUCCESS, 1);
        runner.assertTransferCount(PutSQL.REL_FAILURE, 1);
        runner.assertTransferCount(PutSQL.REL_RETRY, 2);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("Mark", rs.getString(2));
                assertEquals(84, rs.getInt(3));
                assertFalse(rs.next());
            }
        }
    }



    @Test
    public void testStatementsWithPreparedParameters() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(PutSQL.class);
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

        runner.setProperty(PutSQL.CONNECTION_POOL, "dbcp");
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.1.value", "1");

        attributes.put("sql.args.2.type", String.valueOf(Types.VARCHAR));
        attributes.put("sql.args.2.value", "Mark");

        attributes.put("sql.args.3.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.3.value", "84");

        runner.enqueue("INSERT INTO PERSONS (ID, NAME, CODE) VALUES (?, ?, ?)".getBytes(), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSQL.REL_SUCCESS, 1);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("Mark", rs.getString(2));
                assertEquals(84, rs.getInt(3));
                assertFalse(rs.next());
            }
        }

        runner.clearTransferState();

        attributes.clear();
        attributes.put("sql.args.1.type", String.valueOf(Types.VARCHAR));
        attributes.put("sql.args.1.value", "George");

        attributes.put("sql.args.2.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.2.value", "1");

        runner.enqueue("UPDATE PERSONS SET NAME=? WHERE ID=?".getBytes(), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(PutSQL.REL_SUCCESS, 1);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("George", rs.getString(2));
                assertEquals(84, rs.getInt(3));
                assertFalse(rs.next());
            }
        }
    }


    @Test
    public void testMultipleStatementsWithinFlowFile() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(PutSQL.class);
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

        runner.setProperty(PutSQL.CONNECTION_POOL, "dbcp");

        final String sql = "INSERT INTO PERSONS (ID, NAME, CODE) VALUES (?, ?, ?); " +
                "UPDATE PERSONS SET NAME='George' WHERE ID=?; ";
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.1.value", "1");

        attributes.put("sql.args.2.type", String.valueOf(Types.VARCHAR));
        attributes.put("sql.args.2.value", "Mark");

        attributes.put("sql.args.3.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.3.value", "84");

        attributes.put("sql.args.4.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.4.value", "1");

        runner.enqueue(sql.getBytes(), attributes);
        runner.run();

        // should fail because of the semicolon
        runner.assertAllFlowFilesTransferred(PutSQL.REL_FAILURE, 1);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
                assertFalse(rs.next());
            }
        }
    }


    @Test
    public void testWithNullParameter() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(PutSQL.class);
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

        runner.setProperty(PutSQL.CONNECTION_POOL, "dbcp");
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.1.value", "1");

        attributes.put("sql.args.2.type", String.valueOf(Types.VARCHAR));
        attributes.put("sql.args.2.value", "Mark");

        attributes.put("sql.args.3.type", String.valueOf(Types.INTEGER));

        runner.enqueue("INSERT INTO PERSONS (ID, NAME, CODE) VALUES (?, ?, ?)".getBytes(), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSQL.REL_SUCCESS, 1);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("Mark", rs.getString(2));
                assertEquals(0, rs.getInt(3));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testInvalidStatement() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(PutSQL.class);
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

        runner.setProperty(PutSQL.CONNECTION_POOL, "dbcp");

        final String sql = "INSERT INTO PERSONS (ID, NAME, CODE) VALUES (?, ?, ?); " +
                "UPDATE SOME_RANDOM_TABLE NAME='George' WHERE ID=?; ";
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.1.value", "1");

        attributes.put("sql.args.2.type", String.valueOf(Types.VARCHAR));
        attributes.put("sql.args.2.value", "Mark");

        attributes.put("sql.args.3.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.3.value", "84");

        attributes.put("sql.args.4.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.4.value", "1");

        runner.enqueue(sql.getBytes(), attributes);
        runner.run();

        // should fail because of the semicolon
        runner.assertAllFlowFilesTransferred(PutSQL.REL_FAILURE, 1);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
                assertFalse(rs.next());
            }
        }
    }


    @Test
    public void testRetryableFailure() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(PutSQL.class);
        final DBCPService service = new SQLExceptionService(null);
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        runner.setProperty(PutSQL.CONNECTION_POOL, "dbcp");

        final String sql = "INSERT INTO PERSONS (ID, NAME, CODE) VALUES (?, ?, ?); " +
                "UPDATE PERSONS SET NAME='George' WHERE ID=?; ";
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.1.value", "1");

        attributes.put("sql.args.2.type", String.valueOf(Types.VARCHAR));
        attributes.put("sql.args.2.value", "Mark");

        attributes.put("sql.args.3.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.3.value", "84");

        attributes.put("sql.args.4.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.4.value", "1");

        runner.enqueue(sql.getBytes(), attributes);
        runner.run();

        // should fail because of the semicolon
        runner.assertAllFlowFilesTransferred(PutSQL.REL_RETRY, 1);
    }


    @Test
    public void testMultipleFlowFilesSuccessfulInTransaction() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(PutSQL.class);
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

        runner.setProperty(PutSQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(PutSQL.BATCH_SIZE, "1");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.1.value", "1");

        attributes.put("sql.args.2.type", String.valueOf(Types.VARCHAR));
        attributes.put("sql.args.2.value", "Mark");

        attributes.put("sql.args.3.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.3.value", "84");

        attributes.put("fragment.identifier", "1");
        attributes.put("fragment.count", "2");
        attributes.put("fragment.index", "0");
        runner.enqueue("INSERT INTO PERSONS (ID, NAME, CODE) VALUES (?, ?, ?)".getBytes(), attributes);
        runner.run();

        // No FlowFiles should be transferred because there were not enough flowfiles with the same fragment identifier
        runner.assertAllFlowFilesTransferred(PutSQL.REL_SUCCESS, 0);

        attributes.clear();
        attributes.put("fragment.identifier", "1");
        attributes.put("fragment.count", "2");
        attributes.put("fragment.index", "1");

        runner.clearTransferState();
        runner.enqueue("UPDATE PERSONS SET NAME='Leonard' WHERE ID=1".getBytes(), attributes);
        runner.run();

        // Both FlowFiles with fragment identifier 1 should be successful
        runner.assertTransferCount(PutSQL.REL_SUCCESS, 2);
        runner.assertTransferCount(PutSQL.REL_FAILURE, 0);
        runner.assertTransferCount(PutSQL.REL_RETRY, 0);
        for (final MockFlowFile mff : runner.getFlowFilesForRelationship(PutSQL.REL_SUCCESS)) {
            mff.assertAttributeEquals("fragment.identifier", "1");
        }

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("Leonard", rs.getString(2));
                assertEquals(84, rs.getInt(3));
                assertFalse(rs.next());
            }
        }
    }


    @Test
    public void testTransactionTimeout() throws InitializationException, ProcessException, SQLException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(PutSQL.class);
        final File tempDir = folder.getRoot();
        final File dbDir = new File(tempDir, "db");
        final DBCPService service = new MockDBCPService(dbDir.getAbsolutePath());
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        runner.setProperty(PutSQL.TRANSACTION_TIMEOUT, "5 secs");
        runner.setProperty(PutSQL.CONNECTION_POOL, "dbcp");
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("fragment.identifier", "1");
        attributes.put("fragment.count", "2");
        attributes.put("fragment.index", "0");

        final MockFlowFile mff = new MockFlowFile(0L) {
            @Override
            public Long getLastQueueDate() {
                return System.currentTimeMillis() - 10000L;   // return 10 seconds ago
            }

            @Override
            public Map<String, String> getAttributes() {
                return attributes;
            }

            @Override
            public String getAttribute(final String attrName) {
                return attributes.get(attrName);
            }
        };

        runner.enqueue(mff);
        runner.run();

        // No FlowFiles should be transferred because there were not enough flowfiles with the same fragment identifier
        runner.assertAllFlowFilesTransferred(PutSQL.REL_FAILURE, 1);
    }

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
                final Connection conn = DriverManager.getConnection("jdbc:derby:" + dbLocation + ";create=true");
                return conn;
            } catch (final Exception e) {
                e.printStackTrace();
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }

    /**
     * Simple implementation only for testing purposes
     */
    private static class SQLExceptionService extends AbstractControllerService implements DBCPService {
        private final DBCPService service;
        private int allowedBeforeFailure = 0;
        private int successful = 0;

        public SQLExceptionService(final DBCPService service) {
            this.service = service;
        }

        @Override
        public String getIdentifier() {
            return "dbcp";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                if (++successful > allowedBeforeFailure) {
                    final Connection conn = Mockito.mock(Connection.class);
                    Mockito.when(conn.prepareStatement(Mockito.any(String.class))).thenThrow(new SQLException("Unit Test Generated SQLException"));
                    return conn;
                } else {
                    return service.getConnection();
                }
            } catch (final Exception e) {
                e.printStackTrace();
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }
}
