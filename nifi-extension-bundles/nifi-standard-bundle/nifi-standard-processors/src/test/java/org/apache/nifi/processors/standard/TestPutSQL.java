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

import jakarta.xml.bind.DatatypeConverter;
import org.apache.commons.io.FileUtils;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.pattern.RollbackOnFailure;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_CONTINUE;
import static org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_TERMINATE;
import static org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult.REJECT_AND_CONTINUE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPutSQL {
    private static final String createPersons = "CREATE TABLE PERSONS (id integer primary key, name varchar(100), code integer)";
    private static final String createPersonsAutoId = "CREATE TABLE PERSONS_AI (id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1), name VARCHAR(100), code INTEGER check(code <= 100))";

    private static final String DERBY_LOG_PROPERTY = "derby.stream.error.file";
    private static final Path SYSTEM_TEMP_DIR = Paths.get(System.getProperty("java.io.tmpdir"));
    private static final String TEST_DIRECTORY_NAME = "%s-%s".formatted(TestPutSQL.class.getSimpleName(), UUID.randomUUID());
    private static final Path DB_DIRECTORY = SYSTEM_TEMP_DIR.resolve(TEST_DIRECTORY_NAME);
    private static final Random random = new Random();

    /**
     * Setting up Connection pooling is expensive operation.
     * So let's do this only once and reuse MockDBCPService in each test.
     */
    static protected MockDBCPService service;

    @BeforeAll
    public static void setupBeforeAll() throws ProcessException, SQLException {
        System.setProperty(DERBY_LOG_PROPERTY, "target/derby.log");
        service = new MockDBCPService(DB_DIRECTORY.toAbsolutePath().toString());
        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPersons);
                stmt.executeUpdate(createPersonsAutoId);
            }
        }
    }

    @AfterAll
    public static void cleanupAfterAll() {
        System.clearProperty(DERBY_LOG_PROPERTY);

        try {
            FileUtils.deleteDirectory(DB_DIRECTORY.toFile());
        } catch (final Exception ignored) {

        }
    }

    @Test
    public void testDirectStatements() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();

        recreateTable("PERSONS", createPersons);

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
    public void testCommitOnCleanup() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        runner.setProperty(PutSQL.AUTO_COMMIT, "false");

        recreateTable("PERSONS", createPersons);

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
    }

    @Test
    public void testInsertWithGeneratedKeys() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        runner.setProperty(PutSQL.OBTAIN_GENERATED_KEYS, "true");

        recreateTable("PERSONS_AI", createPersonsAutoId);
        runner.enqueue("INSERT INTO PERSONS_AI (NAME, CODE) VALUES ('Mark', 84)".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSQL.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(PutSQL.REL_SUCCESS).get(0);
        mff.assertAttributeEquals("sql.generated.key", "1");

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS_AI");
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("Mark", rs.getString(2));
                assertEquals(84, rs.getInt(3));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testProvenanceEventsWithBatchMode() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        runner.setProperty(PutSQL.BATCH_SIZE, "10");
        runner.setProperty(PutSQL.SUPPORT_TRANSACTIONS, "false");
        runner.setProperty(PutSQL.OBTAIN_GENERATED_KEYS, "false");

        testProvenanceEvents(runner);
    }

    @Test
    public void testProvenanceEventsWithFragmentedTransaction() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        runner.setProperty(PutSQL.BATCH_SIZE, "10");
        runner.setProperty(PutSQL.SUPPORT_TRANSACTIONS, "true");
        runner.setProperty(PutSQL.OBTAIN_GENERATED_KEYS, "false");

        testProvenanceEvents(runner);
    }

    @Test
    public void testProvenanceEventsWithObtainGeneratedKeys() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        runner.setProperty(PutSQL.BATCH_SIZE, "10");
        runner.setProperty(PutSQL.SUPPORT_TRANSACTIONS, "false");
        runner.setProperty(PutSQL.OBTAIN_GENERATED_KEYS, "true");

        testProvenanceEvents(runner);
    }

    private void testProvenanceEvents(final TestRunner runner) throws ProcessException, SQLException {
        recreateTable("PERSONS", createPersons);

        runner.enqueue("DELETE FROM PERSONS WHERE ID = 1");
        runner.enqueue("INSERT INTO PERSONS (ID, NAME, CODE) VALUES (1, 'Mark', 84)");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSQL.REL_SUCCESS, 2);

        List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(2, provenanceEvents.size());
        for (ProvenanceEventRecord event: provenanceEvents) {
            assertEquals(ProvenanceEventType.SEND, event.getEventType());
        }
    }

    @Test
    public void testKeepFlowFileOrderingWithBatchMode() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        runner.setProperty(PutSQL.BATCH_SIZE, "10");
        runner.setProperty(PutSQL.SUPPORT_TRANSACTIONS, "false");
        runner.setProperty(PutSQL.OBTAIN_GENERATED_KEYS, "false");

        testKeepFlowFileOrdering(runner);
    }

    @Test
    public void testKeepFlowFileOrderingWithFragmentedTransaction() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        runner.setProperty(PutSQL.BATCH_SIZE, "10");
        runner.setProperty(PutSQL.SUPPORT_TRANSACTIONS, "true");
        runner.setProperty(PutSQL.OBTAIN_GENERATED_KEYS, "false");

        testKeepFlowFileOrdering(runner);
    }

    @Test
    public void testKeepFlowFileOrderingWithObtainGeneratedKeys() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        runner.setProperty(PutSQL.BATCH_SIZE, "10");
        runner.setProperty(PutSQL.SUPPORT_TRANSACTIONS, "false");
        runner.setProperty(PutSQL.OBTAIN_GENERATED_KEYS, "true");

        testKeepFlowFileOrdering(runner);
    }

    private void testKeepFlowFileOrdering(final TestRunner runner) throws ProcessException, SQLException {
        recreateTable("PERSONS", createPersons);

        final String delete = "DELETE FROM PERSONS WHERE ID = ?";
        final String insert = "INSERT INTO PERSONS (ID) VALUES (?)";

        final String[] statements = {delete, insert, insert, delete, delete, insert};

        final Function<Integer, Map<String, String>> createSqlAttributes = (id) -> {
            final Map<String, String> attributes = new HashMap<>();
            attributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
            attributes.put("sql.args.1.value", String.valueOf(id));
            return attributes;
        };

        final int flowFileCount = statements.length;

        for (int i = 0; i < flowFileCount; i++) {
            runner.enqueue(statements[i], createSqlAttributes.apply(i));
        }

        runner.run();

        runner.assertAllFlowFilesTransferred(PutSQL.REL_SUCCESS, flowFileCount);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutSQL.REL_SUCCESS);
        for (int i = 0; i < flowFileCount; i++) {
            MockFlowFile flowFile = flowFiles.get(i);
            assertEquals(statements[i], flowFile.getContent());
            assertEquals(String.valueOf(i), flowFile.getAttribute("sql.args.1.value"));
        }
        List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(flowFileCount, provenanceEvents.size());
        for (int i = 0; i < flowFileCount; i++) {
            ProvenanceEventRecord event = provenanceEvents.get(i);
            assertEquals(String.valueOf(i), event.getAttribute("sql.args.1.value"));
        }
    }


    @Test
    public void testFailInMiddleWithBadStatementAndSupportTransaction() throws InitializationException, ProcessException {
        final TestRunner runner = initTestRunner();
        testFailInMiddleWithBadStatement(runner);
        runner.run();

        runner.assertTransferCount(PutSQL.REL_FAILURE, 4);
        runner.assertTransferCount(PutSQL.REL_SUCCESS, 0);
        assertErrorAttributesInTransaction(runner, PutSQL.REL_FAILURE);
    }

    @Test
    public void testFailInMiddleWithBadStatementAndNotSupportTransaction() throws InitializationException, ProcessException {
        final TestRunner runner = initTestRunner();
        runner.setProperty(PutSQL.SUPPORT_TRANSACTIONS, "false");
        testFailInMiddleWithBadStatement(runner);
        runner.run();

        runner.assertTransferCount(PutSQL.REL_FAILURE, 1);
        runner.assertTransferCount(PutSQL.REL_SUCCESS, 3);

        assertSQLExceptionRelatedAttributes(runner, PutSQL.REL_FAILURE);
    }

    @Test
    public void testFailInMiddleWithBadStatementRollbackOnFailure() throws InitializationException, ProcessException {
        final TestRunner runner = initTestRunner();
        runner.setProperty(PutSQL.OBTAIN_GENERATED_KEYS, "false");
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, "true");
        runner.enqueue("INSERT INTO PERSONS_AI (NAME, CODE) VALUES ('Mark', 84)".getBytes());
        runner.enqueue("INSERT INTO PERSONS_AI".getBytes()); // intentionally wrong syntax
        runner.enqueue("INSERT INTO PERSONS_AI (NAME, CODE) VALUES ('Tom', 3)".getBytes());
        runner.enqueue("INSERT INTO PERSONS_AI (NAME, CODE) VALUES ('Harry', 44)".getBytes());

        final AssertionError e = assertThrows(AssertionError.class, runner::run);
        assertInstanceOf(ProcessException.class, e.getCause());
        runner.assertTransferCount(PutSQL.REL_FAILURE, 0);
        runner.assertTransferCount(PutSQL.REL_SUCCESS, 0);
    }

    @Test
    public void testFailInMiddleWithBadParameterTypeAndNotSupportTransaction() throws InitializationException, ProcessException {
        final TestRunner runner = initTestRunner();
        runner.setProperty(PutSQL.SUPPORT_TRANSACTIONS, "false");
        testFailInMiddleWithBadParameterType(runner);
        runner.run();

        runner.assertTransferCount(PutSQL.REL_FAILURE, 1);
        runner.assertTransferCount(PutSQL.REL_SUCCESS, 3);

        assertErrorAttributesNotSet(runner, PutSQL.REL_SUCCESS);
        assertSQLExceptionRelatedAttributes(runner, PutSQL.REL_FAILURE);
    }

    @Test
    public void testFailInMiddleWithBadParameterTypeAndSupportTransaction() throws InitializationException, ProcessException {
        final TestRunner runner = initTestRunner();
        testFailInMiddleWithBadParameterType(runner);
        runner.run();

        runner.assertTransferCount(PutSQL.REL_FAILURE, 4);
        runner.assertTransferCount(PutSQL.REL_SUCCESS, 0);

        assertErrorAttributesInTransaction(runner, PutSQL.REL_FAILURE);
    }

    @Test
    public void testFailInMiddleWithBadParameterTypeRollbackOnFailure() throws InitializationException, ProcessException {
        final TestRunner runner = initTestRunner();
        runner.setProperty(PutSQL.OBTAIN_GENERATED_KEYS, "false");
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, "true");

        final Map<String, String> goodAttributes = new HashMap<>();
        goodAttributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        goodAttributes.put("sql.args.1.value", "84");

        final Map<String, String> badAttributes = new HashMap<>();
        badAttributes.put("sql.args.1.type", String.valueOf(Types.VARCHAR));
        badAttributes.put("sql.args.1.value", "hello");

        final byte[] data = "INSERT INTO PERSONS_AI (NAME, CODE) VALUES ('Mark', ?)".getBytes();
        runner.enqueue(data, goodAttributes);
        runner.enqueue(data, badAttributes);
        runner.enqueue(data, goodAttributes);
        runner.enqueue(data, goodAttributes);

        final AssertionError e = assertThrows(AssertionError.class, runner::run);
        assertInstanceOf(ProcessException.class, e.getCause());
        runner.assertTransferCount(PutSQL.REL_FAILURE, 0);
        runner.assertTransferCount(PutSQL.REL_SUCCESS, 0);
    }

    @Test
    public void testFailInMiddleWithNumberFormatException() throws InitializationException, ProcessException {
        final TestRunner runner = initTestRunner();
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, "false");
        runner.setProperty(PutSQL.SUPPORT_TRANSACTIONS, "false");
        final Map<String, String> goodAttributes = new HashMap<>();
        goodAttributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        goodAttributes.put("sql.args.1.value", "84");

        final Map<String, String> badAttributes = new HashMap<>();
        badAttributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        badAttributes.put("sql.args.1.value", "hello");

        final byte[] data = "INSERT INTO PERSONS_AI (NAME, CODE) VALUES ('Mark', ?)".getBytes();
        runner.enqueue(data, goodAttributes);
        runner.enqueue(data, badAttributes);
        runner.enqueue(data, goodAttributes);
        runner.enqueue(data, goodAttributes);

        runner.run();

        runner.assertTransferCount(PutSQL.REL_FAILURE, 1);
        runner.assertTransferCount(PutSQL.REL_SUCCESS, 3);
        assertNonSQLErrorRelatedAttributes(runner, PutSQL.REL_FAILURE);
    }

    @Test
    public void testFailInMiddleWithBadParameterValueAndSupportTransaction() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        testFailInMiddleWithBadParameterValue(runner);
        runner.run();

        runner.assertTransferCount(PutSQL.REL_SUCCESS, 0);
        runner.assertTransferCount(PutSQL.REL_FAILURE, 0);
        runner.assertTransferCount(PutSQL.REL_RETRY, 4);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS_AI");
                assertFalse(rs.next());
            }
        }

        assertErrorAttributesInTransaction(runner, PutSQL.REL_RETRY);
        assertOriginalAttributesAreKept(runner);
    }

    @Test
    public void testFailInMiddleWithBadParameterValueAndNotSupportTransaction() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        runner.setProperty(PutSQL.SUPPORT_TRANSACTIONS, "false");
        testFailInMiddleWithBadParameterValue(runner);
        runner.run();

        runner.assertTransferCount(PutSQL.REL_SUCCESS, 1);
        runner.assertTransferCount(PutSQL.REL_FAILURE, 1);
        runner.assertTransferCount(PutSQL.REL_RETRY, 2);

        assertSQLExceptionRelatedAttributes(runner, PutSQL.REL_FAILURE);
        assertOriginalAttributesAreKept(runner);
        assertErrorAttributesNotSet(runner, PutSQL.REL_SUCCESS);
        assertErrorAttributesNotSet(runner, PutSQL.REL_RETRY);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS_AI");
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("Mark", rs.getString(2));
                assertEquals(84, rs.getInt(3));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testFailInMiddleWithBadParameterValueRollbackOnFailure() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        runner.setProperty(PutSQL.OBTAIN_GENERATED_KEYS, "false");
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, "true");

        recreateTable("PERSONS_AI", createPersonsAutoId);

        final Map<String, String> goodAttributes = new HashMap<>();
        goodAttributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        goodAttributes.put("sql.args.1.value", "84");

        final Map<String, String> badAttributes = new HashMap<>();
        badAttributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        badAttributes.put("sql.args.1.value", "9999");

        final byte[] data = "INSERT INTO PERSONS_AI (NAME, CODE) VALUES ('Mark', ?)".getBytes();
        runner.enqueue(data, goodAttributes);
        runner.enqueue(data, badAttributes);
        runner.enqueue(data, goodAttributes);
        runner.enqueue(data, goodAttributes);

        final AssertionError e = assertThrows(AssertionError.class, runner::run);
        assertInstanceOf(ProcessException.class, e.getCause());
        runner.assertTransferCount(PutSQL.REL_FAILURE, 0);
        runner.assertTransferCount(PutSQL.REL_SUCCESS, 0);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS_AI");
                assertFalse(rs.next());
            }
        }
    }


    @Test
    public void testUsingSqlDataTypesWithNegativeValues() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE PERSONS2 (id integer primary key, name varchar(100), code bigint)");
            }
        }

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("sql.args.1.type", "-5");
        attributes.put("sql.args.1.value", "84");
        runner.enqueue("INSERT INTO PERSONS2 (ID, NAME, CODE) VALUES (1, 'Mark', ?)".getBytes(), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSQL.REL_SUCCESS, 1);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS2");
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("Mark", rs.getString(2));
                assertEquals(84, rs.getInt(3));
                assertFalse(rs.next());
            }
        }
    }

    // Not specifying a format for the date fields here to continue to test backwards compatibility
    @Test
    public void testUsingTimestampValuesEpochAndString() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE TIMESTAMPTEST1 (id integer primary key, ts1 timestamp, ts2 timestamp)");
            }
        }

        final String arg2TS = "2001-01-01 00:01:01.001";
        final String art3TS = "2002-02-02 12:02:02.002";
        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        java.util.Date parsedDate = Date.from(LocalDateTime.parse(arg2TS, dateTimeFormatter).atZone(ZoneId.systemDefault()).toInstant());

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("sql.args.1.type", String.valueOf(Types.TIMESTAMP));
        attributes.put("sql.args.1.value", Long.toString(parsedDate.getTime()));
        attributes.put("sql.args.2.type", String.valueOf(Types.TIMESTAMP));
        attributes.put("sql.args.2.value", art3TS);

        runner.enqueue("INSERT INTO TIMESTAMPTEST1 (ID, ts1, ts2) VALUES (1, ?, ?)".getBytes(), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSQL.REL_SUCCESS, 1);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM TIMESTAMPTEST1");
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals(arg2TS, rs.getString(2));
                assertEquals(art3TS, rs.getString(3));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testUsingTimestampValuesWithFormatAttribute() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE TIMESTAMPTEST2 (id integer primary key, ts1 timestamp, ts2 timestamp)");
            }
        }

        final String dateStr1 = "2002-02-02T12:02:02";
        final String dateStrTimestamp1 = "2002-02-02 12:02:02";
        final long dateInt1 = Timestamp.valueOf(dateStrTimestamp1).getTime();

        final String dateStr2 = "2002-02-02T12:02:02.123456789";
        final String dateStrTimestamp2 = "2002-02-02 12:02:02.123456789";
        final long dateInt2 = Timestamp.valueOf(dateStrTimestamp2).getTime();
        final long nanoInt2 = 123456789L;

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("sql.args.1.type", String.valueOf(Types.TIMESTAMP));
        attributes.put("sql.args.1.value", dateStr1);
        attributes.put("sql.args.1.format", "ISO_LOCAL_DATE_TIME");
        attributes.put("sql.args.2.type", String.valueOf(Types.TIMESTAMP));
        attributes.put("sql.args.2.value", dateStr2);
        attributes.put("sql.args.2.format", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS");

        runner.enqueue("INSERT INTO TIMESTAMPTEST2 (ID, ts1, ts2) VALUES (1, ?, ?)".getBytes(), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSQL.REL_SUCCESS, 1);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM TIMESTAMPTEST2");
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals(dateInt1, rs.getTimestamp(2).getTime());
                assertEquals(dateInt2, rs.getTimestamp(3).getTime());
                assertEquals(nanoInt2, rs.getTimestamp(3).getNanos());
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testUsingDateTimeValuesWithFormatAttribute() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE TIMESTAMPTEST3 (id integer primary key, ts1 TIME, ts2 DATE)");
            }
        }

        final String dateStr = "2002-03-04";
        final String timeStr = "02:03:04";

        final DateTimeFormatter timeFormatter = DateTimeFormatter.ISO_LOCAL_TIME;
        LocalTime parsedTime = LocalTime.parse(timeStr, timeFormatter);
        Time expectedTime = Time.valueOf(parsedTime);

        final DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_LOCAL_DATE;
        LocalDate parsedDate = LocalDate.parse(dateStr, dateFormatter);
        Date expectedDate = new Date(Date.from(parsedDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant()).getTime());

        final long expectedTimeInLong = expectedTime.getTime();
        final long expectedDateInLong = expectedDate.getTime();

        // test with ISO LOCAL format attribute
        Map<String, String> attributes = new HashMap<>();
        attributes.put("sql.args.1.type", String.valueOf(Types.TIME));
        attributes.put("sql.args.1.value", timeStr);
        attributes.put("sql.args.1.format", "ISO_LOCAL_TIME");
        attributes.put("sql.args.2.type", String.valueOf(Types.DATE));
        attributes.put("sql.args.2.value", dateStr);
        attributes.put("sql.args.2.format", "ISO_LOCAL_DATE");

        runner.enqueue("INSERT INTO TIMESTAMPTEST3 (ID, ts1, ts2) VALUES (1, ?, ?)".getBytes(), attributes);

        // Since Derby database which is used for unit test does not have timezone in DATE and TIME type,
        // and PutSQL converts date string into long representation using local timezone,
        // we need to use local timezone.
        java.util.Date parsedLocalTime = java.sql.Time.valueOf(timeStr);

        java.util.Date parsedLocalDate = java.sql.Date.valueOf(dateStr);

        // test Long pattern without format attribute
        attributes = new HashMap<>();
        attributes.put("sql.args.1.type", String.valueOf(Types.TIME));
        attributes.put("sql.args.1.value", Long.toString(parsedLocalTime.getTime()));
        attributes.put("sql.args.2.type", String.valueOf(Types.DATE));
        attributes.put("sql.args.2.value", Long.toString(parsedLocalDate.getTime()));

        runner.enqueue("INSERT INTO TIMESTAMPTEST3 (ID, ts1, ts2) VALUES (2, ?, ?)".getBytes(), attributes);

        // test with format attribute
        attributes = new HashMap<>();
        attributes.put("sql.args.1.type", String.valueOf(Types.TIME));
        attributes.put("sql.args.1.value", "020304000");
        attributes.put("sql.args.1.format", "HHmmssSSS");
        attributes.put("sql.args.2.type", String.valueOf(Types.DATE));
        attributes.put("sql.args.2.value", "20020304");
        attributes.put("sql.args.2.format", "yyyyMMdd");

        runner.enqueue("INSERT INTO TIMESTAMPTEST3 (ID, ts1, ts2) VALUES (3, ?, ?)".getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutSQL.REL_SUCCESS, 3);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM TIMESTAMPTEST3 ORDER BY ID");
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals(expectedTimeInLong, rs.getTime(2).getTime());
                assertEquals(expectedDateInLong, rs.getDate(3).getTime());

                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertEquals(parsedLocalTime.getTime(), rs.getTime(2).getTime());
                assertEquals(parsedLocalDate.getTime(), rs.getDate(3).getTime());

                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertEquals(expectedTimeInLong, rs.getTime(2).getTime());
                assertEquals(expectedDateInLong, rs.getDate(3).getTime());

                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testBitType() throws SQLException, InitializationException {
        final TestRunner runner = initTestRunner();
        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE BITTESTS (id integer primary key, bt1 BOOLEAN)");
            }
        }

        final byte[] insertStatement = "INSERT INTO BITTESTS (ID, bt1) VALUES (?, ?)".getBytes();

        Map<String, String> attributes = new HashMap<>();
        attributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.1.value", "1");
        attributes.put("sql.args.2.type", String.valueOf(Types.BIT));
        attributes.put("sql.args.2.value", "1");
        runner.enqueue(insertStatement, attributes);

        attributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.1.value", "2");
        attributes.put("sql.args.2.type", String.valueOf(Types.BIT));
        attributes.put("sql.args.2.value", "0");
        runner.enqueue(insertStatement, attributes);

        attributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.1.value", "3");
        attributes.put("sql.args.2.type", String.valueOf(Types.BIT));
        attributes.put("sql.args.2.value", "-5");
        runner.enqueue(insertStatement, attributes);

        attributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.1.value", "4");
        attributes.put("sql.args.2.type", String.valueOf(Types.BIT));
        attributes.put("sql.args.2.value", "t");
        runner.enqueue(insertStatement, attributes);

        attributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.1.value", "5");
        attributes.put("sql.args.2.type", String.valueOf(Types.BIT));
        attributes.put("sql.args.2.value", "f");
        runner.enqueue(insertStatement, attributes);

        attributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.1.value", "6");
        attributes.put("sql.args.2.type", String.valueOf(Types.BIT));
        attributes.put("sql.args.2.value", "T");
        runner.enqueue(insertStatement, attributes);

        attributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.1.value", "7");
        attributes.put("sql.args.2.type", String.valueOf(Types.BIT));
        attributes.put("sql.args.2.value", "true");
        runner.enqueue(insertStatement, attributes);

        attributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.1.value", "8");
        attributes.put("sql.args.2.type", String.valueOf(Types.BIT));
        attributes.put("sql.args.2.value", "false");
        runner.enqueue(insertStatement, attributes);

        runner.run();
        runner.assertAllFlowFilesTransferred(PutSQL.REL_SUCCESS, 8);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM BITTESTS");

                //First test (true)
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertTrue(rs.getBoolean(2));

                //Second test (false)
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertFalse(rs.getBoolean(2));

                //Third test (false)
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertFalse(rs.getBoolean(2));

                //Fourth test (true)
                assertTrue(rs.next());
                assertEquals(4, rs.getInt(1));
                assertTrue(rs.getBoolean(2));

                //Fifth test (false)
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
                assertFalse(rs.getBoolean(2));

                //Sixth test (true)
                assertTrue(rs.next());
                assertEquals(6, rs.getInt(1));
                assertTrue(rs.getBoolean(2));

                //Seventh test (true)
                assertTrue(rs.next());
                assertEquals(7, rs.getInt(1));
                assertTrue(rs.getBoolean(2));

                //Eighth test (false)
                assertTrue(rs.next());
                assertEquals(8, rs.getInt(1));
                assertFalse(rs.getBoolean(2));

                assertFalse(rs.next());
            }
        }

    }

    @Test
    public void testUsingTimeValuesEpochAndString() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE TIMETESTS (id integer primary key, ts1 time, ts2 time)");
            }
        }

        final String arg2TS = "00:01:02";
        final String art3TS = "02:03:04";
        final String timeFormatString = "HH:mm:ss";
        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(timeFormatString);
        java.util.Date parsedDate = Date.from(LocalTime.parse(arg2TS, dateTimeFormatter).atDate(LocalDate.now()).atZone(ZoneId.systemDefault()).toInstant());


        final Map<String, String> attributes = new HashMap<>();
        attributes.put("sql.args.1.type", String.valueOf(Types.TIME));
        attributes.put("sql.args.1.value", Long.toString(parsedDate.getTime()));
        attributes.put("sql.args.2.type", String.valueOf(Types.TIME));
        attributes.put("sql.args.2.value", art3TS);
        attributes.put("sql.args.2.format", timeFormatString);

        runner.enqueue("INSERT INTO TIMETESTS (ID, ts1, ts2) VALUES (1, ?, ?)".getBytes(), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSQL.REL_SUCCESS, 1);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM TIMETESTS");

                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals(arg2TS, rs.getTime(2).toString());
                assertEquals(art3TS, rs.getString(3));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testUsingDateValuesEpochAndString() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE DATETESTS (id integer primary key, ts1 date, ts2 date)");
            }
        }

        final String arg2TS = "2001-01-01";
        final String art3TS = "2002-02-02";
        java.util.Date parsedDate = java.sql.Date.valueOf(arg2TS);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("sql.args.1.type", String.valueOf(Types.DATE));
        attributes.put("sql.args.1.value", Long.toString(parsedDate.getTime()));
        attributes.put("sql.args.2.type", String.valueOf(Types.DATE));
        attributes.put("sql.args.2.value", art3TS);

        runner.enqueue("INSERT INTO DATETESTS (ID, ts1, ts2) VALUES (1, ?, ?)".getBytes(), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSQL.REL_SUCCESS, 1);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM DATETESTS");
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals(arg2TS, rs.getString(2));
                assertEquals(art3TS, rs.getString(3));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testBinaryColumnTypes() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE BINARYTESTS (id integer primary key, bn1 CHAR(8) FOR BIT DATA, bn2 VARCHAR(100) FOR BIT DATA, " +
                        "bn3 LONG VARCHAR FOR BIT DATA)");
            }
        }

        final byte[] insertStatement = "INSERT INTO BINARYTESTS (ID, bn1, bn2, bn3) VALUES (?, ?, ?, ?)".getBytes();

        final String arg2BIN = fixedSizeByteArrayAsASCIIString(8);
        final String art3VARBIN = fixedSizeByteArrayAsASCIIString(50);
        final String art4LongBin = fixedSizeByteArrayAsASCIIString(32700); //max size supported by Derby

        //ASCII (default) binary formatn
        Map<String, String> attributes = new HashMap<>();
        attributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.1.value", "1");
        attributes.put("sql.args.2.type", String.valueOf(Types.BINARY));
        attributes.put("sql.args.2.value", arg2BIN);
        attributes.put("sql.args.3.type", String.valueOf(Types.VARBINARY));
        attributes.put("sql.args.3.value", art3VARBIN);
        attributes.put("sql.args.4.type", String.valueOf(Types.LONGVARBINARY));
        attributes.put("sql.args.4.value", art4LongBin);

        runner.enqueue(insertStatement, attributes);

        //ASCII with specified format
        attributes = new HashMap<>();
        attributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.1.value", "2");
        attributes.put("sql.args.2.type", String.valueOf(Types.BINARY));
        attributes.put("sql.args.2.value", arg2BIN);
        attributes.put("sql.args.2.format", "ascii");
        attributes.put("sql.args.3.type", String.valueOf(Types.VARBINARY));
        attributes.put("sql.args.3.value", art3VARBIN);
        attributes.put("sql.args.3.format", "ascii");
        attributes.put("sql.args.4.type", String.valueOf(Types.LONGVARBINARY));
        attributes.put("sql.args.4.value", art4LongBin);
        attributes.put("sql.args.4.format", "ascii");

        runner.enqueue(insertStatement, attributes);

        //Hex
        final String arg2HexBIN = fixedSizeByteArrayAsHexString(8);
        final String art3HexVARBIN = fixedSizeByteArrayAsHexString(50);
        final String art4HexLongBin = fixedSizeByteArrayAsHexString(32700);

        attributes = new HashMap<>();
        attributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.1.value", "3");
        attributes.put("sql.args.2.type", String.valueOf(Types.BINARY));
        attributes.put("sql.args.2.value", arg2HexBIN);
        attributes.put("sql.args.2.format", "hex");
        attributes.put("sql.args.3.type", String.valueOf(Types.VARBINARY));
        attributes.put("sql.args.3.value", art3HexVARBIN);
        attributes.put("sql.args.3.format", "hex");
        attributes.put("sql.args.4.type", String.valueOf(Types.LONGVARBINARY));
        attributes.put("sql.args.4.value", art4HexLongBin);
        attributes.put("sql.args.4.format", "hex");

        runner.enqueue(insertStatement, attributes);

        //Base64
        final String arg2Base64BIN = fixedSizeByteArrayAsBase64String(8);
        final String art3Base64VARBIN = fixedSizeByteArrayAsBase64String(50);
        final String art4Base64LongBin = fixedSizeByteArrayAsBase64String(32700);

        attributes = new HashMap<>();
        attributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        attributes.put("sql.args.1.value", "4");
        attributes.put("sql.args.2.type", String.valueOf(Types.BINARY));
        attributes.put("sql.args.2.value", arg2Base64BIN);
        attributes.put("sql.args.2.format", "base64");
        attributes.put("sql.args.3.type", String.valueOf(Types.VARBINARY));
        attributes.put("sql.args.3.value", art3Base64VARBIN);
        attributes.put("sql.args.3.format", "base64");
        attributes.put("sql.args.4.type", String.valueOf(Types.LONGVARBINARY));
        attributes.put("sql.args.4.value", art4Base64LongBin);
        attributes.put("sql.args.4.format", "base64");

        runner.enqueue(insertStatement, attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutSQL.REL_SUCCESS, 4);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM BINARYTESTS");

                //First Batch
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertArrayEquals(arg2BIN.getBytes(US_ASCII), rs.getBytes(2));
                assertArrayEquals(art3VARBIN.getBytes(US_ASCII), rs.getBytes(3));
                assertArrayEquals(art4LongBin.getBytes(US_ASCII), rs.getBytes(4));

                //Second batch
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertArrayEquals(arg2BIN.getBytes(US_ASCII), rs.getBytes(2));
                assertArrayEquals(art3VARBIN.getBytes(US_ASCII), rs.getBytes(3));
                assertArrayEquals(art4LongBin.getBytes(US_ASCII), rs.getBytes(4));

                //Third Batch (Hex)
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertArrayEquals(DatatypeConverter.parseHexBinary(arg2HexBIN), rs.getBytes(2));
                assertArrayEquals(DatatypeConverter.parseHexBinary(art3HexVARBIN), rs.getBytes(3));
                assertArrayEquals(DatatypeConverter.parseHexBinary(art4HexLongBin), rs.getBytes(4));

                //Fourth Batch (Base64)
                assertTrue(rs.next());
                assertEquals(4, rs.getInt(1));
                assertArrayEquals(DatatypeConverter.parseBase64Binary(arg2Base64BIN), rs.getBytes(2));
                assertArrayEquals(DatatypeConverter.parseBase64Binary(art3Base64VARBIN), rs.getBytes(3));
                assertArrayEquals(DatatypeConverter.parseBase64Binary(art4Base64LongBin), rs.getBytes(4));

                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testStatementsWithPreparedParameters() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();

        recreateTable("PERSONS", createPersons);

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
    public void testMultipleStatementsWithinFlowFile() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();

        recreateTable("PERSONS", createPersons);

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
        assertSQLExceptionRelatedAttributes(runner, PutSQL.REL_FAILURE);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testMultipleStatementsWithinFlowFileRollbackOnFailure() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, "true");

        recreateTable("PERSONS", createPersons);

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
        final AssertionError e = assertThrows(AssertionError.class, runner::run);
        assertInstanceOf(ProcessException.class, e.getCause());

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
                assertFalse(rs.next());
            }
        }
    }


    @Test
    public void testWithNullParameter() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
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
    public void testInvalidStatement() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();

        recreateTable("PERSONS", createPersons);

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
        assertSQLExceptionRelatedAttributes(runner, PutSQL.REL_FAILURE);

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testInvalidStatementRollbackOnFailure() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, "true");

        recreateTable("PERSONS", createPersons);

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
        final AssertionError e = assertThrows(AssertionError.class, runner::run);
        assertInstanceOf(ProcessException.class, e.getCause());

        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
                assertFalse(rs.next());
            }
        }
    }


    @Test
    public void testRetryableFailure() throws InitializationException, ProcessException {
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
        assertNonSQLErrorRelatedAttributes(runner, PutSQL.REL_RETRY);
    }

    @Test
    public void testRetryableFailureRollbackOnFailure() throws InitializationException, ProcessException {
        final TestRunner runner = TestRunners.newTestRunner(PutSQL.class);
        final DBCPService service = new SQLExceptionService(null);
        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);

        runner.setProperty(PutSQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, "true");

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
        final AssertionError e = assertThrows(AssertionError.class, runner::run);
        assertInstanceOf(ProcessException.class, e.getCause());
        runner.assertAllFlowFilesTransferred(PutSQL.REL_RETRY, 0);
    }

    @Test
    public void testMultipleFlowFilesSuccessfulInTransaction() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        runner.setProperty(PutSQL.BATCH_SIZE, "1");

        recreateTable("PERSONS", createPersons);

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

        // No FlowFiles should be transferred because there were not enough FlowFiles with the same fragment identifier
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
    public void testMultipleFlowFilesSuccessfulInTransactionRollBackOnFailure() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        runner.setProperty(PutSQL.BATCH_SIZE, "1");
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, "true");

        recreateTable("PERSONS", createPersons);

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
        // ProcessException should not be thrown in this case, because the input FlowFiles are simply differed.
        runner.run();

        // No FlowFiles should be transferred because there were not enough FlowFiles with the same fragment identifier
        runner.assertAllFlowFilesTransferred(PutSQL.REL_SUCCESS, 0);

    }

    @Test
    public void testTransactionTimeout() throws InitializationException, ProcessException {
        final TestRunner runner = initTestRunner();

        runner.setProperty(PutSQL.TRANSACTION_TIMEOUT, "5 secs");
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("fragment.identifier", "1");
        attributes.put("fragment.count", "2");
        attributes.put("fragment.index", "0");

        final MockFlowFile mff = new MockFlowFile(0L) {
            @Override
            public Long getLastQueueDate() {
                return System.currentTimeMillis() - 10000L; // return 10 seconds ago
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

        // No FlowFiles should be transferred because there were not enough FlowFiles with the same fragment identifier
        runner.assertAllFlowFilesTransferred(PutSQL.REL_FAILURE, 1);
        assertNonSQLErrorRelatedAttributes(runner, PutSQL.REL_FAILURE);
    }

    @Test
    public void testTransactionTimeoutRollbackOnFailure() throws InitializationException, ProcessException {
        final TestRunner runner = initTestRunner();

        runner.setProperty(PutSQL.TRANSACTION_TIMEOUT, "5 secs");
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, "true");
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("fragment.identifier", "1");
        attributes.put("fragment.count", "2");
        attributes.put("fragment.index", "0");

        final MockFlowFile mff = new MockFlowFile(0L) {
            @Override
            public Long getLastQueueDate() {
                return System.currentTimeMillis() - 10000L; // return 10 seconds ago
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
        final AssertionError e = assertThrows(AssertionError.class, runner::run);
        assertInstanceOf(ProcessException.class, e.getCause());

        runner.assertAllFlowFilesTransferred(PutSQL.REL_FAILURE, 0);
    }

    @Test
    public void testNullFragmentCountRollbackOnFailure() throws InitializationException, ProcessException {
        final TestRunner runner = initTestRunner();

        runner.setProperty(PutSQL.TRANSACTION_TIMEOUT, "5 secs");
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, "true");
        final Map<String, String> attribute1 = new HashMap<>();
        attribute1.put("fragment.identifier", "1");
        attribute1.put("fragment.count", "2");
        attribute1.put("fragment.index", "0");

        final Map<String, String> attribute2 = new HashMap<>();
        attribute2.put("fragment.identifier", "1");
//        attribute2.put("fragment.count", null);
        attribute2.put("fragment.index", "1");

        runner.enqueue(new byte[]{}, attribute1);
        runner.enqueue(new byte[]{}, attribute2);

        final AssertionError e = assertThrows(AssertionError.class, runner::run);
        assertInstanceOf(ProcessException.class, e.getCause());

        runner.assertAllFlowFilesTransferred(PutSQL.REL_FAILURE, 0);
    }

    @Test
    public void testStatementsFromProperty() throws InitializationException, ProcessException, SQLException {
        final TestRunner runner = initTestRunner();
        runner.setProperty(PutSQL.SQL_STATEMENT, "INSERT INTO PERSONS (ID, NAME, CODE) VALUES (${row.id}, 'Mark', 84)");

        recreateTable("PERSONS", createPersons);

        runner.enqueue("This statement should be ignored".getBytes(), Map.of("row.id", "1"));
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

        runner.setProperty(PutSQL.SQL_STATEMENT, "UPDATE PERSONS SET NAME='George' WHERE ID=${row.id}");
        runner.enqueue("This statement should be ignored".getBytes(), Map.of("row.id", "1"));
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
    public void testTransactionalFlowFileFilter() {
        final MockFlowFile ff0 = new MockFlowFile(0);
        final MockFlowFile ff1 = new MockFlowFile(1);
        final MockFlowFile ff2 = new MockFlowFile(2);
        final MockFlowFile ff3 = new MockFlowFile(3);
        final MockFlowFile ff4 = new MockFlowFile(4);

        ff0.putAttributes(createFragmentedTransactionAttributes("tx-1", 3, 0));
        ff1.putAttributes(Collections.singletonMap("accept", "false"));
        ff2.putAttributes(createFragmentedTransactionAttributes("tx-1", 3, 1));
        ff3.putAttributes(Collections.singletonMap("accept", "true"));
        ff4.putAttributes(createFragmentedTransactionAttributes("tx-1", 3, 2));

        // TEST 1: Fragmented TX with null service filter
        // Even if the controller service does not have filtering rule, tx filter should work.
        FlowFileFilter txFilter = new PutSQL.TransactionalFlowFileFilter(null);
        // Should perform a fragmented tx.
        assertEquals(ACCEPT_AND_CONTINUE, txFilter.filter(ff0));
        assertEquals(REJECT_AND_CONTINUE, txFilter.filter(ff1));
        assertEquals(ACCEPT_AND_CONTINUE, txFilter.filter(ff2));
        assertEquals(REJECT_AND_CONTINUE, txFilter.filter(ff3));
        assertEquals(ACCEPT_AND_TERMINATE, txFilter.filter(ff4));

        // TEST 2: Non-Fragmented TX with null service filter
        txFilter = new PutSQL.TransactionalFlowFileFilter(null);
        // Should perform a non-fragmented tx.
        assertEquals(ACCEPT_AND_CONTINUE, txFilter.filter(ff1));
        assertEquals(REJECT_AND_CONTINUE, txFilter.filter(ff0));
        assertEquals(REJECT_AND_CONTINUE, txFilter.filter(ff2));
        assertEquals(ACCEPT_AND_CONTINUE, txFilter.filter(ff3));
        assertEquals(REJECT_AND_CONTINUE, txFilter.filter(ff4));


        final FlowFileFilter nonTxFilter = flowFile -> "true".equals(flowFile.getAttribute("accept"))
            ? ACCEPT_AND_CONTINUE
            : REJECT_AND_CONTINUE;

        // TEST 3: Fragmented TX with a service filter
        // Even if the controller service does not have filtering rule, tx filter should work.
        txFilter = new PutSQL.TransactionalFlowFileFilter(nonTxFilter);
        // Should perform a fragmented tx. The nonTxFilter doesn't affect in this case.
        assertEquals(ACCEPT_AND_CONTINUE, txFilter.filter(ff0));
        assertEquals(REJECT_AND_CONTINUE, txFilter.filter(ff1));
        assertEquals(ACCEPT_AND_CONTINUE, txFilter.filter(ff2));
        assertEquals(REJECT_AND_CONTINUE, txFilter.filter(ff3));
        assertEquals(ACCEPT_AND_TERMINATE, txFilter.filter(ff4));

        // TEST 4: Non-Fragmented TX with a service filter
        txFilter = new PutSQL.TransactionalFlowFileFilter(nonTxFilter);
        // Should perform a non-fragmented tx and use the nonTxFilter.
        assertEquals(REJECT_AND_CONTINUE, txFilter.filter(ff1));
        assertEquals(REJECT_AND_CONTINUE, txFilter.filter(ff0));
        assertEquals(REJECT_AND_CONTINUE, txFilter.filter(ff2));
        assertEquals(ACCEPT_AND_CONTINUE, txFilter.filter(ff3));
        assertEquals(REJECT_AND_CONTINUE, txFilter.filter(ff4));
    }

    @Test
    public void flowFilesWithMissingButRequiredDatabaseNameAreRoutedToFailure() throws Exception {
        makeServiceLookLikeAnInstanceOfDBCPConnectionPoolLookup();

        try {
            recreateTable("PERSONS", createPersons);

            TestRunner runner = initTestRunner();

            runner.enqueue("INSERT INTO PERSONS (ID, NAME, CODE) VALUES (1, 'Mark', 84)", new HashMap<>());
            runner.run();

            List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutSQL.REL_FAILURE);
            List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(PutSQL.REL_SUCCESS);
            assertEquals(1, failureFlowFiles.size());
            assertEquals(0, successFlowFiles.size());
        } finally {
            service.setFlowFileFilter(null);
        }
    }

    @Test
    public void flowFilesWithExistingRequiredDatabaseNameAreRoutedToSuccess() throws Exception {
        makeServiceLookLikeAnInstanceOfDBCPConnectionPoolLookup();

        try {
            recreateTable("PERSONS", createPersons);

            TestRunner runner = initTestRunner();

            Map<String, String> attributes = new HashMap<>();
            attributes.put("database.name", "someDatabaseName");
            runner.enqueue("INSERT INTO PERSONS (ID, NAME, CODE) VALUES (1, 'Mark', 84)", attributes);
            runner.run();

            List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutSQL.REL_FAILURE);
            List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(PutSQL.REL_SUCCESS);
            assertEquals(0, failureFlowFiles.size());
            assertEquals(1, successFlowFiles.size());
        } finally {
            service.setFlowFileFilter(null);
        }
    }

    private void makeServiceLookLikeAnInstanceOfDBCPConnectionPoolLookup() {
        // service has a FlowFileFilter when, and only when it is a DBCPConnectionPoolLookup
        // As such, it requires incoming FileFiles to have a 'database.name' attribute
        service.setFlowFileFilter(flowFile -> ACCEPT_AND_CONTINUE);
    }

    private void testFailInMiddleWithBadParameterType(final TestRunner runner) throws ProcessException {
        runner.setProperty(PutSQL.OBTAIN_GENERATED_KEYS, "false");

        final Map<String, String> goodAttributes = new HashMap<>();
        goodAttributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        goodAttributes.put("sql.args.1.value", "84");

        final Map<String, String> badAttributes = new HashMap<>();
        badAttributes.put("sql.args.1.type", String.valueOf(Types.VARCHAR));
        badAttributes.put("sql.args.1.value", "hello");

        final byte[] data = "INSERT INTO PERSONS_AI (NAME, CODE) VALUES ('Mark', ?)".getBytes();
        runner.enqueue(data, goodAttributes);
        runner.enqueue(data, badAttributes);
        runner.enqueue(data, goodAttributes);
        runner.enqueue(data, goodAttributes);
    }

    private void testFailInMiddleWithBadParameterValue(final TestRunner runner) throws ProcessException, SQLException {
        runner.setProperty(PutSQL.OBTAIN_GENERATED_KEYS, "false");
        recreateTable("PERSONS_AI", createPersonsAutoId);
        final Map<String, String> goodAttributes = new HashMap<>();
        goodAttributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        goodAttributes.put("sql.args.1.value", "84");

        final Map<String, String> badAttributes = new HashMap<>();
        badAttributes.put("sql.args.1.type", String.valueOf(Types.INTEGER));
        badAttributes.put("sql.args.1.value", "9999");

        final byte[] data = "INSERT INTO PERSONS_AI (NAME, CODE) VALUES ('Mark', ?)".getBytes();
        runner.enqueue(data, goodAttributes);
        runner.enqueue(data, badAttributes);
        runner.enqueue(data, goodAttributes);
        runner.enqueue(data, goodAttributes);
    }

    private void testFailInMiddleWithBadStatement(final TestRunner runner) {
        runner.setProperty(PutSQL.OBTAIN_GENERATED_KEYS, "false");
        runner.enqueue("INSERT INTO PERSONS_AI (NAME, CODE) VALUES ('Mark', 84)".getBytes());
        runner.enqueue("INSERT INTO PERSONS_AI".getBytes()); // intentionally wrong syntax
        runner.enqueue("INSERT INTO PERSONS_AI (NAME, CODE) VALUES ('Tom', 3)".getBytes());
        runner.enqueue("INSERT INTO PERSONS_AI (NAME, CODE) VALUES ('Harry', 44)".getBytes());
    }

    private Map<String, String> createFragmentedTransactionAttributes(String id, int count, int index) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("fragment.identifier", id);
        attributes.put("fragment.count", String.valueOf(count));
        attributes.put("fragment.index", String.valueOf(index));
        return attributes;
    }

    /**
     * Simple implementation only for testing purposes
     */
    private static class MockDBCPService extends AbstractControllerService implements DBCPService {
        private final String dbLocation;
        private volatile FlowFileFilter flowFileFilter;

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

        @Override
        public FlowFileFilter getFlowFileFilter() {
            return flowFileFilter;
        }

        public void setFlowFileFilter(FlowFileFilter flowFileFilter) {
            this.flowFileFilter = flowFileFilter;
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
                    final Connection conn = mock(Connection.class);
                    when(conn.prepareStatement(Mockito.any(String.class))).thenThrow(new SQLException("Unit Test Generated SQLException"));
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


    private void recreateTable(String tableName, String createSQL) throws ProcessException, SQLException {
        try (final Connection conn = service.getConnection()) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("drop table " + tableName);
                stmt.executeUpdate(createSQL);
            }
        }
    }

    private byte[] randomBytes(final int count) {
        final byte[] bytes = new byte[count];
        random.nextBytes(bytes);
        return bytes;
    }

    private String fixedSizeByteArrayAsASCIIString(int length) {
        byte[] bBinary = randomBytes(length);
        ByteBuffer bytes = ByteBuffer.wrap(bBinary);
        StringBuilder sbBytes = new StringBuilder();
        for (int i = bytes.position(); i < bytes.limit(); i++)
            sbBytes.append((char) bytes.get(i));

        return sbBytes.toString();
    }

    private String fixedSizeByteArrayAsHexString(int length) {
        byte[] bBinary = randomBytes(length);
        return DatatypeConverter.printHexBinary(bBinary);
    }

    private String fixedSizeByteArrayAsBase64String(int length) {
        byte[] bBinary = randomBytes(length);
        return DatatypeConverter.printBase64Binary(bBinary);
    }

    private TestRunner initTestRunner() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PutSQL.class);

        runner.addControllerService("dbcp", service);
        runner.enableControllerService(service);
        runner.setProperty(PutSQL.CONNECTION_POOL, "dbcp");

        return runner;
    }

    private static void assertSQLExceptionRelatedAttributes(final TestRunner runner, Relationship relationship) {
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(relationship);
        flowFiles.forEach(ff -> {
            ff.assertAttributeExists("error.message");
            ff.assertAttributeExists("error.code");
            ff.assertAttributeExists("error.sql.state");
        });
    }

    private static void assertNonSQLErrorRelatedAttributes(final TestRunner runner,  Relationship relationship) {
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(relationship);
        flowFiles.forEach(ff -> {
            ff.assertAttributeExists("error.message");
        });
    }

    private static void assertOriginalAttributesAreKept(final TestRunner runner) {
        runner.assertAllFlowFilesContainAttribute("sql.args.1.type");
        runner.assertAllFlowFilesContainAttribute("sql.args.1.value");
    }


    private static void assertErrorAttributesInTransaction(final TestRunner runner, Relationship relationship) {
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(relationship);
        assertEquals(1, flowFiles.stream()
                .filter(TestPutSQL::errorAttributesAreSet)
                .count(),
                "Only one FlowFile should have the error attributes when transaction is used.");
    }

    private static void assertErrorAttributesNotSet(final TestRunner runner, Relationship relationship) {
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(relationship);
        flowFiles.forEach(ff -> {
            ff.assertAttributeNotExists("error.message");
        });
    }

    private static boolean errorAttributesAreSet(MockFlowFile ff) {
        return ff.getAttribute("error.message") != null
                && ff.getAttribute("error.code") != null
                && ff.getAttribute("error.sql.state") != null;
    }
}
