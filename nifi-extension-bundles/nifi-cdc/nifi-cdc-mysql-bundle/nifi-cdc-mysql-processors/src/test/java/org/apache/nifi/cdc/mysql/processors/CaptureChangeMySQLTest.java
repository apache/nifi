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
package org.apache.nifi.cdc.mysql.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.XidEventData;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.github.shyiko.mysql.binlog.network.SSLSocketFactory;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.cdc.event.ColumnDefinition;
import org.apache.nifi.cdc.event.TableInfo;
import org.apache.nifi.cdc.event.TableInfoCacheKey;
import org.apache.nifi.cdc.event.io.EventWriter;
import org.apache.nifi.cdc.event.io.FlowFileEventWriteStrategy;
import org.apache.nifi.cdc.mysql.EventUtils;
import org.apache.nifi.cdc.mysql.MockBinlogClient;
import org.apache.nifi.cdc.mysql.event.BinlogEventInfo;
import org.apache.nifi.cdc.mysql.processors.ssl.BinaryLogSSLSocketFactory;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.Serializable;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * Unit test(s) for MySQL CDC
 */
@ExtendWith(MockitoExtension.class)
public class CaptureChangeMySQLTest {
    // Use an http-based URL driver location because we don't have the driver available in the unit test, and we don't want the processor to
    // be invalid due to a missing file. By specifying an HTTP based URL address, we won't validate whether or not the file exists
    private static final String DRIVER_LOCATION = "http://mysql-driver.com/driver.jar";
    private static final String LOCAL_HOST = "localhost";
    private static final int DEFAULT_PORT = 3306;
    private static final String LOCAL_HOST_DEFAULT_PORT = LOCAL_HOST + ":" + DEFAULT_PORT;
    private static final String ROOT_USER = "root";
    private static final String PASSWORD = "password";
    private static final String CONNECT_TIMEOUT = "2 seconds";
    private static final String INIT_BIN_LOG_FILENAME = "master.000001";
    private static final String SUBSEQUENT_BIN_LOG_FILENAME = "master.000002";
    private static final String MY_DB = "myDB";
    private static final String MY_TABLE = "myTable";
    private static final String NOT_MY_TABLE = "notMyTable";
    private static final String USERS_TABLE = "users";
    private static final String USER_TABLE = "user";
    private static final String BEGIN_SQL_KEYWORD_UPPERCASE = "BEGIN";
    private static final String BEGIN_SQL_KEYWORD_LOWERCASE = BEGIN_SQL_KEYWORD_UPPERCASE.toLowerCase();
    private static final String COMMIT_SQL_KEYWORD_LOWERCASE = "commit";
    private static final String INSERT_SQL_KEYWORD_LOWERCASE = "insert";
    private static final String TYPE_KEY = "type";
    private static final String DATABASE_KEY = "database";
    private static final byte[] COLUMN_TYPES = new byte[]{4, -4};
    private static final String GTID_SOURCE_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    private static final String SMITH = "Smith";
    private static final String CRUZ = "Cruz";
    private static final String JONES = "Jones";
    private static final String ONE = "1";
    private static final String TWO = "2";
    private static final String THREE = "3";
    private static final String FOUR = "4";
    private static final String TEN = "10";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CaptureChangeMySQL processor;
    private TestRunner testRunner;
    private MockBinlogClient client;

    @BeforeEach
    public void setUp(@Mock Connection connection) {
        processor = new MockCaptureChangeMySQL(connection);
        testRunner = TestRunners.newTestRunner(processor);
        client = new MockBinlogClient(LOCAL_HOST, DEFAULT_PORT, ROOT_USER, PASSWORD);
    }

    @Test
    public void testSslModeDisabledSslContextServiceNotRequired() {
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        testRunner.setProperty(CaptureChangeMySQL.SSL_MODE, SSLMode.DISABLED.toString());
        testRunner.assertValid();
    }

    @Test
    public void testSslModeRequiredSslContextServiceRequired() {
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        testRunner.setProperty(CaptureChangeMySQL.SSL_MODE, SSLMode.REQUIRED.toString());
        testRunner.assertNotValid();
    }

    @Test
    public void testSslModeRequiredSslContextServiceConfigured(@Mock SSLContextService sslContextService) throws InitializationException {
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        testRunner.setProperty(CaptureChangeMySQL.SSL_MODE, SSLMode.REQUIRED.toString());

        String identifier = SSLContextService.class.getName();
        when(sslContextService.getIdentifier()).thenReturn(identifier);
        testRunner.addControllerService(identifier, sslContextService);
        testRunner.enableControllerService(sslContextService);

        testRunner.setProperty(CaptureChangeMySQL.SSL_CONTEXT_SERVICE, identifier);
        testRunner.assertValid();
    }

    @Test
    public void testSslModeRequiredSslContextServiceConnected(@Mock SSLContextService sslContextService) throws NoSuchAlgorithmException, InitializationException {
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        SSLMode sslMode = SSLMode.REQUIRED;
        testRunner.setProperty(CaptureChangeMySQL.SSL_MODE, sslMode.toString());

        SSLContext sslContext = SSLContext.getDefault();
        String identifier = SSLContextService.class.getName();
        when(sslContextService.getIdentifier()).thenReturn(identifier);
        doReturn(sslContext).when(sslContextService).createContext();

        testRunner.addControllerService(identifier, sslContextService);
        testRunner.enableControllerService(sslContextService);
        testRunner.setProperty(CaptureChangeMySQL.SSL_CONTEXT_SERVICE, identifier);
        testRunner.assertValid();

        testRunner.run();

        assertEquals(sslMode, client.getSSLMode(), "SSL Mode not matched");
        SSLSocketFactory sslSocketFactory = client.getSslSocketFactory();
        assertNotNull(sslSocketFactory, "Binary Log SSLSocketFactory not found");
        assertEquals(BinaryLogSSLSocketFactory.class, sslSocketFactory.getClass(), "Binary Log SSLSocketFactory class not matched");
    }

    @Test
    public void testConnectionFailures() {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, ROOT_USER);
        client.setConnectionError(true);

        AssertionError assertionError = assertThrows(AssertionError.class, () -> testRunner.run());
        assertInstanceOf(ProcessException.class, assertionError.getCause());
        ProcessException processException = (ProcessException) assertionError.getCause();
        assertInstanceOf(IOException.class, processException.getCause());
        IOException ioe = (IOException) processException.getCause();
        assertEquals("Could not connect binlog client to any of the specified hosts due to: Error during connect", ioe.getMessage());
        assertInstanceOf(IOException.class, ioe.getCause());

        client.setConnectionError(false);
        client.setConnectionTimeout(true);

        assertionError = assertThrows(AssertionError.class, () -> testRunner.run());
        assertInstanceOf(ProcessException.class, assertionError.getCause());
        processException = (ProcessException) assertionError.getCause();
        assertInstanceOf(IOException.class, processException.getCause());
        ioe = (IOException) processException.getCause();
        assertEquals("Could not connect binlog client to any of the specified hosts due to: Connection timed out", ioe.getMessage());
        assertInstanceOf(TimeoutException.class, ioe.getCause());

        client.setConnectionTimeout(false);
    }

    @Test
    public void testBeginCommitTransaction() {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, ROOT_USER);
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_BEGIN_COMMIT, Boolean.TRUE.toString());

        testRunner.run(1, false, true);

        // ROTATE
        EventHeaderV4 eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.ROTATE, 2L);
        RotateEventData rotateEventData = EventUtils.buildRotateEventData(INIT_BIN_LOG_FILENAME, 4L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, rotateEventData));

        // BEGIN
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 4L);
        QueryEventData queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        // COMMIT
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 12L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, new XidEventData()));

        testRunner.run(1, true, false);

        List<MockFlowFile> resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS);
        assertEquals(2, resultFiles.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testBeginCommitTransactionFiltered() throws JsonProcessingException {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, ROOT_USER);
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_BEGIN_COMMIT, Boolean.FALSE.toString());
        testRunner.setProperty(CaptureChangeMySQL.INIT_SEQUENCE_ID, TEN);

        testRunner.run(1, false, true);

        // ROTATE
        EventHeaderV4 eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.ROTATE, 2L);
        RotateEventData rotateEventData = EventUtils.buildRotateEventData(INIT_BIN_LOG_FILENAME, 4L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, rotateEventData));

        // BEGIN
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 4L);
        QueryEventData queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.TABLE_MAP, 6L);
        TableMapEventData tableMapEventData = EventUtils.buildTableMapEventData(1L, MY_DB, MY_TABLE, COLUMN_TYPES);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, tableMapEventData));

        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.EXT_WRITE_ROWS, 8L);
        BitSet cols = new BitSet();
        cols.set(1);
        WriteRowsEventData writeRowsEventData = EventUtils.buildWriteRowsEventData(1L, cols,
                Collections.singletonList(new Serializable[]{2, SMITH}));
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, writeRowsEventData));

        // COMMIT
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 12L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        testRunner.run(1, true, false);

        List<MockFlowFile> resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS);
        assertEquals(1, resultFiles.size());
        assertEquals(TEN, resultFiles.get(0).getAttribute(EventWriter.SEQUENCE_ID_KEY));
        // Verify the contents of the event includes the database and table name even though the cache is not configured
        Map<String, Object> json = MAPPER.readValue(resultFiles.get(0).getContent(), Map.class);
        assertEquals(MY_DB, json.get(DATABASE_KEY));
        assertEquals(MY_TABLE, json.get("table_name"));
    }

    @Test
    public void testInitialSequenceIdIgnoredWhenStatePresent() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, ROOT_USER);
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, PASSWORD);
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
        testRunner.setProperty(CaptureChangeMySQL.INIT_SEQUENCE_ID, TEN);
        testRunner.getStateManager().setState(Collections.singletonMap(EventWriter.SEQUENCE_ID_KEY, ONE), Scope.CLUSTER);

        testRunner.run(1, false, true);

        // ROTATE
        EventHeaderV4 eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.ROTATE, 2L);
        RotateEventData rotateEventData = EventUtils.buildRotateEventData(INIT_BIN_LOG_FILENAME, 4L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, rotateEventData));

        // BEGIN
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 4L);
        QueryEventData queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        // COMMIT
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 12L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        testRunner.run(1, true, false);

        List<MockFlowFile> resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS);
        IntStream.range(0, resultFiles.size()).forEach(index -> assertEquals(index + 1L,
                Long.valueOf(resultFiles.get(index).getAttribute(EventWriter.SEQUENCE_ID_KEY))));
    }

    @Test
    public void testInitialSequenceIdNoStatePresent() {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, ROOT_USER);
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, PASSWORD);
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
        testRunner.setProperty(CaptureChangeMySQL.INIT_SEQUENCE_ID, TEN);

        testRunner.run(1, false, true);

        // ROTATE
        EventHeaderV4 eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.ROTATE, 2L);
        RotateEventData rotateEventData = EventUtils.buildRotateEventData(INIT_BIN_LOG_FILENAME, 4L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, rotateEventData));

        // BEGIN
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 4L);
        QueryEventData queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        // COMMIT
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 12L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        testRunner.run(1, true, false);

        List<MockFlowFile> resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS);

        IntStream.range(0, resultFiles.size()).forEach(index -> assertEquals(index + 10L,
                Long.valueOf(resultFiles.get(index).getAttribute(EventWriter.SEQUENCE_ID_KEY))));
    }

    @Test
    public void testCommitWithoutBegin() {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, ROOT_USER);
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, PASSWORD);
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, CONNECT_TIMEOUT);

        testRunner.run(1, false, true);

        // COMMIT
        EventHeaderV4 eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 12L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        // This should not throw an exception, rather warn that a COMMIT event was sent out-of-sync
        assertDoesNotThrow(() -> testRunner.run(1, true, false));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testExtendedTransaction() {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, ROOT_USER);
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, PASSWORD);
        testRunner.setProperty(CaptureChangeMySQL.SERVER_ID, ONE);
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
        testRunner.setProperty(CaptureChangeMySQL.INIT_BINLOG_FILENAME, INIT_BIN_LOG_FILENAME);
        testRunner.setProperty(CaptureChangeMySQL.INIT_BINLOG_POSITION, FOUR);
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_BEGIN_COMMIT, Boolean.TRUE.toString());
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_DDL_EVENTS, Boolean.TRUE.toString());

        testRunner.run(1, false, true);

        // ROTATE scenario
        EventHeaderV4 eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.ROTATE, 2L);
        RotateEventData rotateEventData = EventUtils.buildRotateEventData(INIT_BIN_LOG_FILENAME, 4L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, rotateEventData));

        // INSERT scenario
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 4L);
        QueryEventData queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.TABLE_MAP, 6L);
        TableMapEventData tableMapEventData = EventUtils.buildTableMapEventData(1L, MY_DB, MY_TABLE, COLUMN_TYPES);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, tableMapEventData));

        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.EXT_WRITE_ROWS, 8L);
        BitSet cols = new BitSet();
        cols.set(1);
        WriteRowsEventData writeRowsEventData = EventUtils.buildWriteRowsEventData(1L, cols,
                Arrays.asList(new Serializable[]{2, SMITH}, new Serializable[]{3, JONES}, new Serializable[]{10, CRUZ}));
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, writeRowsEventData));

        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 12L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        // UPDATE scenario
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 16L);
        queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.TABLE_MAP, 18L);
        tableMapEventData = EventUtils.buildTableMapEventData(1L, MY_DB, MY_TABLE, COLUMN_TYPES);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, tableMapEventData));

        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.UPDATE_ROWS, 20L);
        BitSet colsBefore = new BitSet();
        colsBefore.set(0);
        colsBefore.set(1);
        BitSet colsAfter = new BitSet();
        colsAfter.set(1);
        Map<Serializable[], Serializable[]> updateMap = Collections.singletonMap(new Serializable[]{2, SMITH}, new Serializable[]{3, JONES});
        UpdateRowsEventData updateRowsEventData = EventUtils.buildUpdateRowsEventData(1L, colsBefore, colsAfter,
                new ArrayList<>(updateMap.entrySet()));
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, updateRowsEventData));

        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 24L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        // ROTATE scenario
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.ROTATE, 26L);
        rotateEventData = EventUtils.buildRotateEventData(SUBSEQUENT_BIN_LOG_FILENAME, 4L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, rotateEventData));

        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 28L);
        queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.TABLE_MAP, 30L);
        tableMapEventData = EventUtils.buildTableMapEventData(1L, MY_DB, MY_TABLE, COLUMN_TYPES);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, tableMapEventData));

        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 32L);
        queryEventData = EventUtils.buildQueryEventData(MY_DB, "ALTER TABLE myTable add column col1 int");
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        // DELETE scenario
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.DELETE_ROWS, 36L);
        cols = new BitSet();
        cols.set(0);
        cols.set(1);
        DeleteRowsEventData deleteRowsEventData = EventUtils.buildDeleteRowsEventData(1L, cols,
                Arrays.asList(new Serializable[]{2, SMITH}, new Serializable[]{3, JONES}));
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, deleteRowsEventData));

        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 40L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        testRunner.run(1, true, false);

        List<MockFlowFile> resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS);
        List<String> expectedEventTypes = new ArrayList<>();
        expectedEventTypes.add(BEGIN_SQL_KEYWORD_LOWERCASE);
        expectedEventTypes.addAll(Collections.nCopies(3, INSERT_SQL_KEYWORD_LOWERCASE));
        expectedEventTypes.addAll(Arrays.asList(COMMIT_SQL_KEYWORD_LOWERCASE, BEGIN_SQL_KEYWORD_LOWERCASE, "update", COMMIT_SQL_KEYWORD_LOWERCASE, BEGIN_SQL_KEYWORD_LOWERCASE, "ddl"));
        expectedEventTypes.addAll(Collections.nCopies(2, "delete"));
        expectedEventTypes.add(COMMIT_SQL_KEYWORD_LOWERCASE);

        IntStream.range(0, resultFiles.size()).forEach(index -> {
            MockFlowFile resultFile = resultFiles.get(index);
            assertEquals(EventWriter.APPLICATION_JSON, resultFile.getAttribute(CoreAttributes.MIME_TYPE.key()));
            assertEquals(index, Long.valueOf(resultFile.getAttribute(EventWriter.SEQUENCE_ID_KEY)));
            assertEquals(index < 8 ? INIT_BIN_LOG_FILENAME : SUBSEQUENT_BIN_LOG_FILENAME, resultFile.getAttribute(BinlogEventInfo.BINLOG_FILENAME_KEY));
            assertEquals(0L, Long.parseLong(resultFile.getAttribute(BinlogEventInfo.BINLOG_POSITION_KEY)) % 4);
            assertEquals(expectedEventTypes.get(index), resultFile.getAttribute(EventWriter.CDC_EVENT_TYPE_ATTRIBUTE));
            // Check that DDL didn't change
            if (Objects.equals(resultFile.getAttribute(BinlogEventInfo.BINLOG_POSITION_KEY), "32")) {
                String expectedQuery = "ALTER TABLE myTable add column col1 int";
                try {
                    Map<String, Object> result = MAPPER.readValue(testRunner.getContentAsByteArray(resultFile), Map.class);
                    assertEquals(expectedQuery, result.get("query"));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        assertEquals(13, resultFiles.size());
        assertEquals(13, testRunner.getProvenanceEvents().size());
        testRunner.getProvenanceEvents().forEach(event -> assertEquals(ProvenanceEventType.RECEIVE, event.getEventType()));
    }

    @Test
    public void testExcludeSchemaChanges() {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST); // Don't include port here, should default to 3306
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, ROOT_USER);
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, PASSWORD);
        testRunner.setProperty(CaptureChangeMySQL.SERVER_ID, ONE);
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
        testRunner.setProperty(CaptureChangeMySQL.INIT_BINLOG_FILENAME, INIT_BIN_LOG_FILENAME);
        testRunner.setProperty(CaptureChangeMySQL.INIT_BINLOG_POSITION, FOUR);
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_BEGIN_COMMIT, Boolean.TRUE.toString());
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_DDL_EVENTS, Boolean.FALSE.toString());

        testRunner.run(1, false, true);

        // ROTATE scenario
        EventHeaderV4 eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.ROTATE, 2L);
        RotateEventData rotateEventData = EventUtils.buildRotateEventData(INIT_BIN_LOG_FILENAME, 4L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, rotateEventData));

        // INSERT scenario
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 4L);
        QueryEventData queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.TABLE_MAP, 6L);
        TableMapEventData tableMapEventData = EventUtils.buildTableMapEventData(1L, MY_DB, MY_TABLE, COLUMN_TYPES);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, tableMapEventData));

        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.EXT_WRITE_ROWS, 8L);
        BitSet cols = new BitSet();
        cols.set(1);
        WriteRowsEventData writeRowsEventData = EventUtils.buildWriteRowsEventData(1L, cols,
                (Arrays.asList(new Serializable[]{2, SMITH}, new Serializable[]{3, JONES}, new Serializable[]{10, CRUZ})));
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, writeRowsEventData));

        // ALTER TABLE
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 32L);
        queryEventData = EventUtils.buildQueryEventData(MY_DB, "ALTER TABLE myTable add column col1 int");
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 40L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        testRunner.run(1, true, false);

        List<MockFlowFile> resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS);
        // No DDL events expected
        List<String> expectedEventTypes = new ArrayList<>();
        expectedEventTypes.add(BEGIN_SQL_KEYWORD_LOWERCASE);
        expectedEventTypes.addAll(Collections.nCopies(3, INSERT_SQL_KEYWORD_LOWERCASE));
        expectedEventTypes.add(COMMIT_SQL_KEYWORD_LOWERCASE);

        IntStream.range(0, resultFiles.size()).forEach(index -> {
            MockFlowFile resultFile = resultFiles.get(index);
            assertEquals(index, Long.valueOf(resultFile.getAttribute(EventWriter.SEQUENCE_ID_KEY)));
            assertEquals(EventWriter.APPLICATION_JSON, resultFile.getAttribute(CoreAttributes.MIME_TYPE.key()));
            assertEquals((index < 8) ? INIT_BIN_LOG_FILENAME : SUBSEQUENT_BIN_LOG_FILENAME, resultFile.getAttribute(BinlogEventInfo.BINLOG_FILENAME_KEY));
            assertEquals(0L, Long.parseLong(resultFile.getAttribute(BinlogEventInfo.BINLOG_POSITION_KEY)) % 4);
            assertEquals(expectedEventTypes.get(index), resultFile.getAttribute(EventWriter.CDC_EVENT_TYPE_ATTRIBUTE));
        });

        assertEquals(5, resultFiles.size());
    }

    @Test
    public void testNoTableInformationAvailable() {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST); // Port should default to 3306
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, ROOT_USER);
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, PASSWORD);
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, CONNECT_TIMEOUT);

        testRunner.run(1, false, true);

        // ROTATE scenario
        EventHeaderV4 eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.ROTATE, 2L);
        RotateEventData rotateEventData = EventUtils.buildRotateEventData(INIT_BIN_LOG_FILENAME, 4L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, rotateEventData));

        // INSERT scenario
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 4L);
        QueryEventData queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.EXT_WRITE_ROWS, 8L);
        BitSet cols = new BitSet();
        cols.set(1);
        WriteRowsEventData writeRowsEventData = EventUtils.buildWriteRowsEventData(1L, cols,
                Arrays.asList(new Serializable[]{2, SMITH}, new Serializable[]{3, JONES}, new Serializable[]{10, CRUZ}));
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, writeRowsEventData));

        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 12L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        // Should not throw an exception
        assertDoesNotThrow(() -> testRunner.run(1, true, false));
    }

    @Test
    public void testSkipTable() {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, ROOT_USER);
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, PASSWORD);
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
        testRunner.setProperty(CaptureChangeMySQL.DATABASE_NAME_PATTERN, MY_DB);
        testRunner.setProperty(CaptureChangeMySQL.TABLE_NAME_PATTERN, USER_TABLE);
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_BEGIN_COMMIT, Boolean.TRUE.toString());

        testRunner.run(1, false, true);

        // ROTATE
        EventHeaderV4 eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.ROTATE, 2L);
        RotateEventData rotateEventData = EventUtils.buildRotateEventData(INIT_BIN_LOG_FILENAME, 4L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, rotateEventData));

        // BEGIN
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 4L);
        QueryEventData queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        // TABLE MAP for table not matching the regex (note the s on the end of users vs the regex of 'user')
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.TABLE_MAP, 6L);
        TableMapEventData tableMapEventData = EventUtils.buildTableMapEventData(1L, MY_DB, USERS_TABLE, COLUMN_TYPES);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, tableMapEventData));

        // This WRITE ROWS should be skipped
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.EXT_WRITE_ROWS, 8L);
        BitSet cols = new BitSet();
        cols.set(1);
        WriteRowsEventData writeRowsEventData = EventUtils.buildWriteRowsEventData(1L, cols,
                Collections.singletonList(new Serializable[]{2, SMITH}));
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, writeRowsEventData));

        // TABLE MAP for table matching, all modification events (1) should be emitted
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.TABLE_MAP, 10L);
        tableMapEventData = EventUtils.buildTableMapEventData(1L, MY_DB, USER_TABLE, COLUMN_TYPES);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, tableMapEventData));

        // WRITE ROWS for matching table
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.EXT_WRITE_ROWS, 12L);
        cols = new BitSet();
        cols.set(1);
        writeRowsEventData = EventUtils.buildWriteRowsEventData(1L, cols, Collections.singletonList(new Serializable[]{10, CRUZ}));
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, writeRowsEventData));

        // COMMIT
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 14L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        ////////////////////////
        // Test database filter
        ////////////////////////

        // BEGIN
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 4L);
        queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        // TABLE MAP for database not matching the regex
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.TABLE_MAP, 6L);
        tableMapEventData = EventUtils.buildTableMapEventData(1L, MY_DB, NOT_MY_TABLE, COLUMN_TYPES);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, tableMapEventData));

        // This WRITE ROWS should be skipped
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.EXT_WRITE_ROWS, 8L);
        cols = new BitSet();
        cols.set(1);
        writeRowsEventData = EventUtils.buildWriteRowsEventData(1L, cols, Collections.singletonList(new Serializable[]{2, SMITH}));
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, writeRowsEventData));

        // COMMIT
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 14L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        testRunner.run(1, true, false);

        List<MockFlowFile> resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS);
        // BEGIN + WRITE + COMMIT from table matching, BEGIN + COMMIT for database matching
        assertEquals(5, resultFiles.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSkipTableMultipleEventsPerFlowFile() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, ROOT_USER);
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, PASSWORD);
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
        testRunner.setProperty(CaptureChangeMySQL.DATABASE_NAME_PATTERN, MY_DB);
        testRunner.setProperty(CaptureChangeMySQL.TABLE_NAME_PATTERN, USER_TABLE);
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_BEGIN_COMMIT, Boolean.TRUE.toString());
        testRunner.setProperty(CaptureChangeMySQL.NUMBER_OF_EVENTS_PER_FLOWFILE, TWO);

        testRunner.run(1, false, true);

        // ROTATE
        EventHeaderV4 eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.ROTATE, 2L);
        RotateEventData rotateEventData = EventUtils.buildRotateEventData(INIT_BIN_LOG_FILENAME, 4L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, rotateEventData));

        // BEGIN
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 4L);
        QueryEventData queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        // TABLE MAP for table not matching the regex (note the s on the end of users vs the regex of 'user')
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.TABLE_MAP, 6L);
        TableMapEventData tableMapEventData = EventUtils.buildTableMapEventData(1L, MY_DB, USERS_TABLE, COLUMN_TYPES);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, tableMapEventData));

        // This WRITE ROWS should be skipped
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.EXT_WRITE_ROWS, 8L);
        BitSet cols = new BitSet();
        cols.set(1);
        WriteRowsEventData writeRowsEventData = EventUtils.buildWriteRowsEventData(1L, cols, Collections.singletonList(new Serializable[]{2, SMITH}));
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, writeRowsEventData));

        // TABLE MAP for table matching, all modification events (1) should be emitted
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.TABLE_MAP, 10L);
        tableMapEventData = EventUtils.buildTableMapEventData(1L, MY_DB, USER_TABLE, COLUMN_TYPES);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, tableMapEventData));

        // WRITE ROWS for matching table
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.EXT_WRITE_ROWS, 12L);
        cols = new BitSet();
        cols.set(1);
        writeRowsEventData = EventUtils.buildWriteRowsEventData(1L, cols, Collections.singletonList(new Serializable[]{10, CRUZ}));
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, writeRowsEventData));

        // COMMIT
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 14L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        ////////////////////////
        // Test database filter
        ////////////////////////

        // BEGIN
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 4L);
        queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        // TABLE MAP for database not matching the regex
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.TABLE_MAP, 6L);
        tableMapEventData = EventUtils.buildTableMapEventData(1L, MY_DB, NOT_MY_TABLE, COLUMN_TYPES);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, tableMapEventData));

        // This WRITE ROWS should be skipped
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.EXT_WRITE_ROWS, 8L);
        cols = new BitSet();
        cols.set(1);
        writeRowsEventData = EventUtils.buildWriteRowsEventData(1L, cols, Collections.singletonList(new Serializable[]{2, SMITH}));
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, writeRowsEventData));

        // COMMIT
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 14L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        testRunner.run(1, true, false);

        List<MockFlowFile> resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS);
        // Five events total, 2 max per flow file, so 3 flow files
        assertEquals(3, resultFiles.size());

        List<Map<String, Object>> json = MAPPER.readValue(resultFiles.get(0).toByteArray(), List.class);
        assertEquals(2, json.size());
        // BEGIN, INSERT, COMMIT (verifies that one of the INSERTs was skipped)
        assertEquals(BEGIN_SQL_KEYWORD_LOWERCASE, json.get(0).get(TYPE_KEY));
        assertEquals(INSERT_SQL_KEYWORD_LOWERCASE, json.get(1).get(TYPE_KEY));

        json = MAPPER.readValue(resultFiles.get(1).toByteArray(), List.class);
        assertEquals(2, json.size());
        assertEquals(COMMIT_SQL_KEYWORD_LOWERCASE, json.get(0).get(TYPE_KEY));
        assertEquals(BEGIN_SQL_KEYWORD_LOWERCASE, json.get(1).get(TYPE_KEY));

        json = MAPPER.readValue(resultFiles.get(2).toByteArray(), List.class);
        // One event left
        assertEquals(1, json.size());
        assertEquals(COMMIT_SQL_KEYWORD_LOWERCASE, json.get(0).get(TYPE_KEY));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSkipTableOneTransactionPerFlowFile() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, ROOT_USER);
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, PASSWORD);
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
        testRunner.setProperty(CaptureChangeMySQL.DATABASE_NAME_PATTERN, MY_DB);
        testRunner.setProperty(CaptureChangeMySQL.TABLE_NAME_PATTERN, USER_TABLE);
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_BEGIN_COMMIT, Boolean.TRUE.toString());
        testRunner.setProperty(CaptureChangeMySQL.EVENTS_PER_FLOWFILE_STRATEGY, FlowFileEventWriteStrategy.ONE_TRANSACTION_PER_FLOWFILE);

        testRunner.run(1, false, true);

        // ROTATE
        EventHeaderV4 eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.ROTATE, 2L);
        RotateEventData rotateEventData = EventUtils.buildRotateEventData(INIT_BIN_LOG_FILENAME, 4L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, rotateEventData));

        // BEGIN
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 4L);
        QueryEventData queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        // TABLE MAP for table not matching the regex (note the s on the end of users vs the regex of 'user')
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.TABLE_MAP, 6L);
        TableMapEventData tableMapEventData = EventUtils.buildTableMapEventData(1L, MY_DB, USERS_TABLE, COLUMN_TYPES);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, tableMapEventData));

        // This WRITE ROWS should be skipped
        BitSet cols = new BitSet();
        cols.set(1);
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.EXT_WRITE_ROWS, 8L);
        WriteRowsEventData writeRowsEventData = EventUtils.buildWriteRowsEventData(1L, cols, Collections.singletonList(new Serializable[]{2, SMITH}));
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, writeRowsEventData));

        // TABLE MAP for table matching, all modification events (1) should be emitted
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.TABLE_MAP, 10L);
        tableMapEventData = EventUtils.buildTableMapEventData(1L, MY_DB, USER_TABLE, COLUMN_TYPES);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, tableMapEventData));

        // WRITE ROWS for matching table
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.EXT_WRITE_ROWS, 12L);
        cols = new BitSet();
        cols.set(1);
        writeRowsEventData = EventUtils.buildWriteRowsEventData(1L, cols, Collections.singletonList(new Serializable[]{10, CRUZ}));
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, writeRowsEventData));

        // COMMIT
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 14L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        ////////////////////////
        // Test database filter
        ////////////////////////

        // BEGIN
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 4L);
        queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        // TABLE MAP for database not matching the regex
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.TABLE_MAP, 6L);
        tableMapEventData = EventUtils.buildTableMapEventData(1L, MY_DB, NOT_MY_TABLE, COLUMN_TYPES);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, tableMapEventData));

        // This WRITE ROWS should be skipped
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.EXT_WRITE_ROWS, 8L);
        cols = new BitSet();
        cols.set(1);
        writeRowsEventData = EventUtils.buildWriteRowsEventData(1L, cols, Collections.singletonList(new Serializable[]{2, SMITH}));
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, writeRowsEventData));

        // COMMIT
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 14L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        testRunner.run(1, true, false);

        List<MockFlowFile> resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS);
        // Five events total, 3 max per flow file, so 2 flow files
        assertEquals(2, resultFiles.size());
        List<Map<String, Object>> json = MAPPER.readValue(resultFiles.get(0).toByteArray(), List.class);
        assertEquals(3, json.size());
        // BEGIN, INSERT, COMMIT (verifies that one of the INSERTs was skipped)
        assertEquals(BEGIN_SQL_KEYWORD_LOWERCASE, json.get(0).get(TYPE_KEY));
        assertEquals(INSERT_SQL_KEYWORD_LOWERCASE, json.get(1).get(TYPE_KEY));
        assertEquals(COMMIT_SQL_KEYWORD_LOWERCASE, json.get(2).get(TYPE_KEY));

        json = MAPPER.readValue(resultFiles.get(1).toByteArray(), List.class);
        // Only two events left
        assertEquals(2, json.size());
        assertEquals(BEGIN_SQL_KEYWORD_LOWERCASE, json.get(0).get(TYPE_KEY));
        assertEquals(COMMIT_SQL_KEYWORD_LOWERCASE, json.get(1).get(TYPE_KEY));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFilterDatabase() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, ROOT_USER);
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, PASSWORD);
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
        testRunner.setProperty(CaptureChangeMySQL.DATABASE_NAME_PATTERN, MY_DB);
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_BEGIN_COMMIT, Boolean.TRUE.toString());
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_DDL_EVENTS, Boolean.TRUE.toString());

        testRunner.run(1, false, true);

        // ROTATE
        EventHeaderV4 eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.ROTATE, 2L);
        RotateEventData rotateEventData = EventUtils.buildRotateEventData(INIT_BIN_LOG_FILENAME, 4L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, rotateEventData));

        // BEGIN
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 4L);
        QueryEventData queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 32L);
        queryEventData = EventUtils.buildQueryEventData(MY_DB, "ALTER TABLE myTable add column col1 int");
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        // COMMIT
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 14L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        ////////////////////////
        // Test database filter
        ////////////////////////

        String database = "NotMyDB";
        // BEGIN
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 4L);
        queryEventData = EventUtils.buildQueryEventData(database, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 32L);
        queryEventData = EventUtils.buildQueryEventData(database, "ALTER TABLE myTable add column col1 int");
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        // COMMIT
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 14L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        testRunner.run(1, true, false);

        List<MockFlowFile> resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS);
        // First BEGIN + DDL + COMMIT only
        assertEquals(3, resultFiles.size());

        // Check that the database name is set on the objects
        for (MockFlowFile flowFile : resultFiles) {
            Map<String, Object> json = MAPPER.readValue(flowFile.toByteArray(), Map.class);
            assertEquals(MY_DB, json.get(DATABASE_KEY));
        }
    }

    @Test
    public void testTransactionAcrossMultipleProcessorExecutions() {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, ROOT_USER);
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, PASSWORD);
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_BEGIN_COMMIT, Boolean.TRUE.toString());

        testRunner.run(1, false, true);

        // ROTATE
        EventHeaderV4 eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.ROTATE, 2L);
        RotateEventData rotateEventData = EventUtils.buildRotateEventData(INIT_BIN_LOG_FILENAME, 4L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, rotateEventData));

        // BEGIN
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 4L);
        QueryEventData queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        // TABLE MAP
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.TABLE_MAP, 6L);
        TableMapEventData tableMapEventData = EventUtils.buildTableMapEventData(1L, MY_DB, USERS_TABLE, COLUMN_TYPES);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, tableMapEventData));

        // Run and Stop the processor
        testRunner.run(1, true, false);

        List<MockFlowFile> resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS);
        assertEquals(1, resultFiles.size());

        // Re-initialize the processor so it can receive events
        testRunner.run(1, false, true);

        // This WRITE ROWS should be skipped
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.EXT_WRITE_ROWS, 8L);
        BitSet cols = new BitSet();
        cols.set(1);
        WriteRowsEventData writeRowsEventData = EventUtils.buildWriteRowsEventData(1L, cols, Collections.singletonList(new Serializable[]{2, SMITH}));
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, writeRowsEventData));

        // COMMIT
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 14L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        // Run and Stop the processor
        testRunner.run(1, true, false);

        resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS);
        assertEquals(3, resultFiles.size());
    }

    @Test
    public void testUpdateState() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, ROOT_USER);
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, PASSWORD);
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, CONNECT_TIMEOUT);

        testRunner.run(1, false, true);

        // ROTATE
        EventHeaderV4 eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.ROTATE, 2L);
        RotateEventData rotateEventData = EventUtils.buildRotateEventData(INIT_BIN_LOG_FILENAME, 4L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, rotateEventData));

        testRunner.run(1, false, false);

        // Ensure state not set, as the processor hasn't been stopped
        testRunner.getStateManager().assertStateEquals(BinlogEventInfo.BINLOG_FILENAME_KEY, INIT_BIN_LOG_FILENAME, Scope.CLUSTER);
        testRunner.getStateManager().assertStateEquals(BinlogEventInfo.BINLOG_POSITION_KEY, FOUR, Scope.CLUSTER);
        testRunner.getStateManager().assertStateEquals(BinlogEventInfo.BINLOG_GTIDSET_KEY, null, Scope.CLUSTER);

        testRunner.getStateManager().clear(Scope.CLUSTER);

        // Send some events and verify the state was set
        testRunner.run(1, false, true);

        // ROTATE
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.ROTATE, 2L);
        rotateEventData = EventUtils.buildRotateEventData(INIT_BIN_LOG_FILENAME, 4L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, rotateEventData));

        // BEGIN
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 6L);
        QueryEventData queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        testRunner.run(1, false, false);

        testRunner.getStateManager().assertStateEquals(BinlogEventInfo.BINLOG_FILENAME_KEY, INIT_BIN_LOG_FILENAME, Scope.CLUSTER);
        testRunner.getStateManager().assertStateEquals(BinlogEventInfo.BINLOG_POSITION_KEY, "6", Scope.CLUSTER);
        testRunner.getStateManager().assertStateEquals(BinlogEventInfo.BINLOG_GTIDSET_KEY, null, Scope.CLUSTER);

        // COMMIT
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 12L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        testRunner.run(1, true, false);

        testRunner.getStateManager().assertStateEquals(BinlogEventInfo.BINLOG_FILENAME_KEY, INIT_BIN_LOG_FILENAME, Scope.CLUSTER);
        testRunner.getStateManager().assertStateEquals(BinlogEventInfo.BINLOG_POSITION_KEY, "12", Scope.CLUSTER);
        testRunner.getStateManager().assertStateEquals(BinlogEventInfo.BINLOG_GTIDSET_KEY, null, Scope.CLUSTER);
    }

    @Test
    public void testUpdateStateUseGtid() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, ROOT_USER);
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, PASSWORD);
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
        testRunner.setProperty(CaptureChangeMySQL.USE_BINLOG_GTID, Boolean.TRUE.toString());

        testRunner.run(1, false, true);

        // GTID
        EventHeaderV4 eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.GTID, 2L);
        GtidEventData gtidEventData = EventUtils.buildGtidEventData(GTID_SOURCE_ID, ONE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, gtidEventData));

        // BEGIN
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 4L);
        QueryEventData queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        // COMMIT
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 6L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        testRunner.run(1, true, false);

        // Stop the processor and verify the state is set
        testRunner.getStateManager().assertStateEquals(BinlogEventInfo.BINLOG_FILENAME_KEY, "", Scope.CLUSTER);
        testRunner.getStateManager().assertStateEquals(BinlogEventInfo.BINLOG_POSITION_KEY, "6", Scope.CLUSTER);
        testRunner.getStateManager().assertStateEquals(BinlogEventInfo.BINLOG_GTIDSET_KEY, EventUtils.buildGtid(GTID_SOURCE_ID, "1-1"), Scope.CLUSTER);

        ((CaptureChangeMySQL) testRunner.getProcessor()).clearState();
        testRunner.getStateManager().clear(Scope.CLUSTER);

        // Send some events and verify the state was set
        testRunner.run(1, false, true);

        // GTID
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.GTID, 8L);
        gtidEventData = EventUtils.buildGtidEventData(GTID_SOURCE_ID, TWO);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, gtidEventData));


        // BEGIN
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 10L);
        queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        // COMMIT
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 12L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        testRunner.run(1, false, false);

        testRunner.getStateManager().assertStateEquals(BinlogEventInfo.BINLOG_FILENAME_KEY, "", Scope.CLUSTER);
        testRunner.getStateManager().assertStateEquals(BinlogEventInfo.BINLOG_POSITION_KEY, "12", Scope.CLUSTER);
        testRunner.getStateManager().assertStateEquals(BinlogEventInfo.BINLOG_GTIDSET_KEY, EventUtils.buildGtid(GTID_SOURCE_ID, "2-2"), Scope.CLUSTER);

        // GTID
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.GTID, 14L);
        gtidEventData = EventUtils.buildGtidEventData(GTID_SOURCE_ID, THREE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, gtidEventData));

        // BEGIN
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 16L);
        queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        // COMMIT
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 18L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        testRunner.run(1, true, false);

        testRunner.getStateManager().assertStateEquals(BinlogEventInfo.BINLOG_FILENAME_KEY, "", Scope.CLUSTER);
        testRunner.getStateManager().assertStateEquals(BinlogEventInfo.BINLOG_POSITION_KEY, "18", Scope.CLUSTER);
        testRunner.getStateManager().assertStateEquals(BinlogEventInfo.BINLOG_GTIDSET_KEY,
                EventUtils.buildGtid(GTID_SOURCE_ID, "2-3"), Scope.CLUSTER);
    }

    @Test
    public void testDDLOutsideTransaction() {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, ROOT_USER);
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, PASSWORD);
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_DDL_EVENTS, Boolean.TRUE.toString());

        testRunner.run(1, false, true);

        // ROTATE
        EventHeaderV4 eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.ROTATE, 2L);
        RotateEventData rotateEventData = EventUtils.buildRotateEventData(INIT_BIN_LOG_FILENAME, 4L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, rotateEventData));

        // DROP TABLE
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 4L);
        QueryEventData queryEventData = EventUtils.buildQueryEventData(MY_DB, "DROP TABLE myTable");
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        testRunner.run(1, false, false);
        testRunner.assertTransferCount(CaptureChangeMySQL.REL_SUCCESS, 1);
    }

    @Test
    public void testRenameTable() {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, ROOT_USER);
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_DDL_EVENTS, Boolean.TRUE.toString());

        testRunner.run(1, false, true);

        // ROTATE
        EventHeaderV4 eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.ROTATE, 2L);
        RotateEventData rotateEventData = EventUtils.buildRotateEventData(INIT_BIN_LOG_FILENAME, 4L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, rotateEventData));

        // BEGIN
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 4L);
        QueryEventData queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        // RENAME TABLE
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 4L);
        queryEventData = EventUtils.buildQueryEventData(MY_DB, "RENAME TABLE myTable TO myTable2");
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        // COMMIT
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 12L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        testRunner.run(1, true, false);

        List<MockFlowFile> resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS);
        assertEquals(1, resultFiles.size());
    }

    @Test
    public void testInitialGtidIgnoredWhenStatePresent() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, ROOT_USER);
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
        testRunner.setProperty(CaptureChangeMySQL.USE_BINLOG_GTID, Boolean.TRUE.toString());
        testRunner.setProperty(CaptureChangeMySQL.INIT_BINLOG_GTID, EventUtils.buildGtid(GTID_SOURCE_ID, ONE));
        testRunner.setProperty(CaptureChangeMySQL.INIT_SEQUENCE_ID, TEN);
        testRunner.setProperty(CaptureChangeMySQL.RETRIEVE_ALL_RECORDS, Boolean.FALSE.toString());
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_BEGIN_COMMIT, Boolean.TRUE.toString());
        Map<String, String> state = new HashMap<>();
        state.put(BinlogEventInfo.BINLOG_GTIDSET_KEY, EventUtils.buildGtid(GTID_SOURCE_ID, TWO));
        state.put(EventWriter.SEQUENCE_ID_KEY, ONE);
        testRunner.getStateManager().setState(state, Scope.CLUSTER);

        testRunner.run(1, false, true);

        // GTID
        EventHeaderV4 eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.GTID, 2L);
        GtidEventData gtidEventData = EventUtils.buildGtidEventData(GTID_SOURCE_ID, THREE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, gtidEventData));

        // BEGIN
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 4L);
        QueryEventData queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        // COMMIT
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 12L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        testRunner.run(1, true, false);

        List<MockFlowFile> resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS);

        assertEquals(2, resultFiles.size());
        assertEquals(EventUtils.buildGtid(GTID_SOURCE_ID, "2-3"),
                resultFiles.get(resultFiles.size() - 1).getAttribute(BinlogEventInfo.BINLOG_GTIDSET_KEY));
    }

    @Test
    public void testInitialGtidNoStatePresent() {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, ROOT_USER);
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, PASSWORD);
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
        testRunner.setProperty(CaptureChangeMySQL.USE_BINLOG_GTID, Boolean.TRUE.toString());
        testRunner.setProperty(CaptureChangeMySQL.INIT_BINLOG_GTID, EventUtils.buildGtid(GTID_SOURCE_ID, ONE));
        testRunner.setProperty(CaptureChangeMySQL.RETRIEVE_ALL_RECORDS, Boolean.FALSE.toString());
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_BEGIN_COMMIT, Boolean.TRUE.toString());

        testRunner.run(1, false, true);

        // GTID
        EventHeaderV4 eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.GTID, 2L);
        GtidEventData gtidEventData = EventUtils.buildGtidEventData(GTID_SOURCE_ID, THREE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, gtidEventData));

        // BEGIN
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.QUERY, 4L);
        QueryEventData queryEventData = EventUtils.buildQueryEventData(MY_DB, BEGIN_SQL_KEYWORD_UPPERCASE);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4, queryEventData));

        // COMMIT
        eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 12L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        testRunner.run(1, true, false);

        List<MockFlowFile> resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS);

        assertEquals(2, resultFiles.size());
        assertEquals(EventUtils.buildGtid(GTID_SOURCE_ID, "1-1", "3-3"),
                resultFiles.get(resultFiles.size() - 1).getAttribute(BinlogEventInfo.BINLOG_GTIDSET_KEY));
    }

    @Test
    public void testGetXIDEvents() {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, LOCAL_HOST_DEFAULT_PORT);
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, ROOT_USER);
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_BEGIN_COMMIT, Boolean.TRUE.toString());

        testRunner.run(1, false, true);

        // COMMIT
        EventHeaderV4 eventHeaderV4 = EventUtils.buildEventHeaderV4(EventType.XID, 12L);
        client.sendEvent(EventUtils.buildEvent(eventHeaderV4));

        // when we ge a xid event without having got a 'begin' event , don't throw an exception, just warn the user
        assertDoesNotThrow(() -> testRunner.run(1, false, false));
    }

    @Test
    public void testNormalizeQuery() {
        assertEquals("alter table", processor.normalizeQuery(" alter table"));
        assertEquals("alter table", processor.normalizeQuery(" /* This is a \n multiline comment test */ alter table"));
    }

    /********************************
     * Mock and helper classes below
     ********************************/
    @RequiresInstanceClassLoading
    class MockCaptureChangeMySQL extends CaptureChangeMySQL {
        private final Connection mockConnection;

        public MockCaptureChangeMySQL(Connection mockConnection) {
            this.mockConnection = mockConnection;
        }

        Map<TableInfoCacheKey, TableInfo> cache = new HashMap<>();

        @Override
        protected BinaryLogClient createBinlogClient(String hostname, int port, String username, String password) {
            return client;
        }

        @Override
        protected TableInfo loadTableInfo(TableInfoCacheKey key) {
            TableInfo tableInfo = cache.get(key);
            if (tableInfo == null) {
                tableInfo = new TableInfo(key.getDatabaseName(), key.getTableName(), key.getTableId(),
                        Collections.singletonList(new ColumnDefinition((byte) -4, "string1")));
                cache.put(key, tableInfo);
            }
            return tableInfo;
        }

        @Override
        protected void registerDriver(String locationString, String drvName) {
        }

        @Override
        protected Connection getJdbcConnection() {
            return mockConnection;
        }
    }
}