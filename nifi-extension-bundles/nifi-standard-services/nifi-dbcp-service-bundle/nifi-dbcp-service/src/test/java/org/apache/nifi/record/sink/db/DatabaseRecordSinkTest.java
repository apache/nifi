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
package org.apache.nifi.record.sink.db;

import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.dbcp.DBCPConnectionPool;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.dbcp.utils.DBCPProperties;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.ListRecordSet;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.file.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.nifi.dbcp.utils.DBCPProperties.DATABASE_URL;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_DRIVERNAME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_DRIVER_LOCATION;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_PASSWORD;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_USER;
import static org.apache.nifi.dbcp.utils.DBCPProperties.EVICTION_RUN_PERIOD;
import static org.apache.nifi.dbcp.utils.DBCPProperties.KERBEROS_USER_SERVICE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_CONN_LIFETIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_IDLE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_TOTAL_CONNECTIONS;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_WAIT_TIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MIN_EVICTABLE_IDLE_TIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MIN_IDLE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.SOFT_MIN_EVICTABLE_IDLE_TIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.VALIDATION_QUERY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatabaseRecordSinkTest {

    private static final String SERVICE_ID = DBCPConnectionPool.class.getName();
    private static final String DERBY_LOG_PROPERTY = "derby.stream.error.file";
    private static final String DERBY_SHUTDOWN_STATE = "XJ015";
    private static final String DB_LOCATION = "target/db";

    private TestRunner runner;
    private DBCPConnectionPool dbcpService;
    private File databaseDirectory;

    @BeforeAll
    public static void setDerbyLog() {
        final File derbyLog = new File(getSystemTemporaryDirectory(), "derby.log");
        derbyLog.deleteOnExit();
        System.setProperty(DERBY_LOG_PROPERTY, derbyLog.getAbsolutePath());
    }

    @AfterAll
    public static void clearDerbyLog() {
        System.clearProperty(DERBY_LOG_PROPERTY);
    }

    @BeforeEach
    public void setService() throws InitializationException {
        databaseDirectory = getEmptyDirectory();

        dbcpService = new DBCPConnectionPool();
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService(SERVICE_ID, dbcpService);

        final String url = String.format("jdbc:derby:%s;create=true", databaseDirectory);
        runner.setProperty(dbcpService, DBCPProperties.DATABASE_URL, url);
        runner.setProperty(dbcpService, DBCPProperties.DB_USER, String.class.getSimpleName());
        runner.setProperty(dbcpService, DBCPProperties.DB_PASSWORD, String.class.getName());
        runner.setProperty(dbcpService, DBCPProperties.DB_DRIVERNAME, "org.apache.derby.jdbc.EmbeddedDriver");
    }

    @AfterEach
    public void shutdown() throws IOException {
        if (databaseDirectory.exists()) {
            final SQLException exception = assertThrows(SQLException.class, () -> DriverManager.getConnection("jdbc:derby:;shutdown=true"));
            assertEquals(DERBY_SHUTDOWN_STATE, exception.getSQLState());
            FileUtils.deleteFile(databaseDirectory, true);
        }
    }

    private ConfigurationContext context;

    @Test
    void testRecordFormat() throws IOException, InitializationException, SQLException {
        final DatabaseRecordSink task = initTask("TESTTABLE");

        // Create the table
        final Connection con = DriverManager.getConnection(String.format("jdbc:derby:%s;create=true", DB_LOCATION));
        final Statement stmt = con.createStatement();
        try {
            stmt.execute("drop table TESTTABLE");
        } catch (final SQLException ignored) {
            // Ignore, usually due to Derby not having DROP TABLE IF EXISTS
        }
        try {
            stmt.executeUpdate("CREATE TABLE testTable (field1 integer, field2 varchar(20))");
        } finally {
            stmt.close();
        }

        final List<RecordField> recordFields = Arrays.asList(
                new RecordField("field1", RecordFieldType.INT.getDataType()),
                new RecordField("field2", RecordFieldType.STRING.getDataType())
        );
        final RecordSchema recordSchema = new SimpleRecordSchema(recordFields);

        final Map<String, Object> row1 = new HashMap<>();
        row1.put("field1", 15);
        row1.put("field2", "Hello");

        final Map<String, Object> row2 = new HashMap<>();
        row2.put("field1", 6);
        row2.put("field2", "World!");

        final RecordSet recordSet = new ListRecordSet(recordSchema, Arrays.asList(
                new MapRecord(recordSchema, row1),
                new MapRecord(recordSchema, row2)
        ));

        final WriteResult writeResult = task.sendData(recordSet, Collections.singletonMap("a", "Hello"), true);
        assertNotNull(writeResult);
        assertEquals(2, writeResult.getRecordCount());
        assertEquals("Hello", writeResult.getAttributes().get("a"));

        final Statement st = con.createStatement();
        final ResultSet resultSet = st.executeQuery("select * from testTable");
        assertTrue(resultSet.next());

        Object f1 = resultSet.getObject(1);
        assertNotNull(f1);
        assertInstanceOf(Integer.class, f1);
        assertEquals(15, f1);
        Object f2 = resultSet.getObject(2);
        assertNotNull(f2);
        assertInstanceOf(String.class, f2);
        assertEquals("Hello", f2);

        assertTrue(resultSet.next());

        f1 = resultSet.getObject(1);
        assertNotNull(f1);
        assertInstanceOf(Integer.class, f1);
        assertEquals(6, f1);
        f2 = resultSet.getObject(2);
        assertNotNull(f2);
        assertInstanceOf(String.class, f2);
        assertEquals("World!", f2);

        assertFalse(resultSet.next());
    }

    @Test
    void testMissingTable() throws InitializationException {
        final DatabaseRecordSink task = initTask("NO_SUCH_TABLE");

        final List<RecordField> recordFields = Arrays.asList(
                new RecordField("field1", RecordFieldType.INT.getDataType()),
                new RecordField("field2", RecordFieldType.STRING.getDataType())
        );
        final RecordSchema recordSchema = new SimpleRecordSchema(recordFields);

        final Map<String, Object> row1 = new HashMap<>();
        row1.put("field1", 15);
        row1.put("field2", "Hello");

        final RecordSet recordSet = new ListRecordSet(recordSchema, Collections.singletonList(new MapRecord(recordSchema, row1)));
        assertThrows(IOException.class, () -> task.sendData(recordSet, new HashMap<>(), true),
                "Should have generated an exception for table not present");
    }

    @Test
    void testMissingField() throws InitializationException, SQLException {
        final DatabaseRecordSink task = initTask("TESTTABLE");

        // Create the table
        final Connection con = DriverManager.getConnection(String.format("jdbc:derby:%s;create=true", DB_LOCATION));
        final Statement stmt = con.createStatement();
        try {
            stmt.execute("drop table TESTTABLE");
        } catch (final SQLException ignored) {
            // Ignore, usually due to Derby not having DROP TABLE IF EXISTS
        }
        try {
            stmt.executeUpdate("CREATE TABLE testTable (field1 integer, field2 varchar(20) not null)");
        } finally {
            stmt.close();
        }

        final List<RecordField> recordFields = Arrays.asList(
                new RecordField("field1", RecordFieldType.INT.getDataType())
        );
        final RecordSchema recordSchema = new SimpleRecordSchema(recordFields);

        final Map<String, Object> row1 = new HashMap<>();
        row1.put("field1", 15);
        row1.put("field2", "Hello");
        row1.put("field3", "fail");

        final RecordSet recordSet = new ListRecordSet(recordSchema, Collections.singletonList(new MapRecord(recordSchema, row1)));
        assertThrows(IOException.class, () -> task.sendData(recordSet, new HashMap<>(), true),
                "Should have generated an exception for column not present");

    }

    @Test
    void testMissingColumn() throws InitializationException, SQLException {
        final DatabaseRecordSink task = initTask("TESTTABLE");

        // Create the table
        final Connection con = DriverManager.getConnection(String.format("jdbc:derby:%s;create=true", DB_LOCATION));
        final Statement stmt = con.createStatement();
        try {
            stmt.execute("drop table TESTTABLE");
        } catch (final SQLException ignored) {
            // Ignore, usually due to Derby not having DROP TABLE IF EXISTS
        }
        try {
            stmt.executeUpdate("CREATE TABLE testTable (field1 integer, field2 varchar(20))");
        } finally {
            stmt.close();
        }

        final List<RecordField> recordFields = Arrays.asList(
                new RecordField("field1", RecordFieldType.INT.getDataType()),
                new RecordField("field2", RecordFieldType.STRING.getDataType()),
                new RecordField("field3", RecordFieldType.STRING.getDataType())
        );
        final RecordSchema recordSchema = new SimpleRecordSchema(recordFields);

        final Map<String, Object> row1 = new HashMap<>();
        row1.put("field1", 15);

        final RecordSet recordSet = new ListRecordSet(recordSchema, Collections.singletonList(new MapRecord(recordSchema, row1)));
        assertThrows(IOException.class, () -> task.sendData(recordSet, new HashMap<>(), true),
                "Should have generated an exception for field not present");
    }

    private DatabaseRecordSink initTask(String tableName) throws InitializationException {
        final ComponentLog logger = mock(ComponentLog.class);
        final DatabaseRecordSink task = new DatabaseRecordSink();
        context = mock(ConfigurationContext.class);
        final StateManager stateManager = new MockStateManager(task);

        final PropertyValue pValue = mock(StandardPropertyValue.class);
        final MockRecordWriter writer = new MockRecordWriter(null, false); // No header, don"t quote values
        when(context.getProperty(RecordSinkService.RECORD_WRITER_FACTORY)).thenReturn(pValue);
        when(pValue.asControllerService(RecordSetWriterFactory.class)).thenReturn(writer);
        when(context.getProperty(DatabaseRecordSink.CATALOG_NAME)).thenReturn(new MockPropertyValue(null));
        when(context.getProperty(DatabaseRecordSink.SCHEMA_NAME)).thenReturn(new MockPropertyValue(null));
        when(context.getProperty(DatabaseRecordSink.TABLE_NAME)).thenReturn(new MockPropertyValue(tableName != null ? tableName : "TESTTABLE"));
        when(context.getProperty(DatabaseRecordSink.QUOTED_IDENTIFIERS)).thenReturn(new MockPropertyValue("false"));
        when(context.getProperty(DatabaseRecordSink.QUOTED_TABLE_IDENTIFIER)).thenReturn(new MockPropertyValue("true"));
        when(context.getProperty(DatabaseRecordSink.QUERY_TIMEOUT)).thenReturn(new MockPropertyValue("5 sec"));
        when(context.getProperty(DatabaseRecordSink.TRANSLATE_FIELD_NAMES)).thenReturn(new MockPropertyValue("true"));
        when(context.getProperty(DatabaseRecordSink.UNMATCHED_FIELD_BEHAVIOR)).thenReturn(new MockPropertyValue(DatabaseRecordSink.FAIL_UNMATCHED_FIELD.getValue()));
        when(context.getProperty(DatabaseRecordSink.UNMATCHED_COLUMN_BEHAVIOR)).thenReturn(new MockPropertyValue(DatabaseRecordSink.FAIL_UNMATCHED_COLUMN.getValue()));

        // Set up the DBCPService to connect to a temp H2 database
        when(pValue.asControllerService(DBCPService.class)).thenReturn(dbcpService);
        when(context.getProperty(DatabaseRecordSink.DBCP_SERVICE)).thenReturn(pValue);

        final ConfigurationContext dbContext = mock(ConfigurationContext.class);
        final StateManager dbStateManager = new MockStateManager(dbcpService);

        when(dbContext.getProperty(DATABASE_URL)).thenReturn(new MockPropertyValue(String.format("jdbc:derby:%s", DB_LOCATION)));
        when(dbContext.getProperty(DB_USER)).thenReturn(new MockPropertyValue(null));
        when(dbContext.getProperty(DB_PASSWORD)).thenReturn(new MockPropertyValue(null));
        when(dbContext.getProperty(DB_DRIVERNAME)).thenReturn(new MockPropertyValue("org.apache.derby.jdbc.EmbeddedDriver"));
        when(dbContext.getProperty(DB_DRIVER_LOCATION)).thenReturn(new MockPropertyValue(""));
        when(dbContext.getProperty(MAX_TOTAL_CONNECTIONS)).thenReturn(new MockPropertyValue("1"));
        when(dbContext.getProperty(VALIDATION_QUERY)).thenReturn(new MockPropertyValue(""));
        when(dbContext.getProperty(MAX_WAIT_TIME)).thenReturn(new MockPropertyValue("5 sec"));
        when(dbContext.getProperty(MIN_IDLE)).thenReturn(new MockPropertyValue("0"));
        when(dbContext.getProperty(MAX_IDLE)).thenReturn(new MockPropertyValue("0"));
        when(dbContext.getProperty(MAX_CONN_LIFETIME)).thenReturn(new MockPropertyValue("5 sec"));
        when(dbContext.getProperty(EVICTION_RUN_PERIOD)).thenReturn(new MockPropertyValue("5 sec"));
        when(dbContext.getProperty(MIN_EVICTABLE_IDLE_TIME)).thenReturn(new MockPropertyValue("5 sec"));
        when(dbContext.getProperty(SOFT_MIN_EVICTABLE_IDLE_TIME)).thenReturn(new MockPropertyValue("5 sec"));
        when(dbContext.getProperty(KERBEROS_USER_SERVICE)).thenReturn(new MockPropertyValue(null));

        final ControllerServiceInitializationContext dbInitContext = new MockControllerServiceInitializationContext(dbcpService, UUID.randomUUID().toString(), logger, dbStateManager);
        dbcpService.initialize(dbInitContext);
        dbcpService.onConfigured(dbContext);

        final ControllerServiceInitializationContext initContext = new MockControllerServiceInitializationContext(writer, UUID.randomUUID().toString(), logger, stateManager);
        task.initialize(initContext);
        task.onEnabled(context);

        return task;
    }

    private File getEmptyDirectory() {
        final String randomDirectory = String.format("%s-%s", getClass().getSimpleName(), UUID.randomUUID());
        return Paths.get(getSystemTemporaryDirectory(), randomDirectory).toFile();
    }

    private static String getSystemTemporaryDirectory() {
        return System.getProperty("java.io.tmpdir");
    }
}
