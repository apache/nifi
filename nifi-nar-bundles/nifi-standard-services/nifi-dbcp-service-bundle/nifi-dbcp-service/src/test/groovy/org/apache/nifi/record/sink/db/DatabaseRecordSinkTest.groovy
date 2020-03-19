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
package org.apache.nifi.record.sink.db

import org.apache.nifi.attribute.expression.language.StandardPropertyValue
import org.apache.nifi.components.PropertyValue
import org.apache.nifi.components.state.StateManager
import org.apache.nifi.controller.ConfigurationContext
import org.apache.nifi.controller.ControllerServiceInitializationContext
import org.apache.nifi.dbcp.DBCPConnectionPool
import org.apache.nifi.dbcp.DBCPService
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.record.sink.RecordSinkService
import org.apache.nifi.reporting.InitializationException
import org.apache.nifi.serialization.RecordSetWriterFactory
import org.apache.nifi.serialization.SimpleRecordSchema
import org.apache.nifi.serialization.WriteResult
import org.apache.nifi.serialization.record.ListRecordSet
import org.apache.nifi.serialization.record.MapRecord
import org.apache.nifi.serialization.record.MockRecordWriter
import org.apache.nifi.serialization.record.RecordField
import org.apache.nifi.serialization.record.RecordFieldType
import org.apache.nifi.serialization.record.RecordSchema
import org.apache.nifi.serialization.record.RecordSet
import org.apache.nifi.state.MockStateManager
import org.apache.nifi.util.MockControllerServiceInitializationContext
import org.apache.nifi.util.MockPropertyValue
import org.apache.nifi.util.file.FileUtils
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test

import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.SQLNonTransientConnectionException
import java.sql.Statement

import static org.apache.nifi.dbcp.DBCPConnectionPool.DATABASE_URL
import static org.apache.nifi.dbcp.DBCPConnectionPool.DB_DRIVERNAME
import static org.apache.nifi.dbcp.DBCPConnectionPool.DB_DRIVER_LOCATION
import static org.apache.nifi.dbcp.DBCPConnectionPool.DB_PASSWORD
import static org.apache.nifi.dbcp.DBCPConnectionPool.DB_USER
import static org.apache.nifi.dbcp.DBCPConnectionPool.EVICTION_RUN_PERIOD
import static org.apache.nifi.dbcp.DBCPConnectionPool.KERBEROS_CREDENTIALS_SERVICE
import static org.apache.nifi.dbcp.DBCPConnectionPool.KERBEROS_PASSWORD
import static org.apache.nifi.dbcp.DBCPConnectionPool.KERBEROS_PRINCIPAL
import static org.apache.nifi.dbcp.DBCPConnectionPool.MAX_CONN_LIFETIME
import static org.apache.nifi.dbcp.DBCPConnectionPool.MAX_IDLE
import static org.apache.nifi.dbcp.DBCPConnectionPool.MAX_TOTAL_CONNECTIONS
import static org.apache.nifi.dbcp.DBCPConnectionPool.MAX_WAIT_TIME
import static org.apache.nifi.dbcp.DBCPConnectionPool.MIN_EVICTABLE_IDLE_TIME
import static org.apache.nifi.dbcp.DBCPConnectionPool.MIN_IDLE
import static org.apache.nifi.dbcp.DBCPConnectionPool.SOFT_MIN_EVICTABLE_IDLE_TIME
import static org.apache.nifi.dbcp.DBCPConnectionPool.VALIDATION_QUERY
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertTrue
import static org.junit.Assert.fail
import static org.mockito.Mockito.mock
import static org.mockito.Mockito.when


class DatabaseRecordSinkTest {

    final static String DB_LOCATION = "target/db"

    DBCPService dbcpService

    @BeforeClass
    static void setup() {
        System.setProperty("derby.stream.error.file", "target/derby.log")
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

    private ConfigurationContext context

    @Test
    void testRecordFormat() throws IOException, InitializationException {
        DatabaseRecordSink task = initTask('TESTTABLE')

        // Create the table
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver")
        def con = DriverManager.getConnection("jdbc:derby:${DB_LOCATION};create=true")
        final Statement stmt = con.createStatement()
        try {
            stmt.execute("drop table TESTTABLE");
        } catch (final SQLException sqle) {
            // Ignore, usually due to Derby not having DROP TABLE IF EXISTS
        }
        try {
            stmt.executeUpdate('CREATE TABLE testTable (field1 integer, field2 varchar(20))')
        } finally {
            stmt.close()
        }

        List<RecordField> recordFields = Arrays.asList(
                new RecordField("field1", RecordFieldType.INT.getDataType()),
                new RecordField("field2", RecordFieldType.STRING.getDataType())
        )
        RecordSchema recordSchema = new SimpleRecordSchema(recordFields)

        Map<String, Object> row1 = new HashMap<>()
        row1.put('field1', 15)
        row1.put('field2', 'Hello')

        Map<String, Object> row2 = new HashMap<>()
        row2.put('field1', 6)
        row2.put('field2', 'World!')

        RecordSet recordSet = new ListRecordSet(recordSchema, Arrays.asList(
                new MapRecord(recordSchema, row1),
                new MapRecord(recordSchema, row2)
        ))

        WriteResult writeResult = task.sendData(recordSet, ['a': 'Hello'], true)
        assertNotNull(writeResult)
        assertEquals(2, writeResult.recordCount)
        assertEquals('Hello', writeResult.attributes['a'])

        final Statement st = con.createStatement()
        final ResultSet resultSet = st.executeQuery('select * from testTable')
        assertTrue(resultSet.next())

        def f1 = resultSet.getObject(1)
        assertNotNull(f1)
        assertTrue(f1 instanceof Integer)
        assertEquals(15, f1)
        def f2 = resultSet.getObject(2)
        assertNotNull(f2)
        assertTrue(f2 instanceof String)
        assertEquals('Hello', f2)

        assertTrue(resultSet.next())

        f1 = resultSet.getObject(1)
        assertNotNull(f1)
        assertTrue(f1 instanceof Integer)
        assertEquals(6, f1)
        f2 = resultSet.getObject(2)
        assertNotNull(f2)
        assertTrue(f2 instanceof String)
        assertEquals('World!', f2)

        assertFalse(resultSet.next())
    }

    @Test(expected = IOException.class)
    void testMissingTable() throws IOException, InitializationException {
        DatabaseRecordSink task = initTask('NO_SUCH_TABLE')

        List<RecordField> recordFields = Arrays.asList(
                new RecordField("field1", RecordFieldType.INT.getDataType()),
                new RecordField("field2", RecordFieldType.STRING.getDataType())
        )
        RecordSchema recordSchema = new SimpleRecordSchema(recordFields)

        Map<String, Object> row1 = new HashMap<>()
        row1.put('field1', 15)
        row1.put('field2', 'Hello')

        RecordSet recordSet = new ListRecordSet(recordSchema, Collections.singletonList(new MapRecord(recordSchema, row1)))
        task.sendData(recordSet, new HashMap<>(), true)
        fail('Should have generated an exception for table not present')
    }

    @Test(expected = IOException.class)
    void testMissingField() throws IOException, InitializationException {
        DatabaseRecordSink task = initTask('TESTTABLE')

        // Create the table
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver")
        def con = DriverManager.getConnection("jdbc:derby:${DB_LOCATION};create=true")
        final Statement stmt = con.createStatement()
        try {
            stmt.execute("drop table TESTTABLE");
        } catch (final SQLException sqle) {
            // Ignore, usually due to Derby not having DROP TABLE IF EXISTS
        }
        try {
            stmt.executeUpdate('CREATE TABLE testTable (field1 integer, field2 varchar(20) not null)')
        } finally {
            stmt.close()
        }

        List<RecordField> recordFields = Arrays.asList(
                new RecordField("field1", RecordFieldType.INT.getDataType())
        )
        RecordSchema recordSchema = new SimpleRecordSchema(recordFields)

        Map<String, Object> row1 = new HashMap<>()
        row1.put('field1', 15)
        row1.put('field2', 'Hello')
        row1.put('field3', 'fail')

        RecordSet recordSet = new ListRecordSet(recordSchema, Collections.singletonList(new MapRecord(recordSchema, row1)))
        task.sendData(recordSet, new HashMap<>(), true)
        fail('Should have generated an exception for column not present')
    }

    @Test(expected = IOException.class)
    void testMissingColumn() throws IOException, InitializationException {
        DatabaseRecordSink task = initTask('TESTTABLE')

        // Create the table
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver")
        def con = DriverManager.getConnection("jdbc:derby:${DB_LOCATION};create=true")
        final Statement stmt = con.createStatement()
        try {
            stmt.execute("drop table TESTTABLE");
        } catch (final SQLException sqle) {
            // Ignore, usually due to Derby not having DROP TABLE IF EXISTS
        }
        try {
            stmt.executeUpdate('CREATE TABLE testTable (field1 integer, field2 varchar(20))')
        } finally {
            stmt.close()
        }

        List<RecordField> recordFields = Arrays.asList(
                new RecordField("field1", RecordFieldType.INT.getDataType()),
                new RecordField("field2", RecordFieldType.STRING.getDataType()),
                new RecordField("field3", RecordFieldType.STRING.getDataType())
        )
        RecordSchema recordSchema = new SimpleRecordSchema(recordFields)

        Map<String, Object> row1 = new HashMap<>()
        row1.put('field1', 15)

        RecordSet recordSet = new ListRecordSet(recordSchema, Collections.singletonList(new MapRecord(recordSchema, row1)))
        task.sendData(recordSet, new HashMap<>(), true)
        fail('Should have generated an exception for field not present')
    }

    DatabaseRecordSink initTask(String tableName) throws InitializationException, IOException {

        final ComponentLog logger = mock(ComponentLog.class)
        final DatabaseRecordSink task = new DatabaseRecordSink()
        context = mock(ConfigurationContext.class)
        final StateManager stateManager = new MockStateManager(task)

        final PropertyValue pValue = mock(StandardPropertyValue.class)
        final MockRecordWriter writer = new MockRecordWriter(null, false) // No header, don't quote values
        when(context.getProperty(RecordSinkService.RECORD_WRITER_FACTORY)).thenReturn(pValue)
        when(pValue.asControllerService(RecordSetWriterFactory.class)).thenReturn(writer)
        when(context.getProperty(DatabaseRecordSink.CATALOG_NAME)).thenReturn(new MockPropertyValue(null))
        when(context.getProperty(DatabaseRecordSink.SCHEMA_NAME)).thenReturn(new MockPropertyValue(null))
        when(context.getProperty(DatabaseRecordSink.TABLE_NAME)).thenReturn(new MockPropertyValue(tableName ?: 'TESTTABLE'))
        when(context.getProperty(DatabaseRecordSink.QUOTED_IDENTIFIERS)).thenReturn(new MockPropertyValue('false'))
        when(context.getProperty(DatabaseRecordSink.QUOTED_TABLE_IDENTIFIER)).thenReturn(new MockPropertyValue('true'))
        when(context.getProperty(DatabaseRecordSink.QUERY_TIMEOUT)).thenReturn(new MockPropertyValue('5 sec'))
        when(context.getProperty(DatabaseRecordSink.TRANSLATE_FIELD_NAMES)).thenReturn(new MockPropertyValue('true'))
        when(context.getProperty(DatabaseRecordSink.UNMATCHED_FIELD_BEHAVIOR)).thenReturn(new MockPropertyValue(DatabaseRecordSink.FAIL_UNMATCHED_FIELD.value))
        when(context.getProperty(DatabaseRecordSink.UNMATCHED_COLUMN_BEHAVIOR)).thenReturn(new MockPropertyValue(DatabaseRecordSink.FAIL_UNMATCHED_COLUMN.value))

        // Set up the DBCPService to connect to a temp H2 database
        dbcpService = new DBCPConnectionPool()
        when(pValue.asControllerService(DBCPService.class)).thenReturn(dbcpService)
        when(context.getProperty(DatabaseRecordSink.DBCP_SERVICE)).thenReturn(pValue)

        final ConfigurationContext dbContext = mock(ConfigurationContext.class)
        final StateManager dbStateManager = new MockStateManager(dbcpService)

        when(dbContext.getProperty(DATABASE_URL)).thenReturn(new MockPropertyValue("jdbc:derby:${DB_LOCATION}"))
        when(dbContext.getProperty(DB_USER)).thenReturn(new MockPropertyValue(null))
        when(dbContext.getProperty(DB_PASSWORD)).thenReturn(new MockPropertyValue(null))
        when(dbContext.getProperty(DB_DRIVERNAME)).thenReturn(new MockPropertyValue('org.apache.derby.jdbc.EmbeddedDriver'))
        when(dbContext.getProperty(DB_DRIVER_LOCATION)).thenReturn(new MockPropertyValue(''))
        when(dbContext.getProperty(MAX_TOTAL_CONNECTIONS)).thenReturn(new MockPropertyValue('1'))
        when(dbContext.getProperty(VALIDATION_QUERY)).thenReturn(new MockPropertyValue(''))
        when(dbContext.getProperty(MAX_WAIT_TIME)).thenReturn(new MockPropertyValue('5 sec'))
        when(dbContext.getProperty(MIN_IDLE)).thenReturn(new MockPropertyValue('0'))
        when(dbContext.getProperty(MAX_IDLE)).thenReturn(new MockPropertyValue('0'))
        when(dbContext.getProperty(MAX_CONN_LIFETIME)).thenReturn(new MockPropertyValue('5 sec'))
        when(dbContext.getProperty(EVICTION_RUN_PERIOD)).thenReturn(new MockPropertyValue('5 sec'))
        when(dbContext.getProperty(MIN_EVICTABLE_IDLE_TIME)).thenReturn(new MockPropertyValue('5 sec'))
        when(dbContext.getProperty(SOFT_MIN_EVICTABLE_IDLE_TIME)).thenReturn(new MockPropertyValue('5 sec'))
        when(dbContext.getProperty(KERBEROS_CREDENTIALS_SERVICE)).thenReturn(new MockPropertyValue(null))
        when(dbContext.getProperty(KERBEROS_PRINCIPAL)).thenReturn(new MockPropertyValue(null))
        when(dbContext.getProperty(KERBEROS_PASSWORD)).thenReturn(new MockPropertyValue(null))

        final ControllerServiceInitializationContext dbInitContext = new MockControllerServiceInitializationContext(dbcpService, UUID.randomUUID().toString(), logger, dbStateManager)
        dbcpService.initialize(dbInitContext)
        dbcpService.onConfigured(dbContext)

        final ControllerServiceInitializationContext initContext = new MockControllerServiceInitializationContext(writer, UUID.randomUUID().toString(), logger, stateManager)
        task.initialize(initContext)
        task.onEnabled(context)

        return task
    }
}