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
package org.apache.nifi.lookup.db

import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.dbcp.DBCPService
import org.apache.nifi.lookup.LookupFailureException
import org.apache.nifi.lookup.LookupService
import org.apache.nifi.lookup.TestProcessor
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.reporting.InitializationException
import org.apache.nifi.serialization.record.Record
import org.apache.nifi.serialization.record.RecordFieldType
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test

import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement

import static org.hamcrest.CoreMatchers.instanceOf
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertNull
import static org.junit.Assert.assertThat
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertTrue


class TestPivotDatabaseRecordLookupService {

    private TestRunner runner

    private final static Optional<Record> EMPTY_RECORD = Optional.empty()
    private final static String DB_LOCATION = "target/db"

    @BeforeClass
    static void setupClass() {
        System.setProperty("derby.stream.error.file", "target/derby.log")
    }

    @Before
    void setup() throws InitializationException {
        final DBCPService dbcp = new DBCPServiceSimpleImpl()
        final Map<String, String> dbcpProperties = new HashMap<>()

        runner = TestRunners.newTestRunner(TestProcessor.class)
        runner.addControllerService("dbcp", dbcp, dbcpProperties)
        runner.enableControllerService(dbcp)
    }

    @Test
    void testPivotDatabaseLookupService() throws InitializationException, IOException, LookupFailureException {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION)
        dbLocation.delete()

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).connection
        final Statement stmt = con.createStatement()

        try {
            stmt.execute("drop table TEST")
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST (id integer not null, key_col varchar(10), val_col varchar(10))")
        stmt.execute("insert into TEST (id, key_col, val_col) VALUES (0, 'a', 'One')")

        final PivotDatabaseRecordLookupService service = new PivotDatabaseRecordLookupService()

        runner.addControllerService("db-lookup-service", service)
        runner.setProperty(service, PivotDatabaseRecordLookupService.DBCP_SERVICE, "dbcp")
        runner.assertNotValid()
        runner.setProperty(service, PivotDatabaseRecordLookupService.TABLE_NAME, "TEST")
        runner.setProperty(service, PivotDatabaseRecordLookupService.LOOKUP_KEY_COLUMN, "ID")
        runner.setProperty(service, PivotDatabaseRecordLookupService.LOOKUP_MAP_KEY_COLUMN, "KEY_COL")
        runner.setProperty(service, PivotDatabaseRecordLookupService.LOOKUP_MAP_VALUE_COLUMN, "VAL_COL")
        runner.enableControllerService(service)
        runner.assertValid(service)

        def lookupService = (PivotDatabaseRecordLookupService) runner.processContext.controllerServiceLookup.getControllerService("db-lookup-service")

        assertThat(lookupService, instanceOf(LookupService.class))

        Map<String, String> props = runner.getProcessContext().getAllProperties()

        final Optional<Record> property1 = lookupService.lookup(Collections.singletonMap("key", 0))
        assertEquals("One", ((Map)property1.get().getValue("map")).get("a"))

        assertEquals(property1.get().getSchema().getField(0).getDataType(), RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()))
    }

    @Test
    void testPivotDatabaseLookupServiceSqlStatement() throws InitializationException, IOException, LookupFailureException {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION)
        dbLocation.delete()

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).connection
        final Statement stmt = con.createStatement()

        try {
            stmt.execute("drop table TEST")
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST (id integer not null, key_col varchar(10), val_col varchar(10))")
        stmt.execute("insert into TEST (id, key_col, val_col) VALUES (0, 'a', 'One')")
        stmt.execute("insert into TEST (id, key_col, val_col) VALUES (0, 'b', 'Two')")
        stmt.execute("insert into TEST (id, key_col, val_col) VALUES (1, 'c', 'One')")
        stmt.execute("insert into TEST (id, key_col, val_col) VALUES (1, 'c', 'Two')")

        final PivotDatabaseRecordLookupService service = new PivotDatabaseRecordLookupService()

        runner.addControllerService("db-lookup-service", service)
        runner.setProperty(service, PivotDatabaseRecordLookupService.DBCP_SERVICE, "dbcp")
        runner.assertNotValid()
        runner.setProperty(service, PivotDatabaseRecordLookupService.SQL_STATEMENT, "SELECT key_col, val_col FROM TEST WHERE id = ?")
        runner.setProperty(service, PivotDatabaseRecordLookupService.LOOKUP_MAP_KEY_COLUMN, "KEY_COL")
        runner.setProperty(service, PivotDatabaseRecordLookupService.LOOKUP_MAP_VALUE_COLUMN, "VAL_COL")
        runner.enableControllerService(service)
        runner.assertValid(service)

        def lookupService = (PivotDatabaseRecordLookupService) runner.processContext.controllerServiceLookup.getControllerService("db-lookup-service")

        assertThat(lookupService, instanceOf(LookupService.class))

        final Optional<Record> property1 = lookupService.lookup(Collections.singletonMap("key", 0))
        assertEquals("One", ((Map)property1.get().getValue("map")).get("a"))
        assertEquals("Two", ((Map)property1.get().getValue("map")).get("b"))
        assertNull(((Map)property1.get().getValue("map")).get("c"))

        // Expect LookupFailureException because of duplicate rows
        try {
            final Optional<Record> property3 = lookupService.lookup(Collections.singletonMap("key", 1))
            assertFalse(true)
        } catch (LookupFailureException e) {
            assertTrue(true)
        }
    }

    @Test
    void exerciseCacheLogic() throws InitializationException, IOException, LookupFailureException {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION)
        dbLocation.delete()

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).connection
        final Statement stmt = con.createStatement()

        try {
            stmt.execute("drop table TEST")
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST (id integer not null, key_col varchar(10), val_col varchar(10))")
        stmt.execute("insert into TEST (id, key_col, val_col) VALUES (0, 'a', 'One')")

        final PivotDatabaseRecordLookupService service = new PivotDatabaseRecordLookupService()

        runner.addControllerService("db-lookup-service", service)
        runner.setProperty(service, PivotDatabaseRecordLookupService.DBCP_SERVICE, "dbcp")
        runner.assertNotValid()
        runner.setProperty(service, PivotDatabaseRecordLookupService.SQL_STATEMENT, "SELECT key_col, val_col FROM TEST WHERE id = ?")
        runner.setProperty(service, PivotDatabaseRecordLookupService.LOOKUP_MAP_KEY_COLUMN, "KEY_COL")
        runner.setProperty(service, PivotDatabaseRecordLookupService.LOOKUP_MAP_VALUE_COLUMN, "VAL_COL")
        runner.setProperty(service, PivotDatabaseRecordLookupService.CACHE_SIZE, "10")
        runner.enableControllerService(service)
        runner.assertValid(service)

        def lookupService = (PivotDatabaseRecordLookupService) runner.processContext.controllerServiceLookup.getControllerService("db-lookup-service")

        assertThat(lookupService, instanceOf(LookupService.class))

        final Optional<Record> property1 = lookupService.lookup(Collections.singletonMap("key", 0))
        assertEquals("One", ((Map)property1.get().getValue("map")).get("a"))

        // Delete Row from Table
        stmt.execute("delete from TEST")

        // Cache lookup test
        final Optional<Record> property2 = lookupService.lookup(Collections.singletonMap("key", 0))
        assertEquals("One", ((Map)property2.get().getValue("map")).get("a"))
    }

    @Test
    void testNumericDataType() throws InitializationException, IOException, LookupFailureException {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION)
        dbLocation.delete()

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).connection
        final Statement stmt = con.createStatement()

        try {
            stmt.execute("drop table TEST")
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST (id integer not null, key_col varchar(10), val_col double)")
        stmt.execute("insert into TEST (id, key_col, val_col) VALUES (0, 'a', 1.1)")

        final PivotDatabaseRecordLookupService service = new PivotDatabaseRecordLookupService()

        runner.addControllerService("db-lookup-service", service)
        runner.setProperty(service, PivotDatabaseRecordLookupService.DBCP_SERVICE, "dbcp")
        runner.assertNotValid()
        runner.setProperty(service, PivotDatabaseRecordLookupService.SQL_STATEMENT, "SELECT key_col, val_col FROM TEST WHERE id = ?")
        runner.setProperty(service, PivotDatabaseRecordLookupService.LOOKUP_MAP_KEY_COLUMN, "KEY_COL")
        runner.setProperty(service, PivotDatabaseRecordLookupService.LOOKUP_MAP_VALUE_COLUMN, "VAL_COL")
        runner.setProperty(service, PivotDatabaseRecordLookupService.MAP_FIELD_NAME, "myMap")
        runner.enableControllerService(service)
        runner.assertValid(service)

        def lookupService = (PivotDatabaseRecordLookupService) runner.processContext.controllerServiceLookup.getControllerService("db-lookup-service")

        assertThat(lookupService, instanceOf(LookupService.class))

        final Optional<Record> property1 = lookupService.lookup(Collections.singletonMap("key", 0))
        assertEquals(1.1d, ((Map<Object, Double>)property1.get().getValue("myMap")).get("a"), 0.0d)

        assertEquals(property1.get().getSchema().getField(0).getDataType(), RecordFieldType.MAP.getMapDataType(RecordFieldType.DOUBLE.getDataType()))
    }

    /**
     * Simple implementation for component testing.
     *
     */
    class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {

        @Override
        String getIdentifier() {
            "dbcp"
        }

        @Override
        Connection getConnection() throws ProcessException {
            try {
                Class.forName("org.apache.derby.jdbc.EmbeddedDriver")
                DriverManager.getConnection("jdbc:derby:${DB_LOCATION};create=true")
            } catch (e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }
}