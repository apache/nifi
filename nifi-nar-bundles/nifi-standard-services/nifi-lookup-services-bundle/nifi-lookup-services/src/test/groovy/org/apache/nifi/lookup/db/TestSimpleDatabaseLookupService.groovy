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
import static org.junit.Assert.*

class TestSimpleDatabaseLookupService {

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
    void testDatabaseLookupService() throws InitializationException, IOException, LookupFailureException {
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

        stmt.execute("create table TEST (id integer not null, val1 integer, val2 varchar(10), constraint my_pk primary key (id))")
        stmt.execute("insert into TEST (id, val1, val2) VALUES (0, NULL, 'Hello')")
        stmt.execute("insert into TEST (id, val1, val2) VALUES (1, 1, 'World')")

        final SimpleDatabaseLookupService service = new SimpleDatabaseLookupService()

        runner.addControllerService("db-lookup-service", service)
        runner.setProperty(service, SimpleDatabaseLookupService.DBCP_SERVICE, "dbcp")
        runner.assertNotValid()
        runner.setProperty(service, SimpleDatabaseLookupService.TABLE_NAME, "TEST")
        runner.setProperty(service, SimpleDatabaseLookupService.LOOKUP_KEY_COLUMN, "id")
        runner.setProperty(service, SimpleDatabaseLookupService.LOOKUP_VALUE_COLUMN, "VAL1")
        runner.enableControllerService(service)
        runner.assertValid(service)

        def lookupService = (SimpleDatabaseLookupService) runner.processContext.controllerServiceLookup.getControllerService("db-lookup-service")

        assertThat(lookupService, instanceOf(LookupService.class))

        // Lookup VAL1
        final Optional<String> property1 = lookupService.lookup(Collections.singletonMap("key", "0"))
        assertFalse(property1.isPresent())
        // Key not found
        final Optional<String> property3 = lookupService.lookup(Collections.singletonMap("key", "2"))
        assertEquals(EMPTY_RECORD, property3)

        runner.disableControllerService(service)
        runner.setProperty(service, SimpleDatabaseLookupService.LOOKUP_VALUE_COLUMN, "VAL2")
        runner.enableControllerService(service)
        final Optional<String> property2 = lookupService.lookup(Collections.singletonMap("key", "1"))
        assertEquals("World", property2.get())
    }

    @Test
    void exerciseCacheLogic() {
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

        stmt.execute("create table TEST (id integer not null, val1 integer, val2 varchar(10), constraint my_pk primary key (id))")
        stmt.execute("insert into TEST (id, val1, val2) VALUES (0, NULL, 'Hello')")
        stmt.execute("insert into TEST (id, val1, val2) VALUES (1, 1, 'World')")

        final SimpleDatabaseLookupService service = new SimpleDatabaseLookupService()

        runner.addControllerService("db-lookup-service", service)
        runner.setProperty(service, SimpleDatabaseLookupService.DBCP_SERVICE, "dbcp")
        runner.assertNotValid()
        runner.setProperty(service, SimpleDatabaseLookupService.TABLE_NAME, "TEST")
        runner.setProperty(service, SimpleDatabaseLookupService.LOOKUP_KEY_COLUMN, "id")
        runner.setProperty(service, SimpleDatabaseLookupService.CACHE_SIZE, "10")
        runner.setProperty(service, SimpleDatabaseLookupService.LOOKUP_VALUE_COLUMN, "VAL1")
        runner.enableControllerService(service)
        runner.assertValid(service)

        def lookupService = (SimpleDatabaseLookupService) runner.processContext.controllerServiceLookup.getControllerService("db-lookup-service")

        assertThat(lookupService, instanceOf(LookupService.class))

        // Lookup VAL1
        final Optional<String> property1 = lookupService.lookup(Collections.singletonMap("key", "1"))
        assertEquals("1", property1.get())
        final Optional<String> property3 = lookupService.lookup(Collections.singletonMap("key", "0"))
        assertFalse(property3.isPresent())


        runner.disableControllerService(service)
        runner.setProperty(service, SimpleDatabaseLookupService.LOOKUP_VALUE_COLUMN, "VAL2")
        runner.enableControllerService(service)
        final Optional<String> property2 = lookupService.lookup(Collections.singletonMap("key", "1"))
        assertEquals("World", property2.get())

        final Optional<String> property4 = lookupService.lookup(Collections.singletonMap("key", "0"))
        assertEquals("Hello", property4.get())
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