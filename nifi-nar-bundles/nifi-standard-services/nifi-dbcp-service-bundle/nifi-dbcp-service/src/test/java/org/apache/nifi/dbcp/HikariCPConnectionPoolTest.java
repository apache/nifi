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
package org.apache.nifi.dbcp;

import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HikariCPConnectionPoolTest {
    private final static String DB_LOCATION = "target/db";

    @BeforeClass
    public static void setupBeforeClass() {
        System.setProperty("derby.stream.error.file", "target/derby.log");
    }

    @Before
    public void setup() {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();
    }

    /**
     * Missing property values.
     */
    @Test
    public void testMissingPropertyValues() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        final Map<String, String> properties = new HashMap<>();
        runner.addControllerService("test-bad1", service, properties);
        runner.assertNotValid(service);
    }

    /**
     * Max wait set to -1
     */
    @Test
    public void testMaxWait() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService("test-good1", service);

        // set embedded Derby database connection url
        runner.setProperty(service, HikariCPConnectionPool.DATABASE_URL, "jdbc:derby:" + DB_LOCATION + ";create=true");
        runner.setProperty(service, HikariCPConnectionPool.DB_USER, "tester");
        runner.setProperty(service, HikariCPConnectionPool.DB_PASSWORD, "testerp");
        runner.setProperty(service, HikariCPConnectionPool.DB_DRIVERNAME, "org.apache.derby.jdbc.EmbeddedDriver");
        runner.setProperty(service, HikariCPConnectionPool.MAX_WAIT_TIME, "0 millis");

        runner.enableControllerService(service);
        runner.assertValid(service);
    }

    /**
     * Checks validity of idle limit and time settings including a default
     */
    @Test
    public void testIdleConnectionsSettings() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService("test-good1", service);

        // set embedded Derby database connection url
        runner.setProperty(service, HikariCPConnectionPool.DATABASE_URL, "jdbc:derby:" + DB_LOCATION + ";create=true");
        runner.setProperty(service, HikariCPConnectionPool.DB_USER, "tester");
        runner.setProperty(service, HikariCPConnectionPool.DB_PASSWORD, "testerp");
        runner.setProperty(service, HikariCPConnectionPool.DB_DRIVERNAME, "org.apache.derby.jdbc.EmbeddedDriver");
        runner.setProperty(service, HikariCPConnectionPool.MAX_WAIT_TIME, "0 millis");
        runner.setProperty(service, HikariCPConnectionPool.MAX_CONN_LIFETIME, "1 secs");

        runner.enableControllerService(service);
        runner.assertValid(service);
    }

    @Test
    public void testMinIdleCannotBeNegative() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService("test-good1", service);

        // set embedded Derby database connection url
        runner.setProperty(service, HikariCPConnectionPool.DATABASE_URL, "jdbc:derby:" + DB_LOCATION + ";create=true");
        runner.setProperty(service, HikariCPConnectionPool.DB_USER, "tester");
        runner.setProperty(service, HikariCPConnectionPool.DB_PASSWORD, "testerp");
        runner.setProperty(service, HikariCPConnectionPool.DB_DRIVERNAME, "org.apache.derby.jdbc.EmbeddedDriver");
        runner.setProperty(service, HikariCPConnectionPool.MAX_WAIT_TIME, "0 millis");
        runner.setProperty(service, HikariCPConnectionPool.MIN_IDLE, "-1");

        runner.assertNotValid(service);
    }

    /**
     * Checks to ensure that settings have been passed down into the HikariCP
     */
    @Test
    public void testIdleSettingsAreSet() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService("test-good1", service);

        // set embedded Derby database connection url
        runner.setProperty(service, HikariCPConnectionPool.DATABASE_URL, "jdbc:derby:" + DB_LOCATION + ";create=true");
        runner.setProperty(service, HikariCPConnectionPool.DB_USER, "tester");
        runner.setProperty(service, HikariCPConnectionPool.DB_PASSWORD, "testerp");
        runner.setProperty(service, HikariCPConnectionPool.DB_DRIVERNAME, "org.apache.derby.jdbc.EmbeddedDriver");
        runner.setProperty(service, HikariCPConnectionPool.MAX_WAIT_TIME, "0 millis");
        runner.setProperty(service, HikariCPConnectionPool.MIN_IDLE, "4");
        runner.setProperty(service, HikariCPConnectionPool.MAX_CONN_LIFETIME, "1 secs");

        runner.enableControllerService(service);

        Assert.assertEquals(4, service.getDataSource().getMinimumIdle());
        Assert.assertEquals(1000, service.getDataSource().getMaxLifetime());

        service.getDataSource().close();
    }

    /**
     * Test database connection using Derby. Connect, create table, insert, select, drop table.
     */
    @Test
    public void testCreateInsertSelect() throws InitializationException, SQLException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService("test-good1", service);

        // set embedded Derby database connection url
        runner.setProperty(service, HikariCPConnectionPool.DATABASE_URL, "jdbc:derby:" + DB_LOCATION + ";create=true");
        runner.setProperty(service, HikariCPConnectionPool.DB_USER, "tester");
        runner.setProperty(service, HikariCPConnectionPool.DB_PASSWORD, "testerp");
        runner.setProperty(service, HikariCPConnectionPool.DB_DRIVERNAME, "org.apache.derby.jdbc.EmbeddedDriver");

        runner.enableControllerService(service);

        runner.assertValid(service);
        final DBCPService dbcpService = (DBCPService) runner.getProcessContext().getControllerServiceLookup().getControllerService("test-good1");
        Assert.assertNotNull(dbcpService);
        final Connection connection = dbcpService.getConnection();
        Assert.assertNotNull(connection);

        createInsertSelectDrop(connection);

        connection.close(); // return to pool
    }

    @Rule
    public ExpectedException exception = ExpectedException.none();

    /**
     * Test get database connection using Derby. Get many times, after a while pool should not contain any available connection and getConnection should fail.
     */
    @Test
    public void testExhaustPool() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService("test-exhaust", service);

        // set embedded Derby database connection url
        runner.setProperty(service, HikariCPConnectionPool.DATABASE_URL, "jdbc:derby:" + DB_LOCATION + ";create=true");
        runner.setProperty(service, HikariCPConnectionPool.DB_USER, "tester");
        runner.setProperty(service, HikariCPConnectionPool.DB_DRIVERNAME, "org.apache.derby.jdbc.EmbeddedDriver");

        runner.enableControllerService(service);

        runner.assertValid(service);
        final DBCPService dbcpService = (DBCPService) runner.getProcessContext().getControllerServiceLookup().getControllerService("test-exhaust");
        Assert.assertNotNull(dbcpService);

        exception.expect(ProcessException.class);
        exception.expectMessage("Connection is not available");
        for (int i = 0; i < 100; i++) {
            final Connection connection = dbcpService.getConnection();
            Assert.assertNotNull(connection);
        }
    }

    /**
     * Test get database connection using Derby. Get many times, release immediately and getConnection should not fail.
     */
    @Test
    public void testGetManyNormal() throws InitializationException, SQLException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService("test-exhaust", service);

        // set embedded Derby database connection url
        runner.setProperty(service, HikariCPConnectionPool.DATABASE_URL, "jdbc:derby:" + DB_LOCATION + ";create=true");
        runner.setProperty(service, HikariCPConnectionPool.DB_USER, "tester");
        runner.setProperty(service, HikariCPConnectionPool.DB_PASSWORD, "testerp");
        runner.setProperty(service, HikariCPConnectionPool.DB_DRIVERNAME, "org.apache.derby.jdbc.EmbeddedDriver");

        runner.enableControllerService(service);

        runner.assertValid(service);
        final DBCPService dbcpService = (DBCPService) runner.getProcessContext().getControllerServiceLookup().getControllerService("test-exhaust");
        Assert.assertNotNull(dbcpService);

        for (int i = 0; i < 1000; i++) {
            final Connection connection = dbcpService.getConnection();
            Assert.assertNotNull(connection);
            connection.close(); // will return connection to pool
        }
    }

    @Test
    public void testDriverLoad() throws ClassNotFoundException {
        final Class<?> clazz = Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        assertNotNull(clazz);
    }


    String createTable = "create table restaurants(id integer, name varchar(20), city varchar(50))";
    String dropTable = "drop table restaurants";

    private void createInsertSelectDrop(Connection con) throws SQLException {

        final Statement st = con.createStatement();

        try {
            st.executeUpdate(dropTable);
        } catch (final Exception e) {
            // table may not exist, this is not serious problem.
        }

        st.executeUpdate(createTable);

        st.executeUpdate("insert into restaurants values (1, 'Irifunes', 'San Mateo')");
        st.executeUpdate("insert into restaurants values (2, 'Estradas', 'Daly City')");
        st.executeUpdate("insert into restaurants values (3, 'Prime Rib House', 'San Francisco')");

        int nrOfRows = 0;
        final ResultSet resultSet = st.executeQuery("select * from restaurants");
        while (resultSet.next())
            nrOfRows++;
        assertEquals(3, nrOfRows);

        st.close();
    }
}