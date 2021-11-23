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
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class HikariCPConnectionPoolTest {
    private final static String DB_LOCATION = "target/db";
    private final static String GOOD_CS_NAME = "test-good1";
    private final static String BAD_CS_NAME = "test-bad1";
    private final static String EXHAUST_CS_NAME = "test-exhaust";

    private static String originalDerbyStreamErrorFile;

    private TestRunner runner;

    @BeforeAll
    public static void setupBeforeClass() {
        originalDerbyStreamErrorFile = System.getProperty("derby.stream.error.file");
        System.setProperty("derby.stream.error.file", "target/derby.log");
    }

    @AfterAll
    public static void shutdownAfterClass() {
        if (originalDerbyStreamErrorFile != null) {
            System.setProperty("derby.stream.error.file", originalDerbyStreamErrorFile);
        }
    }

    @BeforeEach
    public void setup() {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        runner = TestRunners.newTestRunner(NoOpProcessor.class);
    }

    /**
     * Missing property values.
     */
    @Test
    public void testMissingPropertyValues() throws InitializationException {
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        final Map<String, String> properties = new HashMap<>();
        runner.addControllerService(BAD_CS_NAME, service, properties);
        runner.assertNotValid(service);
    }

    /**
     * Max wait set to -1
     */
    @Test
    public void testMaxWait() throws InitializationException {
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService(GOOD_CS_NAME, service);

        setDerbyProperties(service);
        runner.setProperty(service, HikariCPConnectionPool.MAX_WAIT_TIME, "0 millis");

        runner.enableControllerService(service);
        runner.assertValid(service);
    }

    /**
     * Checks validity of idle limit and time settings including a default
     */
    @Test
    public void testIdleConnectionsSettings() throws InitializationException {
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService(GOOD_CS_NAME, service);

        setDerbyProperties(service);
        runner.setProperty(service, HikariCPConnectionPool.MAX_WAIT_TIME, "0 millis");
        runner.setProperty(service, HikariCPConnectionPool.MAX_CONN_LIFETIME, "1 secs");

        runner.enableControllerService(service);
        runner.assertValid(service);
    }

    @Test
    public void testMinIdleCannotBeNegative() throws InitializationException {
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService(GOOD_CS_NAME, service);

        setDerbyProperties(service);
        runner.setProperty(service, HikariCPConnectionPool.MAX_WAIT_TIME, "0 millis");
        runner.setProperty(service, HikariCPConnectionPool.MIN_IDLE, "-1");

        runner.assertNotValid(service);
    }

    /**
     * Checks to ensure that settings have been passed down into the HikariCP
     */
    @Test
    public void testIdleSettingsAreSet() throws InitializationException {
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService(GOOD_CS_NAME, service);

        setDerbyProperties(service);
        runner.setProperty(service, HikariCPConnectionPool.MAX_WAIT_TIME, "0 millis");
        runner.setProperty(service, HikariCPConnectionPool.MIN_IDLE, "4");
        runner.setProperty(service, HikariCPConnectionPool.MAX_CONN_LIFETIME, "1 secs");

        runner.enableControllerService(service);

        Assertions.assertEquals(4, service.getDataSource().getMinimumIdle());
        Assertions.assertEquals(1000, service.getDataSource().getMaxLifetime());

        service.getDataSource().close();
    }

    /**
     * Test database connection using Derby. Connect, create table, insert, select, drop table.
     */
    @Test
    public void testCreateInsertSelect() throws InitializationException, SQLException {
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService(GOOD_CS_NAME, service);

        setDerbyProperties(service);

        runner.enableControllerService(service);

        runner.assertValid(service);
        final DBCPService dbcpService = (DBCPService) runner.getProcessContext().getControllerServiceLookup().getControllerService(GOOD_CS_NAME);
        Assertions.assertNotNull(dbcpService);
        final Connection connection = dbcpService.getConnection();
        Assertions.assertNotNull(connection);

        createInsertSelectDrop(connection);

        connection.close(); // return to pool
    }

    /**
     * Test get database connection using Derby. Get many times, after a while pool should not contain any available connection and getConnection should fail.
     */
    @Test
    public void testExhaustPool() throws InitializationException {
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService(EXHAUST_CS_NAME, service);

        setDerbyProperties(service);

        runner.enableControllerService(service);

        runner.assertValid(service);
        Assertions.assertDoesNotThrow(() -> {
            runner.getProcessContext().getControllerServiceLookup().getControllerService(EXHAUST_CS_NAME);
        });
        final DBCPService dbcpService = (DBCPService) runner.getProcessContext().getControllerServiceLookup().getControllerService(EXHAUST_CS_NAME);
        Assertions.assertNotNull(dbcpService);

        try {
            for (int i = 0; i < 100; i++) {
                final Connection connection = dbcpService.getConnection();
                Assertions.assertNotNull(connection);
            }
            Assertions.fail("Should have exhausted the pool and thrown a ProcessException");
        } catch (ProcessException pe) {
            // Do nothing, this is expected
        } catch (Throwable t) {
            Assertions.fail("Should have exhausted the pool and thrown a ProcessException but threw " + t);
        }
    }

    /**
     * Test get database connection using Derby. Get many times, release immediately and getConnection should not fail.
     */
    @Test
    public void testGetManyNormal() throws InitializationException, SQLException {
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService(EXHAUST_CS_NAME, service);

        setDerbyProperties(service);

        runner.enableControllerService(service);

        runner.assertValid(service);
        final DBCPService dbcpService = (DBCPService) runner.getProcessContext().getControllerServiceLookup().getControllerService(EXHAUST_CS_NAME);
        Assertions.assertNotNull(dbcpService);

        for (int i = 0; i < 100; i++) {
            final Connection connection = dbcpService.getConnection();
            Assertions.assertNotNull(connection);
            connection.close(); // will return connection to pool
        }
    }

    private void createInsertSelectDrop(Connection con) throws SQLException {

        final Statement st = con.createStatement();

        try {
            String dropTable = "drop table restaurants";
            st.executeUpdate(dropTable);
        } catch (final Exception e) {
            // table may not exist, this is not serious problem.
        }

        String createTable = "create table restaurants(id integer, name varchar(20), city varchar(50))";
        st.executeUpdate(createTable);

        st.executeUpdate("insert into restaurants values (1, 'Irifunes', 'San Mateo')");
        st.executeUpdate("insert into restaurants values (2, 'Estradas', 'Daly City')");
        st.executeUpdate("insert into restaurants values (3, 'Prime Rib House', 'San Francisco')");

        int nrOfRows = 0;
        final ResultSet resultSet = st.executeQuery("select * from restaurants");
        while (resultSet.next()) {
            nrOfRows++;
        }
        Assertions.assertEquals(3, nrOfRows);

        st.close();
    }

    private void setDerbyProperties(final HikariCPConnectionPool service) {
        runner.setProperty(service, HikariCPConnectionPool.DATABASE_URL, "jdbc:derby:" + DB_LOCATION + ";create=true");
        runner.setProperty(service, HikariCPConnectionPool.DB_DRIVERNAME, "org.apache.derby.jdbc.EmbeddedDriver");
        runner.setProperty(service, HikariCPConnectionPool.DB_USER, "tester");
        runner.setProperty(service, HikariCPConnectionPool.DB_PASSWORD, "testerp");
    }
}