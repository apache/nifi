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

import org.apache.derby.jdbc.EmbeddedDriver;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.file.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
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
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HikariCPConnectionPoolTest {
    private final static String SERVICE_ID = HikariCPConnectionPoolTest.class.getSimpleName();

    private static final String DERBY_LOG_PROPERTY = "derby.stream.error.file";

    private static final String DERBY_SHUTDOWN_STATE = "XJ015";

    private TestRunner runner;

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
    public void setup() {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        databaseDirectory = getEmptyDirectory();
    }

    @AfterEach
    public void shutdown() throws IOException {
        if (databaseDirectory.exists()) {
            final SQLException exception = assertThrows(SQLException.class, () -> DriverManager.getConnection("jdbc:derby:;shutdown=true"));
            assertEquals(DERBY_SHUTDOWN_STATE, exception.getSQLState());
            FileUtils.deleteFile(databaseDirectory, true);
        }
    }

    @Test
    public void testMissingPropertyValues() throws InitializationException {
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService(SERVICE_ID, service);
        runner.assertNotValid(service);
    }

    @Test
    public void testConnectionTimeoutZero() throws InitializationException {
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService(SERVICE_ID, service);

        setDerbyProperties(service);
        runner.setProperty(service, HikariCPConnectionPool.MAX_WAIT_TIME, "0 millis");

        runner.enableControllerService(service);
        runner.assertValid(service);
    }

    @Test
    public void testMaxConnectionLifetime() throws InitializationException {
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService(SERVICE_ID, service);

        setDerbyProperties(service);
        runner.setProperty(service, HikariCPConnectionPool.MAX_CONN_LIFETIME, "1 secs");

        runner.enableControllerService(service);
        runner.assertValid(service);
    }

    @Test
    public void testMinIdleCannotBeNegative() throws InitializationException {
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService(SERVICE_ID, service);

        setDerbyProperties(service);
        runner.setProperty(service, HikariCPConnectionPool.MIN_IDLE, "-1");

        runner.assertNotValid(service);
    }

    @Test
    public void testIdleSettingsAreSet() throws InitializationException {
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService(SERVICE_ID, service);

        setDerbyProperties(service);
        runner.setProperty(service, HikariCPConnectionPool.MIN_IDLE, "4");
        runner.setProperty(service, HikariCPConnectionPool.MAX_CONN_LIFETIME, "1 secs");

        runner.enableControllerService(service);

        Assertions.assertEquals(4, service.getDataSource().getMinimumIdle());
        Assertions.assertEquals(1000, service.getDataSource().getMaxLifetime());

        service.getDataSource().close();
    }

    @Test
    public void testGetConnection() throws SQLException, InitializationException {
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService(SERVICE_ID, service);
        setDerbyProperties(service);
        runner.setProperty(service, HikariCPConnectionPool.MAX_TOTAL_CONNECTIONS, "2");
        runner.enableControllerService(service);
        runner.assertValid(service);

        try (final Connection connection = service.getConnection()) {
            assertNotNull(connection, "First Connection not found");

            try (final Connection secondConnection = service.getConnection()) {
                assertNotNull(secondConnection, "Second Connection not found");
            }
        }
    }

    @Test
    public void testCreateInsertSelect() throws InitializationException, SQLException {
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService(SERVICE_ID, service);

        setDerbyProperties(service);

        runner.enableControllerService(service);

        runner.assertValid(service);

        try (final Connection connection = service.getConnection()) {
            Assertions.assertNotNull(connection);
            createInsertSelectDrop(connection);
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
        final String url = String.format("jdbc:derby:%s;create=true", databaseDirectory);
        runner.setProperty(service, HikariCPConnectionPool.DATABASE_URL, url);
        runner.setProperty(service, HikariCPConnectionPool.DB_DRIVERNAME, EmbeddedDriver.class.getName());
        runner.setProperty(service, HikariCPConnectionPool.MAX_WAIT_TIME, "5 s");
        runner.setProperty(service, HikariCPConnectionPool.DB_USER, String.class.getSimpleName());
        runner.setProperty(service, HikariCPConnectionPool.DB_PASSWORD, String.class.getName());
    }

    private File getEmptyDirectory() {
        final String randomDirectory = String.format("%s-%s", getClass().getSimpleName(), UUID.randomUUID());
        return Paths.get(getSystemTemporaryDirectory(), randomDirectory).toFile();
    }

    private static String getSystemTemporaryDirectory() {
        return System.getProperty("java.io.tmpdir");
    }
}