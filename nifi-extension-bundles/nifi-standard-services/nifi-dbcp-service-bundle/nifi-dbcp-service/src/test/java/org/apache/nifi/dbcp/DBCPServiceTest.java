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

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import org.apache.nifi.dbcp.utils.DBCPProperties;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.file.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DBCPServiceTest {
    private static final String SERVICE_ID = DBCPConnectionPool.class.getName();

    private static final String DERBY_LOG_PROPERTY = "derby.stream.error.file";

    private static final String DERBY_SHUTDOWN_STATE = "XJ015";

    private static final String INVALID_CONNECTION_URL = "jdbc:h2";

    private TestRunner runner;

    private File databaseDirectory;

    private DBCPConnectionPool service;

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

        service = new DBCPConnectionPool();
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService(SERVICE_ID, service);

        final String url = String.format("jdbc:derby:%s;create=true", databaseDirectory);
        runner.setProperty(service, DBCPProperties.DATABASE_URL, url);
        runner.setProperty(service, DBCPProperties.DB_USER, String.class.getSimpleName());
        runner.setProperty(service, DBCPProperties.DB_PASSWORD, String.class.getName());
        runner.setProperty(service, DBCPProperties.DB_DRIVERNAME, "org.apache.derby.jdbc.EmbeddedDriver");
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
    public void testConnectionUrlInvalid() {
        runner.assertValid(service);

        runner.setProperty(service, DBCPProperties.DATABASE_URL, INVALID_CONNECTION_URL);
        runner.assertNotValid(service);
    }

    @Test
    public void testNotValidWithNegativeMinIdleProperty() {
        runner.setProperty(service, DBCPProperties.MIN_IDLE, "-1");
        runner.assertNotValid(service);
    }

    @Test
    public void testGetConnectionDynamicProperty() throws SQLException {
        assertConnectionNotNullDynamicProperty("create", "true");
    }

    @Test
    public void testGetConnectionDynamicPropertyExpressionLanguageSupported() throws SQLException {
        assertConnectionNotNullDynamicProperty("create", "${literal(1):gt(0)}");
    }

    @Test
    public void testGetConnectionDynamicPropertySensitivePrefixSupported() throws SQLException {
        assertConnectionNotNullDynamicProperty("SENSITIVE.create", "true");
    }

    @Test
    public void testGetConnectionSensitiveDynamicPropertyWithoutPrefixAndWithPrefixShouldThrowException() {
        runner.setProperty(service, "SENSITIVE.create", "true");
        runner.setProperty(service, "create", "true");

        final AssertionFailedError e = assertThrows(AssertionFailedError.class, () -> runner.enableControllerService(service));
        assertTrue(e.getMessage().contains("Duplicate"));
    }

    @Test
    public void testGetConnectionExecuteStatements() throws SQLException {
        runner.enableControllerService(service);
        runner.assertValid(service);

        try (final Connection connection = service.getConnection()) {
            assertNotNull(connection);

            try (final Statement st = connection.createStatement()) {
                st.executeUpdate("create table restaurants(id integer, name varchar(20), city varchar(50))");

                st.executeUpdate("insert into restaurants values (1, 'Irifunes', 'San Mateo')");
                st.executeUpdate("insert into restaurants values (2, 'Estradas', 'Daly City')");
                st.executeUpdate("insert into restaurants values (3, 'Prime Rib House', 'San Francisco')");

                try (final ResultSet resultSet = st.executeQuery("select count(*) AS total_rows from restaurants")) {
                    assertTrue(resultSet.next(), "Result Set Row not found");
                    final int rows = resultSet.getInt(1);
                    assertEquals(3, rows);
                }
            }
        }
    }

    @Test
    public void testGetConnection() throws SQLException {
        runner.setProperty(service, DBCPProperties.MAX_TOTAL_CONNECTIONS, "2");
        runner.enableControllerService(service);
        runner.assertValid(service);

        try (final Connection connection = service.getConnection()) {
            assertNotNull(connection, "First Connection not found");
        }
        try (final Connection connection = service.getConnection()) {
            assertNotNull(connection, "Second Connection not found");
        }
    }

    @Test
    public void testGetConnectionMaxTotalConnectionsExceeded() {
        runner.setProperty(service, DBCPProperties.MAX_TOTAL_CONNECTIONS, "1");
        runner.setProperty(service, DBCPProperties.MAX_WAIT_TIME, "1 ms");
        runner.enableControllerService(service);
        runner.assertValid(service);

        final Connection connection = service.getConnection();
        assertNotNull(connection);
        assertThrows(ProcessException.class, service::getConnection);
    }

    @Test
    public void testGetDataSourceProperties() throws SQLException {
        runner.setProperty(service, DBCPProperties.MAX_WAIT_TIME, "-1");
        runner.setProperty(service, DBCPProperties.MAX_IDLE, "6");
        runner.setProperty(service, DBCPProperties.MIN_IDLE, "4");
        runner.setProperty(service, DBCPProperties.MAX_CONN_LIFETIME, "1 secs");
        runner.setProperty(service, DBCPProperties.EVICTION_RUN_PERIOD, "1 secs");
        runner.setProperty(service, DBCPProperties.MIN_EVICTABLE_IDLE_TIME, "1 secs");
        runner.setProperty(service, DBCPProperties.SOFT_MIN_EVICTABLE_IDLE_TIME, "1 secs");

        runner.enableControllerService(service);

        assertEquals(6, service.getDataSource().getMaxIdle());
        assertEquals(4, service.getDataSource().getMinIdle());
        assertEquals(1000, service.getDataSource().getMaxConnDuration().toMillis());
        assertEquals(1000, service.getDataSource().getDurationBetweenEvictionRuns().toMillis());
        assertEquals(1000, service.getDataSource().getMinEvictableIdleDuration().toMillis());
        assertEquals(1000, service.getDataSource().getSoftMinEvictableIdleDuration().toMillis());

        service.getDataSource().close();
    }

    private void assertConnectionNotNullDynamicProperty(final String propertyName, final String propertyValue) throws SQLException {
        runner.setProperty(service, propertyName, propertyValue);

        runner.enableControllerService(service);
        runner.assertValid(service);

        try (final Connection connection = service.getConnection()) {
            assertNotNull(connection);
        }
    }

    private File getEmptyDirectory() {
        final String randomDirectory = String.format("%s-%s", getClass().getSimpleName(), UUID.randomUUID());
        return Paths.get(getSystemTemporaryDirectory(), randomDirectory).toFile();
    }

    private static String getSystemTemporaryDirectory() {
        return System.getProperty("java.io.tmpdir");
    }
}
