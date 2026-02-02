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

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.dbcp.utils.DBCPProperties;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opentest4j.AssertionFailedError;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_DRIVER_LOCATION;
import static org.apache.nifi.dbcp.utils.DBCPProperties.EVICTION_RUN_PERIOD;
import static org.apache.nifi.dbcp.utils.DBCPProperties.KERBEROS_USER_SERVICE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_CONN_LIFETIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_IDLE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MIN_EVICTABLE_IDLE_TIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MIN_IDLE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.SOFT_MIN_EVICTABLE_IDLE_TIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.VALIDATION_QUERY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DBCPServiceTest {
    private static final String SERVICE_ID = DBCPConnectionPool.class.getName();

    private static final String INVALID_CONNECTION_URL = "jdbc:h2";

    private static final String DRIVER_CLASS = "org.hsqldb.jdbc.JDBCDriver";
    private static final String CONNECTION_URL_FORMAT = "jdbc:hsqldb:file:%s";

    private TestRunner runner;

    private DBCPConnectionPool service;

    @BeforeEach
    public void setService(@TempDir final Path tempDir) throws InitializationException {
        service = new DBCPConnectionPool();
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService(SERVICE_ID, service);

        final String url = CONNECTION_URL_FORMAT.formatted(tempDir);
        runner.setProperty(service, DBCPProperties.DATABASE_URL, url);
        runner.setProperty(service, DBCPProperties.DB_USER, String.class.getSimpleName());
        runner.setProperty(service, DBCPProperties.DB_PASSWORD, String.class.getName());
        runner.setProperty(service, DBCPProperties.DB_DRIVERNAME, DRIVER_CLASS);
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
    public void testDeregisterDriver() {
        runner.enableControllerService(service);
        runner.assertValid(service);
        final int serviceRunningNumberOfDrivers = Collections.list(DriverManager.getDrivers()).size();
        runner.disableControllerService(service);
        runner.removeControllerService(service);
        final int expectedDriversAfterRemove = serviceRunningNumberOfDrivers - 1;
        assertEquals(expectedDriversAfterRemove, Collections.list(DriverManager.getDrivers()).size(), "Driver should be deregistered on remove");
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

    @Test
    public void testInvalidDriverClass() throws URISyntaxException, ClassNotFoundException {
        final URL driverURL = Class.forName(DRIVER_CLASS)
                .getProtectionDomain()
                .getCodeSource()
                .getLocation();

        final File jarFile = new File(driverURL.toURI());

        runner.setProperty(service, DBCPProperties.DB_DRIVER_LOCATION, jarFile.getAbsolutePath());
        runner.setProperty(service, DBCPProperties.DB_DRIVERNAME, "a.very.bad.jdbc.Driver");

        final Collection<ConfigVerificationResult> verificationResults = runner.verify(service, Map.of());
        assertEquals(1, verificationResults.size());

        final ConfigVerificationResult result = verificationResults.stream().filter(r -> r.getVerificationStepName().equals("Configure Data Source")).findFirst().get();
        assertTrue(result.getExplanation().contains(DRIVER_CLASS));
    }

    @Test
    public void testInvalidDriverNoResource() {
        runner.setProperty(service, DBCPProperties.DB_DRIVERNAME, "a.very.bad.jdbc.Driver");

        final Collection<ValidationResult> validationResults = runner.validate(service);
        assertEquals(0, validationResults.size());

        AssertionFailedError thrown = assertThrows(AssertionFailedError.class, () -> runner.enableControllerService(service));

        // Verify the underlying cause is what we expect
        assertTrue(thrown.getMessage().contains("ProcessException"));
        assertTrue(thrown.getMessage().contains("JDBC driver class 'a.very.bad.jdbc.Driver' not found. The property 'Database Driver Location(s)' should be set."));
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("database-driver-locations", DB_DRIVER_LOCATION.getName()),
                Map.entry("Database Driver Location(s)", DB_DRIVER_LOCATION.getName()),
                Map.entry(DBCPProperties.OLD_VALIDATION_QUERY_PROPERTY_NAME, VALIDATION_QUERY.getName()),
                Map.entry(DBCPProperties.OLD_MIN_IDLE_PROPERTY_NAME, MIN_IDLE.getName()),
                Map.entry(DBCPProperties.OLD_MAX_IDLE_PROPERTY_NAME, MAX_IDLE.getName()),
                Map.entry(DBCPProperties.OLD_MAX_CONN_LIFETIME_PROPERTY_NAME, MAX_CONN_LIFETIME.getName()),
                Map.entry(DBCPProperties.OLD_EVICTION_RUN_PERIOD_PROPERTY_NAME, EVICTION_RUN_PERIOD.getName()),
                Map.entry(DBCPProperties.OLD_MIN_EVICTABLE_IDLE_TIME_PROPERTY_NAME, MIN_EVICTABLE_IDLE_TIME.getName()),
                Map.entry(DBCPProperties.OLD_SOFT_MIN_EVICTABLE_IDLE_TIME_PROPERTY_NAME, SOFT_MIN_EVICTABLE_IDLE_TIME.getName()),
                Map.entry(DBCPProperties.OLD_KERBEROS_USER_SERVICE_PROPERTY_NAME, KERBEROS_USER_SERVICE.getName())
        );

        final Map<String, String> propertyValues = Map.of();
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        service.migrateProperties(configuration);

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expectedRenamed, propertiesRenamed);

        final Set<String> expectedRemoved = Set.of(
                "kerberos-principal",
                "kerberos-password",
                "kerberos-credentials-service"
        );

        assertEquals(expectedRemoved, result.getPropertiesRemoved());
    }

    private void assertConnectionNotNullDynamicProperty(final String propertyName, final String propertyValue) throws SQLException {
        runner.setProperty(service, propertyName, propertyValue);

        runner.enableControllerService(service);
        runner.assertValid(service);

        try (final Connection connection = service.getConnection()) {
            assertNotNull(connection);
        }
    }
}
