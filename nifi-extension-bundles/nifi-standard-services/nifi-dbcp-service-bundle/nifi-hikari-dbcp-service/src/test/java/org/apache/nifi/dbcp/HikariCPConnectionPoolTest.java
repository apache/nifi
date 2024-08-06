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
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.ControllerServiceConfiguration;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class HikariCPConnectionPoolTest {
    private final static String SERVICE_ID = HikariCPConnectionPoolTest.class.getSimpleName();

    private static final String INVALID_CONNECTION_URL = "jdbc:h2";

    private static final String DB_DRIVERNAME_VALUE = "jdbc:mock";

    private static final String MAX_WAIT_TIME_VALUE = "5 s";

    private TestRunner runner;

    @BeforeEach
    public void setup() {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
    }

    @Test
    public void testConnectionUrlInvalid() throws InitializationException {
        final HikariCPConnectionPool service = new HikariCPConnectionPool();

        runner.addControllerService(SERVICE_ID, service);
        setDatabaseProperties(service);
        runner.assertValid(service);

        runner.setProperty(service, HikariCPConnectionPool.DATABASE_URL, INVALID_CONNECTION_URL);
        runner.assertNotValid(service);
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

        setDatabaseProperties(service);
        runner.setProperty(service, HikariCPConnectionPool.MAX_WAIT_TIME, "0 millis");

        runner.enableControllerService(service);
        runner.assertValid(service);
    }

    @Test
    public void testMaxConnectionLifetime() throws InitializationException {
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService(SERVICE_ID, service);

        setDatabaseProperties(service);
        runner.setProperty(service, HikariCPConnectionPool.MAX_CONN_LIFETIME, "1 secs");

        runner.enableControllerService(service);
        runner.assertValid(service);
    }

    @Test
    public void testMinIdleCannotBeNegative() throws InitializationException {
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService(SERVICE_ID, service);

        setDatabaseProperties(service);
        runner.setProperty(service, HikariCPConnectionPool.MIN_IDLE, "-1");

        runner.assertNotValid(service);
    }

    @Test
    public void testIdleSettingsAreSet() throws InitializationException {
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService(SERVICE_ID, service);

        setDatabaseProperties(service);
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
        setDatabaseProperties(service);
        runner.setProperty(service, HikariCPConnectionPool.MAX_TOTAL_CONNECTIONS, "2");
        runner.enableControllerService(service);
        runner.assertValid(service);

        final Connection mockConnection = mock(Connection.class);
        MockDriver.setConnection(mockConnection);

        try (final Connection connection = service.getConnection()) {
            assertNotNull(connection, "First Connection not found");

            try (final Connection secondConnection = service.getConnection()) {
                assertNotNull(secondConnection, "Second Connection not found");
            }
        }
    }

    @Test
    void testVerifySuccessful() throws Exception {
        final HikariCPConnectionPool service = new HikariCPConnectionPool();
        runner.addControllerService(SERVICE_ID, service);
        final Connection mockConnection = mock(Connection.class);
        MockDriver.setConnection(mockConnection);
        setDatabaseProperties(service);
        runner.setProperty(service, HikariCPConnectionPool.MAX_TOTAL_CONNECTIONS, "2");
        runner.enableControllerService(service);
        runner.assertValid(service);
        MockProcessContext processContext = (MockProcessContext) runner.getProcessContext();
        final ControllerServiceConfiguration configuration = processContext.getConfiguration(service.getIdentifier());
        final MockConfigurationContext configContext = new MockConfigurationContext(service, configuration.getProperties(), processContext, Collections.emptyMap());
        final List<ConfigVerificationResult> results = service.verify(configContext, runner.getLogger(), configContext.getAllProperties());

        assertOutcomeSuccessful(results);
    }

    private void setDatabaseProperties(final HikariCPConnectionPool service) {
        runner.setProperty(service, HikariCPConnectionPool.DATABASE_URL, DB_DRIVERNAME_VALUE);
        runner.setProperty(service, HikariCPConnectionPool.DB_DRIVERNAME, MockDriver.class.getName());
        runner.setProperty(service, HikariCPConnectionPool.MAX_WAIT_TIME, MAX_WAIT_TIME_VALUE);
        runner.setProperty(service, HikariCPConnectionPool.DB_USER, String.class.getSimpleName());
        runner.setProperty(service, HikariCPConnectionPool.DB_PASSWORD, String.class.getName());
    }

    private void assertOutcomeSuccessful(final List<ConfigVerificationResult> results) {
        assertNotNull(results);
        final Iterator<ConfigVerificationResult> resultsFound = results.iterator();

        assertTrue(resultsFound.hasNext());
        final ConfigVerificationResult firstResult = resultsFound.next();
        assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, firstResult.getOutcome(), firstResult.getExplanation());

        assertTrue(resultsFound.hasNext());
        final ConfigVerificationResult secondResult = resultsFound.next();
        assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, secondResult.getOutcome(), secondResult.getExplanation());

        assertFalse(resultsFound.hasNext());
    }
}