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

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.api.DatabaseCredentialPlacement;
import org.apache.nifi.dbcp.api.DatabasePasswordProvider;
import org.apache.nifi.dbcp.api.DatabasePasswordRequestContext;
import org.apache.nifi.dbcp.utils.DBCPProperties;
import org.apache.nifi.dbcp.utils.DataSourceConfiguration;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AbstractDBCPConnectionPoolTest {

    private static final int MAX_TOTAL = 2;

    private static final int TIMEOUT = 0;

    @Mock
    Driver driver;

    @Mock
    Connection connection;

    @Mock
    DataSourceConfiguration dataSourceConfiguration;

    @Mock
    ConfigurationContext configurationContext;

    @Mock
    ComponentLog componentLog;

    @Mock
    PropertyValue kerberosUserServiceProperty;

    @Mock
    PropertyValue passwordProviderProperty;

    @Mock
    KerberosUserService kerberosUserService;

    @Mock
    DatabasePasswordProvider databasePasswordProvider;

    @Test
    void testVerifySuccessful() throws SQLException {
        final AbstractDBCPConnectionPool connectionPool = new MockDBCPConnectionPool();

        mockContextDatabaseProperties();
        mockDataSourceConfigurationDefaults();
        when(configurationContext.getProperty(eq(DBCPProperties.KERBEROS_USER_SERVICE))).thenReturn(kerberosUserServiceProperty);
        when(kerberosUserServiceProperty.asControllerService(eq(KerberosUserService.class))).thenReturn(kerberosUserService);
        when(driver.connect(any(), any())).thenReturn(connection);
        when(connection.isValid(eq(TIMEOUT))).thenReturn(true);
        when(configurationContext.getProperty(eq(DBCPProperties.PASSWORD_SOURCE))).thenReturn(propertyValue(DBCPProperties.PASSWORD_SOURCE,
                DBCPProperties.PasswordSource.PASSWORD.getValue()));

        final List<ConfigVerificationResult> results = connectionPool.verify(configurationContext, componentLog, Collections.emptyMap());

        assertOutcomeSuccessful(results);
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

    @Test
    void testVerifyUsesDatabasePasswordProvider() throws SQLException {
        final AbstractDBCPConnectionPool connectionPool = new MockDBCPConnectionPool();
        final ExposedProviderAwareBasicDataSource passwordDataSource = new ExposedProviderAwareBasicDataSource();
        final ExposedProviderAwareBasicDataSource accessTokenDataSource = new ExposedProviderAwareBasicDataSource();
        final AtomicReference<DatabaseCredentialPlacement> credentialPlacement = new AtomicReference<>(DatabaseCredentialPlacement.PASSWORD);
        final List<char[]> issuedCredentials = new ArrayList<>();
        final AtomicInteger issuedCredentialIndex = new AtomicInteger();
        final String[] credentialValues = {
                "password-token-1",
                "password-token-2",
                "access-token-success",
                "access-token-failure"
        };
        final CountDownLatch passwordConnectEntered = new CountDownLatch(2);
        final CountDownLatch releasePasswordConnect = new CountDownLatch(1);
        final List<Properties> passwordSnapshots = new CopyOnWriteArrayList<>();
        final List<Properties> passwordLiveReferences = new CopyOnWriteArrayList<>();
        final AtomicReference<Properties> accessTokenSuccessSnapshot = new AtomicReference<>();
        final AtomicReference<Properties> accessTokenFailureSnapshot = new AtomicReference<>();
        final AtomicReference<Properties> accessTokenSuccessLiveReference = new AtomicReference<>();
        final AtomicReference<Properties> accessTokenFailureLiveReference = new AtomicReference<>();
        final AtomicReference<String> connectionMode = new AtomicReference<>("VERIFY");
        final AtomicInteger accessTokenAttempt = new AtomicInteger();

        assertEquals(DatabaseCredentialPlacement.PASSWORD, new MockDatabasePasswordProvider().getDatabaseCredentialPlacement());

        mockContextDatabaseProperties();
        mockDataSourceConfigurationDefaults();
        when(configurationContext.getProperty(eq(DBCPProperties.KERBEROS_USER_SERVICE))).thenReturn(kerberosUserServiceProperty);
        when(kerberosUserServiceProperty.asControllerService(eq(KerberosUserService.class))).thenReturn(null);
        when(configurationContext.getProperty(eq(DBCPProperties.DB_PASSWORD_PROVIDER))).thenReturn(passwordProviderProperty);
        when(passwordProviderProperty.asControllerService(eq(DatabasePasswordProvider.class))).thenReturn(databasePasswordProvider);
        when(configurationContext.getProperty(eq(DBCPProperties.PASSWORD_SOURCE))).thenReturn(propertyValue(DBCPProperties.PASSWORD_SOURCE,
                DBCPProperties.PasswordSource.PASSWORD_PROVIDER.getValue()));
        when(connection.isValid(eq(TIMEOUT))).thenReturn(true);
        when(databasePasswordProvider.getDatabaseCredentialPlacement()).thenAnswer(invocation -> credentialPlacement.get());
        when(databasePasswordProvider.getPassword(any())).thenAnswer(invocation -> {
            final char[] credential = credentialValues[issuedCredentialIndex.getAndIncrement()].toCharArray();
            issuedCredentials.add(credential);
            return credential;
        });
        when(driver.connect(any(), any())).thenAnswer(invocation -> {
            final Properties properties = invocation.getArgument(1);
            final Properties snapshot = new Properties();
            snapshot.putAll(properties);

            if ("VERIFY".equals(connectionMode.get())) {
                return connection;
            }

            if ("PASSWORD".equals(connectionMode.get())) {
                passwordSnapshots.add(snapshot);
                passwordLiveReferences.add(properties);
                passwordConnectEntered.countDown();
                assertTrue(releasePasswordConnect.await(5, TimeUnit.SECONDS));
                return connection;
            }

            if (accessTokenAttempt.getAndIncrement() == 0) {
                accessTokenSuccessSnapshot.set(snapshot);
                accessTokenSuccessLiveReference.set(properties);
                return connection;
            }

            accessTokenFailureSnapshot.set(snapshot);
            accessTokenFailureLiveReference.set(properties);
            throw new SQLException("driver failure");
        });

        connectionPool.verify(configurationContext, componentLog, Collections.emptyMap());
        issuedCredentialIndex.set(0);
        issuedCredentials.clear();
        connectionMode.set("PASSWORD");

        verify(databasePasswordProvider, atLeastOnce()).getPassword(any());
        verify(databasePasswordProvider, atLeastOnce()).getDatabaseCredentialPlacement();

        passwordDataSource.setDriver(driver);
        passwordDataSource.setUrl("jdbc:postgresql://example");
        passwordDataSource.setUsername("dbuser");
        passwordDataSource.setPassword("configured-password");
        passwordDataSource.setMaxTotal(MAX_TOTAL);
        passwordDataSource.addConnectionProperty("ssl", "true");
        passwordDataSource.setDatabasePasswordProvider(databasePasswordProvider, DatabasePasswordRequestContext.builder()
                .jdbcUrl("jdbc:postgresql://example")
                .driverClassName("org.postgresql.Driver")
                .databaseUser("dbuser")
                .connectionProperties(Map.of("ssl", "true"))
                .build());

        final ConnectionFactory passwordConnectionFactory = passwordDataSource.callCreateConnectionFactory();
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        try {
            final Future<Connection> firstConnection = executorService.submit(passwordConnectionFactory::createConnection);
            final Future<Connection> secondConnection = executorService.submit(passwordConnectionFactory::createConnection);

            assertTrue(passwordConnectEntered.await(5, TimeUnit.SECONDS));
            releasePasswordConnect.countDown();

            firstConnection.get(5, TimeUnit.SECONDS).close();
            secondConnection.get(5, TimeUnit.SECONDS).close();
        } catch (final Exception e) {
            throw new AssertionError(e);
        } finally {
            executorService.shutdownNow();
            passwordDataSource.close();
        }

        assertEquals(2, passwordSnapshots.size());
        assertNotSame(passwordLiveReferences.get(0), passwordLiveReferences.get(1));
        assertTrue(passwordSnapshots.stream().allMatch(properties -> "true".equals(properties.getProperty("ssl"))));
        assertTrue(passwordSnapshots.stream().anyMatch(properties -> "password-token-1".equals(properties.getProperty("password"))));
        assertTrue(passwordSnapshots.stream().anyMatch(properties -> "password-token-2".equals(properties.getProperty("password"))));
        assertTrue(passwordLiveReferences.stream().allMatch(properties -> properties.getProperty("password") == null));

        credentialPlacement.set(DatabaseCredentialPlacement.ACCESS_TOKEN);
        connectionMode.set("ACCESS");
        accessTokenDataSource.setDriver(driver);
        accessTokenDataSource.setUrl("jdbc:sqlserver://example.database.windows.net:1433;databaseName=test");
        accessTokenDataSource.setUsername("configured-user");
        accessTokenDataSource.setPassword("configured-password");
        accessTokenDataSource.addConnectionProperty("USER", "dynamic-user");
        accessTokenDataSource.addConnectionProperty("UserName", "dynamic-user-name");
        accessTokenDataSource.addConnectionProperty("PASSWORD", "dynamic-password");
        accessTokenDataSource.addConnectionProperty("authentication", "ActiveDirectoryManagedIdentity");
        accessTokenDataSource.addConnectionProperty("integratedSecurity", "true");
        accessTokenDataSource.setDatabasePasswordProvider(databasePasswordProvider, DatabasePasswordRequestContext.builder()
                .jdbcUrl("jdbc:sqlserver://example.database.windows.net:1433;databaseName=test")
                .driverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .databaseUser("configured-user")
                .connectionProperties(Map.of(
                        "USER", "dynamic-user",
                        "UserName", "dynamic-user-name",
                        "PASSWORD", "dynamic-password",
                        "authentication", "ActiveDirectoryManagedIdentity",
                        "integratedSecurity", "true"
                ))
                .build());

        final ConnectionFactory accessTokenConnectionFactory = accessTokenDataSource.callCreateConnectionFactory();
        try {
            accessTokenConnectionFactory.createConnection().close();
            final SQLException exception = assertThrows(SQLException.class, accessTokenConnectionFactory::createConnection);
            assertEquals("driver failure", exception.getMessage());
        } finally {
            accessTokenDataSource.close();
        }

        final Properties successfulAccessTokenSnapshot = accessTokenSuccessSnapshot.get();
        final Properties failedAccessTokenSnapshot = accessTokenFailureSnapshot.get();
        assertNotNull(successfulAccessTokenSnapshot);
        assertNotNull(failedAccessTokenSnapshot);
        assertEquals("access-token-success", successfulAccessTokenSnapshot.getProperty("accessToken"));
        assertEquals("access-token-failure", failedAccessTokenSnapshot.getProperty("accessToken"));
        assertEquals("", successfulAccessTokenSnapshot.getProperty("user"));
        assertEquals("", successfulAccessTokenSnapshot.getProperty("password"));
        assertEquals("", failedAccessTokenSnapshot.getProperty("user"));
        assertEquals("", failedAccessTokenSnapshot.getProperty("password"));
        assertNull(successfulAccessTokenSnapshot.getProperty("USER"));
        assertNull(successfulAccessTokenSnapshot.getProperty("UserName"));
        assertNull(successfulAccessTokenSnapshot.getProperty("PASSWORD"));
        assertNull(failedAccessTokenSnapshot.getProperty("USER"));
        assertNull(failedAccessTokenSnapshot.getProperty("UserName"));
        assertNull(failedAccessTokenSnapshot.getProperty("PASSWORD"));
        assertEquals("ActiveDirectoryManagedIdentity", successfulAccessTokenSnapshot.getProperty("authentication"));
        assertEquals("true", successfulAccessTokenSnapshot.getProperty("integratedSecurity"));
        assertEquals("ActiveDirectoryManagedIdentity", failedAccessTokenSnapshot.getProperty("authentication"));
        assertEquals("true", failedAccessTokenSnapshot.getProperty("integratedSecurity"));
        assertNull(accessTokenSuccessLiveReference.get().getProperty("accessToken"));
        assertNull(accessTokenFailureLiveReference.get().getProperty("accessToken"));
        assertEquals("", accessTokenSuccessLiveReference.get().getProperty("user"));
        assertEquals("", accessTokenSuccessLiveReference.get().getProperty("password"));
        assertEquals("", accessTokenFailureLiveReference.get().getProperty("user"));
        assertEquals("", accessTokenFailureLiveReference.get().getProperty("password"));

        assertTrue(issuedCredentials.stream().allMatch(characters -> {
            for (final char character : characters) {
                if (character != '\0') {
                    return false;
                }
            }
            return true;
        }));

        doThrow(new IllegalStateException("provider failure")).when(databasePasswordProvider).getPassword(any());
        final ProviderAwareBasicDataSource failureDataSource = new ProviderAwareBasicDataSource();
        failureDataSource.setDriver(driver);
        failureDataSource.setUrl("jdbc:postgresql://example");
        failureDataSource.setDatabasePasswordProvider(databasePasswordProvider, DatabasePasswordRequestContext.builder()
                .jdbcUrl("jdbc:postgresql://example")
                .driverClassName("org.postgresql.Driver")
                .databaseUser("dbuser")
                .build());
        try {
            final SQLException exception = assertThrows(SQLException.class, failureDataSource::getConnection);
            assertTrue(exception.getMessage().contains("Failed to obtain database password from provider"));
            assertTrue(exception.getCause().getMessage().contains("Failed to obtain database password from provider"));
            assertEquals("provider failure", exception.getCause().getCause().getMessage());
        } finally {
            failureDataSource.close();
        }
    }

    @Test
    void testGetDatabasePasswordProviderHandlesNullProperty() {
        final MockDBCPConnectionPool connectionPool = new MockDBCPConnectionPool();

        assertNull(connectionPool.callGetDatabasePasswordProvider(configurationContext));
    }

    private void mockDataSourceConfigurationDefaults() {
        when(dataSourceConfiguration.getUrl()).thenReturn("jdbc:postgresql://example");
        when(dataSourceConfiguration.getDriverName()).thenReturn("org.postgresql.Driver");
        when(dataSourceConfiguration.getUserName()).thenReturn("dbuser");
        when(dataSourceConfiguration.getPassword()).thenReturn("secret");
        when(dataSourceConfiguration.getValidationQuery()).thenReturn(null);
        when(dataSourceConfiguration.getMaxWaitMillis()).thenReturn(1000L);
        when(dataSourceConfiguration.getMaxTotal()).thenReturn(MAX_TOTAL);
        when(dataSourceConfiguration.getMinIdle()).thenReturn(0);
        when(dataSourceConfiguration.getMaxIdle()).thenReturn(1);
        when(dataSourceConfiguration.getMaxConnLifetimeMillis()).thenReturn(1000L);
        when(dataSourceConfiguration.getTimeBetweenEvictionRunsMillis()).thenReturn(1000L);
        when(dataSourceConfiguration.getMinEvictableIdleTimeMillis()).thenReturn(1000L);
        when(dataSourceConfiguration.getSoftMinEvictableIdleTimeMillis()).thenReturn(1000L);
    }

    private void mockContextDatabaseProperties() {
        when(configurationContext.getProperties()).thenReturn(Collections.emptyMap());
        lenient().when(configurationContext.getProperty(eq(DBCPProperties.DATABASE_URL))).thenReturn(propertyValue(DBCPProperties.DATABASE_URL, "jdbc:postgresql://example"));
        lenient().when(configurationContext.getProperty(eq(DBCPProperties.DB_DRIVERNAME))).thenReturn(propertyValue(DBCPProperties.DB_DRIVERNAME, "org.postgresql.Driver"));
        lenient().when(configurationContext.getProperty(eq(DBCPProperties.DB_DRIVER_LOCATION))).thenReturn(propertyValue(DBCPProperties.DB_DRIVER_LOCATION, ""));
        lenient().when(configurationContext.getProperty(eq(DBCPProperties.DB_USER))).thenReturn(propertyValue(DBCPProperties.DB_USER, "dbuser"));
        lenient().when(configurationContext.getProperty(eq(DBCPProperties.DB_PASSWORD))).thenReturn(propertyValue(DBCPProperties.DB_PASSWORD, "secret"));
        lenient().when(configurationContext.getProperty(eq(DBCPProperties.PASSWORD_SOURCE))).thenReturn(propertyValue(DBCPProperties.PASSWORD_SOURCE,
                DBCPProperties.PasswordSource.PASSWORD.getValue()));
    }

    private PropertyValue propertyValue(final PropertyDescriptor descriptor, final String value) {
        return new MockPropertyValue(value, null, descriptor, Collections.emptyMap());
    }

    private class MockDBCPConnectionPool extends AbstractDBCPConnectionPool {

        @Override
        protected Driver getDriver(final String driverName, final String url) {
            return driver;
        }

        @Override
        protected DataSourceConfiguration getDataSourceConfiguration(final ConfigurationContext context) {
            return dataSourceConfiguration;
        }

        private DatabasePasswordProvider callGetDatabasePasswordProvider(final ConfigurationContext context) {
            return getDatabasePasswordProvider(context);
        }
    }

    private static class ExposedProviderAwareBasicDataSource extends ProviderAwareBasicDataSource {

        private ConnectionFactory callCreateConnectionFactory() throws SQLException {
            return createConnectionFactory();
        }
    }

    private static class MockDatabasePasswordProvider extends AbstractControllerService implements DatabasePasswordProvider {

        @Override
        public char[] getPassword(final DatabasePasswordRequestContext requestContext) {
            return new char[0];
        }
    }
}
