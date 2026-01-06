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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.api.DatabasePasswordProvider;
import org.apache.nifi.dbcp.utils.DBCPProperties;
import org.apache.nifi.dbcp.utils.DataSourceConfiguration;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AbstractDBCPConnectionPoolTest {

    private static final int MAX_TOTAL = 1;

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

        mockContextDatabaseProperties();
        mockDataSourceConfigurationDefaults();
        when(configurationContext.getProperty(eq(DBCPProperties.KERBEROS_USER_SERVICE))).thenReturn(kerberosUserServiceProperty);
        when(kerberosUserServiceProperty.asControllerService(eq(KerberosUserService.class))).thenReturn(null);
        when(configurationContext.getProperty(eq(DBCPProperties.DB_PASSWORD_PROVIDER))).thenReturn(passwordProviderProperty);
        when(passwordProviderProperty.asControllerService(eq(DatabasePasswordProvider.class))).thenReturn(databasePasswordProvider);
        when(configurationContext.getProperty(eq(DBCPProperties.PASSWORD_SOURCE))).thenReturn(propertyValue(DBCPProperties.PASSWORD_SOURCE,
                DBCPProperties.PasswordSource.PASSWORD_PROVIDER.getValue()));
        when(connection.isValid(eq(TIMEOUT))).thenReturn(true);
        when(databasePasswordProvider.getPassword(any())).thenAnswer(invocation -> "token".toCharArray());

        final ArgumentCaptor<Properties> propertiesCaptor = ArgumentCaptor.forClass(Properties.class);
        when(driver.connect(any(), propertiesCaptor.capture())).thenReturn(connection);

        connectionPool.verify(configurationContext, componentLog, Collections.emptyMap());

        final List<Properties> capturedProperties = propertiesCaptor.getAllValues();
        assertTrue(capturedProperties.stream().anyMatch(props -> "token".equals(props.getProperty("password"))));
        verify(databasePasswordProvider, atLeastOnce()).getPassword(any());
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
}
