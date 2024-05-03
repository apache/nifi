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
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.utils.DBCPProperties;
import org.apache.nifi.dbcp.utils.DataSourceConfiguration;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.logging.ComponentLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
    KerberosUserService kerberosUserService;

    @Test
    void testVerifySuccessful() throws SQLException {
        final AbstractDBCPConnectionPool connectionPool = new MockDBCPConnectionPool();

        when(dataSourceConfiguration.getMaxTotal()).thenReturn(MAX_TOTAL);
        when(configurationContext.getProperty(eq(DBCPProperties.KERBEROS_USER_SERVICE))).thenReturn(kerberosUserServiceProperty);
        when(kerberosUserServiceProperty.asControllerService(eq(KerberosUserService.class))).thenReturn(kerberosUserService);
        when(driver.connect(any(), any())).thenReturn(connection);
        when(connection.isValid(eq(TIMEOUT))).thenReturn(true);

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

    private class MockDBCPConnectionPool extends AbstractDBCPConnectionPool {

        @Override
        protected Driver getDriver(final String driverName, final String url) {
            return driver;
        }

        @Override
        protected DataSourceConfiguration getDataSourceConfiguration(final ConfigurationContext context) {
            return dataSourceConfiguration;
        }
    }
}
