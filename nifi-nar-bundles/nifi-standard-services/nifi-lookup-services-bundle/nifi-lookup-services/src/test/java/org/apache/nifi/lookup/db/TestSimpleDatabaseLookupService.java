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
package org.apache.nifi.lookup.db;

import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestSimpleDatabaseLookupService {

    private static final String SERVICE_ID = SimpleDatabaseLookupService.class.getSimpleName();

    private static final String DBCP_SERVICE_ID = DBCPService.class.getSimpleName();

    private static final String TABLE_NAME = "Person";

    private static final String LOOKUP_KEY_COLUMN = "Name";

    private static final String LOOKUP_VALUE_COLUMN = "ID";

    private static final String LOOKUP_VALUE = "12345";

    private static final String LOOKUP_KEY_PROPERTY = "key";

    private static final String LOOKUP_KEY = "First";

    private static final String EXPECTED_STATEMENT = String.format("SELECT %s FROM %s WHERE %s = ?", LOOKUP_VALUE_COLUMN, TABLE_NAME, LOOKUP_KEY_COLUMN);

    private TestRunner runner;

    @Mock
    private DBCPService dbcpService;

    @Mock
    private Connection connection;

    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private ResultSet resultSet;

    @Captor
    private ArgumentCaptor<String> statementCaptor;

    private SimpleDatabaseLookupService lookupService;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);

        when(dbcpService.getIdentifier()).thenReturn(DBCP_SERVICE_ID);
        runner.addControllerService(DBCP_SERVICE_ID, dbcpService);
        runner.enableControllerService(dbcpService);

        lookupService = new SimpleDatabaseLookupService();
        runner.addControllerService(SERVICE_ID, lookupService);
        runner.setProperty(lookupService, SimpleDatabaseLookupService.DBCP_SERVICE, DBCP_SERVICE_ID);
        runner.setProperty(lookupService, SimpleDatabaseLookupService.TABLE_NAME, TABLE_NAME);
        runner.setProperty(lookupService, SimpleDatabaseLookupService.LOOKUP_KEY_COLUMN, LOOKUP_KEY_COLUMN);
        runner.setProperty(lookupService, SimpleDatabaseLookupService.LOOKUP_VALUE_COLUMN, LOOKUP_VALUE_COLUMN);
    }

    @Test
    void testLookupEmpty() throws LookupFailureException, SQLException {
        runner.enableControllerService(lookupService);

        setConnection();

        final Map<String, Object> coordinates = Collections.singletonMap(LOOKUP_KEY_PROPERTY, LOOKUP_KEY);
        final Optional<String> lookupFound = lookupService.lookup(coordinates);

        assertFalse(lookupFound.isPresent());
        assertPreparedStatementExpected();
    }

    @Test
    void testLookupFound() throws LookupFailureException, SQLException {
        runner.enableControllerService(lookupService);

        setConnection();
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getObject(eq(LOOKUP_VALUE_COLUMN))).thenReturn(LOOKUP_VALUE);

        final Map<String, Object> coordinates = Collections.singletonMap(LOOKUP_KEY_PROPERTY, LOOKUP_KEY);
        final Optional<String> lookupFound = lookupService.lookup(coordinates);

        assertTrue(lookupFound.isPresent());
        assertEquals(LOOKUP_VALUE, lookupFound.get());
        assertPreparedStatementExpected();
    }

    private void setConnection() throws SQLException {
        when(dbcpService.getConnection(any())).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
    }

    private void assertPreparedStatementExpected() throws SQLException {
        verify(connection).prepareStatement(statementCaptor.capture());
        final String statement = statementCaptor.getValue();
        assertEquals(EXPECTED_STATEMENT, statement);
    }
}
