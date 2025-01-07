package org.apache.nifi.parameter; /*
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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockParameterProviderInitializationContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.ArgumentMatchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestDatabaseParameterProvider {

    public static final String DBCP_SERVICE = "dbcp-service";
    public static final String TABLE_NAME = "myTable";
    private DBCPService dbcpService;

    private DatabaseParameterProvider parameterProvider;

    private MockParameterProviderInitializationContext initializationContext;

    private Map<PropertyDescriptor, String> columnBasedProperties;

    private Map<PropertyDescriptor, String> nonColumnBasedProperties;

    @BeforeEach
    public void init() throws InitializationException {
        dbcpService = mock(DBCPService.class);

        final DatabaseParameterProvider rawProvider = new DatabaseParameterProvider();
        initializationContext = new MockParameterProviderInitializationContext("id", "name", mock(ComponentLog.class));
        initializationContext.addControllerService(dbcpService, DBCP_SERVICE);
        rawProvider.initialize(initializationContext);
        parameterProvider = spy(rawProvider);
        // Return the table name
        doAnswer(invocationOnMock -> invocationOnMock.getArgument(1)).when(parameterProvider).getQuery(any(), any(), any(), any());

        columnBasedProperties = new HashMap<>();

        columnBasedProperties.put(DatabaseParameterProvider.DBCP_SERVICE, DBCP_SERVICE);
        columnBasedProperties.put(DatabaseParameterProvider.DB_TYPE, "Generic");
        columnBasedProperties.put(DatabaseParameterProvider.PARAMETER_GROUPING_STRATEGY, DatabaseParameterProvider.GROUPING_BY_COLUMN.getValue());
        columnBasedProperties.put(DatabaseParameterProvider.PARAMETER_GROUP_NAME_COLUMN, "group");
        columnBasedProperties.put(DatabaseParameterProvider.PARAMETER_NAME_COLUMN, "name");
        columnBasedProperties.put(DatabaseParameterProvider.PARAMETER_VALUE_COLUMN, "value");
        columnBasedProperties.put(DatabaseParameterProvider.TABLE_NAME, TABLE_NAME);

        nonColumnBasedProperties = new HashMap<>();
        nonColumnBasedProperties.put(DatabaseParameterProvider.DBCP_SERVICE, DBCP_SERVICE);
        nonColumnBasedProperties.put(DatabaseParameterProvider.DB_TYPE, "Generic");
        nonColumnBasedProperties.put(DatabaseParameterProvider.PARAMETER_GROUPING_STRATEGY, DatabaseParameterProvider.GROUPING_BY_TABLE_NAME.getValue());
        nonColumnBasedProperties.put(DatabaseParameterProvider.PARAMETER_NAME_COLUMN, "name");
        nonColumnBasedProperties.put(DatabaseParameterProvider.PARAMETER_VALUE_COLUMN, "value");
        nonColumnBasedProperties.put(DatabaseParameterProvider.TABLE_NAMES, "KAFKA, S3");
    }

    @Test
    public void testColumnStrategies() throws SQLException {
        runColumnStrategiesTest(columnBasedProperties);
    }

    @Test
    public void testColumnStrategiesWithExtraProperties() throws SQLException {
        // Ensure setting the unrelated properties don't break anything
        columnBasedProperties.put(DatabaseParameterProvider.TABLE_NAMES, "a,b");
        runColumnStrategiesTest(columnBasedProperties);
    }

    private void runColumnStrategiesTest(final Map<PropertyDescriptor, String> properties) throws SQLException {
        final List<Map<String, String>> rows = List.of(
                Map.of("group", "Kafka", "name", "brokers", "value", "my-brokers", "unrelated_column", "unrelated_value"),
                Map.of("group", "Kafka", "name", "topic", "value", "my-topic", "unrelated_column", "unrelated_value"),
                Map.of("group", "Kafka", "name", "password", "value", "my-password", "unrelated_column", "unrelated_value"),
                Map.of("group", "S3", "name", "bucket", "value", "my-bucket", "unrelated_column", "unrelated_value"),
                Map.of("group", "S3", "name", "s3-password", "value", "my-s3-password", "unrelated_column", "unrelated_value")
        );
        mockTableResults(new MockTable(TABLE_NAME, rows));

        final ConfigurationContext context = new MockConfigurationContext(properties, initializationContext, null);
        final List<ParameterGroup> groups = parameterProvider.fetchParameters(context);
        assertEquals(2, groups.size());

        for (final ParameterGroup group : groups) {
            final String groupName = group.getGroupName();
            if (groupName.equals("S3")) {
                final Parameter parameter = group.getParameters().iterator().next();
                assertEquals("bucket", parameter.getDescriptor().getName());
                assertEquals("my-bucket", parameter.getValue());
                assertFalse(parameter.getDescriptor().isSensitive());
            }
        }
    }

    @Test
    public void testNonColumnStrategies() throws SQLException {
        runNonColumnStrategyTest(nonColumnBasedProperties);
    }

    @Test
    public void testNonColumnStrategiesWithExtraProperties() throws SQLException {
        nonColumnBasedProperties.put(DatabaseParameterProvider.TABLE_NAME, TABLE_NAME);
        nonColumnBasedProperties.put(DatabaseParameterProvider.PARAMETER_GROUP_NAME_COLUMN, "group");
        runNonColumnStrategyTest(nonColumnBasedProperties);
    }

    private void runNonColumnStrategyTest(final Map<PropertyDescriptor, String> properties) throws SQLException {
        final List<Map<String, String>> kafkaRows = List.of(
                Map.of("name", "nifi_brokers", "value", "my-brokers"),
                Map.of("name", "nifi_topic", "value", "my-topic"),
                Map.of("name", "unrelated_field", "value", "my-value"),
                Map.of("name", "kafka_password", "value", "my-password"),
                Map.of("name", "nifi_password", "value", "my-nifi-password")
        );
        final List<Map<String, String>> s3Rows = List.of(
                Map.of("name", "nifi_s3_bucket", "value", "my-bucket"),
                Map.of("name", "s3_password", "value", "my-password"),
                Map.of("name", "nifi_other_field", "value", "my-field"),
                Map.of("name", "other_password", "value", "my-password")
        );
        mockTableResults(new MockTable("KAFKA", kafkaRows), new MockTable("S3", s3Rows));

        final ConfigurationContext context = new MockConfigurationContext(properties, initializationContext, null);
        final List<ParameterGroup> groups = parameterProvider.fetchParameters(context);
        assertEquals(2, groups.size());

        for (final ParameterGroup group : groups) {
            if (group.getGroupName().equals("KAFKA")) {
                assertTrue(group.getParameters().stream()
                        .filter(parameter -> parameter.getDescriptor().getName().equals("nifi_brokers"))
                        .anyMatch(parameter -> parameter.getValue().equals("my-brokers")));
            } else {
                assertTrue(group.getParameters().stream()
                        .filter(parameter -> parameter.getDescriptor().getName().equals("nifi_s3_bucket"))
                        .anyMatch(parameter -> parameter.getValue().equals("my-bucket")));
            }
        }
        final Set<String> allParameterNames = groups.stream()
                .flatMap(group -> group.getParameters().stream())
                .map(parameter -> parameter.getDescriptor().getName())
                .collect(Collectors.toSet());
        assertEquals(new HashSet<>(Arrays.asList("nifi_brokers", "nifi_topic", "kafka_password", "nifi_password",
                "s3_password", "nifi_s3_bucket", "unrelated_field", "nifi_other_field", "other_password")), allParameterNames);
    }

    @Test
    public void testNullNameColumn() throws SQLException {
        final Map<String, String> mapWithNullValue = new HashMap<>();
        mapWithNullValue.put("name", null);
        mockTableResults(new MockTable(TABLE_NAME, List.of(mapWithNullValue)));
        runTestWithExpectedFailure(columnBasedProperties);
    }

    @Test
    public void testNullGroupNameColumn() throws SQLException {
        final Map<String, String> mapWithNullGroupNameColumn = new HashMap<>();
        mapWithNullGroupNameColumn.put("name", "param");
        mapWithNullGroupNameColumn.put("value", "value");
        mapWithNullGroupNameColumn.put("group", null);
        mockTableResults(new MockTable(TABLE_NAME, List.of(mapWithNullGroupNameColumn)));
        runTestWithExpectedFailure(columnBasedProperties);
    }

    @Test
    public void testNullValueColumn() throws SQLException {
        final Map<String, String> mapWithNullValueColumn = new HashMap<>();
        mapWithNullValueColumn.put("name", "param");
        mapWithNullValueColumn.put("value", null);
        mockTableResults(new MockTable(TABLE_NAME, List.of(mapWithNullValueColumn)));
        runTestWithExpectedFailure(columnBasedProperties);
    }

    public void runTestWithExpectedFailure(final Map<PropertyDescriptor, String> properties) {
        final ConfigurationContext context = new MockConfigurationContext(properties, initializationContext, null);
        assertThrows(IllegalStateException.class, () -> parameterProvider.fetchParameters(context));
    }

    private void mockTableResults(final MockTable... mockTables) throws SQLException {
        final Connection connection = mock(Connection.class);
        when(dbcpService.getConnection(any(Map.class))).thenReturn(connection);

        OngoingStubbing<Statement> statementStubbing = null;
        for (final MockTable mockTable : mockTables) {
            final ResultSet resultSet = mock(ResultSet.class);
            final ResultSetAnswer resultSetAnswer = new ResultSetAnswer(mockTable.rows);
            when(resultSet.next()).thenAnswer(resultSetAnswer);

            when(resultSet.getString(anyString())).thenAnswer(invocationOnMock -> resultSetAnswer.getValue(invocationOnMock.getArgument(0)));

            final Statement statement = mock(Statement.class);
            when(statement.executeQuery(ArgumentMatchers.contains(mockTable.tableName))).thenReturn(resultSet);

            if (statementStubbing == null) {
                statementStubbing = when(connection.createStatement()).thenReturn(statement);
            } else {
                statementStubbing = statementStubbing.thenReturn(statement);
            }
        }
    }

    private class MockTable {
        private final String tableName;

        private final List<Map<String, String>> rows;

        private MockTable(final String tableName, final List<Map<String, String>> rows) {
            this.tableName = tableName;
            this.rows = rows;
        }
    }

    private class ResultSetAnswer implements Answer<Boolean> {

        private final Iterator<java.util.Map<String, String>> rowIterator;
        private java.util.Map<String, String> currentRow;

        private ResultSetAnswer(final List<java.util.Map<String, String>> rows) {
            this.rowIterator = rows.iterator();
        }

        @Override
        public Boolean answer(final InvocationOnMock invocationOnMock) {
            final boolean hasNext = rowIterator.hasNext();
            if (hasNext) {
                currentRow = rowIterator.next();
            } else {
                currentRow = null;
            }
            return hasNext;
        }

        String getValue(final String column) {
            return currentRow.get(column);
        }
    }
}
