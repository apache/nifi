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
package org.apache.nifi.services.azure.data.explorer;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultColumn;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.apache.nifi.logging.ComponentLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StandardKustoIngestServiceTest {

    private Client mockExecutionClient;
    private KustoOperationResult mockKustoOperationResult;
    private KustoResultSetTable mockKustoResultSetTable;
    private ComponentLog mockLogger;
    private StandardKustoIngestService service;

    private static class TestableKustoIngestService extends StandardKustoIngestService {
        private final ComponentLog logger;
        TestableKustoIngestService(ComponentLog logger, Client mockClient) {
            this.logger = logger;
            this.executionClient = mockClient;
        }
        @Override
        protected ComponentLog getLogger() {
            return logger;
        }
    }

    @BeforeEach
    public void setUp() {
        mockLogger = mock(ComponentLog.class);
        mockExecutionClient = mock(Client.class);
        mockKustoOperationResult = mock(KustoOperationResult.class);
        mockKustoResultSetTable = mock(KustoResultSetTable.class);
        service = new TestableKustoIngestService(mockLogger, mockExecutionClient);
    }

    @ParameterizedTest
    @CsvSource({
        "true",
        "false",
        "null" //needs to be a constant
    })
    void testIsStreamingPolicyEnabled(String isStreamingEnabled) throws Exception {
        KustoResultColumn[] krcArray = Map.of(0, "PolicyName", 1, "EntityName", 2, "Policy", 3, "ChildEntities", 4, "EntityType")
            .entrySet().stream().map(entry -> new KustoResultColumn(entry.getValue(), "string", entry.getKey()))
            .toArray(KustoResultColumn[]::new);

        // Arrange: mock the KustoOperationResult and KustoResultSetTable
        // Simulate a table with a row indicating streaming policy is enabled
        when(mockKustoResultSetTable.hasNext()).thenReturn(true).thenReturn(false);
        when(mockKustoResultSetTable.next()).thenReturn(true).thenReturn(false);
        when(mockKustoResultSetTable.getString(anyInt())).thenReturn("null".equals(isStreamingEnabled) ? null : isStreamingEnabled);
        when(mockKustoResultSetTable.getColumns()).thenReturn(krcArray);

        when(mockKustoOperationResult.getPrimaryResults()).thenReturn(mockKustoResultSetTable);

        // Arrange: mock executeMgmt to return a Results with a KustoResultSetTable
        when(mockExecutionClient.executeMgmt(eq("db"), eq(".show database db policy streamingingestion |  project IsEnabled = todynamic(Policy)['IsEnabled']")))
            .thenReturn(mockKustoOperationResult);

        // Act
        boolean actual = service.isStreamingPolicyEnabled("db");

        // Assert
        boolean expected = "true".equals(isStreamingEnabled);
        assertEquals(expected, actual, "Streaming policy enabled check failed for value: " + isStreamingEnabled);
    }

    @ParameterizedTest
    @CsvSource({
        "100",
        "-1"
    })
    void testIsTableReadable(String rowCount) {
        String databaseName = "db";
        String tableName = "table";

        KustoResultColumn[] krcArray = Map.of(0, "Count")
            .entrySet().stream().map(entry -> new KustoResultColumn(entry.getValue(), "int64", entry.getKey()))
            .toArray(KustoResultColumn[]::new);

        // Arrange: mock the KustoOperationResult and KustoResultSetTable
        // Simulate a table with a row indicating streaming policy is enabled
        when(mockKustoResultSetTable.hasNext()).thenReturn(true).thenReturn(false);
        when(mockKustoResultSetTable.next()).thenReturn(true).thenReturn(false);

        if ("-1".equals(rowCount)) {
            // Simulate a table that is not readable, e.g., no rows or columns
            when(mockKustoResultSetTable.getString(anyInt())).thenThrow(new DataServiceException("Engine", "Table is not readable", true));
            doNothing().when(mockLogger).error(anyString(), anyString(), anyString(), Mockito.any(Throwable.class));
        } else {
            when(mockKustoResultSetTable.getString(anyInt())).thenReturn(rowCount);
        }

        when(mockKustoResultSetTable.getColumns()).thenReturn(krcArray);

        when(mockKustoOperationResult.getPrimaryResults()).thenReturn(mockKustoResultSetTable);

        // Arrange: mock executeMgmt to return a Results with a KustoResultSetTable
        when(mockExecutionClient.executeQuery(eq(databaseName), eq(String.format("%s | count", tableName))))
            .thenReturn(mockKustoOperationResult);

        // Act
        boolean actual = service.isTableReadable(databaseName, tableName);

        // Assert
        if ("-1".equals(rowCount)) {
            // Simulate a table that is not readable, e.g., no rows or columns
            assertFalse(actual, "Table should not be readable for row count: " + rowCount + " where an exception is thrown");
        } else {
            assertTrue(actual, "Table should be readable for row count: " + rowCount);
        }
    }
}
