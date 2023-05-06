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
package org.apache.nifi.processors.adx;


import org.apache.nifi.adx.model.KustoQueryResponse;
import org.apache.nifi.adx.service.AzureAdxSourceConnectionService;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processors.adx.mock.MockAzureAdxSourceConnectionService;
import org.apache.nifi.processors.adx.mock.MockQueryAzureDataExplorer;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opentest4j.AssertionFailedError;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

class QueryAzureDataExplorerTest {

    private static final String MOCK_DB_NAME= "mockDBName";

    private static final String MOCK_ADX_QUERY= "mockAdxQuery";

    private AzureAdxSourceConnectionService azureAdxSourceConnectionService;

    private TestRunner testRunner;

    @BeforeEach
    public void init() throws InitializationException {

        QueryAzureDataExplorer queryAzureDataExplorer = new MockQueryAzureDataExplorer();
        final ProcessorInitializationContext initContext = Mockito.mock(ProcessorInitializationContext.class);
        final String componentId = "componentId";
        when(initContext.getIdentifier()).thenReturn(componentId);
        MockComponentLog componentLog = new MockComponentLog(componentId, queryAzureDataExplorer);
        when(initContext.getLogger()).thenReturn(componentLog);
        queryAzureDataExplorer.initialize(initContext);
        testRunner = TestRunners.newTestRunner(queryAzureDataExplorer);
        testRunner.setValidateExpressionUsage(false);

        azureAdxSourceConnectionService = new MockAzureAdxSourceConnectionService();

        testRunner.addControllerService("adx-source-connection-service", azureAdxSourceConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.APP_ID,"sample");
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.APP_KEY,"sample");
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.APP_TENANT,"sample");
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.CLUSTER_URL, "http://sample.com/");
        testRunner.enableControllerService(azureAdxSourceConnectionService);
        testRunner.assertValid(azureAdxSourceConnectionService);

    }

    /**
     * test successful scenario where all valid parameters of source processor are provided
     */
    @Test
    void testAzureAdxSourceProcessorSuccess() {
        testRunner.setProperty(QueryAzureDataExplorer.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(QueryAzureDataExplorer.ADX_QUERY,MOCK_ADX_QUERY);
        testRunner.setProperty(QueryAzureDataExplorer.ADX_SOURCE_SERVICE,"adx-source-connection-service");

        testRunner.setIncomingConnection(false);
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        testRunner.getFlowFilesForRelationship(QueryAzureDataExplorer.SUCCESS);
        testRunner.assertAllFlowFilesTransferred(QueryAzureDataExplorer.SUCCESS);
    }

    /**
     * test failure scenario of source processor where query parameter databaseName is missing
     */
    @Test
    void testAzureAdxSourceProcessorFailureParamMissing() {
        testRunner.setProperty(QueryAzureDataExplorer.ADX_QUERY,MOCK_ADX_QUERY);
        testRunner.setProperty(QueryAzureDataExplorer.ADX_SOURCE_SERVICE,"adx-source-connection-service");

        testRunner.setIncomingConnection(false);
        assertThrows(AssertionFailedError.class,()-> testRunner.run(1));
    }

    /**
     * test failure scenario of source processor where query parameter adxQuery is missing
     */
    @Test
    void testAzureAdxSourceProcessorFailureInvalidQuery() {
        testRunner.setProperty(QueryAzureDataExplorer.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(QueryAzureDataExplorer.ADX_SOURCE_SERVICE,"adx-source-connection-service");

        testRunner.setIncomingConnection(false);
        assertThrows(AssertionFailedError.class,()-> testRunner.run(1));
    }

    /**
     * test successful scenario where incoming connection exists for source processor
     */
    @Test
    void testAzureAdxSourceProcessorIncomingConnection() {
        testRunner.setProperty(QueryAzureDataExplorer.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(QueryAzureDataExplorer.ADX_QUERY,MOCK_ADX_QUERY);
        testRunner.setProperty(QueryAzureDataExplorer.ADX_SOURCE_SERVICE,"adx-source-connection-service");

        testRunner.setIncomingConnection(true);
        testRunner.enqueue("Storms");
        testRunner.run(1);
        testRunner.assertAllFlowFilesTransferred(QueryAzureDataExplorer.SUCCESS);
    }

    /**
     * test failure scenario of source processor where kusto query limit exceeded
     */
    @Test
    void testAzureAdxSourceProcessorFailureQueryLimitExceeded() throws InitializationException {
        QueryAzureDataExplorer mockQueryAzureDataExplorer = new QueryAzureDataExplorer(){
            @Override
            protected KustoQueryResponse executeQuery(String databaseName, String adxQuery) {
                return new KustoQueryResponse(true, "Query limit exceeded");
            }
        };

        testRunner = TestRunners.newTestRunner(mockQueryAzureDataExplorer);

        testRunner.setProperty(QueryAzureDataExplorer.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(QueryAzureDataExplorer.ADX_QUERY,MOCK_ADX_QUERY);
        testRunner.setProperty(QueryAzureDataExplorer.ADX_SOURCE_SERVICE,"adx-source-connection-service");

        testRunner.setValidateExpressionUsage(false);

        azureAdxSourceConnectionService = new MockAzureAdxSourceConnectionService();

        testRunner.addControllerService("adx-source-connection-service", azureAdxSourceConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.APP_ID,"sample");
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.APP_KEY,"sample");
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.APP_TENANT,"sample");
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.CLUSTER_URL, "http://sample.com/");

        testRunner.enableControllerService(azureAdxSourceConnectionService);
        testRunner.assertValid(azureAdxSourceConnectionService);
        testRunner.setIncomingConnection(false);
        testRunner.run(1);
        testRunner.assertAllFlowFilesTransferred(QueryAzureDataExplorer.FAILED);
    }

}
