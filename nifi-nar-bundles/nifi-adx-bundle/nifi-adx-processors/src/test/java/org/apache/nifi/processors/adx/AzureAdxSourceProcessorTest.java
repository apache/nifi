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


import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import org.apache.nifi.adx.AzureAdxSourceConnectionService;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processors.adx.mock.MockAzureAdxSourceConnectionService;
import org.apache.nifi.processors.adx.mock.MockAzureAdxSourceProcessor;
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

class AzureAdxSourceProcessorTest {

    private static final String MOCK_DB_NAME= "mockDBName";

    private static final String MOCK_ADX_QUERY= "mockAdxQuery";

    private AzureAdxSourceConnectionService azureAdxSourceConnectionService;

    private TestRunner testRunner;

    @BeforeEach
    public void init() throws InitializationException {

        AzureAdxSourceProcessor azureAdxSourceProcessor = new MockAzureAdxSourceProcessor();
        final ProcessorInitializationContext initContext = Mockito.mock(ProcessorInitializationContext.class);
        final String componentId = "componentId";
        when(initContext.getIdentifier()).thenReturn(componentId);
        MockComponentLog componentLog = new MockComponentLog(componentId, azureAdxSourceProcessor);
        when(initContext.getLogger()).thenReturn(componentLog);
        azureAdxSourceProcessor.initialize(initContext);
        testRunner = TestRunners.newTestRunner(azureAdxSourceProcessor);
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
        testRunner.setProperty(AzureAdxSourceProcessor.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(AzureAdxSourceProcessor.ADX_QUERY,MOCK_ADX_QUERY);
        testRunner.setProperty(AzureAdxSourceProcessor.ADX_SOURCE_SERVICE,"adx-source-connection-service");

        testRunner.setIncomingConnection(false);
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        testRunner.getFlowFilesForRelationship(org.apache.nifi.processors.adx.AzureAdxSourceProcessor.RL_SUCCEEDED);
        testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.AzureAdxSourceProcessor.RL_SUCCEEDED);
    }

    /**
     * test failure scenario of source processor where query parameter databaseName is missing
     */
    @Test
    void testAzureAdxSourceProcessorFailureParamMissing() {
        testRunner.setProperty(AzureAdxSourceProcessor.ADX_QUERY,MOCK_ADX_QUERY);
        testRunner.setProperty(AzureAdxSourceProcessor.ADX_SOURCE_SERVICE,"adx-source-connection-service");

        testRunner.setIncomingConnection(false);
        assertThrows(AssertionFailedError.class,()-> testRunner.run(1));
    }

    /**
     * test failure scenario of source processor where query parameter adxQuery is missing
     */
    @Test
    void testAzureAdxSourceProcessorFailureInvalidQuery() {
        testRunner.setProperty(AzureAdxSourceProcessor.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(AzureAdxSourceProcessor.ADX_SOURCE_SERVICE,"adx-source-connection-service");

        testRunner.setIncomingConnection(false);
        assertThrows(AssertionFailedError.class,()-> testRunner.run(1));
    }

    /**
     * test successful scenario where incoming connection exists for source processor
     */
    @Test
    void testAzureAdxSourceProcessorIncomingConnection() {
        testRunner.setProperty(AzureAdxSourceProcessor.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(AzureAdxSourceProcessor.ADX_QUERY,MOCK_ADX_QUERY);
        testRunner.setProperty(AzureAdxSourceProcessor.ADX_SOURCE_SERVICE,"adx-source-connection-service");

        testRunner.setIncomingConnection(true);
        testRunner.enqueue("Storms");
        testRunner.run(1);
        testRunner.assertAllFlowFilesTransferred(AzureAdxSourceProcessor.RL_SUCCEEDED);
    }

    /**
     * test failure scenario of source processor where kusto query limit exceeded
     */
    @Test
    void testAzureAdxSourceProcessorFailureQueryLimitExceeded() throws InitializationException {
        AzureAdxSourceProcessor mockAzureAdxSourceProcessor = new AzureAdxSourceProcessor(){
            @Override
            protected KustoOperationResult executeQuery(String databaseName, String adxQuery) throws DataClientException {
                throw new DataClientException("ingestionSource","Query execution has exceeded the allowed limits");
            }
        };

        testRunner = TestRunners.newTestRunner(mockAzureAdxSourceProcessor);

        testRunner.setProperty(AzureAdxSourceProcessor.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(AzureAdxSourceProcessor.ADX_QUERY,MOCK_ADX_QUERY);
        testRunner.setProperty(AzureAdxSourceProcessor.ADX_SOURCE_SERVICE,"adx-source-connection-service");

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
        testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.AzureAdxSourceProcessor.RL_FAILED);
    }

}
