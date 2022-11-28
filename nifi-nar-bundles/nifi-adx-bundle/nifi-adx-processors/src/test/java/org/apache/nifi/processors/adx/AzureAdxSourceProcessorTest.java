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

import org.apache.nifi.adx.AzureAdxSourceConnectionService;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processors.adx.mock.MockAzureAdxSourceConnectionService;
import org.apache.nifi.processors.adx.mock.MockAzureAdxSourceProcessor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

public class AzureAdxSourceProcessorTest {

    private AzureAdxSourceProcessor azureAdxSourceProcessor;

    private MockProcessSession processSession;

    private SharedSessionState sharedState;


    private static final String MOCK_DB_NAME= "mockDBName";

    private static final String MOCK_ADX_QUERY= "mockAdxQuery";


    private AzureAdxSourceConnectionService azureAdxSourceConnectionService;

    private TestRunner testRunner;

    @BeforeEach
    public void init() {

        azureAdxSourceProcessor = new AzureAdxSourceProcessor();
        final ProcessorInitializationContext initContext = Mockito.mock(ProcessorInitializationContext.class);
        final String componentId = "componentId";
        when(initContext.getIdentifier()).thenReturn(componentId);
        MockComponentLog componentLog = new MockComponentLog(componentId, azureAdxSourceProcessor);
        when(initContext.getLogger()).thenReturn(componentLog);
        azureAdxSourceProcessor.initialize(initContext);

        final ProcessSessionFactory processSessionFactory = Mockito.mock(ProcessSessionFactory.class);

        sharedState = new SharedSessionState(azureAdxSourceProcessor, new AtomicLong(0));
        processSession = new MockProcessSession(sharedState, azureAdxSourceProcessor);

    }

    @Test
    public void testAzureAdxSourceProcessorSuccess() throws InitializationException, IOException {

        testRunner = TestRunners.newTestRunner(azureAdxSourceProcessor);

        testRunner.setProperty(AzureAdxSourceProcessor.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(AzureAdxSourceProcessor.ADX_QUERY,MOCK_ADX_QUERY);
        testRunner.setProperty(AzureAdxSourceProcessor.ADX_SOURCE_SERVICE,"adx-source-connection-service");

        testRunner.setValidateExpressionUsage(false);

        azureAdxSourceConnectionService = new AzureAdxSourceConnectionService();

        testRunner.addControllerService("adx-source-connection-service", azureAdxSourceConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.APP_ID,"sample");
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.APP_KEY,"sample");
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.APP_TENANT,"sample");
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.CLUSTER_URL, "http://sample.com/");

        testRunner.enableControllerService(azureAdxSourceConnectionService);
        testRunner.assertValid(azureAdxSourceConnectionService);
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(org.apache.nifi.processors.adx.AzureAdxSourceProcessor.RL_SUCCEEDED);
        testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.AzureAdxSourceProcessor.RL_SUCCEEDED);

    }

    @Test
    public void testAzureAdxSourceProcessorFailureParamMissing() throws InitializationException, IOException {

        testRunner = TestRunners.newTestRunner(azureAdxSourceProcessor);

        testRunner.setProperty(AzureAdxSourceProcessor.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(AzureAdxSourceProcessor.ADX_SOURCE_SERVICE,"adx-source-connection-service");

        testRunner.setValidateExpressionUsage(false);

        azureAdxSourceConnectionService = new AzureAdxSourceConnectionService();

        testRunner.addControllerService("adx-source-connection-service", azureAdxSourceConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.APP_ID,"sample");
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.APP_KEY,"sample");
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.APP_TENANT,"sample");
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.CLUSTER_URL, "http://sample.com/");

        testRunner.enableControllerService(azureAdxSourceConnectionService);
        testRunner.assertValid(azureAdxSourceConnectionService);
        assertThrows(AssertionFailedError.class,()->{
            testRunner.run(1);
        });
    }

    @Test
    public void testAzureAdxSourceProcessorFailureInvalidQuery() throws InitializationException, IOException {

        testRunner = TestRunners.newTestRunner(azureAdxSourceProcessor);

        testRunner.setProperty(AzureAdxSourceProcessor.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(AzureAdxSourceProcessor.ADX_SOURCE_SERVICE,"adx-source-connection-service");

        testRunner.setValidateExpressionUsage(false);

        azureAdxSourceConnectionService = new AzureAdxSourceConnectionService();

        testRunner.addControllerService("adx-source-connection-service", azureAdxSourceConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.APP_ID,"sample");
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.APP_KEY,"sample");
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.APP_TENANT,"sample");
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.CLUSTER_URL, "http://sample.com/");

        testRunner.enableControllerService(azureAdxSourceConnectionService);
        testRunner.assertValid(azureAdxSourceConnectionService);
        assertThrows(AssertionFailedError.class,()->{
            testRunner.run(1);
        });
    }

    @Test
    public void testAzureAdxSourceProcessorFailureQueryLimitExceeded() throws InitializationException {

        testRunner = TestRunners.newTestRunner(new MockAzureAdxSourceProcessor());

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
        testRunner.run(1);
        testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.AzureAdxSourceProcessor.RL_FAILED);
    }
}
