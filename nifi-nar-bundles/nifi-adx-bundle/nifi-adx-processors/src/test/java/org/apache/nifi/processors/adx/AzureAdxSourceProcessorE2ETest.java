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

import org.apache.nifi.adx.AzureAdxSinkConnectionService;
import org.apache.nifi.adx.AzureAdxSourceConnectionService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;

public class AzureAdxSourceProcessorE2ETest {

    private AzureAdxSourceProcessor azureAdxSourceProcessor;

    private AzureAdxSourceConnectionService azureAdxSourceConnectionService;

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        azureAdxSourceProcessor = new AzureAdxSourceProcessor();
    }

    @Test
    public void testAzureAdxSourceProcessorSuccessE2E() throws InitializationException, IOException {

        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getProperty("executeE2ETests")));

        testRunner = TestRunners.newTestRunner(azureAdxSourceProcessor);

        testRunner.setProperty(AzureAdxSourceProcessor.ADX_QUERY,System.getProperty("adxQuery"));
        testRunner.setProperty(AzureAdxSourceProcessor.DB_NAME,System.getProperty("databaseName"));
        testRunner.setProperty(AzureAdxSourceProcessor.ADX_SOURCE_SERVICE,"adx-connection-service");

        testRunner.setValidateExpressionUsage(false);

        azureAdxSourceConnectionService = new AzureAdxSourceConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxSourceConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.APP_ID,System.getProperty("appId"));
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.APP_KEY,System.getProperty("appKey"));
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.APP_TENANT,System.getProperty("appTenant"));
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.CLUSTER_URL, System.getProperty("clusterUrl"));

        testRunner.enableControllerService(azureAdxSourceConnectionService);
        testRunner.assertValid(azureAdxSourceConnectionService);
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.RL_SUCCEEDED);

    }

    @Test
    public void testAzureAdxIngestProcessorFailureE2E() throws InitializationException, IOException {

        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getProperty("executeE2ETests")));

        testRunner = TestRunners.newTestRunner(azureAdxSourceProcessor);

        testRunner.setProperty(AzureAdxSourceProcessor.ADX_QUERY,System.getProperty("adxQueryLimit"));
        testRunner.setProperty(AzureAdxSourceProcessor.DB_NAME,System.getProperty("databaseName"));
        testRunner.setProperty(AzureAdxSourceProcessor.ADX_SOURCE_SERVICE,"adx-connection-service");

        testRunner.setValidateExpressionUsage(false);

        azureAdxSourceConnectionService = new AzureAdxSourceConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxSourceConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.APP_ID,System.getProperty("appId"));
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.APP_KEY,System.getProperty("appKey"));
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.APP_TENANT,System.getProperty("appTenant"));
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.CLUSTER_URL, System.getProperty("clusterUrl"));

        testRunner.enableControllerService(azureAdxSourceConnectionService);
        testRunner.assertValid(azureAdxSourceConnectionService);
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        testRunner.assertAllFlowFilesTransferred(AzureAdxSinkProcessor.RL_FAILED);

    }

    @Test
    public void testAzureAdxIngestProcessorQueuedIngestionSingleNodeTransactionalFailureE2E() throws InitializationException, IOException {

        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getProperty("executeE2ETests")));

        testRunner = TestRunners.newTestRunner(azureAdxSourceProcessor);

        testRunner.setProperty(AzureAdxSourceProcessor.ADX_QUERY,System.getProperty("adxQuery"));
        testRunner.setProperty(AzureAdxSourceProcessor.DB_NAME,System.getProperty("databaseName"));
        testRunner.setProperty(AzureAdxSourceProcessor.ADX_SOURCE_SERVICE,"adx-connection-service");

        testRunner.setValidateExpressionUsage(false);

        azureAdxSourceConnectionService = new AzureAdxSourceConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxSourceConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.APP_ID,System.getProperty("appId"));
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.APP_KEY,System.getProperty("appKey"));
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.APP_TENANT,System.getProperty("appTenant"));
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.CLUSTER_URL, System.getProperty("clusterUrl"));

        testRunner.enableControllerService(azureAdxSourceConnectionService);
        testRunner.assertValid(azureAdxSourceConnectionService);
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        testRunner.assertAllFlowFilesTransferred(AzureAdxSinkProcessor.RL_FAILED);

    }

}
