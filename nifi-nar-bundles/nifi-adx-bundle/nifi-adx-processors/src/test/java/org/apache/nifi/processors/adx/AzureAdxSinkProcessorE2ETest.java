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

import org.apache.nifi.adx.AzureAdxConnectionService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

public class AzureAdxSinkProcessorE2ETest {

    private org.apache.nifi.processors.adx.AzureAdxSinkProcessor azureAdxSinkProcessor;

    private AzureAdxConnectionService azureAdxConnectionService;

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        azureAdxSinkProcessor = new org.apache.nifi.processors.adx.AzureAdxSinkProcessor();
    }

    @Test
    public void testAzureAdxIngestProcessorQueuedIngestionSingleNodeNonTransactionalE2E() throws InitializationException, IOException {

        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getProperty("executeE2ETests")));

        testRunner = TestRunners.newTestRunner(azureAdxSinkProcessor);

        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.TABLE_NAME,System.getProperty("tableName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DB_NAME,System.getProperty("databaseName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.MAPPING_NAME,System.getProperty("mappingName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.ADX_SERVICE,"adx-connection-service");

        testRunner.setValidateExpressionUsage(false);

        azureAdxConnectionService = new AzureAdxConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxConnectionService, AzureAdxConnectionService.INGEST_URL,System.getProperty("ingestUrl"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_ID,System.getProperty("appId"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_KEY,System.getProperty("appKey"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_TENANT,System.getProperty("appTenant"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.CLUSTER_URL, System.getProperty("clusterUrl"));

        testRunner.enableControllerService(azureAdxConnectionService);
        testRunner.assertValid(azureAdxConnectionService);
        InputStream inputStream = this.getClass().getResourceAsStream("/fileQueuedSuccess.csv");
        testRunner.enqueue(inputStream);
        assert inputStream != null;
        inputStream.close();
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.RL_SUCCEEDED);

    }

    @Test
    public void testAzureAdxIngestProcessorQueuedIngestionSingleNodeTransactionalE2E() throws InitializationException, IOException {

        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getProperty("executeE2ETests")));

        testRunner = TestRunners.newTestRunner(azureAdxSinkProcessor);

        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.TABLE_NAME,System.getProperty("tableName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DB_NAME,System.getProperty("databaseName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.MAPPING_NAME,System.getProperty("mappingName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.ADX_SERVICE,"adx-connection-service");
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.IS_TRANSACTIONAL, org.apache.nifi.processors.adx.AzureAdxSinkProcessor.TRANSACTIONAL_YES.getValue());

        testRunner.setValidateExpressionUsage(false);

        azureAdxConnectionService = new AzureAdxConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxConnectionService, AzureAdxConnectionService.INGEST_URL,System.getProperty("ingestUrl"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_ID,System.getProperty("appId"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_KEY,System.getProperty("appKey"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_TENANT,System.getProperty("appTenant"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.CLUSTER_URL, System.getProperty("clusterUrl"));

        testRunner.enableControllerService(azureAdxConnectionService);
        testRunner.assertValid(azureAdxConnectionService);
        InputStream inputStream = this.getClass().getResourceAsStream("/fileQueuedSuccess.csv");
        testRunner.enqueue(inputStream);
        assert inputStream != null;
        inputStream.close();
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.RL_SUCCEEDED);

    }

    @Test
    public void testAzureAdxIngestProcessorQueuedIngestionSingleNodeTransactionalFailureE2E() throws InitializationException, IOException {

        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getProperty("executeE2ETests")));

        testRunner = TestRunners.newTestRunner(azureAdxSinkProcessor);

        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.TABLE_NAME,System.getProperty("tableName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DB_NAME,System.getProperty("databaseName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.MAPPING_NAME,System.getProperty("mappingName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.ADX_SERVICE,"adx-connection-service");
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.IS_TRANSACTIONAL, org.apache.nifi.processors.adx.AzureAdxSinkProcessor.TRANSACTIONAL_YES.getValue());

        testRunner.setValidateExpressionUsage(false);

        azureAdxConnectionService = new AzureAdxConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxConnectionService, AzureAdxConnectionService.INGEST_URL,System.getProperty("ingestUrl"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_ID,System.getProperty("appId"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_KEY,System.getProperty("appKey"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_TENANT,System.getProperty("appTenant"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.CLUSTER_URL, System.getProperty("clusterUrl"));

        testRunner.enableControllerService(azureAdxConnectionService);
        testRunner.assertValid(azureAdxConnectionService);
        InputStream inputStream = this.getClass().getResourceAsStream("/fileFailure.csv");
        testRunner.enqueue(inputStream);
        assert inputStream != null;
        inputStream.close();
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.RL_FAILED);

    }

    @Test
    public void testAzureAdxIngestProcessorStreamingIngestionE2E() throws InitializationException, IOException {

        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getProperty("executeE2ETests")));

        testRunner = TestRunners.newTestRunner(azureAdxSinkProcessor);

        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.TABLE_NAME,System.getProperty("tableName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DB_NAME,System.getProperty("databaseName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.MAPPING_NAME,System.getProperty("mappingName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.ADX_SERVICE,"adx-connection-service");
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.IS_STREAMING_ENABLED, "true");

        testRunner.setValidateExpressionUsage(false);

        azureAdxConnectionService = new AzureAdxConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxConnectionService, AzureAdxConnectionService.INGEST_URL,System.getProperty("ingestUrl"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_ID,System.getProperty("appId"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_KEY,System.getProperty("appKey"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_TENANT,System.getProperty("appTenant"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.CLUSTER_URL, System.getProperty("clusterUrl"));

        testRunner.enableControllerService(azureAdxConnectionService);
        testRunner.assertValid(azureAdxConnectionService);
        InputStream inputStream = this.getClass().getResourceAsStream("/fileStreaming.csv");
        testRunner.enqueue(inputStream);
        assert inputStream != null;
        inputStream.close();
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.RL_SUCCEEDED);

    }

    @Test
    public void testAzureAdxIngestProcessorStreamingIngestionFailureE2E() throws InitializationException, IOException {

        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getProperty("executeE2ETests")));

        testRunner = TestRunners.newTestRunner(azureAdxSinkProcessor);

        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.TABLE_NAME,System.getProperty("tableName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DB_NAME,System.getProperty("databaseName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.MAPPING_NAME,System.getProperty("mappingName"));
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.ADX_SERVICE,"adx-connection-service");
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.IS_STREAMING_ENABLED, "true");

        testRunner.setValidateExpressionUsage(false);

        azureAdxConnectionService = new AzureAdxConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxConnectionService, AzureAdxConnectionService.INGEST_URL,System.getProperty("ingestUrl"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_ID,System.getProperty("appId"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_KEY,System.getProperty("appKey"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_TENANT,System.getProperty("appTenant"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.CLUSTER_URL, System.getProperty("clusterUrl"));

        testRunner.enableControllerService(azureAdxConnectionService);
        testRunner.assertValid(azureAdxConnectionService);
        InputStream inputStream = this.getClass().getResourceAsStream("/fileFailure.csv");
        testRunner.enqueue(inputStream);
        assert inputStream != null;
        inputStream.close();
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.RL_FAILED);

    }


}
