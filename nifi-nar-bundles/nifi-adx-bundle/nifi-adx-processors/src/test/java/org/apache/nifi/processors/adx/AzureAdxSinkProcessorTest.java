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


import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import org.apache.nifi.adx.AdxSinkConnectionService;
import org.apache.nifi.adx.AzureAdxSinkSinkConnectionService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opentest4j.AssertionFailedError;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class AzureAdxSinkProcessorTest {

    private org.apache.nifi.processors.adx.AzureAdxSinkProcessor azureAdxSinkProcessor;

    private MockProcessSession processSession;

    private SharedSessionState sharedState;


    private static final String MOCK_DB_NAME= "mockDBName";

    private static final String MOCK_TABLE_NAME= "mockTableName";

    private static final String MOCK_MAPPING_NAME= "mockMappingName";

    private AzureAdxSinkSinkConnectionService azureAdxSinkConnectionService;

    private TestRunner testRunner;


    @BeforeEach
    public void init() {

        azureAdxSinkProcessor = new MockAzureAdxSinkProcessor();
        final ProcessorInitializationContext initContext = Mockito.mock(ProcessorInitializationContext.class);
        final String componentId = "componentId";
        when(initContext.getIdentifier()).thenReturn(componentId);
        MockComponentLog componentLog = new MockComponentLog(componentId, azureAdxSinkProcessor);
        when(initContext.getLogger()).thenReturn(componentLog);
        azureAdxSinkProcessor.initialize(initContext);

        final ProcessSessionFactory processSessionFactory = Mockito.mock(ProcessSessionFactory.class);

        sharedState = new SharedSessionState(azureAdxSinkProcessor, new AtomicLong(0));
        processSession = new MockProcessSession(sharedState, azureAdxSinkProcessor);



    }

    @Test
    public void testAzureAdxIngestProcessorQueuedIngestionNonTransactional() throws InitializationException, IOException {

        testRunner = TestRunners.newTestRunner(azureAdxSinkProcessor);

        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.TABLE_NAME,MOCK_TABLE_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.MAPPING_NAME,MOCK_MAPPING_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.ADX_SINK_SERVICE,"adx-connection-service");

        testRunner.setValidateExpressionUsage(false);

        azureAdxSinkConnectionService = new MockAzureAdxSinkSinkConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxSinkConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.INGEST_URL,"http://ingest-sample.com/");
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.APP_ID,"sample");
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.APP_KEY,"sample");
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.APP_TENANT,"sample");
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.CLUSTER_URL, "http://sample.com/");

        testRunner.enableControllerService(azureAdxSinkConnectionService);
        testRunner.assertValid(azureAdxSinkConnectionService);
        InputStream inputStream = this.getClass().getResourceAsStream("/fileQueuedSuccess.csv");
        testRunner.enqueue(inputStream);
        assert inputStream != null;
        inputStream.close();
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.RL_SUCCEEDED);
        testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.RL_SUCCEEDED);

    }

    @Test
    public void testAzureAdxIngestProcessorQueuedIngestionTransactional() throws InitializationException, DataServiceException, DataClientException, IOException {

        testRunner = TestRunners.newTestRunner(azureAdxSinkProcessor);

        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.TABLE_NAME,MOCK_TABLE_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.MAPPING_NAME,MOCK_MAPPING_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.ADX_SINK_SERVICE,"adx-connection-service");
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.IS_TRANSACTIONAL, org.apache.nifi.processors.adx.AzureAdxSinkProcessor.TRANSACTIONAL_YES);

        testRunner.setValidateExpressionUsage(false);

        azureAdxSinkConnectionService = new MockAzureAdxSinkSinkConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxSinkConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.INGEST_URL,"http://ingest-sample.com/");
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.APP_ID,"sample");
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.APP_KEY,"sample");
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.APP_TENANT,"sample");
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.CLUSTER_URL, "http://sample.com/");
        testRunner.enableControllerService(azureAdxSinkConnectionService);
        testRunner.assertValid(azureAdxSinkConnectionService);
        InputStream inputStream = this.getClass().getResourceAsStream("/fileQueuedSuccess.csv");
        testRunner.enqueue(inputStream);
        assert inputStream != null;
        inputStream.close();
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.RL_SUCCEEDED);
        testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.RL_SUCCEEDED);

    }

    @Test
    public void testAzureAdxIngestProcessorManagedStreaming() throws InitializationException, DataServiceException, DataClientException {

        testRunner = TestRunners.newTestRunner(azureAdxSinkProcessor);

        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.TABLE_NAME,MOCK_TABLE_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.MAPPING_NAME,MOCK_MAPPING_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.ADX_SINK_SERVICE,"adx-connection-service");
        testRunner.setProperty(AzureAdxSinkProcessor.IS_STREAMING_ENABLED,"true");

        testRunner.setValidateExpressionUsage(false);

        azureAdxSinkConnectionService = new MockAzureAdxSinkSinkConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxSinkConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.INGEST_URL,"https://ingest-sample.com/");
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.APP_ID,"sample");
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.APP_KEY,"sample");
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.APP_TENANT,"sample");
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.CLUSTER_URL,"https://sample-cluster.com/");

        testRunner.enableControllerService(azureAdxSinkConnectionService);

        testRunner.assertValid(azureAdxSinkConnectionService);

        testRunner.run(1);
        testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.RL_SUCCEEDED);

    }

    @Test
    public void testGetPropertyDescriptors() {

        testRunner = TestRunners.newTestRunner(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.class);

        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.TABLE_NAME,MOCK_TABLE_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.MAPPING_NAME,MOCK_MAPPING_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.ADX_SINK_SERVICE,"adx-connection-service");


        List<PropertyDescriptor> pd = azureAdxSinkProcessor.getSupportedPropertyDescriptors();
        assertTrue(pd.contains(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.ADX_SINK_SERVICE));
        assertTrue(pd.contains(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DATA_FORMAT));
        assertTrue(pd.contains(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DB_NAME));
        assertTrue(pd.contains(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.FLUSH_IMMEDIATE));
        assertTrue(pd.contains(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.MAPPING_NAME));
        assertTrue(pd.contains(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.TABLE_NAME));
    }

    @Test
    public void testMissingPropertyValuesOfAdxIngestProcessor() throws InitializationException {

        testRunner = TestRunners.newTestRunner(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.class);

        //missing property tableName
        //testRunner.setProperty(AzureAdxIngestProcessor.IM_KIND,AzureAdxIngestProcessor.IM_KIND_CSV);
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DB_NAME,MOCK_DB_NAME);
        //testRunner.setProperty(AzureAdxIngestProcessor.MAPPING_NAME,MOCK_MAPPING_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.ADX_SINK_SERVICE,"adx-connection-service");


        testRunner.setValidateExpressionUsage(false);

        azureAdxSinkConnectionService = new AzureAdxSinkSinkConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxSinkConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.INGEST_URL,"http://ingest-sample.com/");
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.APP_ID,"sample");
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.APP_KEY,"sample");
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.APP_TENANT,"sample");
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.CLUSTER_URL, "http://sample.com/");

        testRunner.enableControllerService(azureAdxSinkConnectionService);
        testRunner.assertValid(azureAdxSinkConnectionService);
        AssertionFailedError assertionFailedError = assertThrows(AssertionFailedError.class,()->{
            testRunner.run(1);
        });
        assertTrue(assertionFailedError.getMessage().contains("Table Name"));
        assertTrue(assertionFailedError.getMessage().contains("invalid"));
    }

    @Test
    public void testMissingPropertyValuesOfAdxConnectionService() throws InitializationException {

        testRunner = TestRunners.newTestRunner(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.class);

        //missing property tableName
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.MAPPING_NAME,MOCK_MAPPING_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.ADX_SINK_SERVICE,"adx-connection-service");


        testRunner.setValidateExpressionUsage(false);

        azureAdxSinkConnectionService = new AzureAdxSinkSinkConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxSinkConnectionService, new HashMap<>());

        //missing ingest url required for connection service
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.APP_ID,"sample");
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.APP_KEY,"sample");
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.APP_TENANT,"sample");

        IllegalStateException illegalStateException =Assertions.assertThrows(IllegalStateException.class, () -> {
            testRunner.enableControllerService(azureAdxSinkConnectionService);
        });

        assertTrue(illegalStateException.getMessage().contains("Ingest URL"));
        assertTrue(illegalStateException.getMessage().contains("invalid"));

    }

    @Test
    public void testIngestionFailure() throws InitializationException, IngestionClientException, IngestionServiceException {

        testRunner = TestRunners.newTestRunner(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.class);

        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.TABLE_NAME,MOCK_TABLE_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.MAPPING_NAME,MOCK_MAPPING_NAME);
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.DATA_FORMAT,"CSV");
        testRunner.setProperty(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.ADX_SINK_SERVICE,"adx-connection-service");


        testRunner.setValidateExpressionUsage(false);

        azureAdxSinkConnectionService = new AzureAdxSinkSinkConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxSinkConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.INGEST_URL,"http://ingest-sample.com/");
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.APP_ID,"sample");
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.APP_KEY,"sample");
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.APP_TENANT,"sample");
        testRunner.setProperty(azureAdxSinkConnectionService, AzureAdxSinkSinkConnectionService.CLUSTER_URL, "http://sample.com/");

        testRunner.enableControllerService(azureAdxSinkConnectionService);
        testRunner.assertValid(azureAdxSinkConnectionService);

        AdxSinkConnectionService adxSinkConnectionServiceFromProcessContext = (AdxSinkConnectionService) testRunner.getProcessContext().getControllerServiceLookup().getControllerService("adx-connection-service");
        assertNotNull(adxSinkConnectionServiceFromProcessContext);

        InputStream stream = new ByteArrayInputStream("exampleString".getBytes(StandardCharsets.UTF_8));
        testRunner.enqueue(stream);
        try {
            testRunner.run(1);
        }catch (AssertionError e){
            Assertions.assertNotNull(e);
            Assertions.assertNotNull(e.getCause());
            Assertions.assertNotNull(e.getMessage());
            Assertions.assertTrue(e.getCause() instanceof  ProcessException);
        }
        testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.RL_FAILED);


    }



}
