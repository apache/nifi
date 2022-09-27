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


import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import org.apache.nifi.adx.AdxConnectionService;
import org.apache.nifi.adx.AzureAdxConnectionService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockComponentLog;
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
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


public class AzureAdxIngestProcessorTest {

    private AzureAdxIngestProcessor azureAdxIngestProcessor;

    private MockProcessSession processSession;

    private SharedSessionState sharedState;


    private static final String MOCK_DB_NAME= "mockDBName";

    private static final String MOCK_TABLE_NAME= "mockTableName";

    private static final String MOCK_MAPPING_NAME= "mockMappingName";

    private AzureAdxConnectionService azureAdxConnectionService;

    private TestRunner testRunner;


    @BeforeEach
    public void init() {

        azureAdxIngestProcessor = new AzureAdxIngestProcessor();
        final ProcessorInitializationContext initContext = Mockito.mock(ProcessorInitializationContext.class);
        final String componentId = "componentId";
        when(initContext.getIdentifier()).thenReturn(componentId);
        MockComponentLog componentLog = new MockComponentLog(componentId, azureAdxIngestProcessor);
        when(initContext.getLogger()).thenReturn(componentLog);
        azureAdxIngestProcessor.initialize(initContext);


        final ProcessSessionFactory processSessionFactory = Mockito.mock(ProcessSessionFactory.class);

        sharedState = new SharedSessionState(azureAdxIngestProcessor, new AtomicLong(0));
        processSession = new MockProcessSession(sharedState, azureAdxIngestProcessor);

        when(processSessionFactory.createSession()).thenReturn(processSession);

        testRunner = TestRunners.newTestRunner(AzureAdxIngestProcessor.class);

    }

    @Test
    public void testAzureAdxIngestProcessorQueuedStreaming() throws InitializationException {

        testRunner.setProperty(AzureAdxIngestProcessor.TABLE_NAME,MOCK_TABLE_NAME);
        testRunner.setProperty(AzureAdxIngestProcessor.IM_KIND,AzureAdxIngestProcessor.IM_KIND_CSV);
        testRunner.setProperty(AzureAdxIngestProcessor.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(AzureAdxIngestProcessor.MAPPING_NAME,MOCK_MAPPING_NAME);
        testRunner.setProperty(AzureAdxIngestProcessor.DATA_FORMAT,"CSV");
        testRunner.setProperty(AzureAdxIngestProcessor.IR_LEVEL,"IRL_FAS");
        testRunner.setProperty(AzureAdxIngestProcessor.WAIT_FOR_STATUS,"ST_SUCCESS");
        testRunner.setProperty(AzureAdxIngestProcessor.IR_METHOD,"IRM_TABLE");
        testRunner.setProperty(AzureAdxIngestProcessor.ADX_SERVICE,"adx-connection-service");


        testRunner.setValidateExpressionUsage(false);

        azureAdxConnectionService = new AzureAdxConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.INGEST_URL,"http://ingest-sample.com/");
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_ID,"sample");
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_KEY,"sample");
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_TENANT,"sample");

        testRunner.enableControllerService(azureAdxConnectionService);
        testRunner.assertValid(azureAdxConnectionService);
        testRunner.run(1);
        testRunner.assertAllFlowFilesTransferred(AzureAdxIngestProcessor.RL_SUCCEEDED);

    }

    @Test
    public void testAzureAdxIngestProcessorManagedStreaming() throws InitializationException {

        testRunner.setProperty(AzureAdxIngestProcessor.TABLE_NAME,MOCK_TABLE_NAME);
        testRunner.setProperty(AzureAdxIngestProcessor.IM_KIND,AzureAdxIngestProcessor.IM_KIND_CSV);
        testRunner.setProperty(AzureAdxIngestProcessor.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(AzureAdxIngestProcessor.MAPPING_NAME,MOCK_MAPPING_NAME);
        testRunner.setProperty(AzureAdxIngestProcessor.DATA_FORMAT,"CSV");
        testRunner.setProperty(AzureAdxIngestProcessor.IR_LEVEL,"IRL_FAS");
        testRunner.setProperty(AzureAdxIngestProcessor.WAIT_FOR_STATUS,"ST_SUCCESS");
        testRunner.setProperty(AzureAdxIngestProcessor.IR_METHOD,"IRM_TABLE");
        testRunner.setProperty(AzureAdxIngestProcessor.ADX_SERVICE,"adx-connection-service");


        testRunner.setValidateExpressionUsage(false);

        azureAdxConnectionService = new AzureAdxConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.INGEST_URL,"https://ingest-sample.com/");
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_ID,"sample");
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_KEY,"sample");
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_TENANT,"sample");
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.IS_STREAMING_ENABLED, "true");
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.CLUSTER_URL,"https://sample-cluster.com/");

        testRunner.enableControllerService(azureAdxConnectionService);
        testRunner.assertValid(azureAdxConnectionService);
        testRunner.run(1);
        testRunner.assertAllFlowFilesTransferred(AzureAdxIngestProcessor.RL_SUCCEEDED);

    }

    @Test
    public void testGetPropertyDescriptors() {

        testRunner.setProperty(AzureAdxIngestProcessor.TABLE_NAME,MOCK_TABLE_NAME);
        testRunner.setProperty(AzureAdxIngestProcessor.IM_KIND,AzureAdxIngestProcessor.IM_KIND_CSV);
        testRunner.setProperty(AzureAdxIngestProcessor.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(AzureAdxIngestProcessor.MAPPING_NAME,MOCK_MAPPING_NAME);
        testRunner.setProperty(AzureAdxIngestProcessor.DATA_FORMAT,"CSV");
        testRunner.setProperty(AzureAdxIngestProcessor.IR_LEVEL,"IRL_FAS");
        testRunner.setProperty(AzureAdxIngestProcessor.WAIT_FOR_STATUS,"ST_SUCCESS");
        testRunner.setProperty(AzureAdxIngestProcessor.IR_METHOD,"IRM_TABLE");
        testRunner.setProperty(AzureAdxIngestProcessor.ADX_SERVICE,"adx-connection-service");



        List<PropertyDescriptor> pd = azureAdxIngestProcessor.getSupportedPropertyDescriptors();
        assertTrue(pd.contains(AzureAdxIngestProcessor.ADX_SERVICE));
        assertTrue(pd.contains(AzureAdxIngestProcessor.DATA_FORMAT));
        assertTrue(pd.contains(AzureAdxIngestProcessor.DB_NAME));
        assertTrue(pd.contains(AzureAdxIngestProcessor.IM_KIND));
        assertTrue(pd.contains(AzureAdxIngestProcessor.IR_METHOD));
        assertTrue(pd.contains(AzureAdxIngestProcessor.FLUSH_IMMEDIATE));
        assertTrue(pd.contains(AzureAdxIngestProcessor.MAPPING_NAME));
        assertTrue(pd.contains(AzureAdxIngestProcessor.IR_LEVEL));
        assertTrue(pd.contains(AzureAdxIngestProcessor.TABLE_NAME));
        assertTrue(pd.contains(AzureAdxIngestProcessor.WAIT_FOR_STATUS));
    }

    @Test
    public void testMissingPropertyValuesOfAdxIngestProcessor() throws InitializationException {

        //missing property tableName
        testRunner.setProperty(AzureAdxIngestProcessor.IM_KIND,AzureAdxIngestProcessor.IM_KIND_CSV);
        testRunner.setProperty(AzureAdxIngestProcessor.DB_NAME,MOCK_DB_NAME);
        //testRunner.setProperty(AzureAdxIngestProcessor.MAPPING_NAME,MOCK_MAPPING_NAME);
        testRunner.setProperty(AzureAdxIngestProcessor.DATA_FORMAT,"CSV");
        testRunner.setProperty(AzureAdxIngestProcessor.IR_LEVEL,"IRL_FAS");
        testRunner.setProperty(AzureAdxIngestProcessor.WAIT_FOR_STATUS,"ST_SUCCESS");
        testRunner.setProperty(AzureAdxIngestProcessor.IR_METHOD,"IRM_TABLE");
        testRunner.setProperty(AzureAdxIngestProcessor.ADX_SERVICE,"adx-connection-service");


        testRunner.setValidateExpressionUsage(false);

        azureAdxConnectionService = new AzureAdxConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.INGEST_URL,"http://ingest-sample.com/");
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_ID,"sample");
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_KEY,"sample");
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_TENANT,"sample");

        testRunner.enableControllerService(azureAdxConnectionService);
        testRunner.assertValid(azureAdxConnectionService);
        AssertionFailedError assertionFailedError = assertThrows(AssertionFailedError.class,()->{
            testRunner.run(1);
        });
        assertTrue(assertionFailedError.getMessage().contains("Table Name"));
        assertTrue(assertionFailedError.getMessage().contains("invalid"));
    }

    @Test
    public void testMissingPropertyValuesOfAdxConnectionService() throws InitializationException {

        //missing property tableName
        testRunner.setProperty(AzureAdxIngestProcessor.IM_KIND,AzureAdxIngestProcessor.IM_KIND_CSV);
        testRunner.setProperty(AzureAdxIngestProcessor.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(AzureAdxIngestProcessor.MAPPING_NAME,MOCK_MAPPING_NAME);
        testRunner.setProperty(AzureAdxIngestProcessor.DATA_FORMAT,"CSV");
        testRunner.setProperty(AzureAdxIngestProcessor.IR_LEVEL,"IRL_FAS");
        testRunner.setProperty(AzureAdxIngestProcessor.WAIT_FOR_STATUS,"ST_SUCCESS");
        testRunner.setProperty(AzureAdxIngestProcessor.IR_METHOD,"IRM_TABLE");
        testRunner.setProperty(AzureAdxIngestProcessor.ADX_SERVICE,"adx-connection-service");


        testRunner.setValidateExpressionUsage(false);

        azureAdxConnectionService = new AzureAdxConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxConnectionService, new HashMap<>());

        //missing ingest url required for connection service
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_ID,"sample");
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_KEY,"sample");
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_TENANT,"sample");

        IllegalStateException illegalStateException =Assertions.assertThrows(IllegalStateException.class, () -> {
            testRunner.enableControllerService(azureAdxConnectionService);
        });

        assertTrue(illegalStateException.getMessage().contains("Ingest URL"));
        assertTrue(illegalStateException.getMessage().contains("invalid"));

    }

    @Test
    public void testIngestionFailure() throws InitializationException, IngestionClientException, IngestionServiceException {

        testRunner.setProperty(AzureAdxIngestProcessor.TABLE_NAME,MOCK_TABLE_NAME);
        testRunner.setProperty(AzureAdxIngestProcessor.IM_KIND,AzureAdxIngestProcessor.IM_KIND_CSV);
        testRunner.setProperty(AzureAdxIngestProcessor.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(AzureAdxIngestProcessor.MAPPING_NAME,MOCK_MAPPING_NAME);
        testRunner.setProperty(AzureAdxIngestProcessor.DATA_FORMAT,"CSV");
        testRunner.setProperty(AzureAdxIngestProcessor.IR_LEVEL,"IRL_FAS");
        testRunner.setProperty(AzureAdxIngestProcessor.WAIT_FOR_STATUS,"ST_SUCCESS");
        testRunner.setProperty(AzureAdxIngestProcessor.IR_METHOD,"IRM_TABLE");
        testRunner.setProperty(AzureAdxIngestProcessor.ADX_SERVICE,"adx-connection-service");


        testRunner.setValidateExpressionUsage(false);

        azureAdxConnectionService = new AzureAdxConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.INGEST_URL,"http://ingest-sample.com/");
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_ID,"sample");
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_KEY,"sample");
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_TENANT,"sample");

        testRunner.enableControllerService(azureAdxConnectionService);
        testRunner.assertValid(azureAdxConnectionService);


        AdxConnectionService adxConnectionServiceFromProcessContext = (AdxConnectionService) testRunner.getProcessContext().getControllerServiceLookup().getControllerService("adx-connection-service");
        assertNotNull(adxConnectionServiceFromProcessContext);

        InputStream stream = new ByteArrayInputStream("exampleString".getBytes(StandardCharsets.UTF_8));
        testRunner.enqueue(stream);
        try {
            testRunner.run(1);
        }catch (AssertionError e){
            Assertions.assertNotNull(e);
            Assertions.assertNotNull(e.getCause());
            Assertions.assertNotNull(e.getMessage());
            Assertions.assertTrue(e.getCause() instanceof  ProcessException);
            Assertions.assertTrue(e.getMessage().contains("Ingestion failed likely because the wrong endpoint"));
            Assertions.assertTrue(e.getCause().getCause() instanceof IngestionClientException);
        }
        testRunner.assertAllFlowFilesTransferred(AzureAdxIngestProcessor.RL_FAILED);


    }



}
