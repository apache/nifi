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
package org.apache.nifi.adx;

import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.ManagedStreamingIngestClient;
import com.microsoft.azure.kusto.ingest.QueuedIngestClient;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestAzureAdxSinkConnectionService {

    private TestRunner runner;

    private AzureAdxSinkSinkConnectionService service;

    private static final String MOCK_URI = "https://mockURI:443/";
    private static final String MOCK_APP_ID = "mockAppId";

    private static final String MOCK_APP_KEY = "mockAppKey";

    private static final String MOCK_APP_TENANT = "mockAppTenant";

    private static final String MOCK_CLUSTER_URL = "https://mockClusterUrl.com/";

    @BeforeEach
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(TestAzureAdxIngestProcessor.class);

        service = new AzureAdxSinkSinkConnectionService();
        runner.addControllerService("test-good", service);

    }

    @AfterEach
    public void after() {
        runner.clearProperties();
    }

    @Test
    public void testAdxConnectionControllerWithoutStreaming() {
        configureIngestURL();
        configureAppId();
        configureAppKey();
        configureAppTenant();
        configureClusterURL();

        runner.assertValid(service);
    }

    private void configureIngestURL() {
        runner.setProperty(service, AzureAdxSinkSinkConnectionService.INGEST_URL, MOCK_URI);
    }

    private void configureAppId() {
        runner.setProperty(service, AzureAdxSinkSinkConnectionService.APP_ID, MOCK_APP_ID);
    }

    private void configureAppKey() {
        runner.setProperty(service, AzureAdxSinkSinkConnectionService.APP_KEY, MOCK_APP_KEY);
    }

    private void configureAppTenant() {
        runner.setProperty(service, AzureAdxSinkSinkConnectionService.APP_TENANT, MOCK_APP_TENANT);
    }

    private void configureClusterURL() {
        runner.setProperty(service, AzureAdxSinkSinkConnectionService.CLUSTER_URL, MOCK_CLUSTER_URL);
    }

    @Test
    public void testCreateIngestClientQueued(){

        configureIngestURL();
        configureAppId();
        configureAppKey();
        configureAppTenant();
        configureClusterURL();

        runner.assertValid(service);
        runner.setValidateExpressionUsage(false);

        runner.enableControllerService(service);

        IngestClient ingestClient = service.getAdxClient(false);
        Assertions.assertNotNull(ingestClient);
        Assertions.assertTrue(ingestClient instanceof QueuedIngestClient);

    }

    @Test
    public void testPropertyDescriptor(){

        configureIngestURL();
        configureAppId();
        configureAppKey();
        configureAppTenant();

        List<PropertyDescriptor> pd = service.getSupportedPropertyDescriptors();
        assertTrue(pd.contains(AzureAdxSinkSinkConnectionService.APP_ID));
        assertTrue(pd.contains(AzureAdxSinkSinkConnectionService.APP_KEY));
        assertTrue(pd.contains(AzureAdxSinkSinkConnectionService.INGEST_URL));
        assertTrue(pd.contains(AzureAdxSinkSinkConnectionService.APP_TENANT));
        assertTrue(pd.contains(AzureAdxSinkSinkConnectionService.CLUSTER_URL));
    }


    @Test
    public void testCreateIngestClientWithStreaming(){

        configureIngestURL();
        configureAppId();
        configureAppKey();
        configureAppTenant();
        configureClusterURL();

        runner.assertValid(service);
        runner.setValidateExpressionUsage(false);

        runner.enableControllerService(service);

        IngestClient ingestClient = service.getAdxClient(true);
        Assertions.assertNotNull(ingestClient);
        Assertions.assertTrue(ingestClient instanceof ManagedStreamingIngestClient);

    }


}
