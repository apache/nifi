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

import com.microsoft.azure.kusto.data.StreamingClient;
import org.apache.nifi.adx.service.StandardKustoQueryService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.ControllerServiceConfiguration;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.StandardProcessorTestRunner;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestAzureAdxSourceConnectionService {

    private TestRunner runner;

    private StandardKustoQueryService service;

    private static final String MOCK_APP_ID = "mockAppId";

    private static final String MOCK_APP_KEY = "mockAppKey";

    private static final String MOCK_APP_TENANT = "mockAppTenant";

    private static final String MOCK_CLUSTER_URL = "https://mockClusterUrl.com/";

    @BeforeEach
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new StandardKustoQueryService();
        runner.addControllerService("test-good", service);
    }

    @AfterEach
    public void after() {
        runner.clearProperties();
    }

    /**
     * test successful adx connection scenario where all valid parameters are passed
     */
    @Test
    void testAdxConnectionController() {
        configureAppId();
        configureAppKey();
        configureAppTenant();
        configureClusterURL();

        runner.assertValid(service);
    }

    /**
     * test successful adx connection scenario where all valid parameters are passed
     */
    @Test
    void testCreateExecutionClientSuccess() {
        configureAppId();
        configureAppKey();
        configureAppTenant();
        configureClusterURL();
        runner.assertValid(service);
        runner.setValidateExpressionUsage(false);
        runner.enableControllerService(service);
        StreamingClient executionClient = service.getKustoQueryClient();

        Assertions.assertNotNull(executionClient);
    }

    @Test
    void testPropertyDescriptor() {
        configureAppId();
        configureAppKey();
        configureAppTenant();
        configureClusterURL();
        List<PropertyDescriptor> pd = service.getSupportedPropertyDescriptors();

        assertTrue(pd.contains(StandardKustoQueryService.APP_ID));
        assertTrue(pd.contains(StandardKustoQueryService.APP_KEY));
        assertTrue(pd.contains(StandardKustoQueryService.APP_TENANT));
        assertTrue(pd.contains(StandardKustoQueryService.CLUSTER_URL));
    }


    @Test
    void testInvalidConnectionMissingProperty() {
        configureAppId();
        configureAppKey();
        configureAppTenant();
        runner.assertNotValid(service);
        runner.setValidateExpressionUsage(false);
        MockProcessContext mockProcessContext = ((StandardProcessorTestRunner)runner).getProcessContext();
        Map.Entry<?, ?> entryMap = (Map.Entry<?, ?>) mockProcessContext.getControllerServices().entrySet().toArray()[0];
        ControllerServiceConfiguration controllerServiceConfiguration = (ControllerServiceConfiguration)entryMap.getValue();
        String clusterURL = controllerServiceConfiguration.getProperty(StandardKustoQueryService.CLUSTER_URL);
        Assertions.assertNull(clusterURL);
        assertThrows(IllegalStateException.class, () -> runner.enableControllerService(service));
    }

    private void configureAppId() {
        runner.setProperty(service, StandardKustoQueryService.APP_ID, MOCK_APP_ID);
    }

    private void configureAppKey() {
        runner.setProperty(service, StandardKustoQueryService.APP_KEY, MOCK_APP_KEY);
    }

    private void configureAppTenant() {
        runner.setProperty(service, StandardKustoQueryService.APP_TENANT, MOCK_APP_TENANT);
    }

    private void configureClusterURL() {
        runner.setProperty(service, StandardKustoQueryService.CLUSTER_URL, MOCK_CLUSTER_URL);
    }
}
