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

import com.microsoft.azure.kusto.data.Client;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.ControllerServiceConfiguration;
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

    private AzureAdxSourceConnectionService service;

    private static final String MOCK_APP_ID = "mockAppId";

    private static final String MOCK_APP_KEY = "mockAppKey";

    private static final String MOCK_APP_TENANT = "mockAppTenant";

    private static final String MOCK_CLUSTER_URL = "https://mockClusterUrl.com/";

    @BeforeEach
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(MockAzureAdxSourceProcessor.class);

        service = new AzureAdxSourceConnectionService();
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
        Client executionClient = service.getKustoQueryClient();

        Assertions.assertNotNull(executionClient);
    }

    @Test
    void testPropertyDescriptor() {
        configureAppId();
        configureAppKey();
        configureAppTenant();
        configureClusterURL();
        List<PropertyDescriptor> pd = service.getSupportedPropertyDescriptors();

        assertTrue(pd.contains(AzureAdxSourceConnectionService.APP_ID));
        assertTrue(pd.contains(AzureAdxSourceConnectionService.APP_KEY));
        assertTrue(pd.contains(AzureAdxSourceConnectionService.APP_TENANT));
        assertTrue(pd.contains(AzureAdxSourceConnectionService.CLUSTER_URL));
    }


    @Test
    void testInvalidConnectionMissingProperty() {
        configureAppId();
        configureAppKey();
        configureAppTenant();
        runner.assertNotValid(service);
        runner.setValidateExpressionUsage(false);
        Assertions.assertNull(
                ((ControllerServiceConfiguration)
                        ((Map.Entry<?, ?>) (((StandardProcessorTestRunner)runner).getProcessContext().getControllerServices()).entrySet().toArray()[0])
                                .getValue()).getProperty(AzureAdxSourceConnectionService.CLUSTER_URL));

        assertThrows(IllegalStateException.class, () -> runner.enableControllerService(service));

    }

    private void configureAppId() {
        runner.setProperty(service, AzureAdxSourceConnectionService.APP_ID, MOCK_APP_ID);
    }

    private void configureAppKey() {
        runner.setProperty(service, AzureAdxSourceConnectionService.APP_KEY, MOCK_APP_KEY);
    }

    private void configureAppTenant() {
        runner.setProperty(service, AzureAdxSourceConnectionService.APP_TENANT, MOCK_APP_TENANT);
    }

    private void configureClusterURL() {
        runner.setProperty(service, AzureAdxSourceConnectionService.CLUSTER_URL, MOCK_CLUSTER_URL);
    }
}
