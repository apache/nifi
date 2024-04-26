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
package org.apache.nifi.services.azure.data.explorer;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

public class StandardKustoQueryServiceTest {
    private static final String SERVICE_ID = StandardKustoQueryServiceTest.class.getSimpleName();

    private static final String CLUSTER_URI = "https://cluster.region.kusto.windows.net";

    private static final String APPLICATION_CLIENT_ID = String.class.getSimpleName();

    private StandardKustoQueryService service;

    private TestRunner runner;

    @BeforeEach
    void setRunner() {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new StandardKustoQueryService();
    }

    @Test
    void testProperties() throws InitializationException {
        runner.addControllerService(SERVICE_ID, service);

        runner.assertNotValid(service);

        runner.setProperty(service, StandardKustoQueryService.CLUSTER_URI, CLUSTER_URI);
        runner.setProperty(service, StandardKustoQueryService.APPLICATION_CLIENT_ID, APPLICATION_CLIENT_ID);

        runner.assertValid(service);
    }

    @Test
    void testEnableManagedIdentity() throws InitializationException {
        runner.addControllerService(SERVICE_ID, service);

        runner.setProperty(service, StandardKustoQueryService.CLUSTER_URI, CLUSTER_URI);
        runner.setProperty(service, StandardKustoQueryService.AUTHENTICATION_STRATEGY, KustoAuthenticationStrategy.MANAGED_IDENTITY);
        runner.setProperty(service, StandardKustoQueryService.APPLICATION_CLIENT_ID, APPLICATION_CLIENT_ID);

        runner.assertValid(service);

        runner.enableControllerService(service);
    }

    @Test
    void testEnableApplicationCredentials() throws InitializationException {
        runner.addControllerService(SERVICE_ID, service);

        runner.setProperty(service, StandardKustoQueryService.CLUSTER_URI, CLUSTER_URI);
        runner.setProperty(service, StandardKustoQueryService.AUTHENTICATION_STRATEGY, KustoAuthenticationStrategy.APPLICATION_CREDENTIALS);
        runner.setProperty(service, StandardKustoQueryService.APPLICATION_CLIENT_ID, APPLICATION_CLIENT_ID);
        runner.setProperty(service, StandardKustoQueryService.APPLICATION_KEY, UUID.randomUUID().toString());
        runner.setProperty(service, StandardKustoQueryService.APPLICATION_TENANT_ID, UUID.randomUUID().toString());

        runner.assertValid(service);

        runner.enableControllerService(service);
    }
}
