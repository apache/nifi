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

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestAzureAdxConnectionService {

    private TestRunner runner;

    private AzureAdxConnectionService service;

    private static final String MOCK_URI = "https://mockURI:443/";
    private static final String MOCK_APP_ID = "mockAppId";

    private static final String MOCK_APP_KEY = "mockAppKey";

    private static final String MOCK_APP_TENANT = "mockAppTenant";

    @BeforeEach
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(TestAzureAdxIngestProcessor.class);

        service = new AzureAdxConnectionService();
        runner.addControllerService("test-good", service);

    }

    @Test
    public void testValidWithURIandDBAccessKey() {
        configureIngestURL();
        configureAppId();
        configureAppKey();
        configureAppTenant();
        configureIsStreamingEnabled();


        runner.assertValid(service);
    }

    private void configureIngestURL() {
        runner.setProperty(service, AzureAdxConnectionService.INGEST_URL, MOCK_URI);
    }

    private void configureAppId() {
        runner.setProperty(service, AzureAdxConnectionService.APP_ID, MOCK_APP_ID);
    }

    private void configureAppKey() {
        runner.setProperty(service, AzureAdxConnectionService.APP_KEY, MOCK_APP_KEY);
    }

    private void configureAppTenant() {
        runner.setProperty(service, AzureAdxConnectionService.APP_TENANT, MOCK_APP_TENANT);
    }

    private void configureIsStreamingEnabled() {
        runner.setProperty(service, AzureAdxConnectionService.IS_STREAMING_ENABLED, "true");
    }

    @Test
    public void testCreateIngestClient(){

    }

}
