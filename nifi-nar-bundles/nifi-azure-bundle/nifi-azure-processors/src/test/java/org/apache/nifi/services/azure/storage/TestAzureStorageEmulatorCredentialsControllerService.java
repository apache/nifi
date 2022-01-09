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
package org.apache.nifi.services.azure.storage;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class TestAzureStorageEmulatorCredentialsControllerService {

    private static final String TEST_ENVIRONMENT_URI = "http://127.0.0.1";

    private TestRunner runner;
    private AzureStorageCredentialsService credentialsService;

    @Before
    public void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        credentialsService = new AzureStorageEmulatorCredentialsControllerService();
        runner.addControllerService("credentials-service", credentialsService);
    }

    @Test
    public void testValidWithProxyURI() {
        configureProxyURI();

        runner.assertValid(credentialsService);
    }

    @Test
    public void testValidWithoutProxyURI() {
         runner.assertValid(credentialsService);
    }

    private void configureProxyURI() {
        runner.setProperty(credentialsService, AzureStorageEmulatorCredentialsControllerService.DEVELOPMENT_STORAGE_PROXY_URI, TEST_ENVIRONMENT_URI);
    }
}
