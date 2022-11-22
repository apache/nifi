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
package org.apache.nifi.services.azure;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestStandardAzureCredentialsControllerService {
    private static final String CREDENTIALS_SERVICE_IDENTIFIER = "credentials-service";
    private static final String SAMPLE_MANAGED_CLIENT_ID = "sample-managed-client-id";
    private TestRunner runner;
    private StandardAzureCredentialsControllerService credentialsService;

    @BeforeEach
    public void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        credentialsService = new StandardAzureCredentialsControllerService();
        runner.addControllerService(CREDENTIALS_SERVICE_IDENTIFIER, credentialsService);
    }

    @Test
    public void testValidControllerServiceConfiguration() {
        runner.setProperty(credentialsService,
                StandardAzureCredentialsControllerService.CREDENTIAL_CONFIGURATION_STRATEGY,
                StandardAzureCredentialsControllerService.DEFAULT_CREDENTIAL);
        runner.assertValid(credentialsService);

        // should still be valid be ignored until CREDENTIAL_CONFIGURATION_STRATEGY is set to MANAGED_IDENTITY
        runner.setProperty(credentialsService,
                StandardAzureCredentialsControllerService.MANAGED_IDENTITY_CLIENT_ID,
                SAMPLE_MANAGED_CLIENT_ID);
        runner.assertValid(credentialsService);

        runner.setProperty(credentialsService,
                StandardAzureCredentialsControllerService.CREDENTIAL_CONFIGURATION_STRATEGY,
                StandardAzureCredentialsControllerService.MANAGED_IDENTITY);
        runner.assertValid(credentialsService);
    }

    @Test
    public void testNotValidControllerServiceBlankManagedIdentityClientId() {
        runner.setProperty(credentialsService,
                StandardAzureCredentialsControllerService.CREDENTIAL_CONFIGURATION_STRATEGY,
                StandardAzureCredentialsControllerService.MANAGED_IDENTITY);
        runner.setProperty(credentialsService,
                StandardAzureCredentialsControllerService.MANAGED_IDENTITY_CLIENT_ID,
                " ");
        runner.assertNotValid(credentialsService);

        runner.setProperty(credentialsService,
                StandardAzureCredentialsControllerService.MANAGED_IDENTITY_CLIENT_ID,
                "");
        runner.assertNotValid(credentialsService);

        runner.setProperty(credentialsService,
                StandardAzureCredentialsControllerService.MANAGED_IDENTITY_CLIENT_ID,
                (String) null);
        runner.assertValid(credentialsService);
    }


}
