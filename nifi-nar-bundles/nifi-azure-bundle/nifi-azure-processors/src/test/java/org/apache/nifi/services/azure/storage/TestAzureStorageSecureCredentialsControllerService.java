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

import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.services.azure.keyvault.AzureKeyVaultClientService;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Before;
import org.junit.Test;

public class TestAzureStorageSecureCredentialsControllerService {

    private static final String ACCOUNT_NAME_SECRET = "AccountNameSecret";
    private static final String ACCOUNT_KEY_SECRET = "AccountKeySecret";
    private static final String SAS_TOKEN_SECRET = "SasTokenSecret";

    private TestRunner runner;
    private AzureStorageSecureCredentialsControllerService credentialsService;

    @Before
    public void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        credentialsService = new AzureStorageSecureCredentialsControllerService();
        runner.addControllerService("credentials-service", credentialsService);
    }

    @Test
    public void testNotValidWithEmptyEndpointSuffix() {
        configureAccountName();
        configureAccountKey();
        runner.setProperty(credentialsService, AzureStorageUtils.ENDPOINT_SUFFIX, "");

        runner.assertNotValid(credentialsService);
    }
    @Test
    public void testNotValidWithWhitespaceEndpointSuffix() {
        configureAccountName();
        configureAccountKey();
        runner.setProperty(credentialsService, AzureStorageUtils.ENDPOINT_SUFFIX, " ");

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseAccountNameMissing() {
        configureAccountKey();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseAccountKeyAndSasTokenMissing() {
        configureAccountName();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothAccountKeyAndSasTokenSpecified() {
        configureAccountName();
        configureAccountKey();
        configureSasToken();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testValidWithAccountNameAndAccountKey() throws InitializationException {
        configureAccountName();
        configureAccountKey();
        configureKeyVaultConnectionService();

        runner.assertValid(credentialsService);
    }

    @Test
    public void testValidWithAccountNameAndSasToken() throws InitializationException {
        configureAccountName();
        configureSasToken();
        configureKeyVaultConnectionService();

        runner.assertValid(credentialsService);
    }

    private void configureAccountName() {
        runner.setProperty(
            credentialsService, AzureStorageUtils.ACCOUNT_NAME_SECRET, ACCOUNT_NAME_SECRET);
    }

    private void configureAccountKey() {
        runner.setProperty(
            credentialsService, AzureStorageUtils.ACCOUNT_KEY_SECRET, ACCOUNT_KEY_SECRET);
    }

    private void configureSasToken() {
        runner.setProperty(
            credentialsService, AzureStorageUtils.ACCOUNT_SAS_TOKEN_SECRET, SAS_TOKEN_SECRET);
    }

    private void configureKeyVaultConnectionService() throws InitializationException {
        AzureKeyVaultClientService keyVaultConnectionService = new MockKeyVaultConnectionService();
        String keyVaultServiceName = "keyvault-service";
        runner.addControllerService(keyVaultServiceName, keyVaultConnectionService);
        runner.setProperty(
            credentialsService, AzureStorageUtils.KEYVAULT_CONNECTION_SERVICE, keyVaultServiceName);
    }
}

