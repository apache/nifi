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
package org.apache.nifi.services.azure.cosmos.document;

import com.azure.security.keyvault.secrets.SecretClient;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.services.azure.keyvault.AzureKeyVaultClientService;

import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;

class MockKeyVaultConnectionService extends AzureKeyVaultClientService {
    private static final String URI_SECRET = "uriSecret";
    private static final String DB_ACCESS_KEY_SECRET = "dbAccessKeySecret";

    private String URI_SECRET_VALUE = "https://mockURI:443/";
    private String DB_ACCESS_KEY_SECRET_VALUE = "mockDB_ACCESS_KEY";

    private String MOCK_INVALID_SECRET_VALUE = "INVALID_VALUE";

    @Override
    public SecretClient getKeyVaultSecretClient(){
        return mock(SecretClient.class);
    }

    @Override
    public String getSecretFromKeyVault(String secretName) {
        switch(secretName) {
            case URI_SECRET:
                return URI_SECRET_VALUE;
            case DB_ACCESS_KEY_SECRET:
                return DB_ACCESS_KEY_SECRET_VALUE;
            default:
               return MOCK_INVALID_SECRET_VALUE;
        }
    }
}

public class TestAzureCosmosSecureDBClientService {

    private static final String URI_SECRET = "uriSecret";
    private static final String DB_ACCESS_KEY_SECRET = "dbAccessKeySecret";

    private TestRunner runner;
    private AzureCosmosSecureDBClientService service;

    @Before
    public void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new AzureCosmosSecureDBClientService();
        runner.addControllerService("connService", service);
    }

    @Test
    public void testValidWithUriAndDBAccessKey() throws InitializationException {
        configureURI();
        configureDBAccessKey();
        configureKeyVaultConnectionService();

        runner.assertValid(service);
    }

    @Test
    public void testNotValidBecauseURIMissing() {
        configureDBAccessKey();

        runner.assertNotValid(service);
    }

    @Test
    public void testNotValidBecauseDBAccessKeyMissing() {
        configureURI();

        runner.assertNotValid(service);
    }

    private void configureURI() {
        runner.setProperty(service, AzureCosmosSecureDBClientService.URI_SECRET, URI_SECRET);
    }

    private void configureDBAccessKey() {
        runner.setProperty(service, AzureCosmosSecureDBClientService.DB_ACCESS_KEY_SECRET, DB_ACCESS_KEY_SECRET);
    }

    private void configureKeyVaultConnectionService() throws InitializationException {
        AzureKeyVaultClientService keyVaultConnectionService = new MockKeyVaultConnectionService();
        String keyVaultServiceName = "keyvault-service";
        runner.addControllerService(keyVaultServiceName, keyVaultConnectionService);
        runner.setProperty(service, AzureCosmosSecureDBClientService.KEYVAULT_CONNECTION_SERVICE, keyVaultServiceName);
    }
}

