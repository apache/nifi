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

import java.util.HashMap;

import com.azure.security.keyvault.secrets.SecretClient;

import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.services.azure.keyvault.AzureKeyVaultClientService;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

class MockKeyVaultConnectionService extends AzureKeyVaultClientService {

    private static final String ACCOUNT_NAME_SECRET_NAME = "AccountNameSecret";
    private static final String ACCOUNT_KEY_SECRET_NAME = "AccountKeySecret";
    private static final String SAS_TOKEN_SECRET_NAME = "SasTokenSecret";
    private static final String END_POINT_SUFFIX_SECRET_NAME = "end.point.suffix";

    private String ACCOUNT_NAME_SECRET_VALUE = "AccountNameSecretValue";
    private String ACCOUNT_KEY_SECRET_VALUE = "AccountKeySecretValue";
    private String SAS_TOKEN_SECRET_VALUE = "SasTokenSecretValue";
    private String END_POINT_SUFFIX_SECRET_VALUE = "end.point.suffix";

    private String MOCK_INVALID_SECRET_VALUE = "INVALID_VALUE";

    @Override
    public SecretClient getKeyVaultSecretClient(){
        return mock(SecretClient.class);
    }

    @Override
    public String getSecretFromKeyVault(String secretName) {
        switch(secretName) {
            case ACCOUNT_NAME_SECRET_NAME:
                return ACCOUNT_NAME_SECRET_VALUE;
            case ACCOUNT_KEY_SECRET_NAME:
                return ACCOUNT_KEY_SECRET_VALUE;
            case SAS_TOKEN_SECRET_NAME:
                return SAS_TOKEN_SECRET_VALUE;
            case END_POINT_SUFFIX_SECRET_NAME:
                return END_POINT_SUFFIX_SECRET_VALUE;
            default:
               return MOCK_INVALID_SECRET_VALUE;
        }
    }
}

public class TestADLSKeyVaultCredentialsControllerService {

    public static final String CREDENTIALS_SERVICE_IDENTIFIER = "secure-credentials-service";

    private static final String ACCOUNT_NAME_SECRET = "AccountNameSecret";
    private static final String ACCOUNT_KEY_SECRET = "AccountKeySecret";
    private static final String SAS_TOKEN_SECRET = "SasTokenSecret";
    private static final String END_POINT_SUFFIX_SECRET = "end.point.suffix";

    private String ACCOUNT_NAME_SECRET_VALUE = "AccountNameSecretValue";
    private String ACCOUNT_KEY_SECRET_VALUE = "AccountKeySecretValue";
    private String SAS_TOKEN_SECRET_VALUE = "SasTokenSecretValue";
    private String END_POINT_SUFFIX_SECRET_VALUE = "end.point.suffix";

    private TestRunner runner;
    private ADLSKeyVaultCredentialsControllerService credentialsService;

    @Before
    public void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        credentialsService = new ADLSKeyVaultCredentialsControllerService();
        runner.addControllerService(CREDENTIALS_SERVICE_IDENTIFIER, credentialsService);
    }

    @Test
    public void testNotValidBecauseAccountNameMissing() {
        configureAccountKey();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseNoCredentialsIsSet() {
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
    public void testNotValidWithEmptyEndpointSuffix() {
        configureAccountName();
        configureAccountKey();

        runner.setProperty(credentialsService, AzureStorageUtils.ADLS_ENDPOINT_SUFFIX, "");
        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidWithWhitespaceEndpointSuffix() {
        configureAccountName();
        configureAccountKey();
        runner.setProperty(credentialsService, AzureStorageUtils.ADLS_ENDPOINT_SUFFIX, " ");

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

    @Test
    public void testGetCredentialsDetailsWithAccountKey() throws Exception {
        // GIVEN
        configureAccountName();
        configureAccountKey();
        configureKeyVaultConnectionService();

        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(ACCOUNT_NAME_SECRET_VALUE, actual.getAccountName());
        assertEquals(ACCOUNT_KEY_SECRET_VALUE, actual.getAccountKey());
        assertNull(actual.getSasToken());
        assertFalse(actual.getUseManagedIdentity());
        assertNotNull(actual.getEndpointSuffix());
    }

    @Test
    public void testGetCredentialsDetailsWithSasToken() throws Exception {
        // GIVEN
        configureAccountName();
        configureSasToken();
        configureKeyVaultConnectionService();

        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(ACCOUNT_NAME_SECRET_VALUE, actual.getAccountName());
        assertEquals(SAS_TOKEN_SECRET_VALUE, actual.getSasToken());
        assertNull(actual.getAccountKey());
        assertFalse(actual.getUseManagedIdentity());
        assertNotNull(actual.getEndpointSuffix());
    }

    @Test
    public void testGetCredentialsDetailsWithSetEndpointSuffix() throws Exception {
        // GIVEN
        configureAccountName();
        configureAccountKey();
        configureEndpointSuffix();
        configureKeyVaultConnectionService();

        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(END_POINT_SUFFIX_SECRET, actual.getEndpointSuffix());
    }

    private void configureAccountName() {
        runner.setProperty(credentialsService, AzureStorageUtils.ACCOUNT_NAME_SECRET, ACCOUNT_NAME_SECRET);
    }

    private void configureAccountKey() {
        runner.setProperty(credentialsService, AzureStorageUtils.ACCOUNT_KEY_SECRET, ACCOUNT_KEY_SECRET);
    }

    private void configureSasToken() {
        runner.setProperty(credentialsService, AzureStorageUtils.ACCOUNT_SAS_TOKEN_SECRET, SAS_TOKEN_SECRET);
    }

    private void configureEndpointSuffix() {
        runner.setProperty(credentialsService, AzureStorageUtils.ADLS_ENDPOINT_SUFFIX, END_POINT_SUFFIX_SECRET);
    }

    private void configureKeyVaultConnectionService() throws InitializationException {
        AzureKeyVaultClientService keyVaultConnectionService = new MockKeyVaultConnectionService();
        String keyVaultServiceName = "keyvault-service";
        runner.addControllerService(keyVaultServiceName, keyVaultConnectionService);
        runner.setProperty(credentialsService, AzureStorageUtils.KEYVAULT_CONNECTION_SERVICE, keyVaultServiceName);
    }
}

