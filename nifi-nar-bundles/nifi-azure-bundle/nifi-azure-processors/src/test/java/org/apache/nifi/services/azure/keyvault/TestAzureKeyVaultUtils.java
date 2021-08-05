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
package org.apache.nifi.services.azure.keyvault;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Before;
import org.junit.Test;

public class TestAzureKeyVaultUtils {

    private static final String MOCK_KEYVAULT_NAME = "mockKeyVaultName";
    private static final String MOCK_SP_CLIENT_ID = "mockSpClientId";
    private static final String MOCK_SP_CLIENT_SECRET = "mockSpClientSecret";
    private static final String MOCK_TENANT_ID = "mockTenantId";

    private TestRunner runner;
    private AzureKeyVaultClientService service;

    @Before
    public void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new AzureKeyVaultClientService();
        runner.addControllerService("keyVaultService", service);
    }

    @Test
    public void testValidWithInputs() {
        configureKeyVaultName();
        configureSpClientId();
        configureSpClientSecret();
        configureTenantId();

        runner.assertValid(service);
    }

    @Test
    public void testValidIdentity() {
        configureKeyVaultName();
        configureIdentity();

        runner.assertValid(service);
    }

    @Test
    public void testInValidMissingKeyVaultName() {
        configureSpClientId();
        configureSpClientSecret();
        configureTenantId();

        runner.assertNotValid(service);
    }

    @Test
    public void testInValidMissingSpClientId() {
        configureKeyVaultName();
        configureSpClientId();
        configureTenantId();

        runner.assertNotValid(service);
    }

    @Test
    public void testInValidMissingSpClientSecret() {
        configureKeyVaultName();
        configureSpClientId();
        configureTenantId();

        runner.assertNotValid(service);
    }

    @Test
    public void testInValidMissingTenantId() {
        configureKeyVaultName();
        configureSpClientId();
        configureSpClientSecret();

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidValidOnlyKeyVaultNameProvided() {
        configureIdentity();

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidValidIdentityWithoutKeyVaultName() {
        configureIdentity();

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidValidIdentityWithClientIdProvided() {
        configureIdentity();
        configureSpClientId();

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidValidIdentityWithClientSecretProvided() {
        configureIdentity();
        configureSpClientSecret();

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidValidIdentityWithTenantIdProvided() {
        configureIdentity();
        configureTenantId();

        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidValidIdentityWithAllServicePrincipalDetailsProvided() {
        configureIdentity();
        configureSpClientId();
        configureSpClientSecret();
        configureSpClientId();

        runner.assertNotValid(service);
    }

    private void configureKeyVaultName() {
        runner.setProperty(service, AzureKeyVaultUtils.KEYVAULT_NAME, MOCK_KEYVAULT_NAME);
    }

    private void configureSpClientId() {
        runner.setProperty(service, AzureKeyVaultUtils.SP_CLIENT_ID, MOCK_SP_CLIENT_ID);
    }

    private void configureSpClientSecret() {
        runner.setProperty(service, AzureKeyVaultUtils.SP_CLIENT_SECRET, MOCK_SP_CLIENT_SECRET);
    }

    private void configureTenantId() {
        runner.setProperty(service, AzureKeyVaultUtils.TENANT_ID, MOCK_TENANT_ID);
    }

    private void configureIdentity() {
        runner.setProperty(service, AzureKeyVaultUtils.AUTH_METHOD, AzureKeyVaultUtils.MANAGED_IDENTITY);
    }
}

