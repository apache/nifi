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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestADLSCredentialsControllerService {

    public static final String CREDENTIALS_SERVICE_IDENTIFIER = "credentials-service";

    private static final String ACCOUNT_NAME_VALUE = "AccountName";
    private static final String ACCOUNT_KEY_VALUE = "AccountKey";
    private static final String SAS_TOKEN_VALUE = "SasToken";
    private static final String END_POINT_SUFFIX_VALUE = "end.point.suffix";
    private static final String MANAGED_IDENTITY_CLIENT_ID_VALUE = "ManagedIdentityClientID";
    private static final String SERVICE_PRINCIPAL_TENANT_ID_VALUE = "ServicePrincipalTenantID";
    private static final String SERVICE_PRINCIPAL_CLIENT_ID_VALUE = "ServicePrincipalClientID";
    private static final String SERVICE_PRINCIPAL_CLIENT_SECRET_VALUE = "ServicePrincipalClientSecret";

    private TestRunner runner;
    private ADLSCredentialsControllerService credentialsService;

    @BeforeEach
    public void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        credentialsService = new ADLSCredentialsControllerService();
        runner.addControllerService(CREDENTIALS_SERVICE_IDENTIFIER, credentialsService);
    }

    @Test
    public void testNotValidBecauseAccountNameMissing() {
        configureCredentialsType(AzureStorageCredentialsType.ACCOUNT_KEY);
        configureAccountKey();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidWithEmptyEndpointSuffix() {
        configureCredentialsType(AzureStorageCredentialsType.ACCOUNT_KEY);
        configureAccountName();
        configureAccountKey();

        runner.setProperty(credentialsService, ADLSCredentialsControllerService.ENDPOINT_SUFFIX, "");
        runner.assertNotValid(credentialsService);
    }
    @Test
    public void testNotValidWithWhitespaceEndpointSuffix() {
        configureCredentialsType(AzureStorageCredentialsType.ACCOUNT_KEY);
        configureAccountName();
        configureAccountKey();

        runner.setProperty(credentialsService, ADLSCredentialsControllerService.ENDPOINT_SUFFIX, " ");
        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testValidWithAccountNameAndAccountKey() {
        configureCredentialsType(AzureStorageCredentialsType.ACCOUNT_KEY);
        configureAccountName();
        configureAccountKey();

        runner.assertValid(credentialsService);
    }

    @Test
    public void testValidWithAccountNameAndSasToken() {
        configureCredentialsType(AzureStorageCredentialsType.SAS_TOKEN);
        configureAccountName();
        configureSasToken();

        runner.assertValid(credentialsService);
    }

    @Test
    public void testValidWithAccountNameAndUseManagedIdentity() {
        configureCredentialsType(AzureStorageCredentialsType.MANAGED_IDENTITY);
        configureAccountName();

        runner.assertValid(credentialsService);
    }

    @Test
    public void testValidWithAccountNameAndServicePrincipalWithClientSecret() {
        configureCredentialsType(AzureStorageCredentialsType.SERVICE_PRINCIPAL);
        configureAccountName();
        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientSecret();

        runner.assertValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseNoTenantIdSpecifiedForServicePrincipal() {
        configureCredentialsType(AzureStorageCredentialsType.SERVICE_PRINCIPAL);
        configureAccountName();

        configureServicePrincipalClientId();
        configureServicePrincipalClientSecret();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseNoClientIdSpecifiedForServicePrincipal() {
        configureCredentialsType(AzureStorageCredentialsType.SERVICE_PRINCIPAL);
        configureAccountName();

        configureServicePrincipalTenantId();
        configureServicePrincipalClientSecret();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseNoClientSecretSpecifiedForServicePrincipal() {
        configureCredentialsType(AzureStorageCredentialsType.SERVICE_PRINCIPAL);
        configureAccountName();

        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testGetCredentialsDetailsWithAccountKey() throws Exception {
        // GIVEN
        configureCredentialsType(AzureStorageCredentialsType.ACCOUNT_KEY);
        configureAccountName();
        configureAccountKey();

        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(ACCOUNT_NAME_VALUE, actual.getAccountName());
        assertEquals(ACCOUNT_KEY_VALUE, actual.getAccountKey());
        assertNull(actual.getSasToken());
        assertFalse(actual.getUseManagedIdentity());
        assertNull(actual.getManagedIdentityClientId());
        assertNotNull(actual.getEndpointSuffix());
        assertNull(actual.getServicePrincipalTenantId());
        assertNull(actual.getServicePrincipalClientId());
        assertNull(actual.getServicePrincipalClientSecret());
    }

    @Test
    public void testGetCredentialsDetailsWithAccountKeyUsingEL() throws Exception {
        // GIVEN
        configureCredentialsType(AzureStorageCredentialsType.ACCOUNT_KEY);
        configureAccountNameUsingEL();
        configureAccountKeyUsingEL();

        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(ACCOUNT_NAME_VALUE, actual.getAccountName());
        assertEquals(ACCOUNT_KEY_VALUE, actual.getAccountKey());
        assertNull(actual.getSasToken());
        assertFalse(actual.getUseManagedIdentity());
        assertNull(actual.getManagedIdentityClientId());
        assertNotNull(actual.getEndpointSuffix());
        assertNull(actual.getServicePrincipalTenantId());
        assertNull(actual.getServicePrincipalClientId());
        assertNull(actual.getServicePrincipalClientSecret());
    }

    @Test
    public void testGetCredentialsDetailsWithSasToken() throws Exception {
        // GIVEN
        configureCredentialsType(AzureStorageCredentialsType.SAS_TOKEN);
        configureAccountName();
        configureSasToken();

        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(ACCOUNT_NAME_VALUE, actual.getAccountName());
        assertEquals(SAS_TOKEN_VALUE, actual.getSasToken());
        assertNull(actual.getAccountKey());
        assertFalse(actual.getUseManagedIdentity());
        assertNull(actual.getManagedIdentityClientId());
        assertNotNull(actual.getEndpointSuffix());
        assertNull(actual.getServicePrincipalTenantId());
        assertNull(actual.getServicePrincipalClientId());
        assertNull(actual.getServicePrincipalClientSecret());
    }

    @Test
    public void testGetCredentialsDetailsWithSasTokenUsingEL() throws Exception {
        configureCredentialsType(AzureStorageCredentialsType.SAS_TOKEN);
        configureAccountName();
        configureSasTokenUsingEL();

        runner.enableControllerService(credentialsService);

        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());
        assertEquals(ACCOUNT_NAME_VALUE, actual.getAccountName());
        assertEquals(SAS_TOKEN_VALUE, actual.getSasToken());
        assertNull(actual.getAccountKey());
        assertFalse(actual.getUseManagedIdentity());
        assertNull(actual.getManagedIdentityClientId());
        assertNotNull(actual.getEndpointSuffix());
        assertNull(actual.getServicePrincipalTenantId());
        assertNull(actual.getServicePrincipalClientId());
        assertNull(actual.getServicePrincipalClientSecret());
    }

    @Test
    public void testGetCredentialsDetailsWithSystemAssignedManagedIdentity() throws Exception {
        // GIVEN
        configureCredentialsType(AzureStorageCredentialsType.MANAGED_IDENTITY);
        configureAccountName();

        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(ACCOUNT_NAME_VALUE, actual.getAccountName());
        assertTrue(actual.getUseManagedIdentity());
        assertNull(actual.getManagedIdentityClientId());
        assertNull(actual.getAccountKey());
        assertNull(actual.getSasToken());
        assertNotNull(actual.getEndpointSuffix());
        assertNull(actual.getServicePrincipalTenantId());
        assertNull(actual.getServicePrincipalClientId());
        assertNull(actual.getServicePrincipalClientSecret());
    }

    @Test
    public void testGetCredentialsDetailsWithUserAssignedManagedIdentity() throws Exception {
        // GIVEN
        configureCredentialsType(AzureStorageCredentialsType.MANAGED_IDENTITY);
        configureAccountName();
        configureManagedIdentityClientId();

        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(ACCOUNT_NAME_VALUE, actual.getAccountName());
        assertTrue(actual.getUseManagedIdentity());
        assertEquals(MANAGED_IDENTITY_CLIENT_ID_VALUE, actual.getManagedIdentityClientId());
        assertNull(actual.getAccountKey());
        assertNull(actual.getSasToken());
        assertNotNull(actual.getEndpointSuffix());
        assertNull(actual.getServicePrincipalTenantId());
        assertNull(actual.getServicePrincipalClientId());
        assertNull(actual.getServicePrincipalClientSecret());
    }

    @Test
    public void testGetCredentialsDetailsWithServicePrincipalWithClientSecret() throws Exception {
        // GIVEN
        configureCredentialsType(AzureStorageCredentialsType.SERVICE_PRINCIPAL);
        configureAccountName();
        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientSecret();

        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(ACCOUNT_NAME_VALUE, actual.getAccountName());
        assertNull(actual.getAccountKey());
        assertNull(actual.getSasToken());
        assertFalse(actual.getUseManagedIdentity());
        assertNull(actual.getManagedIdentityClientId());
        assertNotNull(actual.getEndpointSuffix());
        assertEquals(SERVICE_PRINCIPAL_TENANT_ID_VALUE, actual.getServicePrincipalTenantId());
        assertEquals(SERVICE_PRINCIPAL_CLIENT_ID_VALUE, actual.getServicePrincipalClientId());
        assertEquals(SERVICE_PRINCIPAL_CLIENT_SECRET_VALUE, actual.getServicePrincipalClientSecret());
    }

    @Test
    public void testGetCredentialsDetailsWithSetEndpointSuffix() throws Exception {
        // GIVEN
        configureCredentialsType(AzureStorageCredentialsType.ACCOUNT_KEY);
        configureAccountName();
        configureAccountKey();
        configureEndpointSuffix();

        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(END_POINT_SUFFIX_VALUE, actual.getEndpointSuffix());
    }

    @Test
    public void testGetCredentialsDetailsWithSetEndpointSuffixUsingEL() throws Exception {
        // GIVEN
        configureCredentialsType(AzureStorageCredentialsType.ACCOUNT_KEY);
        configureAccountName();
        configureAccountKey();
        configureEndpointSuffixUsingEL();

        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(END_POINT_SUFFIX_VALUE, actual.getEndpointSuffix());
    }
    private void configureCredentialsType(AzureStorageCredentialsType credentialsType) {
        runner.setProperty(credentialsService, AzureStorageUtils.CREDENTIALS_TYPE, credentialsType);
    }

    private void configureAccountName() {
        runner.setProperty(credentialsService, AzureStorageUtils.ACCOUNT_NAME, ACCOUNT_NAME_VALUE);
    }

    private void configureAccountNameUsingEL() {
        configurePropertyUsingEL(AzureStorageUtils.ACCOUNT_NAME, "account.name", ACCOUNT_NAME_VALUE);
    }

    private void configureAccountKey() {
        runner.setProperty(credentialsService, AzureStorageUtils.ACCOUNT_KEY, ACCOUNT_KEY_VALUE);
    }

    private void configureAccountKeyUsingEL() {
        configurePropertyUsingEL(AzureStorageUtils.ACCOUNT_KEY, "account.key", ACCOUNT_KEY_VALUE);
    }

    private void configureSasToken() {
        runner.setProperty(credentialsService, AzureStorageUtils.SAS_TOKEN, SAS_TOKEN_VALUE);
    }

    private void configureSasTokenUsingEL() {
        String variableName = "sas.token";
        configurePropertyUsingEL(AzureStorageUtils.SAS_TOKEN, variableName, SAS_TOKEN_VALUE);
    }

    private void configureManagedIdentityClientId() {
        runner.setProperty(credentialsService, AzureStorageUtils.MANAGED_IDENTITY_CLIENT_ID, MANAGED_IDENTITY_CLIENT_ID_VALUE);
    }

    private void configureEndpointSuffix() {
        runner.setProperty(credentialsService, AzureStorageUtils.ENDPOINT_SUFFIX, END_POINT_SUFFIX_VALUE);
    }

    private void configureEndpointSuffixUsingEL() {
        String variableName = "endpoint.suffix";
        configurePropertyUsingEL(AzureStorageUtils.ENDPOINT_SUFFIX, variableName, END_POINT_SUFFIX_VALUE);
    }

    private void configureServicePrincipalTenantId() {
        runner.setProperty(credentialsService, AzureStorageUtils.SERVICE_PRINCIPAL_TENANT_ID, SERVICE_PRINCIPAL_TENANT_ID_VALUE);
    }

    private void configureServicePrincipalClientId() {
        runner.setProperty(credentialsService, AzureStorageUtils.SERVICE_PRINCIPAL_CLIENT_ID, SERVICE_PRINCIPAL_CLIENT_ID_VALUE);
    }

    private void configureServicePrincipalClientSecret() {
        runner.setProperty(credentialsService, AzureStorageUtils.SERVICE_PRINCIPAL_CLIENT_SECRET, SERVICE_PRINCIPAL_CLIENT_SECRET_VALUE);
    }

    private void configurePropertyUsingEL(PropertyDescriptor propertyDescriptor, String variableName, String variableValue) {
        runner.setProperty(credentialsService, propertyDescriptor, String.format("${%s}", variableName));
        runner.setEnvironmentVariableValue(variableName, variableValue);
    }
}
