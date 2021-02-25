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
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestADLSCredentialsControllerService {

    public static final String CREDENTIALS_SERVICE_IDENTIFIER = "credentials-service";

    private static final String ACCOUNT_NAME_VALUE = "AccountName";
    private static final String ACCOUNT_KEY_VALUE = "AccountKey";
    private static final String SAS_TOKEN_VALUE = "SasToken";
    private static final String END_POINT_SUFFIX_VALUE = "end.point.suffix";
    private static final String SERVICE_PRINCIPAL_TENANT_ID_VALUE = "ServicePrincipalTenantID";
    private static final String SERVICE_PRINCIPAL_CLIENT_ID_VALUE = "ServicePrincipalClientID";
    private static final String SERVICE_PRINCIPAL_CLIENT_SECRET_VALUE = "ServicePrincipalClientSecret";
    private static final String SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PATH_VALUE = "ServicePrincipalClientCertificatePath";
    private static final String SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PASSWORD_VALUE = "ServicePrincipalClientCertificatePassword";

    private TestRunner runner;
    private ADLSCredentialsControllerService credentialsService;

    @Before
    public void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        credentialsService = new ADLSCredentialsControllerService();
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
    public void testNotValidBecauseBothAccountKeyAndUseManagedIdentitySpecified() {
        configureAccountName();

        configureAccountKey();
        configureUseManagedIdentity();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothAccountKeyAndServicePrincipalTenantIdSpecified() {
        configureAccountName();

        configureAccountKey();
        configureServicePrincipalTenantId();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothAccountKeyAndServicePrincipalClientIdSpecified() {
        configureAccountName();

        configureAccountKey();
        configureServicePrincipalClientId();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothAccountKeyAndServicePrincipalClientSecretSpecified() {
        configureAccountName();

        configureAccountKey();
        configureServicePrincipalClientSecret();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothAccountKeyAndServicePrincipalClientCertificateSpecified() throws Exception {
        configureAccountName();

        configureAccountKey();
        configureServicePrincipalClientCertificate();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothSasTokenAndUseManagedIdentitySpecified() {
        configureAccountName();

        configureSasToken();
        configureUseManagedIdentity();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothSasTokenAndServicePrincipalTenantIdSpecified() {
        configureAccountName();

        configureSasToken();
        configureServicePrincipalTenantId();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothSasTokenAndServicePrincipalClientIdSpecified() {
        configureAccountName();

        configureSasToken();
        configureServicePrincipalClientId();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothSasTokenAndServicePrincipalClientSecretSpecified() {
        configureAccountName();

        configureSasToken();
        configureServicePrincipalClientSecret();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothSasTokenAndServicePrincipalClientCertificateSpecified() throws Exception {
        configureAccountName();

        configureSasToken();
        configureServicePrincipalClientCertificate();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothUseManagedIdentityAndServicePrincipalTenantIdSpecified() {
        configureAccountName();

        configureUseManagedIdentity();
        configureServicePrincipalTenantId();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothUseManagedIdentityAndServicePrincipalClientIdSpecified() {
        configureAccountName();

        configureUseManagedIdentity();
        configureServicePrincipalClientId();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothUseManagedIdentityAndServicePrincipalClientSecretSpecified() {
        configureAccountName();

        configureUseManagedIdentity();
        configureServicePrincipalClientSecret();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothUseManagedIdentityAndServicePrincipalClientCertificateSpecified() throws Exception {
        configureAccountName();

        configureUseManagedIdentity();
        configureServicePrincipalClientCertificate();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseAllCredentialsSpecified() throws Exception {
        configureAccountName();

        configureAccountKey();
        configureSasToken();
        configureUseManagedIdentity();
        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientSecret();
        configureServicePrincipalClientCertificate();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidWithEmptyEndpointSuffix() {
        configureAccountName();
        configureAccountKey();

        runner.setProperty(credentialsService, ADLSCredentialsControllerService.ENDPOINT_SUFFIX, "");
        runner.assertNotValid(credentialsService);
    }
    @Test
    public void testNotValidWithWhitespaceEndpointSuffix() {
        configureAccountName();
        configureAccountKey();

        runner.setProperty(credentialsService, ADLSCredentialsControllerService.ENDPOINT_SUFFIX, " ");
        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testValidWithAccountNameAndAccountKey() {
        configureAccountName();
        configureAccountKey();

        runner.assertValid(credentialsService);
    }

    @Test
    public void testValidWithAccountNameAndSasToken() {
        configureAccountName();
        configureSasToken();

        runner.assertValid(credentialsService);
    }

    @Test
    public void testValidWithAccountNameAndUseManagedIdentity() {
        configureAccountName();
        configureUseManagedIdentity();

        runner.assertValid(credentialsService);
    }

    @Test
    public void testValidWithAccountNameAndServicePrincipalWithClientSecret() {
        configureAccountName();
        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientSecret();

        runner.assertValid(credentialsService);
    }

    @Test
    public void testValidWithAccountNameAndServicePrincipalWithClientCertificate() throws Exception {
        configureAccountName();
        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientCertificate();

        runner.assertValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseNoTenantIdSpecifiedForServicePrincipal() {
        configureAccountName();

        configureServicePrincipalClientId();
        configureServicePrincipalClientSecret();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseNoClientIdSpecifiedForServicePrincipal() {
        configureAccountName();

        configureServicePrincipalTenantId();
        configureServicePrincipalClientSecret();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseNeitherClientSecretNorClientCertificateSpecifiedForServicePrincipal() {
        configureAccountName();

        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothClientSecretAndClientCertificateSpecifiedForServicePrincipal() throws Exception {
        configureAccountName();

        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientSecret();
        configureServicePrincipalClientCertificate();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testGetCredentialsDetailsWithAccountKey() throws Exception {
        // GIVEN
        configureAccountName();
        configureAccountKey();

        executeTestGetCredentialsDetailsWithAccountKeyUsingEL();
    }

    @Test
    public void testGetCredentialsDetailsWithAccountKeyUsingEL() throws Exception {
        // GIVEN
        configureAccountNameUsingEL();
        configureAccountKeyUsingEL();

        executeTestGetCredentialsDetailsWithAccountKeyUsingEL();
    }

    private void executeTestGetCredentialsDetailsWithAccountKeyUsingEL() {
        // GIVEN
        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(ACCOUNT_NAME_VALUE, actual.getAccountName());
        assertEquals(ACCOUNT_KEY_VALUE, actual.getAccountKey());
        assertNull(actual.getSasToken());
        assertFalse(actual.getUseManagedIdentity());
        assertNotNull(actual.getEndpointSuffix());
        assertNull(actual.getServicePrincipalTenantId());
        assertNull(actual.getServicePrincipalClientId());
        assertNull(actual.getServicePrincipalClientSecret());
        assertNull(actual.getServicePrincipalClientCertificatePath());
        assertNull(actual.getServicePrincipalClientCertificatePassword());
    }

    @Test
    public void testGetCredentialsDetailsWithSasToken() throws Exception {
        // GIVEN
        configureAccountName();
        configureSasToken();

        executetestGetCredentialsDetailsWithSasToken();
    }

    @Test
    public void testGetCredentialsDetailsWithSasTokenUsingEL() throws Exception {
        // GIVEN
        configureAccountNameUsingEL();
        configureSasTokenUsingEL();

        executetestGetCredentialsDetailsWithSasToken();
    }

    private void executetestGetCredentialsDetailsWithSasToken() {
        // GIVEN
        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(ACCOUNT_NAME_VALUE, actual.getAccountName());
        assertEquals(SAS_TOKEN_VALUE, actual.getSasToken());
        assertNull(actual.getAccountKey());
        assertFalse(actual.getUseManagedIdentity());
        assertNotNull(actual.getEndpointSuffix());
        assertNull(actual.getServicePrincipalTenantId());
        assertNull(actual.getServicePrincipalClientId());
        assertNull(actual.getServicePrincipalClientSecret());
        assertNull(actual.getServicePrincipalClientCertificatePath());
        assertNull(actual.getServicePrincipalClientCertificatePassword());
    }

    @Test
    public void testGetCredentialsDetailsWithUseManagedIdentity() throws Exception {
        // GIVEN
        configureAccountName();
        configureUseManagedIdentity();

        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(ACCOUNT_NAME_VALUE, actual.getAccountName());
        assertTrue(actual.getUseManagedIdentity());
        assertNull(actual.getAccountKey());
        assertNull(actual.getSasToken());
        assertNotNull(actual.getEndpointSuffix());
        assertNull(actual.getServicePrincipalTenantId());
        assertNull(actual.getServicePrincipalClientId());
        assertNull(actual.getServicePrincipalClientSecret());
        assertNull(actual.getServicePrincipalClientCertificatePath());
        assertNull(actual.getServicePrincipalClientCertificatePassword());
    }

    @Test
    public void testGetCredentialsDetailsWithServicePrincipalWithClientSecret() throws Exception {
        // GIVEN
        configureAccountName();
        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientSecret();

        executeTestGetCredentialsDetailsWithServicePrincipalWithClientSecret();
    }

    @Test
    public void testGetCredentialsDetailsWithServicePrincipalWithClientSecretUsingEL() throws Exception {
        // GIVEN
        configureAccountNameUsingEL();
        configureServicePrincipalTenantIdUsingEL();
        configureServicePrincipalClientIdUsingEL();
        configureServicePrincipalClientSecretUsingEL();

        executeTestGetCredentialsDetailsWithServicePrincipalWithClientSecret();
    }

    private void executeTestGetCredentialsDetailsWithServicePrincipalWithClientSecret() {
        // GIVEN
        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(ACCOUNT_NAME_VALUE, actual.getAccountName());
        assertNull(actual.getAccountKey());
        assertNull(actual.getSasToken());
        assertFalse(actual.getUseManagedIdentity());
        assertNotNull(actual.getEndpointSuffix());
        assertEquals(SERVICE_PRINCIPAL_TENANT_ID_VALUE, actual.getServicePrincipalTenantId());
        assertEquals(SERVICE_PRINCIPAL_CLIENT_ID_VALUE, actual.getServicePrincipalClientId());
        assertEquals(SERVICE_PRINCIPAL_CLIENT_SECRET_VALUE, actual.getServicePrincipalClientSecret());
        assertNull(actual.getServicePrincipalClientCertificatePath());
        assertNull(actual.getServicePrincipalClientCertificatePassword());
    }

    @Test
    public void testGetCredentialsDetailsWithServicePrincipalWithClientCertificate() throws Exception {
        // GIVEN
        configureAccountName();
        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientCertificate();

        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(ACCOUNT_NAME_VALUE, actual.getAccountName());
        assertNull(actual.getAccountKey());
        assertNull(actual.getSasToken());
        assertFalse(actual.getUseManagedIdentity());
        assertNotNull(actual.getEndpointSuffix());
        assertEquals(SERVICE_PRINCIPAL_TENANT_ID_VALUE, actual.getServicePrincipalTenantId());
        assertEquals(SERVICE_PRINCIPAL_CLIENT_ID_VALUE, actual.getServicePrincipalClientId());
        assertNull(actual.getServicePrincipalClientSecret());
        assertEquals(SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PATH_VALUE, actual.getServicePrincipalClientCertificatePath());
        assertEquals(SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PASSWORD_VALUE, actual.getServicePrincipalClientCertificatePassword());
    }

    @Test
    public void testGetCredentialsDetailsWithSetEndpointSuffix() throws Exception {
        // GIVEN
        configureAccountName();
        configureAccountKey();
        configureEndpointSuffix();

        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(END_POINT_SUFFIX_VALUE, actual.getEndpointSuffix());
    }

    private void configureAccountName() {
        runner.setProperty(credentialsService, ADLSCredentialsControllerService.ACCOUNT_NAME, ACCOUNT_NAME_VALUE);
    }

    private void configureAccountNameUsingEL() {
        String variableName = "account.name";
        configurePropertyUsingEL(ADLSCredentialsControllerService.ACCOUNT_NAME, variableName, ACCOUNT_NAME_VALUE);
    }

    private void configureAccountKey() {
        runner.setProperty(credentialsService, AzureStorageUtils.ACCOUNT_KEY, ACCOUNT_KEY_VALUE);
    }

    private void configureAccountKeyUsingEL() {
        String variableName = "account.key";
        configurePropertyUsingEL(AzureStorageUtils.ACCOUNT_KEY, variableName, ACCOUNT_KEY_VALUE);
    }

    private void configureSasToken() {
        runner.setProperty(credentialsService, AzureStorageUtils.PROP_SAS_TOKEN, SAS_TOKEN_VALUE);
    }

    private void configureSasTokenUsingEL() {
        String variableName = "sas.token";
        configurePropertyUsingEL(AzureStorageUtils.PROP_SAS_TOKEN, variableName, SAS_TOKEN_VALUE);
    }

    private void configureUseManagedIdentity() {
        runner.setProperty(credentialsService, ADLSCredentialsControllerService.USE_MANAGED_IDENTITY, "true");
    }

    private void configureEndpointSuffix() {
        runner.setProperty(credentialsService, ADLSCredentialsControllerService.ENDPOINT_SUFFIX, END_POINT_SUFFIX_VALUE);
    }

    private void configureServicePrincipalTenantId() {
        runner.setProperty(credentialsService, ADLSCredentialsControllerService.SERVICE_PRINCIPAL_TENANT_ID, SERVICE_PRINCIPAL_TENANT_ID_VALUE);
    }

    private void configureServicePrincipalTenantIdUsingEL() {
        String variableName = "service.principal.tenant.id";
        configurePropertyUsingEL(ADLSCredentialsControllerService.SERVICE_PRINCIPAL_TENANT_ID, variableName, SERVICE_PRINCIPAL_TENANT_ID_VALUE);
    }

    private void configureServicePrincipalClientId() {
        runner.setProperty(credentialsService, ADLSCredentialsControllerService.SERVICE_PRINCIPAL_CLIENT_ID, SERVICE_PRINCIPAL_CLIENT_ID_VALUE);
    }

    private void configureServicePrincipalClientIdUsingEL() {
        String variableName = "service.principal.client.id";
        configurePropertyUsingEL(ADLSCredentialsControllerService.SERVICE_PRINCIPAL_CLIENT_ID, variableName, SERVICE_PRINCIPAL_CLIENT_ID_VALUE);
    }

    private void configureServicePrincipalClientSecret() {
        runner.setProperty(credentialsService, ADLSCredentialsControllerService.SERVICE_PRINCIPAL_CLIENT_SECRET, SERVICE_PRINCIPAL_CLIENT_SECRET_VALUE);
    }

    private void configureServicePrincipalClientSecretUsingEL() {
        String variableName = "service.principal.client.secret";
        configurePropertyUsingEL(ADLSCredentialsControllerService.SERVICE_PRINCIPAL_CLIENT_SECRET, variableName, SERVICE_PRINCIPAL_CLIENT_SECRET_VALUE);
    }

    private void configureServicePrincipalClientCertificate() throws InitializationException {
        String sslContextServiceId = "ssl-context-service";

        SSLContextService sslContextService = mock(SSLContextService.class);
        when(sslContextService.getIdentifier()).thenReturn(sslContextServiceId);
        when(sslContextService.getKeyStoreFile()).thenReturn(SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PATH_VALUE);
        when(sslContextService.getKeyStorePassword()).thenReturn(SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PASSWORD_VALUE);

        runner.addControllerService(sslContextServiceId, sslContextService);
        runner.enableControllerService(sslContextService);
        runner.setProperty(credentialsService, ADLSCredentialsControllerService.SERVICE_PRINCIPAL_CLIENT_CERTIFICATE, sslContextServiceId);
    }

    private void configurePropertyUsingEL(PropertyDescriptor propertyDescriptor, String variableName, String variableValue) {
        runner.setProperty(credentialsService, propertyDescriptor, String.format("${%s}", variableName));
        runner.setVariable(variableName, variableValue);
    }
}
