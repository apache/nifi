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

public class TestADLSCredentialsControllerService {

    public static final String CREDENTIALS_SERVICE_IDENTIFIER = "credentials-service";

    private static final String ACCOUNT_NAME_VALUE = "AccountName";
    private static final String ACCOUNT_KEY_VALUE = "AccountKey";
    private static final String SAS_TOKEN_VALUE = "SasToken";
    public static final String END_POINT_SUFFIX_VALUE = "end.point.suffix";

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
    public void testNotValidBecauseBothSasTokenAndUseManagedIdentitySpecified() {
        configureAccountName();

        configureSasToken();
        configureUseManagedIdentity();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseAllCredentialsSpecified() {
        configureAccountName();

        configureAccountKey();
        configureSasToken();
        configureUseManagedIdentity();

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
    public void testGetCredentialsDetailsWithAccountKey() throws Exception {
        // GIVEN
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
        assertNotNull(actual.getEndpointSuffix());
    }

    @Test
    public void testGetCredentialsDetailsWithSasToken() throws Exception {
        // GIVEN
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
        assertNotNull(actual.getEndpointSuffix());
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

    private void configureAccountKey() {
        runner.setProperty(credentialsService, AzureStorageUtils.ACCOUNT_KEY, ACCOUNT_KEY_VALUE);
    }

    private void configureSasToken() {
        runner.setProperty(credentialsService, AzureStorageUtils.PROP_SAS_TOKEN, SAS_TOKEN_VALUE);
    }

    private void configureUseManagedIdentity() {
        runner.setProperty(credentialsService, ADLSCredentialsControllerService.USE_MANAGED_IDENTITY, "true");
    }

    private void configureEndpointSuffix() {
        runner.setProperty(credentialsService, ADLSCredentialsControllerService.ENDPOINT_SUFFIX, END_POINT_SUFFIX_VALUE);
    }
}
