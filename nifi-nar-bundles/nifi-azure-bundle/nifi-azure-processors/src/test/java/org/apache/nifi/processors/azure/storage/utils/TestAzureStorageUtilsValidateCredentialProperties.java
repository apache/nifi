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
package org.apache.nifi.processors.azure.storage.utils;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.azure.storage.ListAzureBlobStorage;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockValidationContext;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestAzureStorageUtilsValidateCredentialProperties {

    private static final String CREDENTIALS_SERVICE_VALUE = "CredentialsService";
    private static final String ACCOUNT_NAME_VALUE = "AccountName";
    private static final String ACCOUNT_KEY_VALUE = "AccountKey";
    private static final String SAS_TOKEN_VALUE = "SasToken";

    private MockProcessContext processContext;
    private MockValidationContext validationContext;

    @Before
    public void setUp() {
        Processor processor = new ListAzureBlobStorage();
        processContext = new MockProcessContext(processor);
        validationContext = new MockValidationContext(processContext);
    }

    @Test
    public void testValidWithCredentialsService() {
        configureCredentialsService();

        Collection<ValidationResult> result = AzureStorageUtils.validateCredentialProperties(validationContext);

        assertValid(result);
    }

    @Test
    public void testValidWithAccountNameAndAccountKey() {
        configureAccountName();
        configureAccountKey();

        Collection<ValidationResult> result = AzureStorageUtils.validateCredentialProperties(validationContext);

        assertValid(result);
    }

    @Test
    public void testValidWithAccountNameAndSasToken() {
        configureAccountName();
        configureSasToken();

        Collection<ValidationResult> result = AzureStorageUtils.validateCredentialProperties(validationContext);

        assertValid(result);
    }

    @Test
    public void testNotValidBecauseNothingSpecified() {
        Collection<ValidationResult> result = AzureStorageUtils.validateCredentialProperties(validationContext);

        assertNotValid(result);
    }

    @Test
    public void testNotValidBecauseBothCredentialsServiceAndAccountNameSpecified() {
        configureCredentialsService();
        configureAccountName();

        Collection<ValidationResult> result = AzureStorageUtils.validateCredentialProperties(validationContext);

        assertNotValid(result);
    }

    @Test
    public void testNotValidBecauseBothCredentialsServiceAndAccountKeySpecified() {
        configureCredentialsService();
        configureAccountKey();

        Collection<ValidationResult> result = AzureStorageUtils.validateCredentialProperties(validationContext);

        assertNotValid(result);
    }

    @Test
    public void testNotValidBecauseBothCredentialsServiceAndSasTokenSpecified() {
        configureCredentialsService();
        configureSasToken();

        Collection<ValidationResult> result = AzureStorageUtils.validateCredentialProperties(validationContext);

        assertNotValid(result);
    }

    @Test
    public void testNotValidBecauseAccountNameSpecifiedWithoutAccountKeyOrSasToken() {
        configureAccountName();

        Collection<ValidationResult> result = AzureStorageUtils.validateCredentialProperties(validationContext);

        assertNotValid(result);
    }

    @Test
    public void testNotValidBecauseAccountNameSpecifiedWithBothAccountKeyAndSasToken() {
        configureAccountName();
        configureAccountKey();
        configureSasToken();

        Collection<ValidationResult> result = AzureStorageUtils.validateCredentialProperties(validationContext);

        assertNotValid(result);
    }

    private void configureCredentialsService() {
        processContext.setProperty(AzureStorageUtils.STORAGE_CREDENTIALS_SERVICE, CREDENTIALS_SERVICE_VALUE);
    }

    private void configureAccountName() {
        processContext.setProperty(AzureStorageUtils.ACCOUNT_NAME, ACCOUNT_NAME_VALUE);
    }

    private void configureAccountKey() {
        processContext.setProperty(AzureStorageUtils.ACCOUNT_KEY, ACCOUNT_KEY_VALUE);
    }

    private void configureSasToken() {
        processContext.setProperty(AzureStorageUtils.PROP_SAS_TOKEN, SAS_TOKEN_VALUE);
    }

    private void assertValid(Collection<ValidationResult> result) {
        assertTrue("There should be no validation error", result.isEmpty());
    }

    private void assertNotValid(Collection<ValidationResult> result) {
        assertFalse("There should be validation error", result.isEmpty());
    }

}
