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

import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpRequest;
import com.azure.core.util.Base64Util;
import com.azure.storage.common.StorageSharedKeyCredential;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.azure.storage.ListAzureBlobStorage;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsControllerService;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsDetails;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockProcessContext;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestAzureStorageUtilsGetStorageCredentialsDetails {

    private static final String CREDENTIALS_SERVICE_VALUE = "CredentialsService";
    private static final String ACCOUNT_NAME_VALUE = "AccountName";

    private static final String ACCOUNT_KEY_VALUE = Base64Util.encodeToString("AccountKey".getBytes());
    private static final String SAS_TOKEN_VALUE = "SasToken";

    private MockProcessContext processContext;

    @Before
    public void setUp() {
        Processor processor = new ListAzureBlobStorage();
        processContext = new MockProcessContext(processor);
    }

    @Test
    public void testAccountNameAndAccountKeyConfiguredOnProcessor() {
        configureProcessorProperties(ACCOUNT_NAME_VALUE, ACCOUNT_KEY_VALUE, null);

        AzureStorageCredentialsDetails storageCredentialsDetails = AzureStorageUtils
                .getStorageCredentialsDetails(processContext, null);

        assertStorageCredentialsDetailsAccountNameAndAccountKey(storageCredentialsDetails);
    }

    @Test
    public void testAccountNameAndSasTokenConfiguredOnProcessor() {
        configureProcessorProperties(ACCOUNT_NAME_VALUE, null, SAS_TOKEN_VALUE);
        AzureStorageCredentialsDetails storageCredentialsDetails = AzureStorageUtils
                .getStorageCredentialsDetails(processContext, null);

        assertStorageCredentialsDetailsAccountNameAndSasToken(storageCredentialsDetails);
    }

    @Test
    public void testAccountNameAndAccountKeyConfiguredOnControllerService() {

        configureControllerService(ACCOUNT_NAME_VALUE, ACCOUNT_KEY_VALUE, null);
        AzureStorageCredentialsDetails storageCredentialsDetails = AzureStorageUtils
                .getStorageCredentialsDetails(processContext, null);

        assertStorageCredentialsDetailsAccountNameAndAccountKey(storageCredentialsDetails);
    }

    @Test
    public void testAccountNameAndSasTokenConfiguredOnControllerService() {
        configureControllerService(ACCOUNT_NAME_VALUE, null, SAS_TOKEN_VALUE);

        AzureStorageCredentialsDetails storageCredentialsDetails = AzureStorageUtils
                .getStorageCredentialsDetails(processContext, null);

        assertStorageCredentialsDetailsAccountNameAndSasToken(storageCredentialsDetails);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAccountNameMissingConfiguredOnProcessor() {
        configureProcessorProperties(null, ACCOUNT_KEY_VALUE, null);

        AzureStorageUtils.getStorageCredentialsDetails(processContext, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAccountKeyAndSasTokenMissingConfiguredOnProcessor() {
        configureProcessorProperties(ACCOUNT_NAME_VALUE, null, null);
        AzureStorageUtils.getStorageCredentialsDetails(processContext, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAccountNameMissingConfiguredOnControllerService() {
        configureControllerService(null, ACCOUNT_KEY_VALUE, null);

        AzureStorageUtils.getStorageCredentialsDetails(processContext, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAccountKeyAndSasTokenMissingConfiguredOnControllerService() {
        configureControllerService(ACCOUNT_NAME_VALUE, null, null);

        AzureStorageUtils.getStorageCredentialsDetails(processContext, null);
    }

    private void configureProcessorProperties(String accountName, String accountKey, String sasToken) {

        if (accountName != null) {
            processContext.setProperty(AzureStorageUtils.ACCOUNT_NAME, ACCOUNT_NAME_VALUE);
        }
        if (accountKey != null) {
            processContext.setProperty(AzureStorageUtils.ACCOUNT_KEY, ACCOUNT_KEY_VALUE);
        }
        if (sasToken != null) {
            processContext.setProperty(AzureStorageUtils.PROP_SAS_TOKEN, sasToken);
        }
    }

    private void configureControllerService(String accountName, String accountKey, String sasToken) {

        AzureStorageCredentialsControllerService credentialsService = new AzureStorageCredentialsControllerService();
        Map<PropertyDescriptor, String> properties = new HashMap<>();

        if (accountName != null) {
            properties.put(AzureStorageUtils.ACCOUNT_NAME, accountName);
        }
        if (accountKey != null) {
            properties.put(AzureStorageUtils.ACCOUNT_KEY, accountKey);
        }
        if (sasToken != null) {
            properties.put(AzureStorageUtils.PROP_SAS_TOKEN, sasToken);
        }

        MockConfigurationContext configurationContext = new MockConfigurationContext(properties, null);
        credentialsService.onEnabled(configurationContext);

        processContext.addControllerService(credentialsService, CREDENTIALS_SERVICE_VALUE);
        processContext.setProperty(AzureStorageUtils.STORAGE_CREDENTIALS_SERVICE, CREDENTIALS_SERVICE_VALUE);
    }

    private void assertStorageCredentialsDetailsAccountNameAndAccountKey(
            AzureStorageCredentialsDetails storageCredentialsDetails) {

        assertEquals(ACCOUNT_NAME_VALUE, storageCredentialsDetails.getStorageAccountName());
        assertTrue(storageCredentialsDetails.getCredentialType() == AzureStorageCredentialsDetails.CredentialType.STORAGE_ACCOUNT_KEY);
        assertTrue(storageCredentialsDetails.getStorageSharedKeyCredential() instanceof StorageSharedKeyCredential);

        // test credential object
        StorageSharedKeyCredential storageCredentials = (StorageSharedKeyCredential) storageCredentialsDetails
                .getStorageSharedKeyCredential();
        assertEquals(ACCOUNT_NAME_VALUE, storageCredentials.getAccountName());

        final HttpRequest httpRequest = new HttpRequest(HttpMethod.GET, "http://www.microsoft.com")
                .setHeader("Content-Length", "0");
        final StorageSharedKeyCredential storageSharedKeyCredential = new StorageSharedKeyCredential(ACCOUNT_NAME_VALUE,
                ACCOUNT_KEY_VALUE);
        final String testAuthorizationHeader = storageSharedKeyCredential.generateAuthorizationHeader(
                httpRequest.getUrl(), httpRequest.getHttpMethod().toString(), httpRequest.getHeaders().toMap());

        assertEquals(
                storageCredentials.generateAuthorizationHeader(
                        httpRequest.getUrl(),
                        httpRequest.getHttpMethod().toString(),
                        httpRequest.getHeaders().toMap()
                ), testAuthorizationHeader);
    }

    private void assertStorageCredentialsDetailsAccountNameAndSasToken(AzureStorageCredentialsDetails storageCredentialsDetails) {
        assertEquals(ACCOUNT_NAME_VALUE, storageCredentialsDetails.getStorageAccountName());
        assertTrue(storageCredentialsDetails.getCredentialType() == AzureStorageCredentialsDetails.CredentialType.SAS_TOKEN);
        assertTrue(storageCredentialsDetails.getSasToken() instanceof String);

        // test credential object
        String sasToken = storageCredentialsDetails.getSasToken();
        assertEquals(SAS_TOKEN_VALUE, sasToken);
    }
}
