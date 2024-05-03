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

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestAzureStorageCredentialsControllerServiceLookup_v12 {

    private MockAzureStorageCredentialsService serviceA;
    private MockAzureStorageCredentialsService serviceB;

    private AzureStorageCredentialsControllerServiceLookup_v12 lookupService;
    private TestRunner runner;

    @BeforeEach
    public void setup() throws InitializationException {
        serviceA = new MockAzureStorageCredentialsService(AzureStorageCredentialsDetails_v12.createWithAccountKey("Account_Name_A", "core.windows.net", "Account_Key"));
        serviceB = new MockAzureStorageCredentialsService(AzureStorageCredentialsDetails_v12.createWithSasToken("Account_Name_B", "core.windows.net", "SAS_Token"));

        lookupService = new AzureStorageCredentialsControllerServiceLookup_v12();

        runner = TestRunners.newTestRunner(NoOpProcessor.class);

        final String serviceAIdentifier = "service-a";
        runner.addControllerService(serviceAIdentifier, serviceA);

        final String serviceBIdentifier = "service-b";
        runner.addControllerService(serviceBIdentifier, serviceB);

        runner.addControllerService("lookup-service", lookupService);
        runner.setProperty(lookupService, "a", serviceAIdentifier);
        runner.setProperty(lookupService, "b", serviceBIdentifier);

        runner.enableControllerService(serviceA);
        runner.enableControllerService(serviceB);
        runner.enableControllerService(lookupService);
    }

    @Test
    public void testLookupServiceA() {
        final Map<String,String> attributes = new HashMap<>();
        attributes.put(AzureStorageCredentialsControllerServiceLookup_v12.AZURE_STORAGE_CREDENTIALS_NAME_ATTRIBUTE, "a");

        final AzureStorageCredentialsDetails_v12 storageCredentialsDetails = lookupService.getCredentialsDetails(attributes);
        assertNotNull(storageCredentialsDetails);
        assertEquals("Account_Name_A", storageCredentialsDetails.getAccountName());
        assertEquals("core.windows.net", storageCredentialsDetails.getEndpointSuffix());
        assertEquals("Account_Key", storageCredentialsDetails.getAccountKey());
        assertNull(storageCredentialsDetails.getSasToken());
    }

    @Test
    public void testLookupServiceB() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(AzureStorageCredentialsControllerServiceLookup_v12.AZURE_STORAGE_CREDENTIALS_NAME_ATTRIBUTE, "b");

        final AzureStorageCredentialsDetails_v12 storageCredentialsDetails = lookupService.getCredentialsDetails(attributes);
        assertNotNull(storageCredentialsDetails);
        assertEquals("Account_Name_B", storageCredentialsDetails.getAccountName());
        assertEquals("core.windows.net", storageCredentialsDetails.getEndpointSuffix());
        assertEquals("SAS_Token", storageCredentialsDetails.getSasToken());
        assertNull(storageCredentialsDetails.getAccountKey());
    }

    @Test
    public void testLookupMissingCredentialsNameAttribute() {
        final Map<String, String> attributes = new HashMap<>();
        assertThrows(ProcessException.class, () -> lookupService.getCredentialsDetails(attributes));
    }

    @Test
    public void testLookupWithCredentialsNameThatDoesNotExist() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(AzureStorageCredentialsControllerServiceLookup_v12.AZURE_STORAGE_CREDENTIALS_NAME_ATTRIBUTE, "DOES-NOT-EXIST");
        assertThrows(ProcessException.class, () -> lookupService.getCredentialsDetails(attributes));
    }

    @Test
    public void testCustomValidateAtLeaseOneServiceDefined() throws InitializationException {
        // enable lookup service with no services registered, verify not valid
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService("lookup-service", lookupService);
        runner.assertNotValid(lookupService);

        final String serviceAIdentifier = "service-a";
        runner.addControllerService(serviceAIdentifier, serviceA);

        // register a service and now verify valid
        runner.setProperty(lookupService, "a", serviceAIdentifier);
        runner.enableControllerService(lookupService);
        runner.assertValid(lookupService);
    }

    @Test
    public void testCustomValidateSelfReferenceNotAllowed() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService("lookup-service", lookupService);
        runner.setProperty(lookupService, "lookup-service", "lookup-service");
        runner.assertNotValid(lookupService);
    }

    /**
     * A mock AzureStorageCredentialsService_v12 that will always return the passed in AzureStorageCredentialsDetails_v12.
     */
    private static class MockAzureStorageCredentialsService extends AbstractControllerService implements AzureStorageCredentialsService_v12 {

        private final AzureStorageCredentialsDetails_v12 storageCredentialsDetails;

        MockAzureStorageCredentialsService(AzureStorageCredentialsDetails_v12 storageCredentialsDetails) {
            this.storageCredentialsDetails = storageCredentialsDetails;
        }

        @Override
        public AzureStorageCredentialsDetails_v12 getCredentialsDetails(Map<String, String> attributes) {
            return storageCredentialsDetails;
        }
    }
}
