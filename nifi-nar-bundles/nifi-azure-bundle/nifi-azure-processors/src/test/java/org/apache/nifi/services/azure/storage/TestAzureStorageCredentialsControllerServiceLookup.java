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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class TestAzureStorageCredentialsControllerServiceLookup {

    private MockAzureStorageCredentialsService serviceA;
    private MockAzureStorageCredentialsService serviceB;

    private AzureStorageCredentialsControllerServiceLookup lookupService;
    private TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        serviceA = new MockAzureStorageCredentialsService(
                new AzureStorageCredentialsDetails("Account_A", "core.windows.net", null));
        serviceB = new MockAzureStorageCredentialsService(new AzureStorageCredentialsDetails("Account_B", null, null));

        lookupService = new AzureStorageCredentialsControllerServiceLookup();

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
        attributes.put(AzureStorageCredentialsControllerServiceLookup.AZURE_STORAGE_CREDENTIALS_NAME_ATTRIBUTE, "a");

        final AzureStorageCredentialsDetails storageCredentialsDetails = lookupService.getStorageCredentialsDetails(attributes);
        assertNotNull(storageCredentialsDetails);
        assertEquals("Account_A", storageCredentialsDetails.getStorageAccountName());
        assertEquals("core.windows.net", storageCredentialsDetails.getStorageSuffix());
    }

    @Test
    public void testLookupServiceB() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(AzureStorageCredentialsControllerServiceLookup.AZURE_STORAGE_CREDENTIALS_NAME_ATTRIBUTE, "b");

        final AzureStorageCredentialsDetails storageCredentialsDetails = lookupService
                .getStorageCredentialsDetails(attributes);
        assertNotNull(storageCredentialsDetails);
        assertEquals("Account_B", storageCredentialsDetails.getStorageAccountName());
        assertNull(storageCredentialsDetails.getStorageSuffix());
    }

    @Test(expected = ProcessException.class)
    public void testLookupMissingCredentialsNameAttribute() {
        final Map<String, String> attributes = new HashMap<>();
        lookupService.getStorageCredentialsDetails(attributes);
    }

    @Test(expected = ProcessException.class)
    public void testLookupWithCredentialsNameThatDoesNotExist() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(AzureStorageCredentialsControllerServiceLookup.AZURE_STORAGE_CREDENTIALS_NAME_ATTRIBUTE,
                "DOES-NOT-EXIST");
        lookupService.getStorageCredentialsDetails(attributes);
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
     * A mock AzureStorageCredentialsService that will always return the passed in
     * AzureStorageCredentialsDetails.
     */
    private static class MockAzureStorageCredentialsService extends AbstractControllerService
            implements AzureStorageCredentialsService {

        private AzureStorageCredentialsDetails storageCredentialsDetails;

        MockAzureStorageCredentialsService(AzureStorageCredentialsDetails storageCredentialsDetails) {
            this.storageCredentialsDetails = storageCredentialsDetails;
        }

        @Override
        public AzureStorageCredentialsDetails getStorageCredentialsDetails(Map<String, String> attributes) {
            return storageCredentialsDetails;
        }
    }
}
