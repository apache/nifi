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

public class TestADLSCredentialsControllerServiceLookup {

    private MockADLSCredentialsService serviceA;
    private MockADLSCredentialsService serviceB;

    private ADLSCredentialsControllerServiceLookup lookupService;
    private TestRunner runner;

    @BeforeEach
    public void setup() throws InitializationException {
        serviceA = new MockADLSCredentialsService(ADLSCredentialsDetails.Builder.newBuilder()
                .setAccountName("Account_Name_A")
                .setAccountKey("Account_Key")
                .build());
        serviceB = new MockADLSCredentialsService(ADLSCredentialsDetails.Builder.newBuilder()
                .setAccountName("Account_Name_B")
                .setSasToken("SAS_Token")
                .build());

        lookupService = new ADLSCredentialsControllerServiceLookup();

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
        attributes.put(ADLSCredentialsControllerServiceLookup.ADLS_CREDENTIALS_NAME_ATTRIBUTE, "a");

        final ADLSCredentialsDetails adlsCredentialsDetails = lookupService.getCredentialsDetails(attributes);

        assertNotNull(adlsCredentialsDetails);
        assertEquals("Account_Name_A", adlsCredentialsDetails.getAccountName());
        assertEquals("Account_Key", adlsCredentialsDetails.getAccountKey());
        assertNull(adlsCredentialsDetails.getSasToken());
    }

    @Test
    public void testLookupServiceB() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ADLSCredentialsControllerServiceLookup.ADLS_CREDENTIALS_NAME_ATTRIBUTE, "b");

        final ADLSCredentialsDetails adlsCredentialsDetails = lookupService.getCredentialsDetails(attributes);

        assertNotNull(adlsCredentialsDetails);
        assertEquals("Account_Name_B", adlsCredentialsDetails.getAccountName());
        assertEquals("SAS_Token", adlsCredentialsDetails.getSasToken());
        assertNull(adlsCredentialsDetails.getAccountKey());
    }

    @Test
    public void testLookupMissingCredentialsNameAttribute() {
        final Map<String, String> attributes = new HashMap<>();

        assertThrows(ProcessException.class, () -> lookupService.getCredentialsDetails(attributes));
    }

    @Test
    public void testLookupWithCredentialsNameThatDoesNotExist() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ADLSCredentialsControllerServiceLookup.ADLS_CREDENTIALS_NAME_ATTRIBUTE, "DOES-NOT-EXIST");

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
     * A mock ADLSCredentialsService that will always return the passed in ADLSStorageCredentialsDetails.
     */
    private static class MockADLSCredentialsService extends AbstractControllerService implements ADLSCredentialsService {

        private final ADLSCredentialsDetails adlsCredentialsDetails;

        MockADLSCredentialsService(ADLSCredentialsDetails adlsCredentialsDetails) {
            this.adlsCredentialsDetails = adlsCredentialsDetails;
        }

        @Override
        public ADLSCredentialsDetails getCredentialsDetails(final Map<String, String> attributes) {
            return adlsCredentialsDetails;
        }
    }
}
