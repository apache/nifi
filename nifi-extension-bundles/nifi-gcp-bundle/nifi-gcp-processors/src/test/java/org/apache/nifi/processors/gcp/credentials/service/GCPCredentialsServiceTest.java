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
package org.apache.nifi.processors.gcp.credentials.service;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import org.apache.nifi.gcp.credentials.service.GCPCredentialsService;
import org.apache.nifi.processors.gcp.credentials.factory.AuthenticationStrategy;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors.AUTHENTICATION_STRATEGY;
import static org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON;
import static org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON_FILE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


public class GCPCredentialsServiceTest {
    @Test
    public void testToString() throws Exception {
        // toString method shouldn't cause an exception
        final GCPCredentialsControllerService service = new GCPCredentialsControllerService();
        service.toString();
    }

    @Test
    public void testDefaultingToApplicationDefault() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(MockCredentialsServiceProcessor.class);
        final GCPCredentialsControllerService serviceImpl = new GCPCredentialsControllerService();
        runner.addControllerService("gcpCredentialsProvider", serviceImpl);

        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);

        final GCPCredentialsService service = (GCPCredentialsService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("gcpCredentialsProvider");

        assertNotNull(service);
        final GoogleCredentials credentials = service.getGoogleCredentials();
        assertNotNull(credentials);
    }

    @Test
    public void testExplicitApplicationDefault() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(MockCredentialsServiceProcessor.class);
        final GCPCredentialsControllerService serviceImpl = new GCPCredentialsControllerService();
        runner.addControllerService("gcpCredentialsProvider", serviceImpl);

        runner.setProperty(serviceImpl, AUTHENTICATION_STRATEGY, AuthenticationStrategy.APPLICATION_DEFAULT.getValue());
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);

        final GCPCredentialsService service = (GCPCredentialsService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("gcpCredentialsProvider");

        assertNotNull(service);
        final GoogleCredentials credentials = service.getGoogleCredentials();
        assertNotNull(credentials);
    }

    @Test
    public void testFileCredentials() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(MockCredentialsServiceProcessor.class);
        final GCPCredentialsControllerService serviceImpl = new GCPCredentialsControllerService();
        runner.addControllerService("gcpCredentialsProvider", serviceImpl);

        runner.setProperty(serviceImpl, AUTHENTICATION_STRATEGY, AuthenticationStrategy.SERVICE_ACCOUNT_JSON_FILE.getValue());
        runner.setProperty(serviceImpl, SERVICE_ACCOUNT_JSON_FILE,
                "src/test/resources/mock-gcp-service-account.json");
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);

        final GCPCredentialsService service = (GCPCredentialsService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("gcpCredentialsProvider");

        assertNotNull(service);
        final GoogleCredentials credentials = service.getGoogleCredentials();
        assertNotNull(credentials);

        assertEquals(ServiceAccountCredentials.class, credentials.getClass(),
                "Credentials class should be equal");
    }

    @Test
    public void testBadFileCredentials() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(MockCredentialsServiceProcessor.class);
        final GCPCredentialsControllerService serviceImpl = new GCPCredentialsControllerService();
        runner.addControllerService("gcpCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, AUTHENTICATION_STRATEGY, AuthenticationStrategy.SERVICE_ACCOUNT_JSON_FILE.getValue());
        runner.setProperty(serviceImpl, SERVICE_ACCOUNT_JSON_FILE,
                "src/test/resources/bad-mock-gcp-service-account.json");
        runner.assertNotValid(serviceImpl);
    }

    @Test
    public void testMultipleCredentialSources() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(MockCredentialsServiceProcessor.class);
        final GCPCredentialsControllerService serviceImpl = new GCPCredentialsControllerService();

        runner.addControllerService("gcpCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, AUTHENTICATION_STRATEGY, AuthenticationStrategy.SERVICE_ACCOUNT_JSON_FILE.getValue());
        runner.setProperty(serviceImpl, SERVICE_ACCOUNT_JSON_FILE,
                "src/test/resources/mock-gcp-service-account.json");
        runner.setProperty(serviceImpl, SERVICE_ACCOUNT_JSON,
                "{\"mock\":\"json\"}");

        runner.assertNotValid(serviceImpl);
    }

    @Test
    public void testRawJsonCredentials() throws Exception {
        final String jsonRead = new String(
                Files.readAllBytes(Paths.get("src/test/resources/mock-gcp-service-account.json"))
        );

        final TestRunner runner = TestRunners.newTestRunner(MockCredentialsServiceProcessor.class);
        final GCPCredentialsControllerService serviceImpl = new GCPCredentialsControllerService();
        runner.addControllerService("gcpCredentialsProvider", serviceImpl);

        runner.setProperty(serviceImpl, AUTHENTICATION_STRATEGY, AuthenticationStrategy.SERVICE_ACCOUNT_JSON.getValue());
        runner.setProperty(serviceImpl, SERVICE_ACCOUNT_JSON,
                jsonRead);
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);

        final GCPCredentialsService service = (GCPCredentialsService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("gcpCredentialsProvider");

        assertNotNull(service);
        final GoogleCredentials credentials = service.getGoogleCredentials();
        assertNotNull(credentials);

        assertEquals(ServiceAccountCredentials.class, credentials.getClass(),
                "Credentials class should be equal");
    }
}
