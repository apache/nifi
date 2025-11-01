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
import org.apache.nifi.services.gcp.GCPIdentityFederationTokenProvider;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON;
import static org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON_FILE;
import static org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors.USE_APPLICATION_DEFAULT_CREDENTIALS;
import static org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors.USE_COMPUTE_ENGINE_CREDENTIALS;
import static org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors.IDENTITY_FEDERATION_TOKEN_PROVIDER;
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

        runner.setProperty(serviceImpl, USE_APPLICATION_DEFAULT_CREDENTIALS, "true");
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
    public void testIdentityFederationCredentials() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(MockCredentialsServiceProcessor.class);
        final GCPCredentialsControllerService serviceImpl = new GCPCredentialsControllerService();
        runner.addControllerService("gcpCredentialsProvider", serviceImpl);

        final MockGCPIdentityFederationTokenProvider tokenProvider = new MockGCPIdentityFederationTokenProvider();
        runner.addControllerService("gcpIdentityFederation", tokenProvider);
        runner.enableControllerService(tokenProvider);

        runner.setProperty(serviceImpl, IDENTITY_FEDERATION_TOKEN_PROVIDER, "gcpIdentityFederation");

        runner.enableControllerService(serviceImpl);
        runner.assertValid(serviceImpl);

        final GCPCredentialsService service = (GCPCredentialsService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("gcpCredentialsProvider");

        assertNotNull(service);
        final GoogleCredentials credentials = service.getGoogleCredentials();
        assertNotNull(credentials);

        final com.google.auth.oauth2.AccessToken accessToken = credentials.getAccessToken();
        assertNotNull(accessToken);
        assertEquals(MockGCPIdentityFederationTokenProvider.ACCESS_TOKEN_VALUE, accessToken.getTokenValue());
    }

    @Test
    public void testIdentityFederationExclusiveConfiguration() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(MockCredentialsServiceProcessor.class);
        final GCPCredentialsControllerService serviceImpl = new GCPCredentialsControllerService();
        runner.addControllerService("gcpCredentialsProvider", serviceImpl);

        final MockGCPIdentityFederationTokenProvider tokenProvider = new MockGCPIdentityFederationTokenProvider();
        runner.addControllerService("gcpIdentityFederation", tokenProvider);
        runner.enableControllerService(tokenProvider);

        runner.setProperty(serviceImpl, IDENTITY_FEDERATION_TOKEN_PROVIDER, "gcpIdentityFederation");
        runner.setProperty(serviceImpl, SERVICE_ACCOUNT_JSON_FILE,
                "src/test/resources/mock-gcp-service-account.json");

        runner.assertNotValid(serviceImpl);
    }

    @Test
    public void testBadFileCredentials() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(MockCredentialsServiceProcessor.class);
        final GCPCredentialsControllerService serviceImpl = new GCPCredentialsControllerService();
        runner.addControllerService("gcpCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, SERVICE_ACCOUNT_JSON_FILE,
                "src/test/resources/bad-mock-gcp-service-account.json");
        runner.assertNotValid(serviceImpl);
    }

    @Test
    public void testMultipleCredentialSources() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(MockCredentialsServiceProcessor.class);
        final GCPCredentialsControllerService serviceImpl = new GCPCredentialsControllerService();

        runner.addControllerService("gcpCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, SERVICE_ACCOUNT_JSON_FILE,
                "src/test/resources/mock-gcp-service-account.json");
        runner.setProperty(serviceImpl, USE_APPLICATION_DEFAULT_CREDENTIALS, "true");
        runner.setProperty(serviceImpl, USE_COMPUTE_ENGINE_CREDENTIALS, "true");

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

    private static final class MockGCPIdentityFederationTokenProvider extends org.apache.nifi.controller.AbstractControllerService implements GCPIdentityFederationTokenProvider {
        private static final String ACCESS_TOKEN_VALUE = "federated-access-token";
        private static final long EXPIRES_IN_SECONDS = 3600;

        @Override
        public org.apache.nifi.oauth2.AccessToken getAccessDetails() {
            final org.apache.nifi.oauth2.AccessToken accessToken = new org.apache.nifi.oauth2.AccessToken();
            accessToken.setAccessToken(ACCESS_TOKEN_VALUE);
            accessToken.setExpiresIn(EXPIRES_IN_SECONDS);
            return accessToken;
        }
    }
}
