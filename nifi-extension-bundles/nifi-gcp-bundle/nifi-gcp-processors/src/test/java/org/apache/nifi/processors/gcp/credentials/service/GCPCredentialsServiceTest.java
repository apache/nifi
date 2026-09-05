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

import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdentityPoolCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.gcp.credentials.service.GCPCredentialsService;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processors.gcp.credentials.factory.AuthenticationStrategy;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors.AUTHENTICATION_STRATEGY;
import static org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON;
import static org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON_FILE;
import static org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors.TARGET_SERVICE_ACCOUNT;
import static org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors.WORKLOAD_IDENTITY_AUDIENCE;
import static org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors.WORKLOAD_IDENTITY_SCOPE;
import static org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors.WORKLOAD_IDENTITY_SUBJECT_TOKEN_PROVIDER;
import static org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors.WORKLOAD_IDENTITY_SUBJECT_TOKEN_TYPE;
import static org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors.WORKLOAD_IDENTITY_TOKEN_ENDPOINT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GCPCredentialsServiceTest {
    private static final String WORKLOAD_IDENTITY_AUDIENCE_VALUE = "projects/123456789/locations/global/workloadIdentityPools/pool/providers/provider";
    private static final String WORKLOAD_IDENTITY_SCOPE_VALUE = "https://www.googleapis.com/auth/cloud-platform";
    private static final String WORKLOAD_IDENTITY_TOKEN_ENDPOINT_VALUE = "https://sts.googleapis.com/v1/token";
    private static final String WORKLOAD_IDENTITY_SUBJECT_TOKEN_TYPE_VALUE = "urn:ietf:params:oauth:token-type:jwt";
    private static final String TARGET_SERVICE_ACCOUNT_VALUE = "target-account@test-project.iam.gserviceaccount.com";

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
    public void testWorkloadIdentityFederationCredentials() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(MockCredentialsServiceProcessor.class);
        final GCPCredentialsControllerService serviceImpl = new GCPCredentialsControllerService();
        runner.addControllerService("gcpCredentialsProvider", serviceImpl);

        final MockOAuth2AccessTokenProvider subjectTokenProvider = new MockOAuth2AccessTokenProvider();
        runner.addControllerService("subjectTokenProvider", subjectTokenProvider);
        runner.enableControllerService(subjectTokenProvider);

        configureWorkloadIdentityFederation(runner, serviceImpl);

        runner.enableControllerService(serviceImpl);
        runner.assertValid(serviceImpl);

        final GCPCredentialsService service = (GCPCredentialsService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("gcpCredentialsProvider");

        assertNotNull(service);
        final GoogleCredentials credentials = service.getGoogleCredentials();
        assertNotNull(credentials);
        assertEquals(IdentityPoolCredentials.class, credentials.getClass());
    }

    @Test
    public void testWorkloadIdentityFederationImpersonationCredentials() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(MockCredentialsServiceProcessor.class);
        final GCPCredentialsControllerService serviceImpl = new GCPCredentialsControllerService();
        runner.addControllerService("gcpCredentialsProvider", serviceImpl);

        final MockOAuth2AccessTokenProvider subjectTokenProvider = new MockOAuth2AccessTokenProvider();
        runner.addControllerService("subjectTokenProvider", subjectTokenProvider);
        runner.enableControllerService(subjectTokenProvider);

        configureWorkloadIdentityFederation(runner, serviceImpl);
        runner.setProperty(serviceImpl, TARGET_SERVICE_ACCOUNT, TARGET_SERVICE_ACCOUNT_VALUE);

        runner.enableControllerService(serviceImpl);
        runner.assertValid(serviceImpl);

        final GCPCredentialsService service = (GCPCredentialsService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("gcpCredentialsProvider");

        assertNotNull(service);
        final GoogleCredentials credentials = service.getGoogleCredentials();
        assertNotNull(credentials);
        final ImpersonatedCredentials impersonatedCredentials = assertInstanceOf(ImpersonatedCredentials.class, credentials);
        assertEquals(TARGET_SERVICE_ACCOUNT_VALUE, impersonatedCredentials.getAccount());
        assertInstanceOf(IdentityPoolCredentials.class, impersonatedCredentials.getSourceCredentials());
        assertTrue(impersonatedCredentials.createScopedRequired());

        final GoogleCredentials scopedCredentials = credentials.createScoped(List.of("https://www.googleapis.com/auth/sqlservice.login"));

        final ImpersonatedCredentials scopedImpersonatedCredentials = assertInstanceOf(ImpersonatedCredentials.class, scopedCredentials);
        assertEquals(TARGET_SERVICE_ACCOUNT_VALUE, scopedImpersonatedCredentials.getAccount());
        assertEquals(impersonatedCredentials.getSourceCredentials(), scopedImpersonatedCredentials.getSourceCredentials());
        assertEquals(List.of("https://www.googleapis.com/auth/sqlservice.login"), scopedImpersonatedCredentials.toBuilder().getScopes());
        assertFalse(scopedImpersonatedCredentials.createScopedRequired());
        assertNotNull(impersonatedCredentials.toBuilder().getHttpTransportFactory());
        assertEquals(impersonatedCredentials.toBuilder().getHttpTransportFactory(), scopedImpersonatedCredentials.toBuilder().getHttpTransportFactory());
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
    public void testMultipleCredentialSourcesRemainValid() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(MockCredentialsServiceProcessor.class);
        final GCPCredentialsControllerService serviceImpl = new GCPCredentialsControllerService();

        runner.addControllerService("gcpCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, AUTHENTICATION_STRATEGY, AuthenticationStrategy.SERVICE_ACCOUNT_JSON_FILE.getValue());
        runner.setProperty(serviceImpl, SERVICE_ACCOUNT_JSON_FILE,
                "src/test/resources/mock-gcp-service-account.json");
        runner.setProperty(serviceImpl, SERVICE_ACCOUNT_JSON,
                "{\"mock\":\"json\"}");

        runner.assertValid(serviceImpl);
    }

    @Test
    public void testComputeEngineCredentials() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(MockCredentialsServiceProcessor.class);
        final GCPCredentialsControllerService serviceImpl = new GCPCredentialsControllerService();
        runner.addControllerService("gcpCredentialsProvider", serviceImpl);

        runner.setProperty(serviceImpl, AUTHENTICATION_STRATEGY, AuthenticationStrategy.COMPUTE_ENGINE.getValue());
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);

        final GCPCredentialsService service = (GCPCredentialsService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("gcpCredentialsProvider");

        assertNotNull(service);
        final GoogleCredentials credentials = service.getGoogleCredentials();
        assertNotNull(credentials);

        assertEquals(ComputeEngineCredentials.class, credentials.getClass(),
                "Credentials class should be equal");
    }

    @Test
    public void testTargetServiceAccountBlankRejected() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(MockCredentialsServiceProcessor.class);
        final GCPCredentialsControllerService serviceImpl = new GCPCredentialsControllerService();
        runner.addControllerService("gcpCredentialsProvider", serviceImpl);

        final MockOAuth2AccessTokenProvider subjectTokenProvider = new MockOAuth2AccessTokenProvider();
        runner.addControllerService("subjectTokenProvider", subjectTokenProvider);
        runner.enableControllerService(subjectTokenProvider);

        configureWorkloadIdentityFederation(runner, serviceImpl);
        runner.setProperty(serviceImpl, TARGET_SERVICE_ACCOUNT, " ");

        assertHasInvalidResult(runner.validate(serviceImpl), "must contain at least one character that is not white space");
    }

    @Test
    public void testTargetServiceAccountRequiresWorkloadIdentityFederation() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(MockCredentialsServiceProcessor.class);
        final GCPCredentialsControllerService serviceImpl = new GCPCredentialsControllerService();
        runner.addControllerService("gcpCredentialsProvider", serviceImpl);

        runner.setProperty(serviceImpl, AUTHENTICATION_STRATEGY, AuthenticationStrategy.APPLICATION_DEFAULT.getValue());
        runner.setProperty(serviceImpl, TARGET_SERVICE_ACCOUNT, TARGET_SERVICE_ACCOUNT_VALUE);

        assertHasInvalidResult(runner.validate(serviceImpl), "Target Service Account requires Workload Identity Federation");
    }

    @Test
    public void testAdditionalDetailsDocumentTargetServiceAccountVerificationSemantics() throws IOException {
        final String resourcePath = "docs/%s/additionalDetails.md".formatted(GCPCredentialsControllerService.class.getName());
        try (InputStream inputStream = GCPCredentialsControllerService.class.getClassLoader().getResourceAsStream(resourcePath)) {
            assertNotNull(inputStream);
            final String additionalDetails = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            assertTrue(additionalDetails.contains("Target Service Account"));
            assertTrue(additionalDetails.contains("roles/iam.workloadIdentityUser"));
            assertTrue(additionalDetails.contains("Successful verification confirms only that NiFi can"));
            assertTrue(additionalDetails.contains("Verify does not perform STS exchange"));
        }
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

    private static void configureWorkloadIdentityFederation(final TestRunner runner, final GCPCredentialsControllerService serviceImpl) {
        runner.setProperty(serviceImpl, AUTHENTICATION_STRATEGY, AuthenticationStrategy.WORKLOAD_IDENTITY_FEDERATION.getValue());
        runner.setProperty(serviceImpl, WORKLOAD_IDENTITY_AUDIENCE, WORKLOAD_IDENTITY_AUDIENCE_VALUE);
        runner.setProperty(serviceImpl, WORKLOAD_IDENTITY_SCOPE, WORKLOAD_IDENTITY_SCOPE_VALUE);
        runner.setProperty(serviceImpl, WORKLOAD_IDENTITY_TOKEN_ENDPOINT, WORKLOAD_IDENTITY_TOKEN_ENDPOINT_VALUE);
        runner.setProperty(serviceImpl, WORKLOAD_IDENTITY_SUBJECT_TOKEN_TYPE, WORKLOAD_IDENTITY_SUBJECT_TOKEN_TYPE_VALUE);
        runner.setProperty(serviceImpl, WORKLOAD_IDENTITY_SUBJECT_TOKEN_PROVIDER, "subjectTokenProvider");
    }

    private static void assertHasInvalidResult(final Collection<ValidationResult> validationResults, final String explanationFragment) {
        final List<String> explanations = new ArrayList<>(validationResults.size());
        for (final ValidationResult validationResult : validationResults) {
            if (!validationResult.isValid()) {
                explanations.add(validationResult.getExplanation());
            }
        }

        final boolean explanationFound = explanations.stream()
                .anyMatch(explanation -> explanation != null && explanation.contains(explanationFragment));
        assertTrue(explanationFound, () -> "Expected invalid result containing [%s] but found %s".formatted(explanationFragment, explanations));
    }

    private static final class MockOAuth2AccessTokenProvider extends AbstractControllerService implements OAuth2AccessTokenProvider {
        private static final String ACCESS_TOKEN_VALUE = "federated-access-token";
        private static final long EXPIRES_IN_SECONDS = 3600;

        @Override
        public AccessToken getAccessDetails() {
            final AccessToken accessToken = new AccessToken();
            accessToken.setAccessToken(ACCESS_TOKEN_VALUE);
            accessToken.setExpiresIn(EXPIRES_IN_SECONDS);
            return accessToken;
        }

        @Override
        public void refreshAccessDetails() {
            // Intentionally left blank for test implementation
        }
    }
}
