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
package org.apache.nifi.services.azure;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link StandardAzureIdentityFederationTokenProvider}.
 *
 * Note: These tests validate configuration and property handling only.
 * Actual token exchange with Azure AD requires integration testing with real credentials.
 */
public class TestStandardAzureIdentityFederationTokenProvider {

    private TestRunner runner;
    private StandardAzureIdentityFederationTokenProvider tokenProvider;

    @BeforeEach
    public void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);

        tokenProvider = new StandardAzureIdentityFederationTokenProvider();
        runner.addControllerService("identity-provider", tokenProvider);

        final MockOAuth2AccessTokenProvider assertionProvider = new MockOAuth2AccessTokenProvider();
        runner.addControllerService("assertion-provider", assertionProvider);
        runner.enableControllerService(assertionProvider);
    }

    @Test
    public void testValidConfiguration() {
        runner.setProperty(tokenProvider, StandardAzureIdentityFederationTokenProvider.TENANT_ID, "tenant-id");
        runner.setProperty(tokenProvider, StandardAzureIdentityFederationTokenProvider.CLIENT_ID, "client-id");
        runner.setProperty(tokenProvider, StandardAzureIdentityFederationTokenProvider.CLIENT_ASSERTION_PROVIDER, "assertion-provider");

        runner.assertValid(tokenProvider);
    }

    @Test
    public void testInvalidWithoutTenantId() {
        runner.setProperty(tokenProvider, StandardAzureIdentityFederationTokenProvider.CLIENT_ID, "client-id");
        runner.setProperty(tokenProvider, StandardAzureIdentityFederationTokenProvider.CLIENT_ASSERTION_PROVIDER, "assertion-provider");

        runner.assertNotValid(tokenProvider);
    }

    @Test
    public void testInvalidWithoutClientId() {
        runner.setProperty(tokenProvider, StandardAzureIdentityFederationTokenProvider.TENANT_ID, "tenant-id");
        runner.setProperty(tokenProvider, StandardAzureIdentityFederationTokenProvider.CLIENT_ASSERTION_PROVIDER, "assertion-provider");

        runner.assertNotValid(tokenProvider);
    }

    @Test
    public void testInvalidWithoutClientAssertionProvider() {
        runner.setProperty(tokenProvider, StandardAzureIdentityFederationTokenProvider.TENANT_ID, "tenant-id");
        runner.setProperty(tokenProvider, StandardAzureIdentityFederationTokenProvider.CLIENT_ID, "client-id");

        runner.assertNotValid(tokenProvider);
    }

    private static class MockOAuth2AccessTokenProvider extends AbstractControllerService implements OAuth2AccessTokenProvider {
        @Override
        public AccessToken getAccessDetails() {
            final AccessToken accessToken = new AccessToken();
            accessToken.setAccessToken("client-assertion-token");
            accessToken.setExpiresIn(600L);
            return accessToken;
        }
    }
}
