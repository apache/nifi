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

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestStandardAzureCredentialsControllerService {
    private static final String CREDENTIALS_SERVICE_IDENTIFIER = "credentials-service";
    private static final String SAMPLE_MANAGED_CLIENT_ID = "sample-managed-client-id";
    private static final String TOKEN_PROVIDER_IDENTIFIER = "identity-provider";

    private TestRunner runner;
    private StandardAzureCredentialsControllerService credentialsService;
    private MockIdentityFederationTokenProvider tokenProvider;

    @BeforeEach
    public void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        credentialsService = new StandardAzureCredentialsControllerService();
        runner.addControllerService(CREDENTIALS_SERVICE_IDENTIFIER, credentialsService);

        tokenProvider = new MockIdentityFederationTokenProvider();
        runner.addControllerService(TOKEN_PROVIDER_IDENTIFIER, tokenProvider);
        runner.enableControllerService(tokenProvider);
    }

    @Test
    public void testValidControllerServiceConfiguration() {
        runner.setProperty(credentialsService,
                StandardAzureCredentialsControllerService.CREDENTIAL_CONFIGURATION_STRATEGY,
                StandardAzureCredentialsControllerService.DEFAULT_CREDENTIAL);
        runner.assertValid(credentialsService);

        runner.setProperty(credentialsService,
                StandardAzureCredentialsControllerService.MANAGED_IDENTITY_CLIENT_ID,
                SAMPLE_MANAGED_CLIENT_ID);
        runner.assertValid(credentialsService);

        runner.setProperty(credentialsService,
                StandardAzureCredentialsControllerService.CREDENTIAL_CONFIGURATION_STRATEGY,
                StandardAzureCredentialsControllerService.MANAGED_IDENTITY);
        runner.assertValid(credentialsService);
    }

    @Test
    public void testNotValidControllerServiceBlankManagedIdentityClientId() {
        runner.setProperty(credentialsService,
                StandardAzureCredentialsControllerService.CREDENTIAL_CONFIGURATION_STRATEGY,
                StandardAzureCredentialsControllerService.MANAGED_IDENTITY);
        runner.setProperty(credentialsService,
                StandardAzureCredentialsControllerService.MANAGED_IDENTITY_CLIENT_ID,
                " ");
        runner.assertNotValid(credentialsService);

        runner.setProperty(credentialsService,
                StandardAzureCredentialsControllerService.MANAGED_IDENTITY_CLIENT_ID,
                "");
        runner.assertNotValid(credentialsService);

        runner.setProperty(credentialsService,
                StandardAzureCredentialsControllerService.MANAGED_IDENTITY_CLIENT_ID,
                (String) null);
        runner.assertValid(credentialsService);
    }

    @Test
    public void testOAuth2StrategyRequiresProvider() {
        runner.setProperty(credentialsService,
                StandardAzureCredentialsControllerService.CREDENTIAL_CONFIGURATION_STRATEGY,
                StandardAzureCredentialsControllerService.OAUTH2);

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testOAuth2StrategyProvidesTokenCredential() throws Exception {
        runner.setProperty(credentialsService,
                StandardAzureCredentialsControllerService.CREDENTIAL_CONFIGURATION_STRATEGY,
                StandardAzureCredentialsControllerService.OAUTH2);
        runner.setProperty(credentialsService,
                StandardAzureCredentialsControllerService.OAUTH2_ACCESS_TOKEN_PROVIDER,
                TOKEN_PROVIDER_IDENTIFIER);

        runner.assertValid(credentialsService);
        runner.enableControllerService(credentialsService);

        final TokenCredential tokenCredential = credentialsService.getCredentials();
        final AccessToken accessToken = tokenCredential.getToken(new TokenRequestContext().addScopes("https://storage.azure.com/.default")).block();
        assertNotNull(accessToken);
        assertEquals(MockIdentityFederationTokenProvider.ACCESS_TOKEN_VALUE, accessToken.getToken());
    }

    private static final class MockIdentityFederationTokenProvider extends AbstractControllerService implements AzureIdentityFederationTokenProvider {
        private static final String ACCESS_TOKEN_VALUE = "access-token";

        @Override
        public TokenCredential getCredentials() {
            return tokenRequestContext -> Mono.just(new AccessToken(ACCESS_TOKEN_VALUE, OffsetDateTime.now().plusHours(1)));
        }
    }
}
