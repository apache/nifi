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

import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.identity.ClientAssertionCredential;
import com.azure.identity.ClientAssertionCredentialBuilder;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Controller Service that exchanges an external identity token for an Azure AD access token
 * using the Microsoft Entra workload identity federation flow via {@link ClientAssertionCredential}.
 */
@Tags({"azure", "identity", "federation", "credentials", "workload"})
@CapabilityDescription("Exchanges workload identity tokens for Azure AD access tokens suitable for accessing Azure services. "
        + "Uses Azure Identity SDK's ClientAssertionCredential for robust token exchange with built-in caching and retry logic.")
public class StandardAzureIdentityFederationTokenProvider extends AbstractControllerService
        implements AzureIdentityFederationTokenProvider, VerifiableControllerService {

    private static final String DEFAULT_SCOPE = "https://storage.azure.com/.default";
    private static final String ERROR_EXCHANGE_FAILED = "Failed to exchange workload identity token: %s";
    private static final String STEP_EXCHANGE_TOKEN = "Exchange workload identity token";

    public static final PropertyDescriptor TENANT_ID = new PropertyDescriptor.Builder()
            .name("Tenant ID")
            .description("Microsoft Entra tenant ID.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLIENT_ID = new PropertyDescriptor.Builder()
            .name("Client ID")
            .description("Application (client) ID of the Microsoft Entra application registration configured for workload identity federation.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor SCOPE = new PropertyDescriptor.Builder()
            .name("Scope")
            .description("OAuth2 scope requested from Azure AD. Defaults to https://storage.azure.com/.default for Azure Storage access.")
            .required(true)
            .defaultValue(DEFAULT_SCOPE)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLIENT_ASSERTION_PROVIDER = new PropertyDescriptor.Builder()
            .name("Client Assertion Provider")
            .description("Controller Service that retrieves the external workload identity token (client assertion) exchanged with Azure AD.")
            .identifiesControllerService(OAuth2AccessTokenProvider.class)
            .required(true)
            .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = List.of(
            TENANT_ID,
            CLIENT_ID,
            SCOPE,
            CLIENT_ASSERTION_PROVIDER
    );

    private volatile ClientAssertionCredential credential;
    private volatile OAuth2AccessTokenProvider clientAssertionProvider;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final String tenantId = context.getProperty(TENANT_ID).getValue();
        final String clientId = context.getProperty(CLIENT_ID).getValue();
        this.clientAssertionProvider = context.getProperty(CLIENT_ASSERTION_PROVIDER)
                .asControllerService(OAuth2AccessTokenProvider.class);

        this.credential = new ClientAssertionCredentialBuilder()
                .tenantId(tenantId)
                .clientId(clientId)
                .clientAssertion(this::getClientAssertion)
                .httpClient(new NettyAsyncHttpClientBuilder().build())
                .build();
    }

    @Override
    public TokenCredential getCredentials() {
        return credential;
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context,
                                                  final ComponentLog verificationLogger,
                                                  final Map<String, String> variables) {
        final ConfigVerificationResult.Builder resultBuilder = new ConfigVerificationResult.Builder()
                .verificationStepName(STEP_EXCHANGE_TOKEN);

        try {
            final String tenantId = context.getProperty(TENANT_ID).getValue();
            final String clientId = context.getProperty(CLIENT_ID).getValue();
            final String verificationScope = context.getProperty(SCOPE).getValue();
            final OAuth2AccessTokenProvider verificationAssertionProvider = context.getProperty(CLIENT_ASSERTION_PROVIDER)
                    .asControllerService(OAuth2AccessTokenProvider.class);

            final ClientAssertionCredential verificationCredential = new ClientAssertionCredentialBuilder()
                    .tenantId(tenantId)
                    .clientId(clientId)
                    .clientAssertion(() -> verificationAssertionProvider.getAccessDetails().getAccessToken())
                    .httpClient(new NettyAsyncHttpClientBuilder().build())
                    .build();

            final TokenRequestContext tokenRequestContext = new TokenRequestContext().addScopes(verificationScope);
            verificationCredential.getToken(tokenRequestContext).block();

            return Collections.singletonList(resultBuilder
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation("Successfully exchanged workload identity token for an Azure AD access token")
                    .build());
        } catch (final Exception e) {
            final String explanation = String.format(ERROR_EXCHANGE_FAILED, e.getMessage());
            verificationLogger.error(explanation, e);
            return Collections.singletonList(resultBuilder
                    .outcome(Outcome.FAILED)
                    .explanation(explanation)
                    .build());
        }
    }

    private String getClientAssertion() {
        final AccessToken assertionToken = clientAssertionProvider.getAccessDetails();
        if (assertionToken == null) {
            throw new IllegalStateException("Client assertion provider returned null");
        }
        final String assertion = assertionToken.getAccessToken();
        if (assertion == null || assertion.isBlank()) {
            throw new IllegalStateException("Client assertion provider returned empty token");
        }
        return assertion;
    }
}
