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
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.azure.util.AzureWorkloadIdentityCredentialUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Controller Service that provides Azure {@link TokenCredential} for workload identity federation.
 * Uses {@link AzureWorkloadIdentityCredentialUtils} to build the credential from an external identity token.
 */
@Tags({"azure", "identity", "federation", "credentials", "workload"})
@CapabilityDescription("Provides Azure TokenCredential for workload identity federation. "
        + "Exchanges external identity tokens (from an OAuth2 provider) for Azure AD access tokens "
        + "using Azure Identity SDK's ClientAssertionCredential with built-in caching and retry logic.")
public class StandardAzureIdentityFederationTokenProvider extends AbstractControllerService
        implements AzureIdentityFederationTokenProvider, VerifiableControllerService {

    private static final String DEFAULT_VERIFICATION_SCOPE = "https://storage.azure.com/.default";
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

    public static final PropertyDescriptor CLIENT_ASSERTION_PROVIDER = new PropertyDescriptor.Builder()
            .name("Client Assertion Provider")
            .description("Controller Service that retrieves the external workload identity token (client assertion) exchanged with Azure AD.")
            .identifiesControllerService(OAuth2AccessTokenProvider.class)
            .required(true)
            .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = List.of(
            TENANT_ID,
            CLIENT_ID,
            CLIENT_ASSERTION_PROVIDER
    );

    private volatile TokenCredential credential;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final String tenantId = context.getProperty(TENANT_ID).getValue();
        final String clientId = context.getProperty(CLIENT_ID).getValue();
        final OAuth2AccessTokenProvider clientAssertionProvider = context.getProperty(CLIENT_ASSERTION_PROVIDER)
                .asControllerService(OAuth2AccessTokenProvider.class);

        this.credential = AzureWorkloadIdentityCredentialUtils.createCredential(tenantId, clientId, clientAssertionProvider);
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
            final OAuth2AccessTokenProvider verificationAssertionProvider = context.getProperty(CLIENT_ASSERTION_PROVIDER)
                    .asControllerService(OAuth2AccessTokenProvider.class);

            final TokenCredential verificationCredential = AzureWorkloadIdentityCredentialUtils.createCredential(
                    tenantId, clientId, verificationAssertionProvider);

            final TokenRequestContext tokenRequestContext = new TokenRequestContext().addScopes(DEFAULT_VERIFICATION_SCOPE);
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
}
