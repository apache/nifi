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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Controller Service that exchanges an external identity token for an Azure AD access token
 * using the Microsoft Entra workload identity federation flow.
 */
@Tags({ "azure", "oauth2", "identity", "federation", "credentials" })
@CapabilityDescription("Exchanges workload identity tokens for Azure AD access tokens suitable for accessing Azure services.")
public class StandardAzureIdentityFederationTokenProvider extends AbstractControllerService implements AzureIdentityFederationTokenProvider, VerifiableControllerService {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String DEFAULT_SCOPE = "https://storage.azure.com/.default";
    private static final String FORM_CONTENT_TYPE = "application/x-www-form-urlencoded";
    private static final String ACCEPT_HEADER = "application/json";
    private static final String HEADER_CONTENT_TYPE = "Content-Type";
    private static final String HEADER_ACCEPT = "Accept";
    private static final String PARAM_GRANT_TYPE = "grant_type";
    private static final String PARAM_CLIENT_ID = "client_id";
    private static final String PARAM_SCOPE = "scope";
    private static final String PARAM_CLIENT_ASSERTION_TYPE = "client_assertion_type";
    private static final String PARAM_CLIENT_ASSERTION = "client_assertion";
    private static final String GRANT_TYPE_CLIENT_CREDENTIALS = "client_credentials";
    private static final String CLIENT_ASSERTION_TYPE_BEARER = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";
    private static final String ERROR_EXCHANGE_FAILED = "Failed to exchange workload identity token: %s";
    private static final String ERROR_READ_RESPONSE = "Failed reading response from Azure token endpoint";
    private static final String ERROR_NO_ACCESS_TOKEN = "Azure token endpoint response did not contain an access_token";
    private static final String STEP_EXCHANGE_TOKEN = "Exchange workload identity token";
    private static final String FIELD_SCOPE = "scope";
    private static final String FIELD_TOKEN_TYPE = "token_type";
    private static final String FIELD_EXPIRES_IN = "expires_in";

    public static final PropertyDescriptor TENANT_ID = new PropertyDescriptor.Builder()
            .name("Tenant ID")
            .description("Microsoft Entra tenant ID. Used to build the token endpoint when an explicit endpoint is not configured.")
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

    public static final PropertyDescriptor TOKEN_ENDPOINT = new PropertyDescriptor.Builder()
            .name("Token Endpoint")
            .description("Azure AD OAuth2 token endpoint. When not set, defaults to https://login.microsoftonline.com/<Tenant ID>/oauth2/v2.0/token.")
            .required(false)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor CLIENT_ASSERTION_PROVIDER = new PropertyDescriptor.Builder()
            .name("Client Assertion Provider")
            .description("Controller Service that retrieves the external workload identity token (client assertion) exchanged with Azure AD.")
            .identifiesControllerService(OAuth2AccessTokenProvider.class)
            .required(true)
            .build();

    public static final PropertyDescriptor WEB_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("Web Client Service")
            .description("Controller Service that provides the HTTP client used for exchanging tokens with Azure AD.")
            .identifiesControllerService(WebClientServiceProvider.class)
            .required(true)
            .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = List.of(
            TENANT_ID,
            CLIENT_ID,
            SCOPE,
            TOKEN_ENDPOINT,
            CLIENT_ASSERTION_PROVIDER,
            WEB_CLIENT_SERVICE
    );

    private volatile WebClientServiceProvider webClientServiceProvider;
    private volatile OAuth2AccessTokenProvider clientAssertionProvider;
    private volatile String clientId;
    private volatile String scope;
    private volatile String tokenEndpoint;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.webClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE).asControllerService(WebClientServiceProvider.class);
        this.clientAssertionProvider = context.getProperty(CLIENT_ASSERTION_PROVIDER).asControllerService(OAuth2AccessTokenProvider.class);
        this.clientId = context.getProperty(CLIENT_ID).getValue();
        this.scope = context.getProperty(SCOPE).getValue();
        this.tokenEndpoint = resolveTokenEndpoint(context);
    }

    @Override
    public AccessToken getAccessDetails() {
        return exchangeAccessToken(webClientServiceProvider, clientAssertionProvider, clientId, scope, tokenEndpoint);
    }

    @Override
    public void refreshAccessDetails() {
        clientAssertionProvider.refreshAccessDetails();
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger, final Map<String, String> variables) {
        final ConfigVerificationResult.Builder resultBuilder = new ConfigVerificationResult.Builder()
                .verificationStepName(STEP_EXCHANGE_TOKEN);

        try {
            final WebClientServiceProvider verificationWebClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE)
                    .asControllerService(WebClientServiceProvider.class);
            final OAuth2AccessTokenProvider verificationClientAssertionProvider = context.getProperty(CLIENT_ASSERTION_PROVIDER)
                    .asControllerService(OAuth2AccessTokenProvider.class);
            final String verificationClientId = context.getProperty(CLIENT_ID).getValue();
            final String verificationScope = context.getProperty(SCOPE).getValue();
            final String verificationEndpoint = resolveTokenEndpoint(context);

            exchangeAccessToken(verificationWebClientServiceProvider, verificationClientAssertionProvider,
                    verificationClientId, verificationScope, verificationEndpoint);

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

    private AccessToken exchangeAccessToken(final WebClientServiceProvider webClientServiceProvider,
                                            final OAuth2AccessTokenProvider assertionProvider,
                                            final String clientId,
                                            final String scope,
                                            final String tokenEndpoint) {

        final AccessToken clientAssertionAccessToken = assertionProvider.getAccessDetails();

        if (clientAssertionAccessToken == null || StringUtils.isBlank(clientAssertionAccessToken.getAccessToken())) {
            throw new ProcessException("Client assertion provider returned no access token");
        }

        final WebClientService webClientService = webClientServiceProvider.getWebClientService();
        final URI tokenUri = URI.create(tokenEndpoint);
        final String requestBody = buildRequestBody(clientId, scope, clientAssertionAccessToken.getAccessToken());

        final HttpResponseEntity responseEntity = webClientService.post()
                .uri(tokenUri)
                .header(HEADER_CONTENT_TYPE, FORM_CONTENT_TYPE)
                .header(HEADER_ACCEPT, ACCEPT_HEADER)
                .body(requestBody)
                .retrieve();

        try (responseEntity) {
            final int statusCode = responseEntity.statusCode();
            final String responseBody = IOUtils.toString(responseEntity.body(), StandardCharsets.UTF_8);
            if (statusCode < 200 || statusCode >= 300) {
                throw new ProcessException(String.format(
                        "Azure token endpoint %s returned status %d: %s",
                        tokenUri, statusCode, responseBody));
            }

            return parseAccessToken(responseBody);
        } catch (final IOException e) {
            throw new ProcessException(ERROR_READ_RESPONSE, e);
        }
    }

    private AccessToken parseAccessToken(final String responseBody) throws IOException {
        final JsonNode responseNode = OBJECT_MAPPER.readTree(responseBody);
        final String accessTokenValue = responseNode.path("access_token").asText(null);
        if (StringUtils.isBlank(accessTokenValue)) {
            throw new ProcessException(ERROR_NO_ACCESS_TOKEN);
        }

        final AccessToken accessToken = new AccessToken();
        accessToken.setAccessToken(accessTokenValue);

        final JsonNode expiresInNode = responseNode.path(FIELD_EXPIRES_IN);
        final long expiresIn = parseExpiresIn(expiresInNode);
        if (expiresIn > 0) {
            accessToken.setExpiresIn(expiresIn);
        }

        final String tokenType = responseNode.path(FIELD_TOKEN_TYPE).asText(null);
        if (StringUtils.isNotBlank(tokenType)) {
            accessToken.setTokenType(tokenType);
        }

        final String scopeValue = responseNode.path(FIELD_SCOPE).asText(null);
        if (StringUtils.isNotBlank(scopeValue)) {
            accessToken.setScope(scopeValue);
        }

        return accessToken;
    }

    private long parseExpiresIn(final JsonNode expiresInNode) {
        if (expiresInNode == null || expiresInNode.isMissingNode()) {
            return 0;
        }

        if (expiresInNode.isNumber()) {
            return expiresInNode.asLong();
        }

        if (expiresInNode.isTextual()) {
            final String expiresInText = expiresInNode.asText();
            if (StringUtils.isNotBlank(expiresInText)) {
                try {
                    return Long.parseLong(expiresInText);
                } catch (final NumberFormatException e) {
                    throw new ProcessException(String.format("Azure token endpoint returned invalid expires_in value [%s]", expiresInText), e);
                }
            }
        }

        return 0;
    }

    private static String buildRequestBody(final String clientId, final String scope, final String clientAssertion) {
        final StringBuilder builder = new StringBuilder();
        appendFormParameter(builder, PARAM_GRANT_TYPE, GRANT_TYPE_CLIENT_CREDENTIALS);
        appendFormParameter(builder, PARAM_CLIENT_ID, clientId);
        appendFormParameter(builder, PARAM_SCOPE, scope);
        appendFormParameter(builder, PARAM_CLIENT_ASSERTION_TYPE, CLIENT_ASSERTION_TYPE_BEARER);
        appendFormParameter(builder, PARAM_CLIENT_ASSERTION, clientAssertion);
        return builder.toString();
    }

    private static void appendFormParameter(final StringBuilder builder, final String name, final String value) {
        Objects.requireNonNull(name, "Form parameter name required");
        if (builder.length() > 0) {
            builder.append('&');
        }
        builder.append(encode(name)).append('=').append(encode(value));
    }

    private static String encode(final String value) {
        return URLEncoder.encode(value == null ? "" : value, StandardCharsets.UTF_8);
    }

    private String resolveTokenEndpoint(final ConfigurationContext context) {
        final String configuredEndpoint = context.getProperty(TOKEN_ENDPOINT).getValue();
        if (StringUtils.isNotBlank(configuredEndpoint)) {
            return configuredEndpoint;
        }

        final String tenantId = context.getProperty(TENANT_ID).getValue();
        return String.format("https://login.microsoftonline.com/%s/oauth2/v2.0/token", tenantId);
    }
}
