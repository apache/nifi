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
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.gcp.GCPIdentityFederationTokenProvider;
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
 * Controller Service that exchanges an external identity token for a Google Cloud access token
 * using the Workload Identity Federation token exchange endpoint.
 */
@Tags({ "gcp", "oauth2", "identity", "federation", "credentials" })
@CapabilityDescription("Exchanges workload identity tokens for Google Cloud access tokens using the STS token exchange API.")
public class StandardGCPIdentityFederationTokenProvider extends AbstractControllerService implements GCPIdentityFederationTokenProvider,
        VerifiableControllerService {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String DEFAULT_TOKEN_ENDPOINT = "https://sts.googleapis.com/v1/token";
    private static final String DEFAULT_SUBJECT_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:jwt";
    private static final String DEFAULT_REQUESTED_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:access_token";
    private static final String DEFAULT_SCOPE = "https://www.googleapis.com/auth/cloud-platform";
    private static final String FORM_CONTENT_TYPE = "application/x-www-form-urlencoded";
    private static final String ACCEPT_HEADER = "application/json";
    private static final String GRANT_TYPE_TOKEN_EXCHANGE = "urn:ietf:params:oauth:grant-type:token-exchange";
    private static final String PARAM_GRANT_TYPE = "grant_type";
    private static final String PARAM_SUBJECT_TOKEN_TYPE = "subject_token_type";
    private static final String PARAM_SUBJECT_TOKEN = "subject_token";
    private static final String PARAM_REQUESTED_TOKEN_TYPE = "requested_token_type";
    private static final String PARAM_SCOPE = "scope";
    private static final String PARAM_AUDIENCE = "audience";

    private static final String FIELD_ACCESS_TOKEN = "access_token";
    private static final String FIELD_TOKEN_TYPE = "token_type";
    private static final String FIELD_EXPIRES_IN = "expires_in";
    private static final String FIELD_ISSUED_TOKEN_TYPE = "issued_token_type";
    private static final String FIELD_SCOPE = "scope";

    private static final String ERROR_EXCHANGE_FAILED = "Failed to exchange workload identity token: %s";
    private static final String ERROR_NO_SUBJECT_TOKEN = "Subject token provider returned no usable token";
    private static final String ERROR_READ_RESPONSE = "Failed reading response from Google STS endpoint";
    private static final String ERROR_NO_ACCESS_TOKEN = "Google STS response did not contain an access_token";
    private static final String STEP_EXCHANGE_TOKEN = "Exchange workload identity token";

    public static final PropertyDescriptor AUDIENCE = new PropertyDescriptor.Builder()
            .name("Audience")
            .description("The audience corresponding to the target Workload Identity Provider, typically the full resource name.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor SCOPE = new PropertyDescriptor.Builder()
            .name("Scope")
            .description("OAuth2 scope requested for the exchanged access token.")
            .required(true)
            .defaultValue(DEFAULT_SCOPE)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor TOKEN_ENDPOINT = new PropertyDescriptor.Builder()
            .name("STS Token Endpoint")
            .description("Google Security Token Service endpoint used for token exchange.")
            .required(false)
            .defaultValue(DEFAULT_TOKEN_ENDPOINT)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor SUBJECT_TOKEN_PROVIDER = new PropertyDescriptor.Builder()
            .name("Subject Token Provider")
            .description("Controller Service that retrieves the external workload identity token to exchange.")
            .identifiesControllerService(OAuth2AccessTokenProvider.class)
            .required(true)
            .build();

    public static final PropertyDescriptor SUBJECT_TOKEN_TYPE = new PropertyDescriptor.Builder()
            .name("Subject Token Type")
            .description("The type of token returned by the Subject Token Provider.")
            .required(true)
            .defaultValue(DEFAULT_SUBJECT_TOKEN_TYPE)
            .allowableValues(
                    DEFAULT_SUBJECT_TOKEN_TYPE,
                    "urn:ietf:params:oauth:token-type:access_token"
            )
            .build();

    public static final PropertyDescriptor REQUESTED_TOKEN_TYPE = new PropertyDescriptor.Builder()
            .name("Requested Token Type")
            .description("The type of token requested from Google STS.")
            .required(true)
            .defaultValue(DEFAULT_REQUESTED_TOKEN_TYPE)
            .allowableValues(
                    DEFAULT_REQUESTED_TOKEN_TYPE,
                    "urn:ietf:params:oauth:token-type:refresh_token"
            )
            .build();

    public static final PropertyDescriptor WEB_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("Web Client Service")
            .description("Controller Service providing the HTTP client used to communicate with Google STS.")
            .identifiesControllerService(WebClientServiceProvider.class)
            .required(true)
            .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = List.of(
            AUDIENCE,
            SCOPE,
            TOKEN_ENDPOINT,
            SUBJECT_TOKEN_PROVIDER,
            SUBJECT_TOKEN_TYPE,
            REQUESTED_TOKEN_TYPE,
            WEB_CLIENT_SERVICE
    );

    private volatile WebClientServiceProvider webClientServiceProvider;
    private volatile OAuth2AccessTokenProvider subjectTokenProvider;
    private volatile String audience;
    private volatile String scope;
    private volatile String tokenEndpoint;
    private volatile String subjectTokenType;
    private volatile String requestedTokenType;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.webClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE).asControllerService(WebClientServiceProvider.class);
        this.subjectTokenProvider = context.getProperty(SUBJECT_TOKEN_PROVIDER).asControllerService(OAuth2AccessTokenProvider.class);
        this.audience = context.getProperty(AUDIENCE).getValue();
        this.scope = context.getProperty(SCOPE).getValue();
        this.tokenEndpoint = context.getProperty(TOKEN_ENDPOINT).getValue();
        this.subjectTokenType = context.getProperty(SUBJECT_TOKEN_TYPE).getValue();
        this.requestedTokenType = context.getProperty(REQUESTED_TOKEN_TYPE).getValue();
    }

    @Override
    public AccessToken getAccessDetails() {
        return exchangeAccessToken(webClientServiceProvider, subjectTokenProvider, audience, scope, tokenEndpoint, subjectTokenType, requestedTokenType);
    }

    @Override
    public void refreshAccessDetails() {
        subjectTokenProvider.refreshAccessDetails();
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger, final Map<String, String> variables) {
        final ConfigVerificationResult.Builder resultBuilder = new ConfigVerificationResult.Builder()
                .verificationStepName(STEP_EXCHANGE_TOKEN);

        try {
            final WebClientServiceProvider verificationClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE)
                    .asControllerService(WebClientServiceProvider.class);
            final OAuth2AccessTokenProvider verificationSubjectTokenProvider = context.getProperty(SUBJECT_TOKEN_PROVIDER)
                    .asControllerService(OAuth2AccessTokenProvider.class);
            final String verificationAudience = context.getProperty(AUDIENCE).getValue();
            final String verificationScope = context.getProperty(SCOPE).getValue();
            final String verificationEndpoint = context.getProperty(TOKEN_ENDPOINT).getValue();
            final String verificationSubjectType = context.getProperty(SUBJECT_TOKEN_TYPE).getValue();
            final String verificationRequestedType = context.getProperty(REQUESTED_TOKEN_TYPE).getValue();

            exchangeAccessToken(verificationClientServiceProvider, verificationSubjectTokenProvider,
                    verificationAudience, verificationScope, verificationEndpoint, verificationSubjectType, verificationRequestedType);

            return Collections.singletonList(resultBuilder
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation("Successfully exchanged workload identity token for a Google Cloud access token")
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

    private AccessToken exchangeAccessToken(final WebClientServiceProvider clientServiceProvider,
                                            final OAuth2AccessTokenProvider subjectTokenProvider,
                                            final String audience,
                                            final String scope,
                                            final String tokenEndpoint,
                                            final String subjectTokenType,
                                            final String requestedTokenType) {

        final String subjectToken = getSubjectToken(subjectTokenProvider);
        if (StringUtils.isBlank(subjectToken)) {
            throw new ProcessException(ERROR_NO_SUBJECT_TOKEN);
        }

        final WebClientService webClientService = clientServiceProvider.getWebClientService();
        final URI tokenUri = URI.create(tokenEndpoint);
        final String requestBody = buildRequestBody(subjectToken, audience, scope, subjectTokenType, requestedTokenType);

        final HttpResponseEntity responseEntity = webClientService.post()
                .uri(tokenUri)
                .header("Content-Type", FORM_CONTENT_TYPE)
                .header("Accept", ACCEPT_HEADER)
                .body(requestBody)
                .retrieve();

        try (responseEntity) {
            final int statusCode = responseEntity.statusCode();
            final String responseBody = IOUtils.toString(responseEntity.body(), StandardCharsets.UTF_8);
            if (statusCode < 200 || statusCode >= 300) {
                throw new ProcessException(String.format(
                        "Google STS endpoint %s returned status %d: %s",
                        tokenUri, statusCode, responseBody));
            }

            return parseAccessToken(responseBody);
        } catch (final IOException e) {
            throw new ProcessException(ERROR_READ_RESPONSE, e);
        }
    }

    private String getSubjectToken(final OAuth2AccessTokenProvider subjectTokenProvider) {
        final AccessToken subjectToken = subjectTokenProvider.getAccessDetails();
        if (subjectToken == null) {
            return null;
        }

        final Map<String, Object> additionalParameters = subjectToken.getAdditionalParameters();
        if (additionalParameters != null) {
            final Object idToken = additionalParameters.get("id_token");
            if (idToken instanceof String && StringUtils.isNotBlank((String) idToken)) {
                return (String) idToken;
            }
        }

        final String accessTokenValue = subjectToken.getAccessToken();
        if (StringUtils.isNotBlank(accessTokenValue)) {
            return accessTokenValue;
        }

        return null;
    }

    private static String buildRequestBody(final String subjectToken,
                                           final String audience,
                                           final String scope,
                                           final String subjectTokenType,
                                           final String requestedTokenType) {
        final StringBuilder builder = new StringBuilder();
        appendFormParameter(builder, PARAM_GRANT_TYPE, GRANT_TYPE_TOKEN_EXCHANGE);
        appendFormParameter(builder, PARAM_SUBJECT_TOKEN_TYPE, subjectTokenType);
        appendFormParameter(builder, PARAM_SUBJECT_TOKEN, subjectToken);
        appendFormParameter(builder, PARAM_AUDIENCE, audience);
        appendFormParameter(builder, PARAM_REQUESTED_TOKEN_TYPE, requestedTokenType);
        if (StringUtils.isNotBlank(scope)) {
            appendFormParameter(builder, PARAM_SCOPE, scope);
        }
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

    private AccessToken parseAccessToken(final String responseBody) throws IOException {
        final JsonNode responseNode = OBJECT_MAPPER.readTree(responseBody);
        final String accessTokenValue = responseNode.path(FIELD_ACCESS_TOKEN).asText(null);
        if (StringUtils.isBlank(accessTokenValue)) {
            throw new ProcessException(ERROR_NO_ACCESS_TOKEN);
        }

        final AccessToken accessToken = new AccessToken();
        accessToken.setAccessToken(accessTokenValue);

        final JsonNode expiresInNode = responseNode.path(FIELD_EXPIRES_IN);
        if (expiresInNode.isNumber()) {
            accessToken.setExpiresIn(expiresInNode.asLong());
        }

        final String tokenType = responseNode.path(FIELD_TOKEN_TYPE).asText(null);
        if (StringUtils.isNotBlank(tokenType)) {
            accessToken.setTokenType(tokenType);
        }

        final String issuedTokenType = responseNode.path(FIELD_ISSUED_TOKEN_TYPE).asText(null);
        if (StringUtils.isNotBlank(issuedTokenType)) {
            accessToken.setAdditionalParameter(FIELD_ISSUED_TOKEN_TYPE, issuedTokenType);
        }

        final String scopeValue = responseNode.path(FIELD_SCOPE).asText(null);
        if (StringUtils.isNotBlank(scopeValue)) {
            accessToken.setScope(scopeValue);
        }

        return accessToken;
    }
}
