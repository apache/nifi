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

import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdentityPoolCredentials;
import com.google.auth.oauth2.IdentityPoolSubjectTokenSupplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.gcp.ProxyAwareTransportFactory;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.services.gcp.GCPIdentityFederationTokenProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Controller Service that configures Workload Identity Federation using Google IdentityPoolCredentials.
 * The service obtains subject tokens from an upstream OAuth2AccessTokenProvider and allows the Google
 * client library to exchange and refresh Google Cloud access tokens as needed.
 */
@Tags({ "gcp", "oauth2", "identity", "federation", "credentials" })
@CapabilityDescription("Provides Google Cloud credentials using Workload Identity Federation. "
        + "Tokens are exchanged through Google Identity Pool credentials and refreshed automatically.")
public class StandardGCPIdentityFederationTokenProvider extends AbstractControllerService implements GCPIdentityFederationTokenProvider,
        VerifiableControllerService {

    private static final String DEFAULT_TOKEN_ENDPOINT = "https://sts.googleapis.com/v1/token";
    private static final String DEFAULT_SUBJECT_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:jwt";
    private static final String DEFAULT_SCOPE = "https://www.googleapis.com/auth/cloud-platform";
    private static final long SUBJECT_TOKEN_REFRESH_SKEW_SECONDS = 60;

    private static final String ERROR_EXCHANGE_FAILED = "Failed to exchange workload identity token: %s";
    private static final String ERROR_NO_SUBJECT_TOKEN = "Subject token provider returned no usable token";
    private static final String STEP_EXCHANGE_TOKEN = "Exchange workload identity token";

    public static final PropertyDescriptor AUDIENCE = new PropertyDescriptor.Builder()
            .name("Audience")
            .description("The audience corresponding to the target Workload Identity Provider, typically the full resource name.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor SCOPE = new PropertyDescriptor.Builder()
            .name("Scope")
            .description("OAuth2 scopes requested for the exchanged access token. Multiple scopes can be separated by space or comma.")
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

    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE = ProxyConfiguration.createProxyConfigPropertyDescriptor(ProxyAwareTransportFactory.PROXY_SPECS);

    private static final List<PropertyDescriptor> DESCRIPTORS = List.of(
            AUDIENCE,
            SCOPE,
            TOKEN_ENDPOINT,
            SUBJECT_TOKEN_PROVIDER,
            SUBJECT_TOKEN_TYPE,
            PROXY_CONFIGURATION_SERVICE
    );

    private volatile OAuth2AccessTokenProvider subjectTokenProvider;
    private volatile String audience;
    private volatile List<String> scopes;
    private volatile String tokenEndpoint;
    private volatile String subjectTokenType;
    private volatile ProxyConfiguration proxyConfiguration;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        ProxyConfiguration.validateProxySpec(validationContext, results, ProxyAwareTransportFactory.PROXY_SPECS);
        return results;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.subjectTokenProvider = context.getProperty(SUBJECT_TOKEN_PROVIDER).asControllerService(OAuth2AccessTokenProvider.class);
        this.audience = context.getProperty(AUDIENCE).getValue();
        this.scopes = parseScopes(context.getProperty(SCOPE).getValue());
        this.tokenEndpoint = context.getProperty(TOKEN_ENDPOINT).getValue();
        this.subjectTokenType = context.getProperty(SUBJECT_TOKEN_TYPE).getValue();
        this.proxyConfiguration = ProxyConfiguration.getConfiguration(context);
    }

    @Override
    public GoogleCredentials getGoogleCredentials(final HttpTransportFactory transportFactory) {
        final HttpTransportFactory effectiveFactory = transportFactory != null
                ? transportFactory
                : new ProxyAwareTransportFactory(proxyConfiguration);

        return buildIdentityPoolCredentials(subjectTokenProvider, audience, scopes, tokenEndpoint, subjectTokenType, effectiveFactory);
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger, final Map<String, String> variables) {
        final ConfigVerificationResult.Builder resultBuilder = new ConfigVerificationResult.Builder()
                .verificationStepName(STEP_EXCHANGE_TOKEN);

        try {
            final OAuth2AccessTokenProvider verificationSubjectTokenProvider = context.getProperty(SUBJECT_TOKEN_PROVIDER)
                    .asControllerService(OAuth2AccessTokenProvider.class);
            final String verificationAudience = context.getProperty(AUDIENCE).getValue();
            final List<String> verificationScopes = parseScopes(context.getProperty(SCOPE).getValue());
            final String verificationTokenEndpoint = context.getProperty(TOKEN_ENDPOINT).getValue();
            final String verificationSubjectTokenType = context.getProperty(SUBJECT_TOKEN_TYPE).getValue();
            final ProxyConfiguration verificationProxyConfiguration = ProxyConfiguration.getConfiguration(context);

            final IdentityPoolCredentials credentials = buildIdentityPoolCredentials(
                    verificationSubjectTokenProvider,
                    verificationAudience,
                    verificationScopes,
                    verificationTokenEndpoint,
                    verificationSubjectTokenType,
                    new ProxyAwareTransportFactory(verificationProxyConfiguration));

            credentials.refreshAccessToken();

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

    private IdentityPoolCredentials buildIdentityPoolCredentials(final OAuth2AccessTokenProvider tokenProvider,
                                                                 final String audience,
                                                                 final List<String> scopes,
                                                                 final String tokenEndpoint,
                                                                 final String subjectTokenType,
                                                                 final HttpTransportFactory transportFactory) {
        final IdentityPoolSubjectTokenSupplier tokenSupplier = createSubjectTokenSupplier(tokenProvider);
        final IdentityPoolCredentials.Builder builder = IdentityPoolCredentials.newBuilder()
                .setAudience(audience)
                .setTokenUrl(tokenEndpoint)
                .setSubjectTokenType(subjectTokenType)
                .setSubjectTokenSupplier(tokenSupplier);

        if (!scopes.isEmpty()) {
            builder.setScopes(scopes);
        }

        if (transportFactory != null) {
            builder.setHttpTransportFactory(transportFactory);
        }

        return builder.build();
    }

    private IdentityPoolSubjectTokenSupplier createSubjectTokenSupplier(final OAuth2AccessTokenProvider tokenProvider) {
        return context -> getSubjectToken(tokenProvider);
    }

    private String getSubjectToken(final OAuth2AccessTokenProvider tokenProvider) throws IOException {
        AccessToken subjectToken = tokenProvider.getAccessDetails();

        if (requiresRefresh(subjectToken)) {
            tokenProvider.refreshAccessDetails();
            subjectToken = tokenProvider.getAccessDetails();
        }

        final String subjectTokenValue = extractTokenValue(subjectToken);
        if (StringUtils.isBlank(subjectTokenValue)) {
            throw new IOException(ERROR_NO_SUBJECT_TOKEN);
        }

        return subjectTokenValue;
    }

    private boolean requiresRefresh(final AccessToken subjectToken) {
        if (subjectToken == null) {
            return true;
        }

        final long expiresIn = subjectToken.getExpiresIn();
        if (expiresIn <= 0) {
            return false;
        }

        final Instant fetchTime = subjectToken.getFetchTime();
        final Instant refreshTime = fetchTime.plusSeconds(expiresIn).minusSeconds(SUBJECT_TOKEN_REFRESH_SKEW_SECONDS);
        return Instant.now().isAfter(refreshTime);
    }

    private String extractTokenValue(final AccessToken subjectToken) {
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

    private static List<String> parseScopes(final String scopeValue) {
        if (StringUtils.isBlank(scopeValue)) {
            return Collections.emptyList();
        }

        return Arrays.stream(scopeValue.split("[\\s,]+"))
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
    }
}
