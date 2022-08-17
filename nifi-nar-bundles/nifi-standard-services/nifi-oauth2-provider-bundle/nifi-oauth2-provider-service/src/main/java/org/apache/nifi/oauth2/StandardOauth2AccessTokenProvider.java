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
package org.apache.nifi.oauth2;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Tags({"oauth2", "provider", "authorization", "access token", "http"})
@CapabilityDescription("Provides OAuth 2.0 access tokens that can be used as Bearer authorization header in HTTP requests." +
    " Uses Resource Owner Password Credentials Grant.")
public class StandardOauth2AccessTokenProvider extends AbstractControllerService implements OAuth2AccessTokenProvider {
    public static final PropertyDescriptor AUTHORIZATION_SERVER_URL = new PropertyDescriptor.Builder()
        .name("authorization-server-url")
        .displayName("Authorization Server URL")
        .description("The URL of the authorization server that issues access tokens.")
        .required(true)
        .addValidator(StandardValidators.URL_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static AllowableValue RESOURCE_OWNER_PASSWORD_CREDENTIALS_GRANT_TYPE = new AllowableValue(
        "password",
        "User Password",
        "Resource Owner Password Credentials Grant. Used to access resources available to users. Requires username and password and usually Client ID and Client Secret"
    );
    public static AllowableValue CLIENT_CREDENTIALS_GRANT_TYPE = new AllowableValue(
        "client_credentials",
        "Client Credentials",
        "Client Credentials Grant. Used to access resources available to clients. Requires Client ID and Client Secret"
    );

    public static final PropertyDescriptor GRANT_TYPE = new PropertyDescriptor.Builder()
        .name("grant-type")
        .displayName("Grant Type")
        .description("The OAuth2 Grant Type to be used when acquiring an access token.")
        .required(true)
        .allowableValues(RESOURCE_OWNER_PASSWORD_CREDENTIALS_GRANT_TYPE, CLIENT_CREDENTIALS_GRANT_TYPE)
        .defaultValue(RESOURCE_OWNER_PASSWORD_CREDENTIALS_GRANT_TYPE.getValue())
        .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
        .name("service-user-name")
        .displayName("Username")
        .description("Username on the service that is being accessed.")
        .dependsOn(GRANT_TYPE, RESOURCE_OWNER_PASSWORD_CREDENTIALS_GRANT_TYPE)
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
        .name("service-password")
        .displayName("Password")
        .description("Password for the username on the service that is being accessed.")
        .dependsOn(GRANT_TYPE, RESOURCE_OWNER_PASSWORD_CREDENTIALS_GRANT_TYPE)
        .required(true)
        .sensitive(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();

    public static final PropertyDescriptor CLIENT_ID = new PropertyDescriptor.Builder()
        .name("client-id")
        .displayName("Client ID")
        .required(false)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static final PropertyDescriptor CLIENT_SECRET = new PropertyDescriptor.Builder()
        .name("client-secret")
        .displayName("Client secret")
        .dependsOn(CLIENT_ID)
        .required(true)
        .sensitive(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();

    public static final PropertyDescriptor SCOPE = new PropertyDescriptor.Builder()
        .name("scope")
        .displayName("Scope")
        .description("Space-delimited, case-sensitive list of scopes of the access request (as per the OAuth 2.0 specification)")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor REFRESH_WINDOW = new PropertyDescriptor.Builder()
            .name("refresh-window")
            .displayName("Refresh Window")
            .description("The service will attempt to refresh tokens expiring within the refresh window, subtracting the configured duration from the token expiration.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 s")
            .required(true)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
        .name("ssl-context-service")
        .displayName("SSL Context Service")
        .addValidator(Validator.VALID)
        .identifiesControllerService(SSLContextService.class)
        .required(false)
        .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
        AUTHORIZATION_SERVER_URL,
        GRANT_TYPE,
        USERNAME,
        PASSWORD,
        CLIENT_ID,
        CLIENT_SECRET,
        SCOPE,
        REFRESH_WINDOW,
        SSL_CONTEXT
    ));

    public static final ObjectMapper ACCESS_DETAILS_MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);

    private volatile String authorizationServerUrl;
    private volatile OkHttpClient httpClient;

    private volatile String grantType;
    private volatile String username;
    private volatile String password;
    private volatile String clientId;
    private volatile String clientSecret;
    private volatile String scope;
    private volatile long refreshWindowSeconds;

    private volatile AccessToken accessDetails;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        authorizationServerUrl = context.getProperty(AUTHORIZATION_SERVER_URL).evaluateAttributeExpressions().getValue();

        httpClient = createHttpClient(context);

        grantType = context.getProperty(GRANT_TYPE).getValue();
        username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        password = context.getProperty(PASSWORD).getValue();
        clientId = context.getProperty(CLIENT_ID).evaluateAttributeExpressions().getValue();
        clientSecret = context.getProperty(CLIENT_SECRET).getValue();
        scope = context.getProperty(SCOPE).getValue();

        refreshWindowSeconds = context.getProperty(REFRESH_WINDOW).asTimePeriod(TimeUnit.SECONDS);
    }

    @OnDisabled
    public void onDisabled() {
        accessDetails = null;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(validationContext));

        if (
            validationContext.getProperty(GRANT_TYPE).getValue().equals(CLIENT_CREDENTIALS_GRANT_TYPE.getValue())
                && !validationContext.getProperty(CLIENT_ID).isSet()
        ) {
            validationResults.add(new ValidationResult.Builder().subject(CLIENT_ID.getDisplayName())
                .valid(false)
                .explanation(String.format(
                    "When '%s' is set to '%s', '%s' is required",
                    GRANT_TYPE.getDisplayName(),
                    CLIENT_CREDENTIALS_GRANT_TYPE.getDisplayName(),
                    CLIENT_ID.getDisplayName())
                )
                .build());
        }

        return validationResults;
    }

    protected OkHttpClient createHttpClient(ConfigurationContext context) {
        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder();

        SSLContextService sslService = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextService.class);
        if (sslService != null) {
            final X509TrustManager trustManager = sslService.createTrustManager();
            SSLContext sslContext = sslService.createContext();
            clientBuilder.sslSocketFactory(sslContext.getSocketFactory(), trustManager);
        }

        return clientBuilder.build();
    }

    @Override
    public AccessToken getAccessDetails() {
        if (this.accessDetails == null) {
            acquireAccessDetails();
        } else if (isRefreshRequired()) {
            if (this.accessDetails.getRefreshToken() == null) {
                acquireAccessDetails();
            } else {
                try {
                    refreshAccessDetails();
                } catch (Exception e) {
                    getLogger().info("Refresh Access Token request failed [{}]", authorizationServerUrl, e);
                    acquireAccessDetails();
                }
            }
        }

        return accessDetails;
    }

    private void acquireAccessDetails() {
        getLogger().debug("New Access Token request started [{}]", authorizationServerUrl);

        FormBody.Builder acquireTokenBuilder = new FormBody.Builder();

        if (grantType.equals(RESOURCE_OWNER_PASSWORD_CREDENTIALS_GRANT_TYPE.getValue())) {
            acquireTokenBuilder.add("grant_type", "password")
                .add("username", username)
                .add("password", password);
        } else if (grantType.equals(CLIENT_CREDENTIALS_GRANT_TYPE.getValue())) {
            acquireTokenBuilder.add("grant_type", "client_credentials");
        }

        if (clientId != null) {
            acquireTokenBuilder.add("client_id", clientId);
            acquireTokenBuilder.add("client_secret", clientSecret);
        }

        if (scope != null) {
            acquireTokenBuilder.add("scope", scope);
        }

        RequestBody acquireTokenRequestBody = acquireTokenBuilder.build();

        Request acquireTokenRequest = new Request.Builder()
            .url(authorizationServerUrl)
            .post(acquireTokenRequestBody)
            .build();

        this.accessDetails = getAccessDetails(acquireTokenRequest);
    }

    private void refreshAccessDetails() {
        getLogger().debug("Refresh Access Token request started [{}]", authorizationServerUrl);

        FormBody.Builder refreshTokenBuilder = new FormBody.Builder()
            .add("grant_type", "refresh_token")
            .add("refresh_token", this.accessDetails.getRefreshToken());

        if (clientId != null) {
            refreshTokenBuilder.add("client_id", clientId);
            refreshTokenBuilder.add("client_secret", clientSecret);
        }

        if (scope != null) {
            refreshTokenBuilder.add("scope", scope);
        }

        RequestBody refreshTokenRequestBody = refreshTokenBuilder.build();

        Request refreshRequest = new Request.Builder()
            .url(authorizationServerUrl)
            .post(refreshTokenRequestBody)
            .build();

        this.accessDetails = getAccessDetails(refreshRequest);
    }

    private AccessToken getAccessDetails(final Request newRequest) {
        try {
            final Response response = httpClient.newCall(newRequest).execute();
            final String responseBody = response.body().string();
            if (response.isSuccessful()) {
                getLogger().debug("OAuth2 Access Token retrieved [HTTP {}]", response.code());
                return ACCESS_DETAILS_MAPPER.readValue(responseBody, AccessToken.class);
            } else {
                getLogger().error(String.format("OAuth2 access token request failed [HTTP %d], response:%n%s", response.code(), responseBody));
                throw new ProcessException(String.format("OAuth2 access token request failed [HTTP %d]", response.code()));
            }
        } catch (final IOException e) {
            throw new UncheckedIOException("OAuth2 access token request failed", e);
        }
    }

    private boolean isRefreshRequired() {
        final Instant expirationRefreshTime = accessDetails.getFetchTime()
                .plusSeconds(accessDetails.getExpiresIn())
                .minusSeconds(refreshWindowSeconds);

        return Instant.now().isAfter(expirationRefreshTime);
    }
}
