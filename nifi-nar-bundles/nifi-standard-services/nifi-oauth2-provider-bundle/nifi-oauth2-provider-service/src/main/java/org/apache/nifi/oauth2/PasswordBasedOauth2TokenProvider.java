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

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Tags({"oauth2", "provider", "authorization", "access token", "http" })
@CapabilityDescription("Provides OAuth 2.0 access tokens that can be used as Bearer authorization header in HTTP requests." +
    " Uses Resource Owner Password Credentials Grant.")
public class PasswordBasedOauth2TokenProvider extends AbstractControllerService implements OAuth2AccessTokenProvider {
    public static final PropertyDescriptor AUTHORIZATION_SERVER_URL = new PropertyDescriptor.Builder()
        .name("authorization-server-url")
        .displayName("Authorization Server URL")
        .description("The URL of the authorization server that issues access tokens.")
        .required(true)
        .addValidator(StandardValidators.URL_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
        .name("service-user-name")
        .displayName("Username")
        .description("Username on the service that is being accessed.")
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
        .name("service-password")
        .displayName("Password")
        .description("Password for the username on the service that is being accessed.")
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

    public static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
        .name("ssl-context")
        .displayName("SSL Context")
        .addValidator(Validator.VALID)
        .identifiesControllerService(SSLContextService.class)
        .required(false)
        .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
        AUTHORIZATION_SERVER_URL,
        USERNAME,
        PASSWORD,
        CLIENT_ID,
        CLIENT_SECRET,
        SSL_CONTEXT
    ));

    private String authorizationServerUrl;
    private OkHttpClient httpClient;

    private String username;
    private String password;
    private String clientId;
    private String clientSecret;

    private volatile AccessDetails accessDetails;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        authorizationServerUrl = context.getProperty(AUTHORIZATION_SERVER_URL).evaluateAttributeExpressions().getValue();

        httpClient = createHttpClient(context);

        username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        password = context.getProperty(PASSWORD).getValue();
        clientId = context.getProperty(CLIENT_ID).evaluateAttributeExpressions().getValue();
        clientSecret = context.getProperty(CLIENT_SECRET).getValue();
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
    public String getAccessToken() {
        String accessToken;

        if (this.accessDetails == null) {
            acquireAuthorizationDetails();
        } else if (this.accessDetails.isExpired()) {
            try {
                refreshAuthorizationDetails();
            } catch (Exception e) {
                getLogger().info("Couldn't refresh access token, probably refresh token has expired." +
                    " Getting new token using credentials.");
                acquireAuthorizationDetails();
            }
        }
        accessToken = accessDetails.getAccessToken();

        return accessToken;
    }

    private void acquireAuthorizationDetails() {
        FormBody.Builder acquireTokenBuilder = new FormBody.Builder()
            .add("grant_type", "password")
            .add("username", username)
            .add("password", password);

        if (clientId != null) {
            acquireTokenBuilder.add("client_id", clientId);
            acquireTokenBuilder.add("client_secret", clientSecret);
        }

        RequestBody acquireTokenRequestBody = acquireTokenBuilder.build();

        Request acquireTokenRequest = new Request.Builder()
            .url(authorizationServerUrl)
            .post(acquireTokenRequestBody)
            .build();

        this.accessDetails = getAccessDetails(acquireTokenRequest);
    }

    private void refreshAuthorizationDetails() {
        FormBody.Builder refreshTokenBuilder = new FormBody.Builder()
            .add("grant_type", "refresh_token")
            .add("refresh_token", this.accessDetails.getRefreshToken());

        if (clientId != null) {
            refreshTokenBuilder.add("client_id", clientId);
            refreshTokenBuilder.add("client_secret", clientSecret);
        }

        RequestBody refreshTokenRequestBody = refreshTokenBuilder.build();

        Request refreshRequest = new Request.Builder()
                .url(authorizationServerUrl)
                .post(refreshTokenRequestBody)
                .build();

        this.accessDetails = getAccessDetails(refreshRequest);
    }

    private AccessDetails getAccessDetails(Request newRequest) {
        try {
            Response response = httpClient.newCall(newRequest).execute();
            String responseBody = response.body().string();
            if (response.code() != 200) {
                getLogger().error(String.format("Bad response from the server during oauth2 request:\n%s", responseBody));
                throw new ProcessException(String.format("Got HTTP %d during oauth2 request.", response.code()));
            }

            ObjectMapper mapper = new ObjectMapper()
                .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .setPropertyNamingStrategy(com.fasterxml.jackson.databind.PropertyNamingStrategies.SNAKE_CASE);

            AccessDetails accessDetails = mapper.readValue(responseBody, AccessDetails.class);

            return accessDetails;
        } catch (IOException e) {
            throw new ProcessException(e);
        }
    }
}
