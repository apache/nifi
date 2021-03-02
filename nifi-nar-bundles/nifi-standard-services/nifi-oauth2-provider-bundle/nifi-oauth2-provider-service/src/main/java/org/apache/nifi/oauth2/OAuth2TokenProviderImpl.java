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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StringUtils;

@Tags({"oauth2", "provider", "authorization" })
@CapabilityDescription("This controller service provides a way of working with access and refresh tokens via the " +
        "password and client_credential grant flows in the OAuth2 specification. It is meant to provide a way for components " +
        "to get a token from an oauth2 provider and pass that token as a part of a header to another service.")
public class OAuth2TokenProviderImpl extends AbstractControllerService implements OAuth2TokenProvider {
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    private String resourceServerUrl;
    private SSLContext sslContext;
    private SSLContextService sslService;

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        resourceServerUrl = context.getProperty(ACCESS_TOKEN_URL).evaluateAttributeExpressions().getValue();

        sslService = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextService.class);

        sslContext = sslService == null ? null : sslService.createContext();
    }


    @Override
    public AccessToken getAccessTokenByPassword(String clientId, String clientSecret,
                                                String username, String password) throws AccessTokenAcquisitionException {
        OkHttpClient.Builder builder = getClientBuilder();
        OkHttpClient client = builder.build();

        RequestBody body = new FormBody.Builder()
                .add("username", username)
                .add("password", password)
                .add("client_id", clientId)
                .add("client_secret", clientSecret)
                .add("grant_type", "password")
                .build();

        Request newRequest = new Request.Builder()
                .url(resourceServerUrl)
                .post(body)
                .build();

        return executePost(client, newRequest);
    }

    private OkHttpClient.Builder getClientBuilder() {
        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder();

        if (sslService != null) {
            final X509TrustManager trustManager = sslService.createTrustManager();
            clientBuilder.sslSocketFactory(sslContext.getSocketFactory(), trustManager);
        }

        return clientBuilder;
    }

    private AccessToken executePost(OkHttpClient httpClient, Request newRequest) throws AccessTokenAcquisitionException {
        try {
            Response response = httpClient.newCall(newRequest).execute();
            String body = response.body().string();
            if (response.code() >= 300) {
                getLogger().error(String.format("Bad response from the server during oauth2 request:\n%s", body));
                throw new AccessTokenAcquisitionException(String.format("Got HTTP %d during oauth2 request.", response.code()));
            }

            return parseTokenResponse(body);
        } catch (IOException e) {
            throw new AccessTokenAcquisitionException(e);
        }
    }

    @Override
    public AccessToken getAccessTokenByClientCredentials(String clientId, String clientSecret) throws AccessTokenAcquisitionException {
        OkHttpClient.Builder builder = getClientBuilder();
        OkHttpClient client = builder.build();

        RequestBody body = new FormBody.Builder()
                .add("grant_type", "client_credentials")
                .add("client_id", clientId)
                .add("client_secret", clientSecret)
                .build();

        Request newRequest = new Request.Builder()
                .url(resourceServerUrl)
                .post(body)
                .build();

        return executePost(client, newRequest);
    }

    @Override
    public AccessToken refreshToken(AccessToken refreshThis) throws AccessTokenAcquisitionException {
        if (StringUtils.isEmpty(refreshThis.getRefreshToken())) {
            throw new ProcessException("Missing refresh token. Refresh cannot happen.");
        }
        OkHttpClient.Builder builder = getClientBuilder();
        OkHttpClient client = builder.build();
        RequestBody body = new FormBody.Builder()
            .add("grant_type", "refresh_token")
            .add("refresh_token", refreshThis.getRefreshToken())
            .build();

        Request newRequest = new Request.Builder()
                .url(resourceServerUrl)
                .post(body)
                .build();

        return executePost(client, newRequest);
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final String KEY_ACCESS_TOKEN = "access_token";
    public static final String KEY_REFRESH_TOKEN = "refresh_token";
    public static final String KEY_EXPIRES = "expires_in";
    public static final String KEY_TOKEN_TYPE = "token_type";
    public static final String KEY_SCOPE = "scope";

    public AccessToken parseTokenResponse(String rawResponse) {
        try {
            Map<String, Object> parsed = MAPPER.readValue(rawResponse, Map.class);
            String accessToken = (String)parsed.get(KEY_ACCESS_TOKEN);
            String refreshToken = (String)parsed.get(KEY_REFRESH_TOKEN);
            Integer expires = (Integer)parsed.get(KEY_EXPIRES);
            String tokenType = (String)parsed.get(KEY_TOKEN_TYPE);
            String scope = (String)parsed.get(KEY_SCOPE);

            if (StringUtils.isEmpty(accessToken)) {
                throw new Exception(String.format("Missing value for %s", KEY_ACCESS_TOKEN));
            }

            if (StringUtils.isEmpty(tokenType)) {
                throw new Exception(String.format("Missing value for %s", KEY_TOKEN_TYPE));
            }

            return new AccessToken(accessToken, refreshToken, tokenType, expires, scope);
        } catch (Exception ex) {
            throw new ProcessException(ex);
        }
    }
}
