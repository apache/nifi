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
package org.apache.nifi.services.iceberg.catalog;

import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.nifi.logging.ComponentLog;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Custom implementation of REST Client supporting Token Refresh using Client Credentials instead of Token Exchange from RFC 8693
 */
class CredentialsRefreshRESTClient implements RESTClient {
    protected static final String CLIENT_ID_PARAMETER = "client_id";

    protected static final String CLIENT_SECRET_PARAMETER = "client_secret";

    protected static final String GRANT_TYPE_PARAMETER = "grant_type";

    protected static final String CLIENT_CREDENTIALS_GRANT_TYPE = "client_credentials";

    protected static final String TOKEN_EXCHANGE_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:token-exchange";

    private static final String SUBJECT_TOKEN_PARAMETER = "subject_token";

    private static final String SUBJECT_TOKEN_TYPE_PARAMETER = "subject_token_type";

    private static final Set<String> TOKEN_EXCHANGE_PARAMETERS = Set.of(GRANT_TYPE_PARAMETER, SUBJECT_TOKEN_PARAMETER, SUBJECT_TOKEN_TYPE_PARAMETER);

    private final ComponentLog log;

    private final RESTClient restClient;

    private final String clientId;

    private final String clientSecret;

    public CredentialsRefreshRESTClient(final ComponentLog log, final RESTClient restClient, final String clientId, final String clientSecret) {
        this.log = Objects.requireNonNull(log, "Component Log required");
        this.restClient = Objects.requireNonNull(restClient, "REST Client required");
        this.clientId = Objects.requireNonNull(clientId, "Client ID required");
        this.clientSecret = Objects.requireNonNull(clientSecret, "Client Secret required");
    }

    @Override
    public void head(final String path, final Map<String, String> headers, final Consumer<ErrorResponse> errorHandler) {
        restClient.head(path, headers, errorHandler);
    }

    @Override
    public <T extends RESTResponse> T delete(final String path, final Class<T> responseType, final Map<String, String> headers, final Consumer<ErrorResponse> errorHandler) {
        return restClient.delete(path, responseType, headers, errorHandler);
    }

    @Override
    public <T extends RESTResponse> T get(final String path, final Map<String, String> queryParams, final Class<T> responseType, final Map<String, String> headers,
                                          final Consumer<ErrorResponse> errorHandler) {
        return restClient.get(path, queryParams, responseType, headers, errorHandler);
    }

    @Override
    public <T extends RESTResponse> T post(final String path, final RESTRequest body, final Class<T> responseType, final Map<String, String> headers, final Consumer<ErrorResponse> errorHandler) {
        return restClient.post(path, body, responseType, headers, errorHandler);
    }

    @Override
    public <T extends RESTResponse> T postForm(final String path, final Map<String, String> formData, final Class<T> responseType, final Map<String, String> headers,
                                               final Consumer<ErrorResponse> errorHandler) {
        final Map<String, String> postFormData;

        if (isTokenExchangeFormData(formData)) {
            // Replace Token Exchange parameters with Client Credentials parameters for requests from OAuth2Util.tokenExchangeRequest()
            postFormData = getClientCredentialsFormData(formData);
            log.debug("Token Refresh with Client Credentials requested");
        } else {
            postFormData = formData;
        }

        return restClient.postForm(path, postFormData, responseType, headers, errorHandler);
    }

    @Override
    public void close() throws IOException {
        restClient.close();
    }

    /**
     * Build REST Client with authentication session using current configuration
     *
     * @param session Authentication Session
     * @return REST Client
     */
    @Override
    public RESTClient withAuthSession(final AuthSession session) {
        final RESTClient sessionRestClient = restClient.withAuthSession(session);
        return new CredentialsRefreshRESTClient(log, sessionRestClient, clientId, clientSecret);
    }

    private boolean isTokenExchangeFormData(final Map<String, String> formData) {
        final boolean tokenExchangeFound;

        final String grantType = formData.get(GRANT_TYPE_PARAMETER);
        if (grantType == null) {
            tokenExchangeFound = false;
        } else {
            tokenExchangeFound = TOKEN_EXCHANGE_GRANT_TYPE.contentEquals(grantType);
        }

        return tokenExchangeFound;
    }

    private Map<String, String> getClientCredentialsFormData(final Map<String, String> formData) {
        final Map<String, String> clientCredentialsFormData = new HashMap<>();

        formData.keySet()
                .stream()
                .filter(Predicate.not(TOKEN_EXCHANGE_PARAMETERS::contains))
                .forEach(formDataKey -> {
                    final String formDataValue = formData.get(formDataKey);
                    clientCredentialsFormData.put(formDataKey, formDataValue);
                });

        clientCredentialsFormData.put(GRANT_TYPE_PARAMETER, CLIENT_CREDENTIALS_GRANT_TYPE);
        clientCredentialsFormData.put(CLIENT_ID_PARAMETER, clientId);
        clientCredentialsFormData.put(CLIENT_SECRET_PARAMETER, clientSecret);

        return clientCredentialsFormData;
    }
}
