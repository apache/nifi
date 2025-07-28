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

import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.nifi.logging.ComponentLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CredentialsRefreshRESTClientTest {
    private static final String CLIENT_ID = "CLIENT-ID";

    private static final String CLIENT_SECRET = "CLIENT-SECRET";

    private static final String PATH = "http://localhost";

    @Mock
    private ComponentLog log;

    @Mock
    private RESTClient restClient;

    @Captor
    private ArgumentCaptor<Map<String, String>> formDataCaptor;

    private CredentialsRefreshRESTClient client;

    @BeforeEach
    void setClient() {
        client = new CredentialsRefreshRESTClient(log, restClient, CLIENT_ID, CLIENT_SECRET);
    }

    @Test
    void testPostForm() {
        final Map<String, String> formData = Map.of();
        final Map<String, String> headers = Map.of();

        final OAuthTokenResponse tokenResponse = OAuthTokenResponse.builder().build();
        when(restClient.postForm(eq(PATH), eq(formData), eq(OAuthTokenResponse.class), eq(headers), any())).thenReturn(tokenResponse);

        final OAuthTokenResponse response = client.postForm(PATH, formData, OAuthTokenResponse.class, headers, ErrorHandlers.oauthErrorHandler());

        assertEquals(tokenResponse, response);
    }

    @Test
    void testPostFormTokenExchangeGrantType() {
        final Map<String, String> formData = Map.of(
            CredentialsRefreshRESTClient.GRANT_TYPE_PARAMETER, CredentialsRefreshRESTClient.TOKEN_EXCHANGE_GRANT_TYPE
        );
        final Map<String, String> headers = Map.of();

        final OAuthTokenResponse tokenResponse = OAuthTokenResponse.builder().build();
        when(restClient.postForm(eq(PATH), formDataCaptor.capture(), eq(OAuthTokenResponse.class), eq(headers), any())).thenReturn(tokenResponse);

        final OAuthTokenResponse response = client.postForm(PATH, formData, OAuthTokenResponse.class, headers, ErrorHandlers.oauthErrorHandler());

        assertEquals(tokenResponse, response);

        final Map<String, String> postFormData = formDataCaptor.getValue();
        assertEquals(CredentialsRefreshRESTClient.CLIENT_CREDENTIALS_GRANT_TYPE, postFormData.get(CredentialsRefreshRESTClient.GRANT_TYPE_PARAMETER));
        assertEquals(CLIENT_ID, postFormData.get(CredentialsRefreshRESTClient.CLIENT_ID_PARAMETER));
        assertEquals(CLIENT_SECRET, postFormData.get(CredentialsRefreshRESTClient.CLIENT_SECRET_PARAMETER));
    }
}
