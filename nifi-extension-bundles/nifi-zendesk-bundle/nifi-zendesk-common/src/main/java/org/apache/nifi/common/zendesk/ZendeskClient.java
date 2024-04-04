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
package org.apache.nifi.common.zendesk;

import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.OptionalLong;

import static java.lang.String.format;
import static java.util.Base64.getEncoder;
import static org.apache.nifi.common.zendesk.ZendeskProperties.HTTPS;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_HOST_TEMPLATE;

public class ZendeskClient {

    private static final String AUTHORIZATION_HEADER_NAME = "Authorization";
    private static final String CONTENT_TYPE_HEADER_NAME = "Content-Type";
    private static final String BASIC_AUTH_PREFIX = "Basic ";

    private final WebClientServiceProvider webClientServiceProvider;
    private final ZendeskAuthenticationContext authenticationContext;

    public ZendeskClient(WebClientServiceProvider webClientServiceProvider, ZendeskAuthenticationContext authenticationContext) {
        this.webClientServiceProvider = webClientServiceProvider;
        this.authenticationContext = authenticationContext;
    }

    /**
     * Sends a POST request to the Zendesk API.
     *
     * @param uri         target uri
     * @param inputStream body of the request
     * @return response from the Zendesk API
     * @throws IOException error while performing the POST request
     */
    public HttpResponseEntity performPostRequest(URI uri, InputStream inputStream) throws IOException {
        return webClientServiceProvider.getWebClientService()
                .post()
                .uri(uri)
                .header(CONTENT_TYPE_HEADER_NAME, ZendeskProperties.APPLICATION_JSON)
                .header(AUTHORIZATION_HEADER_NAME, basicAuthHeaderValue())
                .body(inputStream, OptionalLong.of(inputStream.available()))
                .retrieve();
    }

    /**
     * Sends a GET request to the Zendesk API.
     *
     * @param uri target uri
     * @return response from the Zendesk API
     */
    public HttpResponseEntity performGetRequest(URI uri) {
        return webClientServiceProvider.getWebClientService()
                .get()
                .uri(uri)
                .header(AUTHORIZATION_HEADER_NAME, basicAuthHeaderValue())
                .retrieve();
    }

    /**
     * Creates a Uri builder with the provided resource path.
     *
     * @param resourcePath resource path
     * @return Uri builder
     */
    public HttpUriBuilder uriBuilder(String resourcePath) {
        return webClientServiceProvider.getHttpUriBuilder()
                .scheme(HTTPS)
                .host(format(ZENDESK_HOST_TEMPLATE, authenticationContext.getSubdomain()))
                .encodedPath(resourcePath);
    }

    /**
     * Constructs the header for the Zendesk API call.
     *
     * @return the constructed header
     */
    private String basicAuthHeaderValue() {
        final String user = authenticationContext.getAuthenticationType().enrichUserName(authenticationContext.getUser());
        final String userWithPassword = user + ":" + authenticationContext.getAuthenticationCredentials();
        return BASIC_AUTH_PREFIX + getEncoder().encodeToString(userWithPassword.getBytes());
    }
}
