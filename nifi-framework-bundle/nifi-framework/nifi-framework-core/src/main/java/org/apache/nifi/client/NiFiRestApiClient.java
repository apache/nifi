/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jakarta.xmlbind.JakartaXmlBindAnnotationIntrospector;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.web.client.api.HttpRequestBodySpec;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpResponseStatus;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.api.WebClientServiceException;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Base class for calling the NiFi Rest API from framework code.
 */
public abstract class NiFiRestApiClient {

    private static final String HTTP_SCHEME = "http";
    private static final String HTTPS_SCHEME = "https";

    protected static final String ACCEPT_HEADER = "Accept";
    protected static final String REQUEST_REPLICATED_HEADER = "request-replicated";
    protected static final String APPLICATION_JSON = "application/json";
    protected static final String APPLICATION_OCTET_STREAM = "application/octet-stream";

    protected final URI baseUri;
    protected final WebClientService webClientService;
    protected final ObjectMapper objectMapper;

    /**
     * @param webClientService the web client service provided by the framework
     * @param host the base host
     * @param port the base port
     * @param secure indicates if the API is secure
     */
    public NiFiRestApiClient(final WebClientService webClientService, final String host, final int port, final boolean secure) {
        try {
            this.baseUri = new URI(secure ? HTTPS_SCHEME : HTTP_SCHEME, null, host, port, null, null, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        this.webClientService = Objects.requireNonNull(webClientService, "WebClientService is required");
        this.objectMapper = new ObjectMapper();
        this.objectMapper.setAnnotationIntrospector(new JakartaXmlBindAnnotationIntrospector(objectMapper.getTypeFactory()));
    }

    /**
     * Executes the request specified by the given body spec and unmarshalls the response into the given entity, or throws an exception if the request was not successful.
     *
     * @param requestUri the URI being requested
     * @param requestBodySpec the body spec
     * @param responseEntityClass the entity class of the response
     * @return an instance of the given entity class containing the response
     * @param <T> the type of response
     */
    protected <T> T executeEntityRequest(final URI requestUri, final HttpRequestBodySpec requestBodySpec, final Class<T> responseEntityClass) {
        try (final HttpResponseEntity response = requestBodySpec.retrieve()) {
            final InputStream responseBody = getResponseBody(requestUri, response);
            return objectMapper.readValue(responseBody, responseEntityClass);
        } catch (final WebClientServiceException | IOException e) {
            throw new NiFiRestApiRetryableException(e.getMessage(), e);
        }
    }

    /**
     * Inspects the response and returns the body for a successful response, otherwise throws an exception based on the response code.
     *
     * @param requestUri the URI that was requested
     * @param response the response
     * @return the input stream of the response body
     * @throws NiFiRestApiRetryableException if a retryable response or exception happens
     * @throws IllegalStateException if a non-retryable response happens
     */
    protected InputStream getResponseBody(final URI requestUri, final HttpResponseEntity response) {
        final int statusCode = response.statusCode();
        if (HttpResponseStatus.OK.getCode() == statusCode) {
            return response.body();
        } else {
            final String responseMessage;
            try {
                responseMessage = IOUtils.toString(response.body(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new NiFiRestApiRetryableException("Error reading response from %s - %s".formatted(requestUri, statusCode), e);
            }
            if (statusCode == HttpResponseStatus.CONFLICT.getCode()) {
                throw new NiFiRestApiRetryableException("Error calling %s - %s - %s".formatted(requestUri, statusCode, responseMessage));
            } else {
                throw new IllegalStateException("Error calling %s - %s - %s".formatted(requestUri, statusCode, responseMessage));
            }
        }
    }
}
