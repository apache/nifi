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

package org.apache.nifi.nar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jakarta.xmlbind.JakartaXmlBindAnnotationIntrospector;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.web.api.entity.NarSummariesEntity;
import org.apache.nifi.web.client.StandardHttpUriBuilder;
import org.apache.nifi.web.client.api.HttpRequestBodySpec;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpResponseStatus;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.api.WebClientServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Encapsulate API calls for listing and downloading NARs through the REST API of a NiFi node.
 */
public class NarRestApiClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(NarRestApiClient.class);

    private static final String HTTP_SCHEME = "http";
    private static final String HTTPS_SCHEME = "https";

    private static final String NIFI_API_PATH = "nifi-api";
    private static final String CONTROLLER_PATH = "controller";
    private static final String NAR_MANAGER_PATH = "nar-manager";
    private static final String NARS_PATH = "nars";
    private static final String NAR_CONTENT_PATH = "content";

    private final URI baseUri;
    private final WebClientService webClientService;
    private final ObjectMapper objectMapper;

    public NarRestApiClient(final WebClientService webClientService, final String host, final int port, final boolean secure) {
        try {
            this.baseUri = new URI(secure ? HTTPS_SCHEME : HTTP_SCHEME, null, host, port, null, null, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        this.webClientService = Objects.requireNonNull(webClientService, "WebClientService is required");
        this.objectMapper = new ObjectMapper();
        this.objectMapper.setAnnotationIntrospector(new JakartaXmlBindAnnotationIntrospector(objectMapper.getTypeFactory()));
    }

    public NarSummariesEntity listNarSummaries() {
        final URI requestUri = new StandardHttpUriBuilder()
                .scheme(baseUri.getScheme())
                .host(baseUri.getHost())
                .port(baseUri.getPort())
                .addPathSegment(NIFI_API_PATH)
                .addPathSegment(CONTROLLER_PATH)
                .addPathSegment(NAR_MANAGER_PATH)
                .addPathSegment(NARS_PATH)
                .build();
        LOGGER.debug("Requesting NAR summaries from {}", requestUri);

        // Send the replicated header so that the cluster coordinator does not replicate the request for listing the NAR summaries, otherwise this call
        // can happen when no nodes are considered connected and result in a 500 exception that can't easily be differentiated from other unknown errors
        final HttpRequestBodySpec requestBodySpec = webClientService.get()
                .uri(requestUri)
                .header("Accept", "application/json")
                .header("X-Request-Replicated", "true");

        try (final HttpResponseEntity response = requestBodySpec.retrieve()) {
            final InputStream responseBody = getResponseBody(requestUri, response);
            return objectMapper.readValue(responseBody, NarSummariesEntity.class);
        } catch (final WebClientServiceException | IOException e) {
            throw new NarRestApiRetryableException(e.getMessage(), e);
        }
    }

    public InputStream downloadNar(final String identifier) {
        final URI requestUri = new StandardHttpUriBuilder()
                .scheme(baseUri.getScheme())
                .host(baseUri.getHost())
                .port(baseUri.getPort())
                .addPathSegment(NIFI_API_PATH)
                .addPathSegment(CONTROLLER_PATH)
                .addPathSegment(NAR_MANAGER_PATH)
                .addPathSegment(NARS_PATH)
                .addPathSegment(identifier)
                .addPathSegment(NAR_CONTENT_PATH)
                .build();
        LOGGER.debug("Downloading NAR [{}] from {}", identifier, requestUri);

        try {
            final HttpResponseEntity response = webClientService.get()
                    .uri(requestUri)
                    .header("Accept", "application/octet-stream")
                    .retrieve();
            return getResponseBody(requestUri, response);
        } catch (final WebClientServiceException e) {
            throw new NarRestApiRetryableException(e.getMessage(), e);
        }
    }

    private InputStream getResponseBody(final URI requestUri, final HttpResponseEntity response) {
        final int statusCode = response.statusCode();
        if (HttpResponseStatus.OK.getCode() == statusCode) {
            return response.body();
        } else {
            final String responseMessage;
            try {
                responseMessage = IOUtils.toString(response.body(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new NarRestApiRetryableException("Error reading response from %s - %s".formatted(requestUri, statusCode), e);
            }
            if (statusCode == HttpResponseStatus.CONFLICT.getCode()) {
                throw new NarRestApiRetryableException("Error calling %s - %s - %s".formatted(requestUri, statusCode, responseMessage));
            } else {
                throw new IllegalStateException("Error calling %s - %s - %s".formatted(requestUri, statusCode, responseMessage));
            }
        }
    }
}
