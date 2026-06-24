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

package org.apache.nifi.snowflake.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.processors.snowflake.snowpipe.InsertFiles;
import org.apache.nifi.processors.snowflake.snowpipe.InsertReport;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.api.WebClientServiceException;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Objects;
import java.util.UUID;

/**
 * Client for Snowflake Snowpipe REST API using Java HttpClient
 */
class SnowpipeIngestClient {

    private static final String INSERT_FILES_PATH = "/v1/data/pipes/%s/insertFiles";

    private static final String INSERT_REPORT_PATH = "/v1/data/pipes/%s/insertReport";

    private static final String REQUEST_ID_FORMAT = "%s?requestId=%s";

    private static final String AUTHORIZATION_HEADER = "Authorization";

    private static final String CONTENT_TYPE_HEADER = "Content-Type";

    private static final String ACCEPT_HEADER = "Accept";

    private static final String APPLICATION_JSON = "application/json";

    private static final ObjectMapper objectMapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private final WebClientService webClientService;

    private final URI insertFilesUri;

    private final URI insertReportUri;

    private final RSAKeyAuthorizationProvider authorizationProvider;

    /**
     * Snowpipe Ingest Client with required properties
     *
     * @param baseUri Base URI for the Snowpipe REST API
     * @param pipeName Fully qualified pipe name
     * @param authorizationProvider RSA Key Authorization Provider for JWT authentication
     */
    SnowpipeIngestClient(
            final URI baseUri,
            final String pipeName,
            final RSAKeyAuthorizationProvider authorizationProvider,
            final WebClientService webClientService
    ) {
        Objects.requireNonNull(baseUri, "Base URI required");
        Objects.requireNonNull(pipeName, "Pipe Name required");
        Objects.requireNonNull(authorizationProvider, "Authorization Provider required");
        Objects.requireNonNull(webClientService, "Web Service Client required");

        final String insertFilesPath = INSERT_FILES_PATH.formatted(pipeName);
        this.insertFilesUri = baseUri.resolve(insertFilesPath);

        final String insertReportPath = INSERT_REPORT_PATH.formatted(pipeName);
        this.insertReportUri = baseUri.resolve(insertReportPath);

        this.authorizationProvider = authorizationProvider;
        this.webClientService = webClientService;
    }

    /**
     * Submit staged files for ingestion via the insertFiles REST API endpoint
     *
     * @param insertFiles Files to ingest
     * @throws SnowpipeResponseException on HTTP error responses or transport failures
     */
    void insertFiles(final InsertFiles insertFiles) {
        Objects.requireNonNull(insertFiles, "Insert Files required");

        final String requestBody = serializeInsertFiles(insertFiles);
        final URI requestUri = appendRequestId(insertFilesUri);
        final String authorization = authorizationProvider.getRequestAuthorization().authorization();

        try (
                HttpResponseEntity responseEntity = webClientService.post()
                        .uri(requestUri)
                        .header(AUTHORIZATION_HEADER, authorization)
                        .header(CONTENT_TYPE_HEADER, APPLICATION_JSON)
                        .body(requestBody)
                        .retrieve();
                InputStream responseBodyStream = responseEntity.body()
        ) {
            final int statusCode = responseEntity.statusCode();
            if (statusCode != HttpURLConnection.HTTP_OK) {
                final byte[] responseBodyBytes = responseBodyStream.readAllBytes();
                final String responseBody = new String(responseBodyBytes);
                final String message = "Insert Files POST [%s] HTTP %d Response [%s]".formatted(requestUri, statusCode, responseBody);
                throw new SnowpipeResponseException(message);
            }
        } catch (final IOException | WebClientServiceException e) {
            final String message = "Insert Files POST [%s] request failed".formatted(requestUri);
            throw new SnowpipeResponseException(message, e);
        }
    }

    /**
     * Retrieve the insert report for recently ingested files via the insertReport REST API endpoint
     *
     * @return Insert report containing file entries with ingestion status
     * @throws SnowpipeResponseException on HTTP error responses or transport failures
     */
    InsertReport getInsertReport() {
        final URI requestUri = appendRequestId(insertReportUri);
        final String authorization = authorizationProvider.getRequestAuthorization().authorization();

        try (
                HttpResponseEntity responseEntity = webClientService.get()
                        .uri(requestUri)
                        .header(AUTHORIZATION_HEADER, authorization)
                        .header(ACCEPT_HEADER, APPLICATION_JSON)
                        .retrieve();
                InputStream responseBodyStream = responseEntity.body()
        ) {
            final int statusCode = responseEntity.statusCode();
            if (statusCode == HttpURLConnection.HTTP_OK) {
                try {
                    return objectMapper.readValue(responseBodyStream, InsertReport.class);
                } catch (final JsonProcessingException e) {
                    final byte[] responseBodyBytes = responseBodyStream.readAllBytes();
                    final String responseBody = new String(responseBodyBytes);
                    final String message = "Insert Report [%s] response parsing failed [%s]".formatted(requestUri, responseBody);
                    throw new SnowpipeResponseException(message, e);
                }
            } else {
                final byte[] responseBodyBytes = responseBodyStream.readAllBytes();
                final String responseBody = new String(responseBodyBytes);
                final String message = "Insert Report failed GET [%s] HTTP %d Response [%s]".formatted(requestUri, statusCode, responseBody);
                throw new SnowpipeResponseException(message);
            }

        } catch (final IOException | WebClientServiceException e) {
            final String message = "Insert Report [%s] request failed".formatted(requestUri);
            throw new SnowpipeResponseException(message, e);
        }
    }

    private String serializeInsertFiles(final InsertFiles insertFiles) {
        try {
            return objectMapper.writeValueAsString(insertFiles);
        } catch (final JsonProcessingException e) {
            throw new SnowpipeResponseException("Failed to serialize Snowpipe insertFiles request", e);
        }
    }

    private URI appendRequestId(final URI baseUri) {
        final String requestId = UUID.randomUUID().toString();
        final String uriString = REQUEST_ID_FORMAT.formatted(baseUri, requestId);
        return URI.create(uriString);
    }
}
