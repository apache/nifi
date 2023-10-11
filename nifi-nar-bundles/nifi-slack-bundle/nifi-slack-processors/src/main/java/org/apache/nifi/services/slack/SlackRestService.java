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
package org.apache.nifi.services.slack;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.OptionalLong;

public class SlackRestService {
    private static final ObjectMapper objectMapper;
    static {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private static final String POST_MESSAGE_PATH = "chat.postMessage";
    private final WebClientServiceProvider webClientServiceProvider;
    private final String accessToken;
    private final String apiUrl;
    private final ComponentLog logger;
    private final String charset;

    public SlackRestService(final WebClientServiceProvider webClientServiceProvider,
                            final String accessToken,
                            final String apiUrl,
                            final String charset,
                            final ComponentLog logger) {
        this.webClientServiceProvider = webClientServiceProvider;
        this.accessToken = accessToken;
        this.apiUrl = apiUrl;
        this.charset = charset;
        this.logger = logger;
    }

    public void sendMessageToChannel(final String message, final String channel) throws SlackRestServiceException {
        final URI apiUri = URI.create(apiUrl);
        final HttpUriBuilder uriBuilder = webClientServiceProvider.getHttpUriBuilder()
                .scheme(apiUri.getScheme())
                .host(apiUri.getHost())
                .encodedPath(apiUri.getPath())
                .addPathSegment(POST_MESSAGE_PATH);
        if (apiUri.getPort() != -1) {
            uriBuilder.port(apiUri.getPort());
        }
        final URI uri = uriBuilder.build();

        final ObjectNode requestBodyJson = createRequestBody(channel, message);

        final InputStream requestBodyInputStream;
        try {
            requestBodyInputStream = new ByteArrayInputStream(objectMapper.writeValueAsBytes(requestBodyJson));
        } catch (final JsonProcessingException e) {
            throw new SlackRestServiceException("JSON message serialization failed", e);
        }

        try (final HttpResponseEntity response = webClientServiceProvider.getWebClientService()
                .post()
                .uri(uri)
                .header("Authorization", String.format("Bearer %s", accessToken))
                .header("Content-Type", String.format("application/json; charset=\"%s\"", charset))
                .body(requestBodyInputStream, OptionalLong.of(requestBodyInputStream.available()))
                .retrieve()) {
            final int statusCode = response.statusCode();
            if (!(statusCode >= 200 && statusCode < 300)) {
                throw new SlackRestServiceException("HTTP error code: " + statusCode);
            }

            try {
                final PostMessageResponse slackResponse = objectMapper.readValue(response.body(), PostMessageResponse.class);
                checkResponse(slackResponse, channel);
            } catch (final IOException e) {
                throw new SlackRestServiceException("JSON response parsing failed", e);
            }

        } catch (final IOException e) {
            throw new ProcessException("Slack HTTP request failed", e);
        }
    }

    private ObjectNode createRequestBody(final String channel, final String message) {
        final ObjectNode requestBodyJson = objectMapper.createObjectNode();
        requestBodyJson.put("channel", channel);
        requestBodyJson.put("text", message);
        return requestBodyJson;
    }

    private void checkResponse(final PostMessageResponse response, final String channel) throws SlackRestServiceException {
        if (!response.isOk()) {
            throw new SlackRestServiceException("Slack error response: " + response.getError());
        }

        if (response.getWarning() != null) {
            logger.warn("Post message to channel [{}] warning: {}", channel, response.getWarning());
        }
    }
}
