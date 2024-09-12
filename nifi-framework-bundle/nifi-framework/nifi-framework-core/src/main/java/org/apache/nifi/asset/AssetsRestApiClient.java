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

package org.apache.nifi.asset;

import org.apache.nifi.client.NiFiRestApiClient;
import org.apache.nifi.client.NiFiRestApiRetryableException;
import org.apache.nifi.web.api.entity.AssetsEntity;
import org.apache.nifi.web.client.StandardHttpUriBuilder;
import org.apache.nifi.web.client.api.HttpRequestBodySpec;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.api.WebClientServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URI;

/**
 * Encapsulates the REST API calls required to synchronize assets.
 */
class AssetsRestApiClient extends NiFiRestApiClient {

    private static final Logger logger = LoggerFactory.getLogger(AssetsRestApiClient.class);

    private static final String NIFI_API_PATH_PART = "nifi-api";
    private static final String PARAMETER_CONTEXTS_PATH_PART = "parameter-contexts";
    private static final String ASSETS_PATH_PART = "assets";

    public AssetsRestApiClient(final WebClientService webClientService, final String host, final int port, final boolean secure) {
        super(webClientService, host, port, secure);
    }

    public AssetsEntity getAssets(final String parameterContextId) {
        final URI requestUri = new StandardHttpUriBuilder()
                .scheme(baseUri.getScheme())
                .host(baseUri.getHost())
                .port(baseUri.getPort())
                .addPathSegment(NIFI_API_PATH_PART)
                .addPathSegment(PARAMETER_CONTEXTS_PATH_PART)
                .addPathSegment(parameterContextId)
                .addPathSegment(ASSETS_PATH_PART)
                .build();
        logger.debug("Requesting Asset listing from {}", requestUri);

        // Send the replicated header so that the cluster coordinator does not replicate the request, otherwise this call can happen when no nodes
        // are considered connected and result in a 500 exception that can't easily be differentiated from other unknown errors
        final HttpRequestBodySpec requestBodySpec = webClientService.get()
                .uri(requestUri)
                .header(ACCEPT_HEADER, APPLICATION_JSON)
                .header(REQUEST_REPLICATED_HEADER, Boolean.TRUE.toString());

        return executeEntityRequest(requestUri, requestBodySpec, AssetsEntity.class);
    }

    public InputStream getAssetContent(final String parameterContextId, final String assetId) {
        final URI requestUri = new StandardHttpUriBuilder()
                .scheme(baseUri.getScheme())
                .host(baseUri.getHost())
                .port(baseUri.getPort())
                .addPathSegment(NIFI_API_PATH_PART)
                .addPathSegment(PARAMETER_CONTEXTS_PATH_PART)
                .addPathSegment(parameterContextId)
                .addPathSegment(ASSETS_PATH_PART)
                .addPathSegment(assetId)
                .build();
        logger.debug("Getting asset content from {}", requestUri);

        try {
            final HttpResponseEntity response = webClientService.get()
                    .uri(requestUri)
                    .header(ACCEPT_HEADER, APPLICATION_OCTET_STREAM)
                    .retrieve();
            return getResponseBody(requestUri, response);
        } catch (final WebClientServiceException e) {
            throw new NiFiRestApiRetryableException(e.getMessage(), e);
        }
    }

}
