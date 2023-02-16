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
package org.apache.nifi.processors.shopify.rest;

import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.shopify.model.IncrementalLoadingParameter;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

public class ShopifyRestService {

    private static final String HTTPS = "https";
    static final String ACCESS_TOKEN_KEY = "X-Shopify-Access-Token";

    private final WebClientServiceProvider webClientServiceProvider;
    private final String version;
    private final String baseUrl;
    private final String accessToken;
    private final String resourceName;
    private final String limit;
    private final IncrementalLoadingParameter incrementalLoadingParameter;

    public ShopifyRestService(
            final WebClientServiceProvider webClientServiceProvider,
            final String version,
            final String baseUrl,
            final String accessToken,
            final String resourceName,
            final String limit,
            final IncrementalLoadingParameter incrementalLoadingParameter) {
        this.webClientServiceProvider = webClientServiceProvider;
        this.version = version;
        this.baseUrl = baseUrl;
        this.accessToken = accessToken;
        this.resourceName = resourceName;
        this.limit = limit;
        this.incrementalLoadingParameter = incrementalLoadingParameter;
    }

    public HttpResponseEntity getShopifyObjects() {
        final URI uri = getUri();
        return retrieveResponse(uri);
    }

    public HttpResponseEntity getShopifyObjects(final String startTime, final String endTime) {
        final URI uri = getIncrementalUri(startTime, endTime);
        return retrieveResponse(uri);
    }

    public HttpResponseEntity getShopifyObjects(final String cursor) {
        final URI uri;
        try {
            uri = new URI(cursor);
        } catch (URISyntaxException e) {
            throw new ProcessException("Could not create URI from cursor while paging", e);
        }
        return retrieveResponse(uri);
    }

    public String getBaseUriString() {
        return getBaseUri().build().toString();
    }

    private HttpResponseEntity retrieveResponse(URI uri) {
        return webClientServiceProvider.getWebClientService()
                .get()
                .uri(uri)
                .header(ACCESS_TOKEN_KEY, accessToken)
                .retrieve();
    }

    protected HttpUriBuilder getBaseUri() {
        return webClientServiceProvider.getHttpUriBuilder()
                .scheme(HTTPS)
                .host(baseUrl)
                .addPathSegment("admin")
                .addPathSegment("api")
                .addPathSegment(version)
                .addPathSegment(resourceName + ".json");
    }

    URI getUri() {
        final HttpUriBuilder uriBuilder = getBaseUri();
        if (limit != null) {
            uriBuilder.addQueryParameter("limit", limit);
        }
        return uriBuilder.build();
    }

    URI getIncrementalUri(final String startTime, final String endTime) {
        final HttpUriBuilder uriBuilder = getBaseUri();
        if (limit != null) {
            uriBuilder.addQueryParameter("limit", limit);
        }
        if (incrementalLoadingParameter != IncrementalLoadingParameter.NONE) {
            if (startTime != null) {
                final String minTime = incrementalLoadingParameter.name().toLowerCase(Locale.ROOT) + "_min";
                uriBuilder.addQueryParameter(minTime, startTime);
            }
            final String maxTime = incrementalLoadingParameter.name().toLowerCase(Locale.ROOT) + "_max";
            uriBuilder.addQueryParameter(maxTime, endTime);
        }

        return uriBuilder.build();
    }
}
