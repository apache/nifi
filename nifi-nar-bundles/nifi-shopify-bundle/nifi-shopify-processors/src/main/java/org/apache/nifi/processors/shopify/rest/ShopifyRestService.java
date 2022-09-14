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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import org.apache.nifi.processors.shopify.model.IncrementalLoadingParameter;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.api.WebClientService;

public class ShopifyRestService {

    static final String ACCESS_TOKEN_KEY = "X-Shopify-Access-Token";
    public static final String HTTPS = "https";
    private final WebClientService webClientService;
    private final HttpUriBuilder uriBuilder;
    private final String version;
    private final String baseUrl;
    private final String accessToken;
    private final String resourceName;
    private final String limit;
    private final IncrementalLoadingParameter incrementalLoadingParameter;

    public ShopifyRestService(final WebClientService webClientService,
            final HttpUriBuilder uriBuilder,
            final String version,
            final String baseUrl,
            final String accessToken,
            final String resourceName,
            final String limit,
            final IncrementalLoadingParameter incrementalLoadingParameter) {
        this.webClientService = webClientService;
        this.uriBuilder = uriBuilder;
        this.version = version;
        this.baseUrl = baseUrl;
        this.accessToken = accessToken;
        this.resourceName = resourceName;
        this.limit = limit;
        this.incrementalLoadingParameter = incrementalLoadingParameter;
    }

    public HttpResponseEntity getShopifyObjects(final boolean isIncremental,
                                                final String startTime,
                                                final String endTime,
                                                final String cursor) throws URISyntaxException {
        final URI uri;
        if (cursor != null) {
            uri = new URI(cursor);
        } else {
            uri = getUri(isIncremental, startTime, endTime);
        }
        return webClientService
                .get()
                .uri(uri)
                .header(ACCESS_TOKEN_KEY, accessToken)
                .retrieve();
    }

    protected HttpUriBuilder getBaseUri() {
        return uriBuilder.scheme(HTTPS)
                .host(baseUrl)
                .addPathSegment("admin")
                .addPathSegment("api")
                .addPathSegment(version)
                .addPathSegment(resourceName + ".json");
    }


    URI getUri(final boolean isIncremental, final String startTime, final String endTime) {
        final HttpUriBuilder uriBuilder = getBaseUri();
        if (limit != null) {
            uriBuilder.addQueryParameter("limit", limit);
        }
        if (isIncremental && incrementalLoadingParameter != IncrementalLoadingParameter.NONE) {
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
