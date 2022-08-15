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

import org.apache.nifi.processors.shopify.model.IncrementalLoadingParameter;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.api.WebClientService;

import java.io.InputStream;
import java.net.URI;
import java.util.Locale;

public class ShopifyRestService {

    static final String ACCESS_TOKEN_KEY = "X-Shopify-Access-Token";
    public static final String HTTPS = "https";
    private final WebClientService webClientService;
    private final HttpUriBuilder uriBuilder;
    private final String version;
    private final String baseUrl;
    private final String accessToken;
    private final String resourceName;
    private final IncrementalLoadingParameter incrementalLoadingParameter;

    public ShopifyRestService(final WebClientService webClientService,
                              final HttpUriBuilder uriBuilder,
                              final String version,
                              final String baseUrl,
                              final String accessToken,
                              final String resourceName,
                              final IncrementalLoadingParameter incrementalLoadingParameter) {
        this.webClientService = webClientService;
        this.uriBuilder = uriBuilder;
        this.version = version;
        this.baseUrl = baseUrl;
        this.accessToken = accessToken;
        this.resourceName = resourceName;
        this.incrementalLoadingParameter = incrementalLoadingParameter;
    }

    public InputStream getShopifyObjects(final String fromDateTime) {
        final URI url = getUri(fromDateTime);

        final HttpResponseEntity response = webClientService
                .get()
                .uri(url)
                .header(ACCESS_TOKEN_KEY, accessToken)
                .retrieve();

        return response.body();
    }

    protected HttpUriBuilder getBaseUri() {
        return uriBuilder.scheme(HTTPS)
                .host(baseUrl)
                .addPathSegment("admin")
                .addPathSegment("api")
                .addPathSegment(version)
                .addPathSegment(resourceName + ".json");
    }


    URI getUri(String fromDateTime) {
        final HttpUriBuilder uriBuilder = getBaseUri();

        if (incrementalLoadingParameter != IncrementalLoadingParameter.NONE && fromDateTime != null) {
            uriBuilder.addQueryParameter(incrementalLoadingParameter.name().toLowerCase(Locale.ROOT), fromDateTime);
        }

        return uriBuilder.build();
    }

    public String getResourceName() {
        return resourceName;
    }
}
