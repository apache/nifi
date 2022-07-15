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

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.shopify.model.IncrementalLoadingParameter;

import java.io.InputStream;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class ShopifyRestService {

    private static final String API_URL_TEMPLATE = "%s/admin/api/%s/%s.json";
    private static final String OAUTH_URL_TEMPLATE = "%s/admin/oauth/%s.json";

    private final String version;
    private final String baseUrl;
    private final String accessToken;
    private final String resourceName;
    private final RestUriType restUriType;
    private final IncrementalLoadingParameter incrementalLoadingParameter;
    private final OkHttpClient httpClient;

    public ShopifyRestService(final String version, final String baseUrl, final String accessToken, final String resourceName, final RestUriType restUriType,
                              final IncrementalLoadingParameter incrementalLoadingParameter, final int responseTimeoutMillis) {
        this.version = version;
        this.baseUrl = baseUrl;
        this.accessToken = accessToken;
        this.resourceName = resourceName;
        this.restUriType = restUriType;
        this.incrementalLoadingParameter = incrementalLoadingParameter;
        httpClient = new OkHttpClient.Builder()
                .readTimeout(responseTimeoutMillis, TimeUnit.MILLISECONDS)
                .build();
    }

    public InputStream getShopifyObjects(final String fromDateTime) {
        String url = null;
        if (restUriType == RestUriType.API) {
            url = String.format(API_URL_TEMPLATE, baseUrl, version, resourceName);
        } else if (restUriType == RestUriType.OAUTH) {
            url = String.format(OAUTH_URL_TEMPLATE, baseUrl, resourceName);
        }
        HttpUrl.Builder httpUrlBuilder = HttpUrl.parse(url).newBuilder();
        if (incrementalLoadingParameter != IncrementalLoadingParameter.NONE && fromDateTime != null) {
            httpUrlBuilder.addQueryParameter(incrementalLoadingParameter.name().toLowerCase(Locale.ROOT), fromDateTime);
        }
        Request request = new Request.Builder()
                .addHeader("X-Shopify-Access-Token", accessToken)
                .url(httpUrlBuilder.build())
                .get()
                .build();

        return request(request);
    }

    private InputStream request(Request request) {
        Response response = null;
        try {
            response = httpClient.newCall(request).execute();
            if (response.code() != 200) {
                throw new ProcessException("Invalid response" +
                        " Code: " + response.code() +
                        " Message: " + response.message() +
                        " Body: " + (response.body() == null ? null : response.body().string())
                );
            }
            return response.body().byteStream();
        } catch (Exception e) {
            if (response != null) {
                response.close();
            }
            throw new ProcessException(String.format("Shopify HTTP request failed [%s]", request.url()), e);
        }
    }

    public String getResourceName() {
        return resourceName;
    }
}
