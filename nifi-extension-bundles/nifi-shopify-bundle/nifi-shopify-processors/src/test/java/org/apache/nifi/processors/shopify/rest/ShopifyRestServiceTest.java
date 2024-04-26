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

import org.apache.nifi.web.client.api.HttpRequestBodySpec;
import org.apache.nifi.web.client.api.HttpRequestUriSpec;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URI;
import java.util.Locale;

import static org.apache.nifi.processors.shopify.model.IncrementalLoadingParameter.CREATED_AT;
import static org.apache.nifi.processors.shopify.rest.ShopifyRestService.ACCESS_TOKEN_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ShopifyRestServiceTest {

    private static final String VERSION = "v1";
    private static final String TEST_URL = "www.test.shopify.com";
    private static final String TEST_ACCESS_TOKEN = "testShopifyAccessToken";
    private static final String TEST_RESOURCE_NAME = "testResource";
    private static final String START_TIME = "2022-09-14T16:26:32.250Z";
    private static final String END_TIME = "2022-09-14T16:27:54.112Z";
    private static final String LIMIT = "2";

    @Mock
    private WebClientServiceProvider webClientServiceProvider;
    @Mock
    private WebClientService webClientService;
    @Mock
    private HttpUriBuilder uriBuilder;

    @BeforeEach
    void setup() {
        doReturn(uriBuilder).when(webClientServiceProvider).getHttpUriBuilder();
        doReturn(uriBuilder).when(uriBuilder).scheme(anyString());
        doReturn(uriBuilder).when(uriBuilder).host(anyString());
        doReturn(uriBuilder).when(uriBuilder).addPathSegment(anyString());
        doReturn(URI.create(TEST_URL)).when(uriBuilder).build();
    }

    @Test
    void testParameterAreAddedToUri() {
        final ShopifyRestService shopifyRestService = new ShopifyRestService(
                webClientServiceProvider,
                VERSION,
                TEST_URL,
                TEST_ACCESS_TOKEN,
                TEST_RESOURCE_NAME,
                LIMIT,
                CREATED_AT
        );

        doReturn(uriBuilder).when(uriBuilder).addQueryParameter(anyString(), anyString());

        shopifyRestService.getIncrementalUri(START_TIME, END_TIME);

        verify(uriBuilder).addQueryParameter(CREATED_AT.toString().toLowerCase(Locale.ROOT) + "_min", START_TIME);
        verify(uriBuilder).addQueryParameter(CREATED_AT.toString().toLowerCase(Locale.ROOT) + "_max", END_TIME);
        verify(uriBuilder).addQueryParameter("limit", LIMIT);
    }

    @Test
    void testGetShopifyObjects() {
        final ShopifyRestService shopifyRestService = new ShopifyRestService(
                webClientServiceProvider,
                VERSION,
                TEST_URL,
                TEST_ACCESS_TOKEN,
                TEST_RESOURCE_NAME,
                LIMIT,
                CREATED_AT
        );

        doReturn(uriBuilder).when(uriBuilder).addQueryParameter(anyString(), anyString());

        final HttpRequestUriSpec mockHttpRequestUriSpec = mock(HttpRequestUriSpec.class);
        final HttpRequestBodySpec mockHttpRequestBodySpec = mock(HttpRequestBodySpec.class);
        final HttpResponseEntity mockHttpResponseEntity = mock(HttpResponseEntity.class);

        doReturn(webClientService).when(webClientServiceProvider).getWebClientService();
        doReturn(mockHttpRequestUriSpec).when(webClientService).get();
        doReturn(mockHttpRequestBodySpec).when(mockHttpRequestUriSpec).uri(any());
        doReturn(mockHttpRequestBodySpec).when(mockHttpRequestBodySpec).header(anyString(), anyString());
        doReturn(mockHttpResponseEntity).when(mockHttpRequestBodySpec).retrieve();

        shopifyRestService.getShopifyObjects(START_TIME, END_TIME);

        verify(mockHttpRequestUriSpec).uri(URI.create(TEST_URL));
        verify(mockHttpRequestBodySpec).header(ACCESS_TOKEN_KEY, TEST_ACCESS_TOKEN);
    }
}
