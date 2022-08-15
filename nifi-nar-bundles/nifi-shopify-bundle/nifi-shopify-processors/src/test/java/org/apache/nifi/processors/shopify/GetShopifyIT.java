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
package org.apache.nifi.processors.shopify;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.processors.shopify.model.IncrementalLoadingParameter;
import org.apache.nifi.processors.shopify.model.ResourceType;
import org.apache.nifi.processors.shopify.rest.ShopifyRestService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.client.StandardHttpUriBuilder;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.provider.service.StandardWebClientServiceProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GetShopifyIT {
    public static final String BASE_URL = "/test/shopify";
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static MockWebServer server;
    private static HttpUrl baseUrl;

    @BeforeEach
    void setup() throws IOException {
        server = new MockWebServer();
        server.start();
        baseUrl = server.url(BASE_URL);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (server != null) {
            server.shutdown();
            server = null;
        }
    }

    @Test
    void testLimitIsAddedToUrl() throws InitializationException, IOException {

        server.enqueue(new MockResponse().setResponseCode(200));

        String expectedExecutionTime = "2022-08-16T10:15:30Z";

        final StandardWebClientServiceProvider standardWebClientServiceProvider = new StandardWebClientServiceProvider();
        final MockGetShopify mockGetShopify = new MockGetShopify(expectedExecutionTime);

        TestRunner runner = TestRunners.newTestRunner(mockGetShopify);
        runner.addControllerService("standardWebClientServiceProvider", standardWebClientServiceProvider);
        runner.enableControllerService(standardWebClientServiceProvider);

        runner.setProperty(GetShopify.WEB_CLIENT_PROVIDER, standardWebClientServiceProvider.getIdentifier());
        runner.setProperty(GetShopify.ACCESS_TOKEN, "testAccessToken");
        runner.setProperty(GetShopify.RESOURCE_TYPE, ResourceType.PRODUCT.getValue());
        runner.setProperty(GetShopify.API_URL, "test.shopify.nifi");
        runner.setProperty(ResourceType.PRODUCT.getValue(), "products");

        runner.run(1);

        final StateMap state = runner.getStateManager().getState(Scope.CLUSTER);
        final String actualExecutionTime = state.get("products");

        assertEquals(expectedExecutionTime, actualExecutionTime);
    }

    static class MockGetShopify extends GetShopify {

        private final String executionTime;

        public MockGetShopify(String executionTime) {
            this.executionTime = executionTime;
        }

        @Override
        ShopifyRestService getShopifyRestService(WebClientService webClientService, HttpUriBuilder uriBuilder, String apiVersion, String baseUrl,
                                                 String accessToken, String resourceName, IncrementalLoadingParameter incrementalLoadingParameter) {
            return new MockShopifyRestService(webClientService, uriBuilder, apiVersion, baseUrl, accessToken, resourceName, incrementalLoadingParameter);
        }

        @Override
        String getCurrentExecutionTime() {
            return executionTime;
        }
    }

    static class MockShopifyRestService extends ShopifyRestService {

        public MockShopifyRestService(WebClientService webClientService, HttpUriBuilder uriBuilder, String version, String baseUrl,
                                      String accessToken, String resourceName, IncrementalLoadingParameter incrementalLoadingParameter) {
            super(webClientService, uriBuilder, version, baseUrl, accessToken, resourceName, incrementalLoadingParameter);
        }

        @Override
        protected HttpUriBuilder getBaseUri() {
            return new StandardHttpUriBuilder()
                    .scheme(baseUrl.scheme())
                    .host(baseUrl.host())
                    .port(baseUrl.port())
                    .encodedPath(baseUrl.encodedPath());
        }
    }
}
