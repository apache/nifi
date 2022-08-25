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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.processors.shopify.model.IncrementalLoadingParameter;
import org.apache.nifi.processors.shopify.model.ResourceType;
import org.apache.nifi.processors.shopify.rest.ShopifyRestService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.client.StandardHttpUriBuilder;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.provider.service.StandardWebClientServiceProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

@RunWith(value = BlockJUnit4ClassRunner.class)
class GetShopifyIT {

    private static final String BASE_URL = "/test/shopify";
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

        final StandardWebClientServiceProvider standardWebClientServiceProvider =
                new StandardWebClientServiceProvider();
        final CustomGetShopify customGetShopify = new CustomGetShopify(expectedExecutionTime);

        TestRunner runner = TestRunners.newTestRunner(customGetShopify);
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

    @Test
    void testHttpError429() throws InitializationException {
        server.enqueue(new MockResponse().setResponseCode(429));

        String expectedExecutionTime = "2022-08-16T10:15:30Z";

        final StandardWebClientServiceProvider standardWebClientServiceProvider =
                new StandardWebClientServiceProvider();
        final CustomGetShopify customGetShopify = new CustomGetShopify(expectedExecutionTime);

        TestRunner runner = TestRunners.newTestRunner(customGetShopify);
        runner.addControllerService("standardWebClientServiceProvider", standardWebClientServiceProvider);
        runner.enableControllerService(standardWebClientServiceProvider);

        runner.setProperty(GetShopify.WEB_CLIENT_PROVIDER, standardWebClientServiceProvider.getIdentifier());
        runner.setProperty(GetShopify.ACCESS_TOKEN, "testAccessToken");
        runner.setProperty(GetShopify.RESOURCE_TYPE, ResourceType.PRODUCT.getValue());
        runner.setProperty(GetShopify.API_URL, "test.shopify.nifi");
        runner.setProperty(ResourceType.PRODUCT.getValue(), "products");

        assertThrows(AssertionError.class, () -> runner.run(1));
        assertTrue(((MockProcessContext) runner.getProcessContext()).isYieldCalled(),
                "In case of 429 the processor should be yielded.");
    }

    @Test
    void testHttpError404() throws InitializationException {
        server.enqueue(new MockResponse().setResponseCode(404));

        String expectedExecutionTime = "2022-08-16T10:15:30Z";

        final StandardWebClientServiceProvider standardWebClientServiceProvider =
                new StandardWebClientServiceProvider();
        final CustomGetShopify customGetShopify = new CustomGetShopify(expectedExecutionTime);

        TestRunner runner = TestRunners.newTestRunner(customGetShopify);
        runner.addControllerService("standardWebClientServiceProvider", standardWebClientServiceProvider);
        runner.enableControllerService(standardWebClientServiceProvider);

        runner.setProperty(GetShopify.WEB_CLIENT_PROVIDER, standardWebClientServiceProvider.getIdentifier());
        runner.setProperty(GetShopify.ACCESS_TOKEN, "testAccessToken");
        runner.setProperty(GetShopify.RESOURCE_TYPE, ResourceType.PRODUCT.getValue());
        runner.setProperty(GetShopify.API_URL, "test.shopify.nifi");
        runner.setProperty(ResourceType.PRODUCT.getValue(), "products");

        runner.run(1);

        assertTrue(((MockProcessContext) runner.getProcessContext()).isYieldCalled(),
                "In case of 404 the processor should be yielded.");
    }

    @Test
    void testNonEmptyJsonResponseSentToSuccess() throws InitializationException, IOException {
        final MockResponse mockResponse = new MockResponse()
                .setResponseCode(200)
                .setBody(getResourceAsString("collection_listings.json"));
        server.enqueue(mockResponse);

        String expectedExecutionTime = "2022-08-16T10:15:30Z";

        final StandardWebClientServiceProvider standardWebClientServiceProvider =
                new StandardWebClientServiceProvider();
        final CustomGetShopify customGetShopify = new CustomGetShopify(expectedExecutionTime);

        TestRunner runner = TestRunners.newTestRunner(customGetShopify);
        runner.addControllerService("standardWebClientServiceProvider", standardWebClientServiceProvider);
        runner.enableControllerService(standardWebClientServiceProvider);

        runner.setProperty(GetShopify.WEB_CLIENT_PROVIDER, standardWebClientServiceProvider.getIdentifier());
        runner.setProperty(GetShopify.ACCESS_TOKEN, "testAccessToken");
        runner.setProperty(GetShopify.RESOURCE_TYPE, ResourceType.SALES_CHANNELS.getValue());
        runner.setProperty(GetShopify.API_URL, "test.shopify.nifi");
        runner.setProperty(ResourceType.SALES_CHANNELS.getValue(), "collection_listings");

        runner.run(1);

        List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(GetShopify.REL_SUCCESS);
        assertEquals(1, flowFilesForRelationship.size());
    }

    static class CustomGetShopify extends GetShopify {

        private final String executionTime;

        public CustomGetShopify(String executionTime) {
            this.executionTime = executionTime;
        }

        @Override
        ShopifyRestService getShopifyRestService(WebClientService webClientService, HttpUriBuilder uriBuilder,
                String apiVersion, String baseUrl,
                String accessToken, String resourceName, IncrementalLoadingParameter incrementalLoadingParameter) {
            return new CustomShopifyRestService(webClientService, uriBuilder, apiVersion, baseUrl, accessToken,
                    resourceName, incrementalLoadingParameter);
        }

        @Override
        String getCurrentExecutionTime() {
            return executionTime;
        }
    }

    static class CustomShopifyRestService extends ShopifyRestService {

        public CustomShopifyRestService(WebClientService webClientService, HttpUriBuilder uriBuilder, String version,
                String baseUrl,
                String accessToken, String resourceName, IncrementalLoadingParameter incrementalLoadingParameter) {
            super(webClientService, uriBuilder, version, baseUrl, accessToken, resourceName,
                    incrementalLoadingParameter);
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

    private String getResourceAsString(final String resourceName) throws IOException {
        return IOUtils.toString(
                this.getClass().getClassLoader().getResourceAsStream(resourceName),
                StandardCharsets.UTF_8
        );
    }
}
