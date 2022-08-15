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
package org.apache.nifi.processors.hubspot;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.client.StandardHttpUriBuilder;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.provider.service.StandardWebClientServiceProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GetHubSpotTest {

    public static final String BASE_URL = "/test/hubspot";
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String RESPONSE_WITHOUT_PAGING_CURSOR_JSON = "response-without-paging-cursor.json";
    public static final String RESPONSE_WITH_PAGING_CURSOR_JSON = "response-with-paging-cursor.json";
    private static MockWebServer server;
    private static HttpUrl baseUrl;

    private TestRunner runner;

    @BeforeEach
    void setup() throws IOException, InitializationException {
        server = new MockWebServer();
        server.start();
        baseUrl = server.url(BASE_URL);

        final StandardWebClientServiceProvider standardWebClientServiceProvider = new StandardWebClientServiceProvider();
        final MockGetHubSpot mockGetHubSpot = new MockGetHubSpot();

        runner = TestRunners.newTestRunner(mockGetHubSpot);
        runner.addControllerService("standardWebClientServiceProvider", standardWebClientServiceProvider);
        runner.enableControllerService(standardWebClientServiceProvider);

        runner.setProperty(GetHubSpot.WEB_CLIENT_SERVICE_PROVIDER, standardWebClientServiceProvider.getIdentifier());
        runner.setProperty(GetHubSpot.ACCESS_TOKEN, "testToken");
        runner.setProperty(GetHubSpot.OBJECT_TYPE, HubSpotObjectType.COMPANIES.getValue());
        runner.setProperty(GetHubSpot.RESULT_LIMIT, "1");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (server != null) {
            server.shutdown();
            server = null;
        }
    }

    @Test
    void testLimitIsAddedToUrl() throws InterruptedException, IOException {

        final String response = getResourceAsString(RESPONSE_WITHOUT_PAGING_CURSOR_JSON);
        server.enqueue(new MockResponse().setResponseCode(200).setBody(response));

        runner.run(1);

        RecordedRequest request = server.takeRequest();
        assertEquals(BASE_URL + "?limit=1", request.getPath());
    }

    @Test
    void testPageCursorIsAddedToUrlFromState() throws InterruptedException, IOException {

        final String response = getResourceAsString(RESPONSE_WITHOUT_PAGING_CURSOR_JSON);
        server.enqueue(new MockResponse().setBody(response));

        runner.getStateManager().setState(Collections.singletonMap(HubSpotObjectType.COMPANIES.getValue(), "12345"), Scope.CLUSTER);

        runner.run(1);

        RecordedRequest request = server.takeRequest();
        assertEquals(BASE_URL + "?limit=1&after=12345", request.getPath());
    }

    @Test
    void testFlowFileContainsResultsArray() throws IOException {

        final String response = getResourceAsString(RESPONSE_WITH_PAGING_CURSOR_JSON);
        server.enqueue(new MockResponse().setBody(response));

        runner.run(1);

        final List<MockFlowFile> flowFile = runner.getFlowFilesForRelationship(GetHubSpot.REL_SUCCESS);
        final String expectedFlowFileContent = getResourceAsString("expected_flowfile_content.json");

        final JsonNode expectedJsonNode = OBJECT_MAPPER.readTree(expectedFlowFileContent);
        final JsonNode actualJsonNode = OBJECT_MAPPER.readTree(flowFile.get(0).getContent());

        assertEquals(expectedJsonNode, actualJsonNode);
    }

    @Test
    void testStateIsStoredWhenPagingCursorFound() throws IOException {

        final String response = getResourceAsString(RESPONSE_WITH_PAGING_CURSOR_JSON);
        final String expectedPagingCursor = OBJECT_MAPPER.readTree(response)
                .path("paging")
                .path("next")
                .path("after")
                .asText();

        server.enqueue(new MockResponse().setBody(response));

        runner.run(1);

        final StateMap state = runner.getStateManager().getState(Scope.CLUSTER);
        final String actualPagingCursor = state.get(HubSpotObjectType.COMPANIES.getValue());

        assertEquals(expectedPagingCursor, actualPagingCursor);
    }


    static class MockGetHubSpot extends GetHubSpot {
        @Override
        HttpUriBuilder getBaseUri(ProcessContext context) {
            return new StandardHttpUriBuilder()
                    .scheme(baseUrl.scheme())
                    .host(baseUrl.host())
                    .port(baseUrl.port())
                    .encodedPath(baseUrl.encodedPath());
        }
    }

    private String getResourceAsString(final String resourceName) throws IOException {
        return IOUtils.toString(
                Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream(resourceName), resourceName),
                StandardCharsets.UTF_8
        );
    }

}
