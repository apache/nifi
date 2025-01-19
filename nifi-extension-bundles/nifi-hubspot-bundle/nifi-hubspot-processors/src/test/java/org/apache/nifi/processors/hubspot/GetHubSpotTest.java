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

import static org.apache.nifi.processors.hubspot.GetHubSpot.CURSOR_KEY;
import static org.apache.nifi.processors.hubspot.GetHubSpot.END_INCREMENTAL_KEY;
import static org.apache.nifi.processors.hubspot.GetHubSpot.START_INCREMENTAL_KEY;
import static org.apache.nifi.processors.hubspot.HubSpotObjectType.COMPANIES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.client.StandardHttpUriBuilder;
import org.apache.nifi.web.client.provider.service.StandardWebClientServiceProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GetHubSpotTest {

    private static final long TEST_EPOCH_TIME = 1662665787;
    private static final String BASE_URL = "/test/hubspot";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static MockWebServer server;
    private static HttpUrl baseUrl;
    private TestRunner runner;

    @BeforeEach
    void setup() throws IOException, InitializationException {
        server = new MockWebServer();
        server.start();
        baseUrl = server.url(BASE_URL);

        final StandardWebClientServiceProvider standardWebClientServiceProvider =
                new StandardWebClientServiceProvider();
        final MockGetHubSpot mockGetHubSpot = new MockGetHubSpot(TEST_EPOCH_TIME);

        runner = TestRunners.newTestRunner(mockGetHubSpot);
        runner.addControllerService("standardWebClientServiceProvider", standardWebClientServiceProvider);
        runner.enableControllerService(standardWebClientServiceProvider);

        runner.setProperty(GetHubSpot.WEB_CLIENT_SERVICE_PROVIDER, standardWebClientServiceProvider.getIdentifier());
        runner.setProperty(GetHubSpot.ACCESS_TOKEN, "testToken");
        runner.setProperty(GetHubSpot.OBJECT_TYPE, COMPANIES.getValue());
    }

    @AfterEach
    void tearDown() throws IOException {
        if (server != null) {
            server.shutdown();
            server = null;
        }
    }

    @Test
    void testFlowFileContainsResultsArray() throws IOException {

        final String response = getResourceAsString("simple_response.json");
        server.enqueue(new MockResponse().setBody(response));

        runner.run(1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetHubSpot.REL_SUCCESS);
        final MockFlowFile flowFile = flowFiles.getFirst();

        final String expectedFlowFileContent = getResourceAsString("expected_flowfile_content.json");

        final JsonNode expectedJsonNode = OBJECT_MAPPER.readTree(expectedFlowFileContent);
        final JsonNode actualJsonNode = OBJECT_MAPPER.readTree(flowFile.getContent());

        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        assertEquals(expectedJsonNode, actualJsonNode);
        List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(baseUrl.toString(), provenanceEvents.getFirst().getTransitUri());
    }

    @Test
    void testFlowFileNotCreatedWhenZeroResult() throws IOException {

        final String response = getResourceAsString("zero_result.json");
        server.enqueue(new MockResponse().setBody(response));

        runner.run(1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetHubSpot.REL_SUCCESS);

        assertTrue(flowFiles.isEmpty());
        assertTrue(runner.getProvenanceEvents().isEmpty());
    }

    @Test
    void testExceptionIsThrownWhenTooManyRequest() throws IOException {

        final String response = getResourceAsString("zero_result.json");
        server.enqueue(new MockResponse().setBody(response).setResponseCode(429));

        assertThrows(AssertionError.class, () -> runner.run(1));
        assertTrue(runner.getProvenanceEvents().isEmpty());
    }

    @Test
    void testSimpleIncrementalLoadingFilter() throws IOException, InterruptedException {
        final String response = getResourceAsString("simple_response.json");
        server.enqueue(new MockResponse().setBody(response));

        final String limit = "2";
        final int defaultDelay = 30000;
        final String endTime = String.valueOf(Instant.now().toEpochMilli());
        final Map<String, String> stateMap = new HashMap<>();
        stateMap.put(END_INCREMENTAL_KEY, endTime);

        runner.getStateManager().setState(stateMap, Scope.CLUSTER);
        runner.setProperty(GetHubSpot.IS_INCREMENTAL, "true");
        runner.setProperty(GetHubSpot.RESULT_LIMIT, limit);

        runner.run(1);

        final String requestBodyString = new String(server.takeRequest().getBody().readByteArray());

        final ObjectNode startTimeNode = OBJECT_MAPPER.createObjectNode();
        startTimeNode.put("propertyName", "hs_lastmodifieddate");
        startTimeNode.put("operator", "GTE");
        startTimeNode.put("value", endTime);

        final ObjectNode endTimeNode = OBJECT_MAPPER.createObjectNode();
        endTimeNode.put("propertyName", "hs_lastmodifieddate");
        endTimeNode.put("operator", "LT");
        endTimeNode.put("value", String.valueOf(TEST_EPOCH_TIME - defaultDelay));

        final ArrayNode filtersNode = OBJECT_MAPPER.createArrayNode();
        filtersNode.add(startTimeNode);
        filtersNode.add(endTimeNode);

        final ObjectNode root = OBJECT_MAPPER.createObjectNode();
        root.put("limit", limit);
        root.set("filters", filtersNode);

        final String expectedJsonString = root.toString();

        assertEquals(OBJECT_MAPPER.readTree(expectedJsonString), OBJECT_MAPPER.readTree(requestBodyString));
        List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(baseUrl.toString(), provenanceEvents.getFirst().getTransitUri());
    }

    @Test
    void testIncrementalLoadingFilterWithPagingCursor() throws IOException, InterruptedException {
        final String response = getResourceAsString("simple_response.json");
        server.enqueue(new MockResponse().setBody(response));

        final String limit = "2";
        final String after = "nextPage";
        final Instant now = Instant.now();
        final String startTime = String.valueOf(now.toEpochMilli());
        final String endTime = String.valueOf(now.plus(2, ChronoUnit.MINUTES).toEpochMilli());
        final Map<String, String> stateMap = new HashMap<>();
        stateMap.put(CURSOR_KEY, after);
        stateMap.put(START_INCREMENTAL_KEY, startTime);
        stateMap.put(END_INCREMENTAL_KEY, endTime);

        runner.getStateManager().setState(stateMap, Scope.CLUSTER);
        runner.setProperty(GetHubSpot.IS_INCREMENTAL, "true");
        runner.setProperty(GetHubSpot.RESULT_LIMIT, limit);

        runner.run(1);

        final String requestBodyString = new String(server.takeRequest().getBody().readByteArray());

        final ObjectNode startTimeNode = OBJECT_MAPPER.createObjectNode();
        startTimeNode.put("propertyName", "hs_lastmodifieddate");
        startTimeNode.put("operator", "GTE");
        startTimeNode.put("value", startTime);

        final ObjectNode endTimeNode = OBJECT_MAPPER.createObjectNode();
        endTimeNode.put("propertyName", "hs_lastmodifieddate");
        endTimeNode.put("operator", "LT");
        endTimeNode.put("value", endTime);

        final ArrayNode filtersNode = OBJECT_MAPPER.createArrayNode();
        filtersNode.add(startTimeNode);
        filtersNode.add(endTimeNode);

        final ObjectNode root = OBJECT_MAPPER.createObjectNode();
        root.put("limit", limit);
        root.put("after", after);
        root.set("filters", filtersNode);

        final String expectedJsonString = root.toString();

        assertEquals(OBJECT_MAPPER.readTree(expectedJsonString), OBJECT_MAPPER.readTree(requestBodyString));
        List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(baseUrl.toString(), provenanceEvents.getFirst().getTransitUri());
    }

    static class MockGetHubSpot extends GetHubSpot {

        private final long currentEpochTime;

        public MockGetHubSpot(long currentEpochTime) {
            this.currentEpochTime = currentEpochTime;
        }

        @Override
        URI getBaseUri(ProcessContext context) {
            return new StandardHttpUriBuilder()
                    .scheme(baseUrl.scheme())
                    .host(baseUrl.host())
                    .port(baseUrl.port())
                    .encodedPath(baseUrl.encodedPath())
                    .build();
        }

        @Override
        long getCurrentEpochTime() {
            return currentEpochTime;
        }

        @Override
        public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue,
                final String newValue) {
        }
    }

    private String getResourceAsString(final String resourceName) throws IOException {
        return IOUtils.toString(
                Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream(resourceName),
                        resourceName),
                StandardCharsets.UTF_8
        );
    }

}
