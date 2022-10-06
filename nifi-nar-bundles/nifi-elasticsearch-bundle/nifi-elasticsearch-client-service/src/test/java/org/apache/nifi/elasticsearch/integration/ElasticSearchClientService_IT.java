/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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

package org.apache.nifi.elasticsearch.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.nifi.elasticsearch.DeleteOperationResponse;
import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.elasticsearch.ElasticSearchClientServiceImpl;
import org.apache.nifi.elasticsearch.ElasticsearchException;
import org.apache.nifi.elasticsearch.IndexOperationRequest;
import org.apache.nifi.elasticsearch.IndexOperationResponse;
import org.apache.nifi.elasticsearch.SearchResponse;
import org.apache.nifi.elasticsearch.TestControllerServiceProcessor;
import org.apache.nifi.elasticsearch.UpdateOperationResponse;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class ElasticSearchClientService_IT extends AbstractElasticsearch_IT {
    private TestRunner runner;
    private ElasticSearchClientServiceImpl service;

    static final String INDEX = "messages";
    public static String TYPE;

    private static TlsConfiguration generatedTlsConfiguration;
    private static TlsConfiguration truststoreTlsConfiguration;

    @BeforeAll
    static void beforeAll() throws IOException {
        startTestcontainer();
        TYPE = getElasticMajorVersion() == 6 ? "_doc" : "";
        System.out.println(
                String.format("%n%n%n%n%n%n%n%n%n%n%n%n%n%n%nTYPE: %s%n%s%s%n%n%n%n%n%n%n%n%n%n%n%n%n%n%n",
                        TYPE, IMAGE.getRepository(), IMAGE.getVersionPart())
        );

        generatedTlsConfiguration = new TemporaryKeyStoreBuilder().build();
        truststoreTlsConfiguration = new StandardTlsConfiguration(
                null,
                null,
                null,
                generatedTlsConfiguration.getTruststorePath(),
                generatedTlsConfiguration.getTruststorePassword(),
                generatedTlsConfiguration.getTruststoreType()
        );

        setupTestData();
    }

    @AfterAll
    public static void afterAll() throws IOException {
        tearDownTestData();
        stopTestcontainer();
    }

    @BeforeEach
    void before() throws Exception {
        runner = TestRunners.newTestRunner(TestControllerServiceProcessor.class);
        service = new ElasticSearchClientServiceImpl();
        runner.addControllerService("Client Service", service);
        runner.setProperty(service, ElasticSearchClientService.HTTP_HOSTS, ELASTIC_HOST);
        runner.setProperty(service, ElasticSearchClientService.CONNECT_TIMEOUT, "10000");
        runner.setProperty(service, ElasticSearchClientService.SOCKET_TIMEOUT, "60000");
        runner.setProperty(service, ElasticSearchClientService.RETRY_TIMEOUT, "60000");
        runner.setProperty(service, ElasticSearchClientService.SUPPRESS_NULLS, ElasticSearchClientService.ALWAYS_SUPPRESS.getValue());
        runner.setProperty(service, ElasticSearchClientService.USERNAME, "elastic");
        runner.setProperty(service, ElasticSearchClientService.PASSWORD, ELASTIC_USER_PASSWORD);

        try {
            runner.enableControllerService(service);
        } catch (Exception ex) {
            throw ex;
        }

        service.refresh(null, null);
    }

    @AfterEach
    void after() throws Exception {
        service.onDisabled();
    }

    private String prettyJson(Object o) throws JsonProcessingException {
        return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(o);
    }

    private class MapBuilder {
        private Map<String, Object> toBuild;

        public MapBuilder() {
            toBuild = new HashMap<>();
        }

        public MapBuilder of(String key, Object value) {
            toBuild.put(key, value);
            return this;
        }

        public MapBuilder of(String key, Object value, String key2, Object value2) {
            toBuild.put(key, value);
            toBuild.put(key2, value2);
            return this;
        }

        public MapBuilder of(String key, Object value, String key2, Object value2, String key3, Object value3) {
            toBuild.put(key, value);
            toBuild.put(key2, value2);
            toBuild.put(key3, value3);
            return this;
        }

        public MapBuilder of(String key, Object value, String key2, Object value2, String key3, Object value3,
                             String key4, Object value4) {
            toBuild.put(key, value);
            toBuild.put(key2, value2);
            toBuild.put(key3, value3);
            toBuild.put(key4, value4);
            return this;
        }

        public MapBuilder of(String key, Object value, String key2, Object value2, String key3, Object value3,
                             String key4, Object value4, String key5, Object value5) {
            toBuild.put(key, value);
            toBuild.put(key2, value2);
            toBuild.put(key3, value3);
            toBuild.put(key4, value4);
            toBuild.put(key5, value5);
            return this;
        }

        public Map<String, Object> build() {
            return toBuild;
        }
    }

    @Test
    void testBasicSearch() throws Exception {
        Map<String, Object> temp = new MapBuilder()
            .of("size", 10, "query", new MapBuilder().of("match_all", new HashMap<>()).build(),
                    "aggs", new MapBuilder()
                            .of("term_counts", new MapBuilder()
                                    .of("terms", new MapBuilder()
                                            .of("field", "msg", "size", 5)
                                            .build())
                                    .build())
                            .build())
                .build();
        String query = prettyJson(temp);

        SearchResponse response = service.search(query, INDEX, TYPE, null);
        assertNotNull(response, "Response was null");

        assertEquals(15, response.getNumberOfHits(), "Wrong count");
        assertFalse(response.isTimedOut(), "Timed out");
        assertNotNull(response.getHits(), "Hits was null");
        assertEquals(10, response.getHits().size(), "Wrong number of hits");
        assertNotNull(response.getAggregations(), "Aggregations are missing");
        assertEquals(1, response.getAggregations().size(), "Aggregation count is wrong");
        assertNull(response.getScrollId(), "Unexpected ScrollId");
        assertNull(response.getSearchAfter(), "Unexpected Search_After");
        assertNull(response.getPitId(), "Unexpected pitId");

        Map termCounts = (Map) response.getAggregations().get("term_counts");
        assertNotNull(termCounts, "Term counts was missing");
        List<Map<String, Object>> buckets = (List<java.util.Map<String, Object>>) termCounts.get("buckets");
        assertNotNull(buckets, "Buckets branch was empty");
        Map<String, Object> expected = new MapBuilder()
                .of("one", 1, "two", 2, "three", 3,
                        "four", 4, "five", 5)
                .build();

        buckets.forEach( aggRes -> {
            String key = (String) aggRes.get("key");
            Integer docCount = (Integer) aggRes.get("doc_count");
            assertEquals(expected.get(key), docCount, "${key} did not match.");
        });
    }

    @Test
    void testBasicSearchRequestParameters() throws Exception {
        Map<String, Object> temp = new MapBuilder()
                .of("size", 10, "query", new MapBuilder().of("match_all", new HashMap<>()).build(),
                        "aggs", new MapBuilder()
                        .of("term_counts", new MapBuilder()
                                .of("terms", new MapBuilder()
                                        .of("field", "msg", "size", 5)
                                        .build())
                                .build())
                        .build())
                .build();
        String query = prettyJson(temp);


        SearchResponse response = service.search(query, "messages", TYPE, createParameters("preference", "_local"));
        assertNotNull(response, "Response was null");

        assertEquals(15, response.getNumberOfHits(), "Wrong count");
        assertFalse(response.isTimedOut(), "Timed out");
        assertNotNull(response.getHits(), "Hits was null");
        assertEquals(10, response.getHits().size(), "Wrong number of hits");
        assertNotNull(response.getAggregations(), "Aggregations are missing");
        assertEquals(1, response.getAggregations().size(), "Aggregation count is wrong");

        Map termCounts = (Map) response.getAggregations().get("term_counts");
        assertNotNull(termCounts, "Term counts was missing");
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) termCounts.get("buckets");
        assertNotNull(buckets, "Buckets branch was empty");
        Map<String, Object> expected = new MapBuilder()
                .of("one", 1, "two", 2, "three", 3,
                        "four", 4, "five", 5)
                .build();

        buckets.forEach( (aggRes) -> {
            String key = (String) aggRes.get("key");
            Integer docCount = (Integer) aggRes.get("doc_count");
            assertEquals(expected.get(key), docCount, String.format("%s did not match.", key));
        });
    }

    @Test
    void testV5SearchWarnings() throws JsonProcessingException {
        int version = getElasticMajorVersion();
        assumeTrue(version == 5, "Requires version 5 (no search API deprecations yet for 8.x)");

        String query = prettyJson(new MapBuilder()
                .of("size", 1, "query", new MapBuilder().of("query_string", new MapBuilder()
                        .of("query", 1, "all_fields", true).build()).build())
                .build());
        final SearchResponse response = service.search(query, INDEX, TYPE, null);
        assertTrue(!response.getWarnings().isEmpty(), "Missing warnings");
    }

    @Test
    public void testV7SearchWarnings() throws JsonProcessingException {
        assumeTrue(getElasticMajorVersion() == 7,
                "Requires Elasticsearch 7");
        String query = prettyJson(new MapBuilder()
                .of("size", 1, "query", new MapBuilder().of("match_all", new HashMap<>()).build())
                .build());
        String type = "a-type";
        final SearchResponse response = service.search(query, INDEX, type, null);
        assertTrue(!response.getWarnings().isEmpty(), "Missing warnings");
    }

    @Test
    void testScroll() throws JsonProcessingException {
        final String query = prettyJson(new MapBuilder()
                .of("size", 2, "query", new MapBuilder().of("match_all", new HashMap<>()).build(),
                        "aggs", new MapBuilder()
                        .of("term_counts", new MapBuilder()
                                .of("terms", new MapBuilder()
                                        .of("field", "msg", "size", 5)
                                        .build())
                                .build())
                        .build())
                .build());

        // initiate the scroll
        final SearchResponse response = service.search(query, INDEX, TYPE, Collections.singletonMap("scroll", "10s"));
        assertNotNull(response, "Response was null");

        assertEquals(15, response.getNumberOfHits(), "Wrong count");
        assertFalse(response.isTimedOut(), "Timed out");
        assertNotNull(response.getHits(), "Hits was null");
        assertEquals(2, response.getHits().size(), "Wrong number of hits");
        assertNotNull(response.getAggregations(), "Aggregations are missing");
        assertEquals(1, response.getAggregations().size(), "Aggregation count is wrong");
        assertNotNull(response.getScrollId(), "ScrollId missing");
        assertNull(response.getSearchAfter(), "Unexpected Search_After");
        assertNull(response.getPitId(), "Unexpected pitId");

        final Map termCounts = (Map) response.getAggregations().get("term_counts");
        assertNotNull(termCounts, "Term counts was missing");
        assertEquals(5, ((List)termCounts.get("buckets")).size(), "Buckets count is wrong");

        // scroll the next page
        Map<String, String> parameters = createParameters("scroll_id", response.getScrollId(), "scroll", "10s");
        final SearchResponse scrollResponse = service.scroll(prettyJson(parameters));
        assertNotNull(scrollResponse, "Scroll Response was null");

        assertEquals(15, scrollResponse.getNumberOfHits(), "Wrong count");
        assertFalse(scrollResponse.isTimedOut(), "Timed out");
        assertNotNull(scrollResponse.getHits(), "Hits was null");
        assertEquals(2, scrollResponse.getHits().size(), "Wrong number of hits");
        assertNotNull(scrollResponse.getAggregations(), "Aggregations missing");
        assertEquals(0, scrollResponse.getAggregations().size(), "Aggregation count is wrong");
        assertNotNull(scrollResponse.getScrollId(), "ScrollId missing");
        assertNull(scrollResponse.getSearchAfter(), "Unexpected Search_After");
        assertNull(scrollResponse.getPitId(), "Unexpected pitId");

        assertNotEquals(scrollResponse.getHits(), response.getHits(), () -> "Same results");

        // delete the scroll
        DeleteOperationResponse deleteResponse = service.deleteScroll(scrollResponse.getScrollId());
        assertNotNull(deleteResponse, "Delete Response was null");
        assertTrue(deleteResponse.getTook() > 0);

        // delete scroll again (should now be unknown but the 404 caught and ignored)
        deleteResponse = service.deleteScroll(scrollResponse.getScrollId());
        assertNotNull(deleteResponse, "Delete Response was null");
        assertEquals(0L, deleteResponse.getTook());
    }

    @Test
    void testSearchAfter() throws JsonProcessingException {
        final Map<String, Object> queryMap = new MapBuilder()
                .of("size", 2, "query", new MapBuilder()
                        .of("match_all", new HashMap<>()).build(), "aggs", new MapBuilder()
                        .of("term_counts", new MapBuilder()
                            .of("terms", new MapBuilder()
                                    .of("field", "msg", "size", 5)
                                    .build())
                            .build()).build())
                .of("sort", Arrays.asList(
                    new MapBuilder().of("msg", "desc").build()
                ))
                .build();
        final String query = prettyJson(queryMap);

        // search first page
        final SearchResponse response = service.search(query, INDEX, TYPE, null);
        assertNotNull(response, "Response was null");

        assertEquals(15, response.getNumberOfHits(), "Wrong count");
        assertFalse(response.isTimedOut(), "Timed out");
        assertNotNull(response.getHits(), "Hits was null");
        assertEquals(2, response.getHits().size(), "Wrong number of hits");
        assertNotNull(response.getAggregations(), "Aggregations missing");
        assertEquals(1, response.getAggregations().size(), "Aggregation count is wrong");
        assertNull(response.getScrollId(), "Unexpected ScrollId");
        assertNotNull(response.getSearchAfter(), "Search_After missing");
        assertNull(response.getPitId(), "Unexpected pitId");

        final Map termCounts = (Map)response.getAggregations().get("term_counts");
        assertNotNull(termCounts, "Term counts was missing");
        assertEquals(5, ((List)termCounts.get("buckets")).size(), "Buckets count is wrong");

        // search the next page
        queryMap.put("search_after", MAPPER.readValue(response.getSearchAfter(), List.class));
        queryMap.remove("aggs");
        final String secondPage = prettyJson(queryMap);
        final SearchResponse secondResponse = service.search(secondPage, INDEX, TYPE, null);
        assertNotNull(secondResponse, "Second Response was null");

        assertEquals(15, secondResponse.getNumberOfHits(), "Wrong count");
        assertFalse(secondResponse.isTimedOut(), "Timed out");
        assertNotNull(secondResponse.getHits(), "Hits was null");
        assertEquals(2, secondResponse.getHits().size(), "Wrong number of hits");
        assertNotNull(secondResponse.getAggregations(), "Aggregations missing");
        assertEquals(0, secondResponse.getAggregations().size(), "Aggregation count is wrong");
        assertNull(secondResponse.getScrollId(), "Unexpected ScrollId");
        assertNotNull(secondResponse.getSearchAfter(), "Unexpected Search_After");
        assertNull(secondResponse.getPitId(), "Unexpected pitId");

        assertNotEquals(secondResponse.getHits(), response.getHits(), "Same results");
    }

    @Test
    void testPointInTime() throws JsonProcessingException {
        // Point in Time only available in 7.10+ with XPack enabled
        double majorVersion = getElasticMajorVersion();
        double minorVersion = getElasticMinorVersion();
        assumeTrue(majorVersion >= 8 || (majorVersion == 7 && minorVersion >= 10), "Requires version 7.10+");

        // initialise
        final String pitId = service.initialisePointInTime(INDEX, "10s");

        final Map<String, Object> queryMap = new MapBuilder()
                .of("size", 2, "query", new MapBuilder().of("match_all", new HashMap<>()).build())
                .of("aggs", new MapBuilder().of("term_counts", new MapBuilder()
                        .of("terms", new MapBuilder()
                                .of("field", "msg", "size", 5)
                                .build())
                        .build()).build())
                .of("sort", Arrays.asList(
                        new MapBuilder().of("msg", "desc").build()
                ))
                .of("pit", new MapBuilder()
                        .of("id", pitId, "keep_alive", "10s")
                        .build())
                .build();
        final String query = prettyJson(queryMap);

        // search first page
        final SearchResponse response = service.search(query, null, TYPE, null);
        assertNotNull(response, "Response was null");

        assertEquals(15, response.getNumberOfHits(), "Wrong count");
        assertFalse(response.isTimedOut(), "Timed out");
        assertNotNull(response.getHits(), "Hits was null");
        assertEquals(2, response.getHits().size(), "Wrong number of hits");
        assertNotNull(response.getAggregations(), "Aggregations missing");
        assertEquals(1, response.getAggregations().size(), "Aggregation count is wrong");
        assertNull(response.getScrollId(), "Unexpected ScrollId");
        assertNotNull(response.getSearchAfter(), "Unexpected Search_After");
        assertNotNull(response.getPitId(), "pitId missing");

        final Map termCounts = (Map) response.getAggregations().get("term_counts");
        assertNotNull(termCounts, "Term counts was missing");
        assertEquals(5, ((List)termCounts.get("buckets")).size(), "Buckets count is wrong");

        // search the next page
        queryMap.put("search_after", MAPPER.readValue(response.getSearchAfter(), List.class));
        queryMap.remove("aggs");
        final String secondPage = prettyJson(queryMap);
        final SearchResponse secondResponse = service.search(secondPage, null, TYPE, null);
        assertNotNull(secondResponse, "Second Response was null");

        assertEquals(15, secondResponse.getNumberOfHits(), "Wrong count");
        assertFalse(secondResponse.isTimedOut(), "Timed out");
        assertNotNull(secondResponse.getHits(), "Hits was null");
        assertEquals(2, secondResponse.getHits().size(), "Wrong number of hits");
        assertNotNull(secondResponse.getAggregations(), "Aggregations missing");
        assertEquals(0, secondResponse.getAggregations().size(), "Aggregation count is wrong");
        assertNull(secondResponse.getScrollId(), "Unexpected ScrollId");
        assertNotNull(secondResponse.getSearchAfter(), "Unexpected Search_After");
        assertNotNull(secondResponse.getPitId(), "pitId missing");

        assertNotEquals(secondResponse.getHits(), response.getHits(), "Same results");

        // delete pitId
        DeleteOperationResponse deleteResponse = service.deletePointInTime(pitId);
        assertNotNull(deleteResponse, "Delete Response was null");
        assertTrue(deleteResponse.getTook() > 0);

        // delete pitId again (should now be unknown but the 404 caught and ignored)
        deleteResponse = service.deletePointInTime(pitId);
        assertNotNull(deleteResponse, "Delete Response was null");
        assertEquals(0L, deleteResponse.getTook());
    }

    @Test
    void testDeleteByQuery() throws Exception {
        String query = prettyJson(new MapBuilder()
                .of("query", new MapBuilder()
                        .of("match", new MapBuilder().of("msg", "five").build())
                                .build()).build());
        DeleteOperationResponse response = service.deleteByQuery(query, INDEX, TYPE, null);
        assertNotNull(response);
        assertTrue(response.getTook() > 0);
    }

    @Test
    void testDeleteByQueryRequestParameters() throws Exception {
        String query = prettyJson(new MapBuilder()
                .of("query", new MapBuilder()
                        .of("match", new MapBuilder().of("msg", "six").build())
                        .build()).build());
        Map<String, String> parameters = new HashMap<>();
        parameters.put("refresh", "true");
        DeleteOperationResponse response = service.deleteByQuery(query, INDEX, TYPE, parameters);
        assertNotNull(response);
        assertTrue(response.getTook() > 0);
    }

    @Test
    void testUpdateByQuery() throws Exception {
        String query = prettyJson(new MapBuilder()
                .of("query", new MapBuilder()
                        .of("match", new MapBuilder().of("msg", "four").build())
                        .build()).build());
        UpdateOperationResponse response = service.updateByQuery(query, INDEX, TYPE, null);
        assertNotNull(response);
        assertTrue(response.getTook() > 0);
    }

    @Test
    void testUpdateByQueryRequestParameters() throws Exception {
        String query = prettyJson(new MapBuilder()
                .of("query", new MapBuilder()
                        .of("match", new MapBuilder().of("msg", "four").build())
                        .build()).build());
        Map<String, String> parameters = new HashMap<>();
        parameters.put("refresh", "true");
        parameters.put("slices", "1");
        UpdateOperationResponse response = service.updateByQuery(query, INDEX, TYPE, parameters);
        assertNotNull(response);
        assertTrue(response.getTook() > 0);
    }

    @Test
    void testDeleteById() throws Exception {
        final String ID = "1";
        final Map<String, Object> originalDoc = service.get(INDEX, TYPE, ID, null);
        try {
            DeleteOperationResponse response = service.deleteById(INDEX, TYPE, ID, null);
            assertNotNull(response);
            assertTrue(response.getTook() > 0);
            final ElasticsearchException ee = assertThrows(ElasticsearchException.class, () ->
                service.get(INDEX, TYPE, ID, null));
            assertTrue(ee.isNotFound());
            final Map<String, Object> doc = service.get(INDEX, TYPE, "2", null);
            assertNotNull(doc);
        } finally {
            // replace the deleted doc
            service.add(new IndexOperationRequest(INDEX, TYPE, "1", originalDoc, IndexOperationRequest.Operation.Index), null);
            waitForIndexRefresh(); // (affects later tests using _search or _bulk)
        }
    }

    @Test
    void testGet() {
        for (int index = 1; index <= 15; index++) {
            String id = String.valueOf(index);
            Map<String, Object> doc = service.get(INDEX, TYPE, id, null);
            assertNotNull(doc, "Doc was null");
            assertNotNull(doc.get("msg"), "${doc.toString()}\t${doc.keySet().toString()}");
        }
    }

    @Test
    void testGetNotFound() {
        final ElasticsearchException ee = assertThrows(ElasticsearchException.class, () -> service.get(INDEX, TYPE, "not_found", null));
        assertTrue(ee.isNotFound());
    }

    @Test
    void testNullSuppression() throws InterruptedException {
        Map<String, Object> doc = new HashMap<>();
        doc.put("msg", "test");
        doc.put("is_null", null);
        doc.put("is_empty", "");
        doc.put("is_blank", " ");
        doc.put("empty_nested", Collections.emptyMap());
        doc.put("empty_array", Collections.emptyList());

        // index with nulls
        suppressNulls(false);
        IndexOperationResponse response = service.bulk(Arrays.asList(new IndexOperationRequest("nulls", TYPE, "1", doc, IndexOperationRequest.Operation.Index)), null);
        assertNotNull(response);
        assertTrue(response.getTook() > 0);
        waitForIndexRefresh();

        Map<String, Object> result = service.get("nulls", TYPE, "1", null);
        assertEquals(doc, result);

        // suppress nulls
        suppressNulls(true);
        response = service.bulk(Arrays.asList(new IndexOperationRequest("nulls", TYPE, "2", doc, IndexOperationRequest.Operation.Index)), null);
        assertNotNull(response);
        assertTrue(response.getTook() > 0);
        waitForIndexRefresh();

        result = service.get("nulls", TYPE, "2", null);
        assertTrue(result.keySet().containsAll(Arrays.asList("msg", "is_blank")), "Non-nulls (present): " + result);
        assertFalse(result.keySet().contains("is_null"), "is_null (should be omitted): " + result);
        assertFalse(result.keySet().contains("is_empty"), "is_empty (should be omitted): " + result);
        assertFalse(result.keySet().contains("empty_nested"), "empty_nested (should be omitted): " + result);
        assertFalse(result.keySet().contains("empty_array"), "empty_array (should be omitted): " + result);
    }

    private void suppressNulls(final boolean suppressNulls) {
        runner.setProperty(TestControllerServiceProcessor.CLIENT_SERVICE, "Client Service");
        runner.disableControllerService(service);
        runner.setProperty(service, ElasticSearchClientService.SUPPRESS_NULLS, suppressNulls
                ? ElasticSearchClientService.ALWAYS_SUPPRESS.getValue()
                : ElasticSearchClientService.NEVER_SUPPRESS.getValue());
        runner.enableControllerService(service);
        runner.assertValid();
    }

    @Test
    void testBulkAddTwoIndexes() throws Exception {
        List<IndexOperationRequest> payload = new ArrayList<>();
        for (int x = 0; x < 20; x++) {
            String index = x % 2 == 0 ? "bulk_a": "bulk_b";
            payload.add(new IndexOperationRequest(index, TYPE, String.valueOf(x), new HashMap<String, Object>(){{
                put("msg", "test");
            }}, IndexOperationRequest.Operation.Index));
        }
        for (int x = 0; x < 5; x++) {
            payload.add(new IndexOperationRequest("bulk_c", TYPE, String.valueOf(x), new HashMap<String, Object>(){{
                put("msg", "test");
            }}, IndexOperationRequest.Operation.Index));
        }
        IndexOperationResponse response = service.bulk(payload, createParameters("refresh", "true"));
        assertNotNull(response);
        assertTrue(response.getTook() > 0);
        waitForIndexRefresh();

        /*
         * Now, check to ensure that both indexes got populated appropriately.
         */
        String query = "{ \"query\": { \"match_all\": {}}}";
        Long indexA = service.count(query, "bulk_a", TYPE, null);
        Long indexB = service.count(query, "bulk_b", TYPE, null);
        Long indexC = service.count(query, "bulk_c", TYPE, null);

        assertNotNull(indexA);
        assertNotNull(indexB);
        assertNotNull(indexC);
        assertEquals(indexA, indexB);
        assertEquals(10, indexA.intValue());
        assertEquals(10, indexB.intValue());
        assertEquals(5, indexC.intValue());

        Long total = service.count(query, "bulk_*", TYPE, null);
        assertNotNull(total);
        assertEquals(25, total.intValue());
    }

    @Test
    void testBulkRequestParameters() throws Exception {
        List<IndexOperationRequest> payload = new ArrayList<>();
        for (int x = 0; x < 20; x++) {
            String index = x % 2 == 0 ? "bulk_a": "bulk_b";
            payload.add(new IndexOperationRequest(index, TYPE, String.valueOf(x), new MapBuilder().of("msg", "test").build(), IndexOperationRequest.Operation.Index));
        }
        for (int x = 0; x < 5; x++) {
            payload.add(new IndexOperationRequest("bulk_c", TYPE, String.valueOf(x), new MapBuilder().of("msg", "test").build(), IndexOperationRequest.Operation.Index));
        }
        IndexOperationResponse response = service.bulk(payload, createParameters("refresh", "true"));
        assertNotNull(response);
        assertTrue(response.getTook() > 0);

        /*
         * Now, check to ensure that both indexes got populated and refreshed appropriately.
         */
        String query = "{ \"query\": { \"match_all\": {}}}";
        Long indexA = service.count(query, "bulk_a", TYPE, null);
        Long indexB = service.count(query, "bulk_b", TYPE, null);
        Long indexC = service.count(query, "bulk_c", TYPE, null);

        assertNotNull(indexA);
        assertNotNull(indexB);
        assertNotNull(indexC);
        assertEquals(indexA, indexB);
        assertEquals(10, indexA.intValue());
        assertEquals(10, indexB.intValue());
        assertEquals(5, indexC.intValue());

        Long total = service.count(query, "bulk_*", TYPE, null);
        assertNotNull(total);
        assertEquals(25, total.intValue());
    }

    @Test
    void testUpdateAndUpsert() throws InterruptedException {
        final String TEST_ID = "update-test";
        Map<String, Object> doc = new HashMap<>();
        doc.put("msg", "Buongiorno, mondo");
        service.add(new IndexOperationRequest(INDEX, TYPE, TEST_ID, doc, IndexOperationRequest.Operation.Index), createParameters("refresh", "true"));
        Map<String, Object> result = service.get(INDEX, TYPE, TEST_ID, null);
        assertEquals(doc, result, "Not the same");

        Map<String, Object> updates = new HashMap<>();
        updates.put("from", "john.smith");
        Map<String, Object> merged = new HashMap<>();
        merged.putAll(updates);
        merged.putAll(doc);
        IndexOperationRequest request = new IndexOperationRequest(INDEX, TYPE, TEST_ID, updates, IndexOperationRequest.Operation.Update);
        service.add(request, createParameters("refresh", "true"));
        result = service.get(INDEX, TYPE, TEST_ID, null);
        assertTrue(result.containsKey("from"));
        assertTrue(result.containsKey("msg"));
        assertEquals(merged, result, "Not the same after update.");

        final String UPSERTED_ID = "upsert-ftw";
        Map<String, Object> upsertItems = new HashMap<>();
        upsertItems.put("upsert_1", "hello");
        upsertItems.put("upsert_2", 1);
        upsertItems.put("upsert_3", true);
        request = new IndexOperationRequest(INDEX, TYPE, UPSERTED_ID, upsertItems, IndexOperationRequest.Operation.Upsert);
        service.add(request, createParameters("refresh", "true"));
        result = service.get(INDEX, TYPE, UPSERTED_ID, null);
        assertEquals(upsertItems, result);

        List<IndexOperationRequest> deletes = new ArrayList<>();
        deletes.add(new IndexOperationRequest(INDEX, TYPE, TEST_ID, null, IndexOperationRequest.Operation.Delete));
        deletes.add(new IndexOperationRequest(INDEX, TYPE, UPSERTED_ID, null, IndexOperationRequest.Operation.Delete));
        assertFalse(service.bulk(deletes, createParameters("refresh", "true")).hasErrors());
        waitForIndexRefresh(); // wait 1s for index refresh (doesn't prevent GET but affects later tests using _search or _bulk)
        ElasticsearchException ee = assertThrows(ElasticsearchException.class, () -> service.get(INDEX, TYPE, TEST_ID, null) );
        assertTrue(ee.isNotFound());
        ee = assertThrows(ElasticsearchException.class, () -> service.get(INDEX, TYPE, UPSERTED_ID, null));
        assertTrue(ee.isNotFound());
    }

    @Test
    void testGetBulkResponsesWithErrors() {
        List ops = Arrays.asList(
                new IndexOperationRequest(INDEX, TYPE, "1", new MapBuilder().of("msg", "one", "intField", 1).build(), IndexOperationRequest.Operation.Index), // OK
                new IndexOperationRequest(INDEX, TYPE, "2", new MapBuilder().of("msg", "two", "intField", 1).build(), IndexOperationRequest.Operation.Create), // already exists
                new IndexOperationRequest(INDEX, TYPE, "1", new MapBuilder().of("msg", "one", "intField", "notaninteger").build(), IndexOperationRequest.Operation.Index) // can't parse int field
        );
        IndexOperationResponse response = service.bulk(ops, createParameters("refresh", "true"));
        assertTrue(response.hasErrors());
        assertEquals(2, response.getItems().stream().filter(it -> {
            String key = it.keySet().stream().findFirst().get();
            return ((Map<String, Object>)it.get(key)).containsKey("error");
        }).count());
    }

    private Map<String, String> createParameters(String... extra) {
        if (extra.length % 2 == 1) { //Putting this here to help maintainers catch stupid bugs before they happen
            throw new RuntimeException("createParameters must have an even number of String parameters.");
        }

        Map<String, String> parameters = new HashMap<>();
        for (int index = 0; index < extra.length; index += 2) {
            parameters.put(extra[index], extra[index + 1]);
        }

        return parameters;
    }

    private static void waitForIndexRefresh() throws InterruptedException {
        Thread.sleep(1000);
    }
}