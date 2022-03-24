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

package org.apache.nifi.elasticsearch.integration

import groovy.json.JsonSlurper
import org.apache.maven.artifact.versioning.ComparableVersion
import org.apache.nifi.elasticsearch.DeleteOperationResponse
import org.apache.nifi.elasticsearch.ElasticSearchClientService
import org.apache.nifi.elasticsearch.ElasticSearchClientServiceImpl
import org.apache.nifi.elasticsearch.ElasticsearchException
import org.apache.nifi.elasticsearch.IndexOperationRequest
import org.apache.nifi.elasticsearch.IndexOperationResponse
import org.apache.nifi.elasticsearch.SearchResponse
import org.apache.nifi.elasticsearch.UpdateOperationResponse
import org.apache.nifi.security.util.StandardTlsConfiguration
import org.apache.nifi.security.util.TemporaryKeyStoreBuilder
import org.apache.nifi.security.util.TlsConfiguration
import org.apache.nifi.util.StringUtils
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

import static groovy.json.JsonOutput.prettyPrint
import static groovy.json.JsonOutput.toJson
import static org.junit.jupiter.api.Assumptions.assumeTrue

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertFalse
import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertNull
import static org.junit.jupiter.api.Assertions.assertTrue

class ElasticSearchClientService_IT {
    private TestRunner runner
    private ElasticSearchClientServiceImpl service

    static final String INDEX = "messages"
    static final String TYPE  = StringUtils.isBlank(System.getProperty("type_name")) ? null : System.getProperty("type_name")

    static final ComparableVersion VERSION = new ComparableVersion(System.getProperty("es_version", "0.0.0"))
    static final ComparableVersion ES_7_10 = new ComparableVersion("7.10")
    static final ComparableVersion ES_8_0 = new ComparableVersion("8.0")

    static final String FLAVOUR = System.getProperty("es_flavour")
    static final String DEFAULT = "default"

    private static TlsConfiguration generatedTlsConfiguration
    private static TlsConfiguration truststoreTlsConfiguration

    static boolean isElasticsearchSetup() {
        boolean setup = true
        if (StringUtils.isBlank(System.getProperty("es_version"))) {
            System.err.println("Cannot run Elasticsearch integration-tests: Elasticsearch version (5, 6, 7) not specified")
            setup = false
        }

        if (StringUtils.isBlank(System.getProperty("es_flavour"))) {
            System.err.println("Cannot run Elasticsearch integration-tests: Elasticsearch flavour (oss, default) not specified")
            setup = false
        }

        return setup
    }

    @BeforeAll
    static void beforeAll() throws Exception {
        Assumptions.assumeTrue(isElasticsearchSetup(), "Elasticsearch integration-tests not setup")

        System.out.println(
                String.format("%n%n%n%n%n%n%n%n%n%n%n%n%n%n%nTYPE: %s%nVERSION: %s%nFLAVOUR %s%n%n%n%n%n%n%n%n%n%n%n%n%n%n%n",
                        TYPE, VERSION, FLAVOUR)
        )

        generatedTlsConfiguration = new TemporaryKeyStoreBuilder().build()
        truststoreTlsConfiguration = new StandardTlsConfiguration(
                null,
                null,
                null,
                generatedTlsConfiguration.getTruststorePath(),
                generatedTlsConfiguration.getTruststorePassword(),
                generatedTlsConfiguration.getTruststoreType()
        )
    }

    @BeforeEach
    void before() throws Exception {
        runner = TestRunners.newTestRunner(TestControllerServiceProcessor.class)
        service = new ElasticSearchClientServiceImpl()
        runner.addControllerService("Client Service", service)
        runner.setProperty(service, ElasticSearchClientService.HTTP_HOSTS, "http://localhost:9400")
        runner.setProperty(service, ElasticSearchClientService.CONNECT_TIMEOUT, "10000")
        runner.setProperty(service, ElasticSearchClientService.SOCKET_TIMEOUT, "60000")
        runner.setProperty(service, ElasticSearchClientService.RETRY_TIMEOUT, "60000")
        runner.setProperty(service, ElasticSearchClientService.SUPPRESS_NULLS, ElasticSearchClientService.ALWAYS_SUPPRESS.getValue())

        try {
            runner.enableControllerService(service)
        } catch (Exception ex) {
            ex.printStackTrace()
            throw ex
        }

        service.refresh(null, null);
    }

    @AfterEach
    void after() throws Exception {
        service.onDisabled()
    }

    @Test
    void testBasicSearch() throws Exception {
        String query = prettyPrint(toJson([
                size: 10,
                query: [
                        match_all: [:]
                ],
                aggs: [
                        term_counts: [
                                terms: [
                                        field: "msg",
                                        size: 5
                                ]
                        ]
                ]
        ]))


        SearchResponse response = service.search(query, INDEX, TYPE, null)
        assertNotNull(response, "Response was null")

        assertEquals(15, response.numberOfHits, "Wrong count")
        assertFalse(response.isTimedOut(), "Timed out")
        assertNotNull(response.getHits(), "Hits was null")
        assertEquals(10, response.hits.size(), "Wrong number of hits")
        assertNotNull(response.aggregations, "Aggregations are missing")
        assertEquals(1, response.aggregations.size(), "Aggregation count is wrong")
        assertNull(response.scrollId, "Unexpected ScrollId")
        assertNull(response.searchAfter, "Unexpected Search_After")
        assertNull(response.pitId, "Unexpected pitId")

        Map termCounts = response.aggregations.get("term_counts") as Map
        assertNotNull(termCounts, "Term counts was missing")
        def buckets = termCounts.get("buckets")
        assertNotNull(buckets, "Buckets branch was empty")
        def expected = [
                "one": 1,
                "two": 2,
                "three": 3,
                "four": 4,
                "five": 5
        ]

        buckets.each { aggRes ->
            def key = aggRes["key"]
            def docCount = aggRes["doc_count"]
            assertEquals(expected[key as String], docCount, "${key} did not match.")
        }
    }

    @Test
    void testBasicSearchRequestParameters() throws Exception {
        String query = prettyPrint(toJson([
                size: 10,
                query: [
                        match_all: [:]
                ],
                aggs: [
                        term_counts: [
                                terms: [
                                        field: "msg",
                                        size: 5
                                ]
                        ]
                ]
        ]))


        SearchResponse response = service.search(query, "messages", TYPE, [preference: "_local"])
        assertNotNull(response, "Response was null")

        assertEquals(15, response.numberOfHits, "Wrong count")
        assertFalse(response.isTimedOut(), "Timed out")
        assertNotNull(response.getHits(), "Hits was null")
        assertEquals(10, response.hits.size(), "Wrong number of hits")
        assertNotNull(response.aggregations, "Aggregations are missing")
        assertEquals(1, response.aggregations.size(), "Aggregation count is wrong")

        Map termCounts = response.aggregations.get("term_counts") as Map
        assertNotNull(termCounts, "Term counts was missing")
        def buckets = termCounts.get("buckets")
        assertNotNull(buckets, "Buckets branch was empty")
        def expected = [
                "one": 1,
                "two": 2,
                "three": 3,
                "four": 4,
                "five": 5
        ]

        buckets.each { aggRes ->
            String key = aggRes["key"]
            def docCount = aggRes["doc_count"]
            assertEquals(expected[key], docCount, "${key} did not match.")
        }
    }

    @Test
    void testSearchWarnings() {
        Assume.assumeTrue("Requires version <8.0 (no search API deprecations yet for 8.x)", VERSION < ES_8_0)

        String query
        String type = TYPE
        if (VERSION.toString().startsWith("8.")) {
            // TODO: something that's deprecated when the 8.x branch progresses to include search-API deprecations
        } else if (VERSION.toString().startsWith("7.")) {
            // querying with _type in ES 7.x is deprecated
            query = prettyPrint(toJson([size: 1, query: [match_all: [:]]]))
            type = "a-type"
        } else if (VERSION.toString().startsWith("6.")) {
            // "query_string" query option "all_fields" in ES 6.x is deprecated
            query = prettyPrint(toJson([size: 1, query: [query_string: [query: 1, all_fields: true]]]))
        } else {
            // "mlt" query in ES 5.x is deprecated
            query = prettyPrint(toJson([size: 1, query: [mlt: [fields: ["msg"], like: 1]]]))
        }
        final SearchResponse response = service.search(query, INDEX, type, null)
        assertTrue(!response.warnings.isEmpty(), "Missing warnings")
    }

    @Test
    void testScroll() {
        final String query = prettyPrint(toJson([
                size: 2,
                query: [ match_all: [:] ],
                aggs: [ term_counts: [ terms: [ field: "msg", size: 5 ] ] ]
        ]))

        // initiate the scroll
        final SearchResponse response = service.search(query, INDEX, TYPE, Collections.singletonMap("scroll", "10s"))
        assertNotNull(response, "Response was null")

        assertEquals(15, response.numberOfHits, "Wrong count")
        assertFalse(response.isTimedOut(), "Timed out")
        assertNotNull(response.getHits(), "Hits was null")
        assertEquals(2, response.hits.size(), "Wrong number of hits")
        assertNotNull(response.aggregations, "Aggregations are missing")
        assertEquals(1, response.aggregations.size(), "Aggregation count is wrong")
        assertNotNull(response.scrollId, "ScrollId missing")
        assertNull(response.searchAfter, "Unexpected Search_After")
        assertNull(response.pitId, "Unexpected pitId")

        final Map termCounts = response.aggregations.get("term_counts") as Map
        assertNotNull(termCounts, "Term counts was missing")
        assertEquals(5, (termCounts.get("buckets") as List).size(), "Buckets count is wrong")

        // scroll the next page
        final SearchResponse scrollResponse = service.scroll(prettyPrint((toJson([scroll_id: response.scrollId, scroll: "10s"]))))
        assertNotNull(scrollResponse, "Scroll Response was null")

        assertEquals(15, scrollResponse.numberOfHits, "Wrong count")
        assertFalse(scrollResponse.isTimedOut(), "Timed out")
        assertNotNull(scrollResponse.getHits(), "Hits was null")
        assertEquals(2, scrollResponse.hits.size(), "Wrong number of hits")
        assertNotNull(scrollResponse.aggregations, "Aggregations missing")
        assertEquals(0, scrollResponse.aggregations.size(), "Aggregation count is wrong")
        assertNotNull(scrollResponse.scrollId, "ScrollId missing")
        assertNull(scrollResponse.searchAfter, "Unexpected Search_After")
        assertNull(scrollResponse.pitId, "Unexpected pitId")

        assertNotEquals(scrollResponse.hits, response.hits, "Same results")

        // delete the scroll
        DeleteOperationResponse deleteResponse = service.deleteScroll(scrollResponse.scrollId)
        assertNotNull("Delete Response was null", deleteResponse)
        assertTrue(deleteResponse.took > 0)

        // delete scroll again (should now be unknown but the 404 caught and ignored)
        deleteResponse = service.deleteScroll(scrollResponse.scrollId)
        assertNotNull(deleteResponse, "Delete Response was null")
        assertEquals(0L, deleteResponse.took)
    }

    @Test
    void testSearchAfter() {
        final Map<String, Object> queryMap = [
                size: 2,
                query: [ match_all: [:] ],
                aggs: [ term_counts: [ terms: [ field: "msg", size: 5 ] ] ],
                sort: [[ msg: "desc" ]]
        ]
        final String query = prettyPrint(toJson(queryMap))

        // search first page
        final SearchResponse response = service.search(query, INDEX, TYPE, null)
        assertNotNull(response, "Response was null")

        assertEquals(15, response.numberOfHits, "Wrong count")
        assertFalse(response.isTimedOut(), "Timed out")
        assertNotNull(response.getHits(), "Hits was null")
        assertEquals(2, response.hits.size(), "Wrong number of hits")
        assertNotNull(response.aggregations, "Aggregations missing")
        assertEquals(1, response.aggregations.size(), "Aggregation count is wrong")
        assertNull(response.scrollId, "Unexpected ScrollId")
        assertNotNull(response.searchAfter, "Search_After missing")
        assertNull(response.pitId, "Unexpected pitId")

        final Map termCounts = response.aggregations.get("term_counts") as Map
        assertNotNull(termCounts, "Term counts was missing")
        assertEquals(5, (termCounts.get("buckets") as List).size(), "Buckets count is wrong")

        // search the next page
        queryMap.search_after = new JsonSlurper().parseText(response.searchAfter) as Serializable
        queryMap.remove("aggs")
        final String secondPage = prettyPrint(toJson(queryMap))
        final SearchResponse secondResponse = service.search(secondPage, INDEX, TYPE, null)
        assertNotNull(secondResponse, "Second Response was null")

        assertEquals(15, secondResponse.numberOfHits, "Wrong count")
        assertFalse(secondResponse.isTimedOut(), "Timed out")
        assertNotNull(secondResponse.getHits(), "Hits was null")
        assertEquals(2, secondResponse.hits.size(), "Wrong number of hits")
        assertNotNull(secondResponse.aggregations, "Aggregations missing")
        assertEquals(0, secondResponse.aggregations.size(), "Aggregation count is wrong")
        assertNull(secondResponse.scrollId, "Unexpected ScrollId")
        assertNotNull(secondResponse.searchAfter, "Unexpected Search_After")
        assertNull(secondResponse.pitId, "Unexpected pitId")

        assertNotEquals(secondResponse.hits, response.hits, "Same results")
    }

    @Test
    void testPointInTime() {
        // Point in Time only available in 7.10+ with XPack enabled
        assumeTrue(VERSION >= ES_7_10, "Requires version 7.10+")
        assumeTrue(FLAVOUR == DEFAULT, "Requires XPack features")

        // initialise
        final String pitId = service.initialisePointInTime(INDEX, "10s")

        final Map<String, Object> queryMap = [
                size: 2,
                query: [ match_all: [:] ],
                aggs: [ term_counts: [ terms: [ field: "msg", size: 5 ] ] ],
                sort: [[ msg: "desc" ]],
                pit: [ id: pitId, keep_alive: "10s" ]
        ]
        final String query = prettyPrint(toJson(queryMap))

        // search first page
        final SearchResponse response = service.search(query, null, TYPE, null)
        assertNotNull(response, "Response was null")

        assertEquals(15, response.numberOfHits, "Wrong count")
        assertFalse(response.isTimedOut(), "Timed out")
        assertNotNull(response.getHits(), "Hits was null")
        assertEquals(2, response.hits.size(), "Wrong number of hits")
        assertNotNull(response.aggregations, "Aggregations missing")
        assertEquals(1, response.aggregations.size(), "Aggregation count is wrong")
        assertNull(response.scrollId, "Unexpected ScrollId")
        assertNotNull(response.searchAfter, "Unexpected Search_After")
        assertNotNull(response.pitId, "pitId missing")

        final Map termCounts = response.aggregations.get("term_counts") as Map
        assertNotNull(termCounts, "Term counts was missing")
        assertEquals(5, (termCounts.get("buckets") as List).size(), "Buckets count is wrong")

        // search the next page
        queryMap.search_after = new JsonSlurper().parseText(response.searchAfter) as Serializable
        queryMap.remove("aggs")
        final String secondPage = prettyPrint(toJson(queryMap))
        final SearchResponse secondResponse = service.search(secondPage, null, TYPE, null)
        assertNotNull(secondResponse, "Second Response was null")

        assertEquals(15, secondResponse.numberOfHits, "Wrong count")
        assertFalse(secondResponse.isTimedOut(), "Timed out")
        assertNotNull(secondResponse.getHits(), "Hits was null")
        assertEquals(2, secondResponse.hits.size(), "Wrong number of hits")
        assertNotNull(secondResponse.aggregations, "Aggregations missing")
        assertEquals(0, secondResponse.aggregations.size(), "Aggregation count is wrong")
        assertNull(secondResponse.scrollId, "Unexpected ScrollId")
        assertNotNull(secondResponse.searchAfter, "Unexpected Search_After")
        assertNotNull(secondResponse.pitId, "pitId missing")

        assertNotEquals(secondResponse.hits, response.hits, "Same results")

        // delete pitId
        DeleteOperationResponse deleteResponse = service.deletePointInTime(pitId)
        assertNotNull(deleteResponse, "Delete Response was null")
        assertTrue(deleteResponse.took > 0)

        // delete pitId again (should now be unknown but the 404 caught and ignored)
        deleteResponse = service.deletePointInTime(pitId)
        assertNotNull(deleteResponse, "Delete Response was null")
        assertEquals(0L, deleteResponse.took)
    }

    @Test
    void testDeleteByQuery() throws Exception {
        String query = prettyPrint(toJson([
                query: [
                        match: [
                                msg: "five"
                        ]
                ]
        ]))
        DeleteOperationResponse response = service.deleteByQuery(query, INDEX, TYPE, null)
        assertNotNull(response)
        assertTrue(response.getTook() > 0)
    }

    @Test
    void testDeleteByQueryRequestParameters() throws Exception {
        String query = prettyPrint(toJson([
                query: [
                        match: [
                                msg: "six"
                        ]
                ]
        ]))
        DeleteOperationResponse response = service.deleteByQuery(query, INDEX, TYPE, [refresh: "true"])
        assertNotNull(response)
        assertTrue(response.getTook() > 0)
    }

    @Test
    void testUpdateByQuery() throws Exception {
        String query = prettyPrint(toJson([
                query: [
                        match: [
                                msg: "four"
                        ]
                ]
        ]))
        UpdateOperationResponse response = service.updateByQuery(query, INDEX, TYPE, null)
        assertNotNull(response)
        assertTrue(response.getTook() > 0)
    }

    @Test
    void testUpdateByQueryRequestParameters() throws Exception {
        String query = prettyPrint(toJson([
                query: [
                        match: [
                                msg: "four"
                        ]
                ]
        ]))
        UpdateOperationResponse response = service.updateByQuery(query, INDEX, TYPE, [refresh: "true", slices: "1"])
        assertNotNull(response)
        assertTrue(response.getTook() > 0)
    }

    @Test
    void testDeleteById() throws Exception {
        final String ID = "1"
        final def originalDoc = service.get(INDEX, TYPE, ID, null)
        try {
            DeleteOperationResponse response = service.deleteById(INDEX, TYPE, ID, null)
            assertNotNull(response)
            assertTrue(response.getTook() > 0)
            final ElasticsearchException ee = assertThrows(ElasticsearchException.class, { ->
                service.get(INDEX, TYPE, ID, null) })
            assertTrue(ee.isNotFound())
            final def doc = service.get(INDEX, TYPE, "2", null)
            assertNotNull(doc)
        } finally {
            // replace the deleted doc
            service.add(new IndexOperationRequest(INDEX, TYPE, "1", originalDoc, IndexOperationRequest.Operation.Index), null)
            waitForIndexRefresh() // (affects later tests using _search or _bulk)
        }
    }

    @Test
    void testGet() {
        Map old
        1.upto(15) { index ->
            String id = String.valueOf(index)
            def doc = service.get(INDEX, TYPE, id, null)
            assertNotNull(doc, "Doc was null")
            assertNotNull(doc.get("msg"), "${doc.toString()}\t${doc.keySet().toString()}")
            old = doc
        }
    }

    @Test
    void testGetNotFound() {
        final ElasticsearchException ee = assertThrows(ElasticsearchException.class, { -> service.get(INDEX, TYPE, "not_found", null) })
        assertTrue(ee.isNotFound())
    }

    @Test
    void testNullSuppression() {
        Map<String, Object> doc = new HashMap<String, Object>(){{
            put("msg", "test")
            put("is_null", null)
            put("is_empty", "")
            put("is_blank", " ")
            put("empty_nested", Collections.emptyMap())
            put("empty_array", Collections.emptyList())
        }}

        // index with nulls
        suppressNulls(false)
        IndexOperationResponse response = service.bulk([new IndexOperationRequest("nulls", TYPE, "1", doc, IndexOperationRequest.Operation.Index)], null)
        assertNotNull(response)
        assertTrue(response.getTook() > 0)
        waitForIndexRefresh()

        Map<String, Object> result = service.get("nulls", TYPE, "1", null)
        assertEquals(doc, result)

        // suppress nulls
        suppressNulls(true)
        response = service.bulk([new IndexOperationRequest("nulls", TYPE, "2", doc, IndexOperationRequest.Operation.Index)], null)
        assertNotNull(response)
        assertTrue(response.getTook() > 0)
        waitForIndexRefresh()

        result = service.get("nulls", TYPE, "2", null)
        assertTrue(result.keySet().containsAll(["msg", "is_blank"]), "Non-nulls (present): " + result.toString())
        assertFalse(result.keySet().contains("is_null"), "is_null (should be omitted): " + result.toString())
        assertFalse(result.keySet().contains("is_empty"), "is_empty (should be omitted): " + result.toString())
        assertFalse(result.keySet().contains("empty_nested"), "empty_nested (should be omitted): " + result.toString())
        assertFalse(result.keySet().contains("empty_array"), "empty_array (should be omitted): " + result.toString())
    }

    private void suppressNulls(final boolean suppressNulls) {
        runner.setProperty(TestControllerServiceProcessor.CLIENT_SERVICE, "Client Service")
        runner.disableControllerService(service)
        runner.setProperty(service, ElasticSearchClientService.SUPPRESS_NULLS, suppressNulls ? ElasticSearchClientService.ALWAYS_SUPPRESS.getValue() : ElasticSearchClientService.NEVER_SUPPRESS.getValue())
        runner.enableControllerService(service)
        runner.assertValid()
    }

    @Test
    void testBulkAddTwoIndexes() throws Exception {
        List<IndexOperationRequest> payload = new ArrayList<>()
        for (int x = 0; x < 20; x++) {
            String index = x % 2 == 0 ? "bulk_a": "bulk_b"
            payload.add(new IndexOperationRequest(index, TYPE, String.valueOf(x), new HashMap<String, Object>(){{
                put("msg", "test")
            }}, IndexOperationRequest.Operation.Index))
        }
        for (int x = 0; x < 5; x++) {
            payload.add(new IndexOperationRequest("bulk_c", TYPE, String.valueOf(x), new HashMap<String, Object>(){{
                put("msg", "test")
            }}, IndexOperationRequest.Operation.Index))
        }
        IndexOperationResponse response = service.bulk(payload, [refresh: "true"])
        assertNotNull(response)
        assertTrue(response.getTook() > 0)
        waitForIndexRefresh()

        /*
         * Now, check to ensure that both indexes got populated appropriately.
         */
        String query = "{ \"query\": { \"match_all\": {}}}"
        Long indexA = service.count(query, "bulk_a", TYPE, null)
        Long indexB = service.count(query, "bulk_b", TYPE, null)
        Long indexC = service.count(query, "bulk_c", TYPE, null)

        assertNotNull(indexA)
        assertNotNull(indexB)
        assertNotNull(indexC)
        assertEquals(indexA, indexB)
        assertEquals(10, indexA.intValue())
        assertEquals(10, indexB.intValue())
        assertEquals(5, indexC.intValue())

        Long total = service.count(query, "bulk_*", TYPE, null)
        assertNotNull(total)
        assertEquals(25, total.intValue())
    }

    @Test
    void testBulkRequestParameters() throws Exception {
        List<IndexOperationRequest> payload = new ArrayList<>()
        for (int x = 0; x < 20; x++) {
            String index = x % 2 == 0 ? "bulk_a": "bulk_b"
            payload.add(new IndexOperationRequest(index, TYPE, String.valueOf(x), new HashMap<String, Object>(){{
                put("msg", "test")
            }}, IndexOperationRequest.Operation.Index))
        }
        for (int x = 0; x < 5; x++) {
            payload.add(new IndexOperationRequest("bulk_c", TYPE, String.valueOf(x), new HashMap<String, Object>(){{
                put("msg", "test")
            }}, IndexOperationRequest.Operation.Index))
        }
        IndexOperationResponse response = service.bulk(payload, [refresh: "true"])
        assertNotNull(response)
        assertTrue(response.getTook() > 0)

        /*
         * Now, check to ensure that both indexes got populated and refreshed appropriately.
         */
        String query = "{ \"query\": { \"match_all\": {}}}"
        Long indexA = service.count(query, "bulk_a", TYPE, null)
        Long indexB = service.count(query, "bulk_b", TYPE, null)
        Long indexC = service.count(query, "bulk_c", TYPE, null)

        assertNotNull(indexA)
        assertNotNull(indexB)
        assertNotNull(indexC)
        assertEquals(indexA, indexB)
        assertEquals(10, indexA.intValue())
        assertEquals(10, indexB.intValue())
        assertEquals(5, indexC.intValue())

        Long total = service.count(query, "bulk_*", TYPE, null)
        assertNotNull(total)
        assertEquals(25, total.intValue())
    }

    @Test
    void testUpdateAndUpsert() {
        final String TEST_ID = "update-test"
        Map<String, Object> doc = new HashMap<>()
        doc.put("msg", "Buongiorno, mondo")
        service.add(new IndexOperationRequest(INDEX, TYPE, TEST_ID, doc, IndexOperationRequest.Operation.Index), [refresh: "true"])
        Map<String, Object> result = service.get(INDEX, TYPE, TEST_ID, null)
        assertEquals("Not the same", doc, result)

        Map<String, Object> updates = new HashMap<>()
        updates.put("from", "john.smith")
        Map<String, Object> merged = new HashMap<>()
        merged.putAll(updates)
        merged.putAll(doc)
        IndexOperationRequest request = new IndexOperationRequest(INDEX, TYPE, TEST_ID, updates, IndexOperationRequest.Operation.Update)
        service.add(request, [refresh: "true"])
        result = service.get(INDEX, TYPE, TEST_ID, null)
        assertTrue(result.containsKey("from"))
        assertTrue(result.containsKey("msg"))
        assertEquals(merged, result, "Not the same after update.")

        final String UPSERTED_ID = "upsert-ftw"
        Map<String, Object> upsertItems = new HashMap<>()
        upsertItems.put("upsert_1", "hello")
        upsertItems.put("upsert_2", 1)
        upsertItems.put("upsert_3", true)
        request = new IndexOperationRequest(INDEX, TYPE, UPSERTED_ID, upsertItems, IndexOperationRequest.Operation.Upsert)
        service.add(request, [refresh: "true"])
        result = service.get(INDEX, TYPE, UPSERTED_ID, null)
        assertEquals(upsertItems, result)

        List<IndexOperationRequest> deletes = new ArrayList<>()
        deletes.add(new IndexOperationRequest(INDEX, TYPE, TEST_ID, null, IndexOperationRequest.Operation.Delete))
        deletes.add(new IndexOperationRequest(INDEX, TYPE, UPSERTED_ID, null, IndexOperationRequest.Operation.Delete))
        assertFalse(service.bulk(deletes, [refresh: "true"]).hasErrors())
        waitForIndexRefresh() // wait 1s for index refresh (doesn't prevent GET but affects later tests using _search or _bulk)
        ElasticsearchException ee = assertThrows(ElasticsearchException.class, { -> service.get(INDEX, TYPE, TEST_ID, null) })
        assertTrue(ee.isNotFound())
        ee = assertThrows(ElasticsearchException.class, { -> service.get(INDEX, TYPE, UPSERTED_ID, null) })
        assertTrue(ee.isNotFound())
    }

    @Test
    void testGetBulkResponsesWithErrors() {
        def ops = [
                new IndexOperationRequest(INDEX, TYPE, "1", [ "msg": "one", intField: 1], IndexOperationRequest.Operation.Index), // OK
                new IndexOperationRequest(INDEX, TYPE, "2", [ "msg": "two", intField: 1], IndexOperationRequest.Operation.Create), // already exists
                new IndexOperationRequest(INDEX, TYPE, "1", [ "msg": "one", intField: "notaninteger"], IndexOperationRequest.Operation.Index) // can't parse int field
        ]
        def response = service.bulk(ops, [refresh: "true"])
        assert response.hasErrors()
        assert response.items.findAll {
            def key = it.keySet().stream().findFirst().get()
            it[key].containsKey("error")
        }.size() == 2
    }

    private static void waitForIndexRefresh() {
        Thread.sleep(1000)
    }
}