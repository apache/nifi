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
import org.apache.nifi.ssl.SSLContextService
import org.apache.nifi.util.StringUtils
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.apache.nifi.web.util.ssl.SslContextUtils
import org.junit.After
import org.junit.Assert
import org.junit.Assume
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test

import javax.net.ssl.SSLContext

import static groovy.json.JsonOutput.prettyPrint
import static groovy.json.JsonOutput.toJson
import static org.hamcrest.CoreMatchers.is
import static org.mockito.Mockito.mock
import static org.mockito.Mockito.when

class ElasticSearchClientService_IT {
    private TestRunner runner
    private ElasticSearchClientServiceImpl service

    static final String INDEX = "messages"
    static final String TYPE  = StringUtils.isBlank(System.getProperty("type_name")) ? null : System.getProperty("type_name")

    static final ComparableVersion VERSION = new ComparableVersion(System.getProperty("es_version", "0.0.0"))
    static final ComparableVersion ES_7_10 = new ComparableVersion("7.10")

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

    @BeforeClass
    static void beforeAll() throws Exception {
        Assume.assumeTrue("Elasticsearch integration-tests not setup", isElasticsearchSetup())

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

    @Before
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
    }

    @After
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
        Assert.assertNotNull("Response was null", response)

        Assert.assertEquals("Wrong count", 15, response.numberOfHits)
        Assert.assertFalse("Timed out", response.isTimedOut())
        Assert.assertNotNull("Hits was null", response.getHits())
        Assert.assertEquals("Wrong number of hits", 10, response.hits.size())
        Assert.assertNotNull("Aggregations are missing", response.aggregations)
        Assert.assertEquals("Aggregation count is wrong", 1, response.aggregations.size())
        Assert.assertNull("Unexpected ScrollId", response.scrollId)
        Assert.assertNull("Unexpected Search_After", response.searchAfter)
        Assert.assertNull("Unexpected pitId", response.pitId)

        Map termCounts = response.aggregations.get("term_counts") as Map
        Assert.assertNotNull("Term counts was missing", termCounts)
        def buckets = termCounts.get("buckets")
        Assert.assertNotNull("Buckets branch was empty", buckets)
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
            Assert.assertEquals("${key} did not match.", expected[key as String], docCount)
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
        Assert.assertNotNull("Response was null", response)

        Assert.assertEquals("Wrong count", 15, response.numberOfHits)
        Assert.assertFalse("Timed out", response.isTimedOut())
        Assert.assertNotNull("Hits was null", response.getHits())
        Assert.assertEquals("Wrong number of hits", 10, response.hits.size())
        Assert.assertNotNull("Aggregations are missing", response.aggregations)
        Assert.assertEquals("Aggregation count is wrong", 1, response.aggregations.size())

        Map termCounts = response.aggregations.get("term_counts") as Map
        Assert.assertNotNull("Term counts was missing", termCounts)
        def buckets = termCounts.get("buckets")
        Assert.assertNotNull("Buckets branch was empty", buckets)
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
            Assert.assertEquals("${key} did not match.", expected[key], docCount)
        }
    }

    @Test
    void testSearchWarnings() {
        String query
        String type = TYPE
        if (VERSION.toString().startsWith("7.")) {
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
        Assert.assertTrue("Missing warnings", !response.warnings.isEmpty())
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
        Assert.assertNotNull("Response was null", response)

        Assert.assertEquals("Wrong count", 15, response.numberOfHits)
        Assert.assertFalse("Timed out", response.isTimedOut())
        Assert.assertNotNull("Hits was null", response.getHits())
        Assert.assertEquals("Wrong number of hits", 2, response.hits.size())
        Assert.assertNotNull("Aggregations are missing", response.aggregations)
        Assert.assertEquals("Aggregation count is wrong", 1, response.aggregations.size())
        Assert.assertNotNull("ScrollId missing", response.scrollId)
        Assert.assertNull("Unexpected Search_After", response.searchAfter)
        Assert.assertNull("Unexpected pitId", response.pitId)

        final Map termCounts = response.aggregations.get("term_counts") as Map
        Assert.assertNotNull("Term counts was missing", termCounts)
        Assert.assertEquals("Buckets count is wrong", 5, (termCounts.get("buckets") as List).size())

        // scroll the next page
        final SearchResponse scrollResponse = service.scroll(prettyPrint((toJson([scroll_id: response.scrollId, scroll: "10s"]))))
        Assert.assertNotNull("Scroll Response was null", scrollResponse)

        Assert.assertEquals("Wrong count", 15, scrollResponse.numberOfHits)
        Assert.assertFalse("Timed out", scrollResponse.isTimedOut())
        Assert.assertNotNull("Hits was null", scrollResponse.getHits())
        Assert.assertEquals("Wrong number of hits", 2, scrollResponse.hits.size())
        Assert.assertNotNull("Aggregations missing", scrollResponse.aggregations)
        Assert.assertEquals("Aggregation count is wrong", 0, scrollResponse.aggregations.size())
        Assert.assertNotNull("ScrollId missing", scrollResponse.scrollId)
        Assert.assertNull("Unexpected Search_After", scrollResponse.searchAfter)
        Assert.assertNull("Unexpected pitId", scrollResponse.pitId)

        Assert.assertNotEquals("Same results", scrollResponse.hits, response.hits)

        // delete the scroll
        DeleteOperationResponse deleteResponse = service.deleteScroll(scrollResponse.scrollId)
        Assert.assertNotNull("Delete Response was null", deleteResponse)
        Assert.assertTrue(deleteResponse.took > 0)

        // delete scroll again (should now be unknown but the 404 caught and ignored)
        deleteResponse = service.deleteScroll(scrollResponse.scrollId)
        Assert.assertNotNull("Delete Response was null", deleteResponse)
        Assert.assertEquals(0L, deleteResponse.took)
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
        Assert.assertNotNull("Response was null", response)

        Assert.assertEquals("Wrong count", 15, response.numberOfHits)
        Assert.assertFalse("Timed out", response.isTimedOut())
        Assert.assertNotNull("Hits was null", response.getHits())
        Assert.assertEquals("Wrong number of hits", 2, response.hits.size())
        Assert.assertNotNull("Aggregations missing", response.aggregations)
        Assert.assertEquals("Aggregation count is wrong", 1, response.aggregations.size())
        Assert.assertNull("Unexpected ScrollId", response.scrollId)
        Assert.assertNotNull("Search_After missing", response.searchAfter)
        Assert.assertNull("Unexpected pitId", response.pitId)

        final Map termCounts = response.aggregations.get("term_counts") as Map
        Assert.assertNotNull("Term counts was missing", termCounts)
        Assert.assertEquals("Buckets count is wrong", 5, (termCounts.get("buckets") as List).size())

        // search the next page
        queryMap.search_after = new JsonSlurper().parseText(response.searchAfter) as Serializable
        queryMap.remove("aggs")
        final String secondPage = prettyPrint(toJson(queryMap))
        final SearchResponse secondResponse = service.search(secondPage, INDEX, TYPE, null)
        Assert.assertNotNull("Second Response was null", secondResponse)

        Assert.assertEquals("Wrong count", 15, secondResponse.numberOfHits)
        Assert.assertFalse("Timed out", secondResponse.isTimedOut())
        Assert.assertNotNull("Hits was null", secondResponse.getHits())
        Assert.assertEquals("Wrong number of hits", 2, secondResponse.hits.size())
        Assert.assertNotNull("Aggregations missing", secondResponse.aggregations)
        Assert.assertEquals("Aggregation count is wrong", 0, secondResponse.aggregations.size())
        Assert.assertNull("Unexpected ScrollId", secondResponse.scrollId)
        Assert.assertNotNull("Unexpected Search_After", secondResponse.searchAfter)
        Assert.assertNull("Unexpected pitId", secondResponse.pitId)

        Assert.assertNotEquals("Same results", secondResponse.hits, response.hits)
    }

    @Test
    void testPointInTime() {
        // Point in Time only available in 7.10+ with XPack enabled
        Assume.assumeTrue("Requires version 7.10+", VERSION >= ES_7_10)
        Assume.assumeThat("Requires XPack features", FLAVOUR, is(DEFAULT))

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
        Assert.assertNotNull("Response was null", response)

        Assert.assertEquals("Wrong count", 15, response.numberOfHits)
        Assert.assertFalse("Timed out", response.isTimedOut())
        Assert.assertNotNull("Hits was null", response.getHits())
        Assert.assertEquals("Wrong number of hits", 2, response.hits.size())
        Assert.assertNotNull("Aggregations missing", response.aggregations)
        Assert.assertEquals("Aggregation count is wrong", 1, response.aggregations.size())
        Assert.assertNull("Unexpected ScrollId", response.scrollId)
        Assert.assertNotNull("Unexpected Search_After", response.searchAfter)
        Assert.assertNotNull("pitId missing", response.pitId)

        final Map termCounts = response.aggregations.get("term_counts") as Map
        Assert.assertNotNull("Term counts was missing", termCounts)
        Assert.assertEquals("Buckets count is wrong", 5, (termCounts.get("buckets") as List).size())

        // search the next page
        queryMap.search_after = new JsonSlurper().parseText(response.searchAfter) as Serializable
        queryMap.remove("aggs")
        final String secondPage = prettyPrint(toJson(queryMap))
        final SearchResponse secondResponse = service.search(secondPage, null, TYPE, null)
        Assert.assertNotNull("Second Response was null", secondResponse)

        Assert.assertEquals("Wrong count", 15, secondResponse.numberOfHits)
        Assert.assertFalse("Timed out", secondResponse.isTimedOut())
        Assert.assertNotNull("Hits was null", secondResponse.getHits())
        Assert.assertEquals("Wrong number of hits", 2, secondResponse.hits.size())
        Assert.assertNotNull("Aggregations missing", secondResponse.aggregations)
        Assert.assertEquals("Aggregation count is wrong", 0, secondResponse.aggregations.size())
        Assert.assertNull("Unexpected ScrollId", secondResponse.scrollId)
        Assert.assertNotNull("Unexpected Search_After", secondResponse.searchAfter)
        Assert.assertNotNull("pitId missing", secondResponse.pitId)

        Assert.assertNotEquals("Same results", secondResponse.hits, response.hits)

        // delete pitId
        DeleteOperationResponse deleteResponse = service.deletePointInTime(pitId)
        Assert.assertNotNull("Delete Response was null", deleteResponse)
        Assert.assertTrue(deleteResponse.took > 0)

        // delete pitId again (should now be unknown but the 404 caught and ignored)
        deleteResponse = service.deletePointInTime(pitId)
        Assert.assertNotNull("Delete Response was null", deleteResponse)
        Assert.assertEquals(0L, deleteResponse.took)
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
        Assert.assertNotNull(response)
        Assert.assertTrue(response.getTook() > 0)
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
        Assert.assertNotNull(response)
        Assert.assertTrue(response.getTook() > 0)
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
        Assert.assertNotNull(response)
        Assert.assertTrue(response.getTook() > 0)
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
        Assert.assertNotNull(response)
        Assert.assertTrue(response.getTook() > 0)
    }

    @Test
    void testDeleteById() throws Exception {
        final String ID = "1"
        final def originalDoc = service.get(INDEX, TYPE, ID, null)
        try {
            DeleteOperationResponse response = service.deleteById(INDEX, TYPE, ID, null)
            Assert.assertNotNull(response)
            Assert.assertTrue(response.getTook() > 0)
            final ElasticsearchException ee = Assert.assertThrows(ElasticsearchException.class, { ->
                service.get(INDEX, TYPE, ID, null) })
            Assert.assertTrue(ee.isNotFound())
            final def doc = service.get(INDEX, TYPE, "2", null)
            Assert.assertNotNull(doc)
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
            Assert.assertNotNull("Doc was null", doc)
            Assert.assertNotNull("${doc.toString()}\t${doc.keySet().toString()}", doc.get("msg"))
            old = doc
        }
    }

    @Test
    void testGetNotFound() {
        final ElasticsearchException ee = Assert.assertThrows(ElasticsearchException.class, { -> service.get(INDEX, TYPE, "not_found", null) })
        Assert.assertTrue(ee.isNotFound())
    }

    @Test
    void testSSL() {
        final String serviceIdentifier = SSLContextService.class.getName()
        final SSLContextService sslContext = mock(SSLContextService.class)
        when(sslContext.getIdentifier()).thenReturn(serviceIdentifier)

        final SSLContext clientSslContext = SslContextUtils.createSslContext(truststoreTlsConfiguration)
        when(sslContext.createContext()).thenReturn(clientSslContext)
        when(sslContext.createTlsConfiguration()).thenReturn(truststoreTlsConfiguration)

        runner.addControllerService(serviceIdentifier, sslContext)
        runner.enableControllerService(sslContext)

        runner.disableControllerService(service)
        runner.setProperty(service, ElasticSearchClientService.PROP_SSL_CONTEXT_SERVICE, serviceIdentifier)
        runner.enableControllerService(service)

        runner.assertValid(service)
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
        Assert.assertNotNull(response)
        Assert.assertTrue(response.getTook() > 0)
        waitForIndexRefresh()

        Map<String, Object> result = service.get("nulls", TYPE, "1", null)
        Assert.assertEquals(doc, result)

        // suppress nulls
        suppressNulls(true)
        response = service.bulk([new IndexOperationRequest("nulls", TYPE, "2", doc, IndexOperationRequest.Operation.Index)], null)
        Assert.assertNotNull(response)
        Assert.assertTrue(response.getTook() > 0)
        waitForIndexRefresh()

        result = service.get("nulls", TYPE, "2", null)
        Assert.assertTrue("Non-nulls (present): " + result.toString(), result.keySet().containsAll(["msg", "is_blank"]))
        Assert.assertFalse("is_null (should be omitted): " + result.toString(), result.keySet().contains("is_null"))
        Assert.assertFalse("is_empty (should be omitted): " + result.toString(), result.keySet().contains("is_empty"))
        Assert.assertFalse("empty_nested (should be omitted): " + result.toString(), result.keySet().contains("empty_nested"))
        Assert.assertFalse("empty_array (should be omitted): " + result.toString(), result.keySet().contains("empty_array"))
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
        Assert.assertNotNull(response)
        Assert.assertTrue(response.getTook() > 0)
        waitForIndexRefresh()

        /*
         * Now, check to ensure that both indexes got populated appropriately.
         */
        String query = "{ \"query\": { \"match_all\": {}}}"
        Long indexA = service.count(query, "bulk_a", TYPE, null)
        Long indexB = service.count(query, "bulk_b", TYPE, null)
        Long indexC = service.count(query, "bulk_c", TYPE, null)

        Assert.assertNotNull(indexA)
        Assert.assertNotNull(indexB)
        Assert.assertNotNull(indexC)
        Assert.assertEquals(indexA, indexB)
        Assert.assertEquals(10, indexA.intValue())
        Assert.assertEquals(10, indexB.intValue())
        Assert.assertEquals(5, indexC.intValue())

        Long total = service.count(query, "bulk_*", TYPE, null)
        Assert.assertNotNull(total)
        Assert.assertEquals(25, total.intValue())
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
        Assert.assertNotNull(response)
        Assert.assertTrue(response.getTook() > 0)

        /*
         * Now, check to ensure that both indexes got populated and refreshed appropriately.
         */
        String query = "{ \"query\": { \"match_all\": {}}}"
        Long indexA = service.count(query, "bulk_a", TYPE, null)
        Long indexB = service.count(query, "bulk_b", TYPE, null)
        Long indexC = service.count(query, "bulk_c", TYPE, null)

        Assert.assertNotNull(indexA)
        Assert.assertNotNull(indexB)
        Assert.assertNotNull(indexC)
        Assert.assertEquals(indexA, indexB)
        Assert.assertEquals(10, indexA.intValue())
        Assert.assertEquals(10, indexB.intValue())
        Assert.assertEquals(5, indexC.intValue())

        Long total = service.count(query, "bulk_*", TYPE, null)
        Assert.assertNotNull(total)
        Assert.assertEquals(25, total.intValue())
    }

    @Test
    void testUpdateAndUpsert() {
        final String TEST_ID = "update-test"
        Map<String, Object> doc = new HashMap<>()
        doc.put("msg", "Buongiorno, mondo")
        service.add(new IndexOperationRequest(INDEX, TYPE, TEST_ID, doc, IndexOperationRequest.Operation.Index), [refresh: "true"])
        Map<String, Object> result = service.get(INDEX, TYPE, TEST_ID, null)
        Assert.assertEquals("Not the same", doc, result)

        Map<String, Object> updates = new HashMap<>()
        updates.put("from", "john.smith")
        Map<String, Object> merged = new HashMap<>()
        merged.putAll(updates)
        merged.putAll(doc)
        IndexOperationRequest request = new IndexOperationRequest(INDEX, TYPE, TEST_ID, updates, IndexOperationRequest.Operation.Update)
        service.add(request, [refresh: "true"])
        result = service.get(INDEX, TYPE, TEST_ID, null)
        Assert.assertTrue(result.containsKey("from"))
        Assert.assertTrue(result.containsKey("msg"))
        Assert.assertEquals("Not the same after update.", merged, result)

        final String UPSERTED_ID = "upsert-ftw"
        Map<String, Object> upsertItems = new HashMap<>()
        upsertItems.put("upsert_1", "hello")
        upsertItems.put("upsert_2", 1)
        upsertItems.put("upsert_3", true)
        request = new IndexOperationRequest(INDEX, TYPE, UPSERTED_ID, upsertItems, IndexOperationRequest.Operation.Upsert)
        service.add(request, [refresh: "true"])
        result = service.get(INDEX, TYPE, UPSERTED_ID, null)
        Assert.assertEquals(upsertItems, result)

        List<IndexOperationRequest> deletes = new ArrayList<>()
        deletes.add(new IndexOperationRequest(INDEX, TYPE, TEST_ID, null, IndexOperationRequest.Operation.Delete))
        deletes.add(new IndexOperationRequest(INDEX, TYPE, UPSERTED_ID, null, IndexOperationRequest.Operation.Delete))
        Assert.assertFalse(service.bulk(deletes, [refresh: "true"]).hasErrors())
        waitForIndexRefresh() // wait 1s for index refresh (doesn't prevent GET but affects later tests using _search or _bulk)
        ElasticsearchException ee = Assert.assertThrows(ElasticsearchException.class, { -> service.get(INDEX, TYPE, TEST_ID, null) })
        Assert.assertTrue(ee.isNotFound())
        ee = Assert.assertThrows(ElasticsearchException.class, { -> service.get(INDEX, TYPE, UPSERTED_ID, null) })
        Assert.assertTrue(ee.isNotFound())
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