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

import org.apache.nifi.elasticsearch.DeleteOperationResponse
import org.apache.nifi.elasticsearch.ElasticSearchClientService
import org.apache.nifi.elasticsearch.ElasticSearchClientServiceImpl
import org.apache.nifi.elasticsearch.IndexOperationRequest
import org.apache.nifi.elasticsearch.IndexOperationResponse
import org.apache.nifi.elasticsearch.SearchResponse
import org.apache.nifi.security.util.KeystoreType
import org.apache.nifi.ssl.StandardSSLContextService
import org.apache.nifi.util.StringUtils
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test

import static groovy.json.JsonOutput.prettyPrint
import static groovy.json.JsonOutput.toJson

class ElasticSearch5ClientService_IT {

    private TestRunner runner
    private ElasticSearchClientServiceImpl service

    static String INDEX = "messages"
    static String TYPE  = StringUtils.isNotBlank(System.getProperty("type_name")) ? System.getProperty("type_name") : null;

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
        
        
        SearchResponse response = service.search(query, "messages", TYPE)
        Assert.assertNotNull("Response was null", response)

        Assert.assertEquals("Wrong count", 15, response.numberOfHits)
        Assert.assertFalse("Timed out", response.isTimedOut())
        Assert.assertNotNull("Hits was null", response.getHits())
        Assert.assertEquals("Wrong number of hits", 10, response.hits.size())
        Assert.assertNotNull("Aggregations are missing", response.aggregations)
        Assert.assertEquals("Aggregation count is wrong", 1, response.aggregations.size())

        Map termCounts = response.aggregations.get("term_counts")
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
            Assert.assertEquals("${key} did not match.", expected[key], docCount)
        }
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
        DeleteOperationResponse response = service.deleteByQuery(query, INDEX, TYPE)
        Assert.assertNotNull(response)
        Assert.assertTrue(response.getTook() > 0)
    }

    @Test
    void testDeleteById() throws Exception {
        final String ID = "1"
        DeleteOperationResponse response = service.deleteById(INDEX, TYPE, ID)
        Assert.assertNotNull(response)
        Assert.assertTrue(response.getTook() > 0)
        def doc = service.get(INDEX, TYPE, ID)
        Assert.assertNull(doc)
        doc = service.get(INDEX, TYPE, "2")
        Assert.assertNotNull(doc)
    }

    @Test
    void testGet() throws IOException {
        Map old
        System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n" + "TYPE: " + TYPE + "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
        1.upto(15) { index ->
            String id = String.valueOf(index)
            def doc = service.get(INDEX, TYPE, id)
            Assert.assertNotNull("Doc was null", doc)
            Assert.assertNotNull("${doc.toString()}\t${doc.keySet().toString()}", doc.get("msg"))
            old = doc
        }
    }

    @Test
    void testSSL() {
        def sslContext = new StandardSSLContextService()
        runner.setProperty(TestControllerServiceProcessor.CLIENT_SERVICE, "Client Service")
        runner.disableControllerService(service)
        runner.addControllerService("sslContext", sslContext)
        runner.setProperty(sslContext, StandardSSLContextService.TRUSTSTORE, "src/test/resources/truststore.jks")
        runner.setProperty(sslContext, StandardSSLContextService.TRUSTSTORE_PASSWORD, "2DZ5i7yvbG2GA3Ld4yiAsH62QDqAjWt4ToCU0yHajwM")
        runner.setProperty(sslContext, StandardSSLContextService.TRUSTSTORE_TYPE, KeystoreType.JKS.getType())
        runner.setProperty(service, ElasticSearchClientService.PROP_SSL_CONTEXT_SERVICE, "sslContext")
        runner.enableControllerService(sslContext)
        runner.enableControllerService(service)
        runner.assertValid()

        runner.disableControllerService(service)
        runner.disableControllerService(sslContext)
        runner.setProperty(sslContext, StandardSSLContextService.KEYSTORE, "src/test/resources/keystore.jks")
        runner.setProperty(sslContext, StandardSSLContextService.KEYSTORE_PASSWORD, "pben4DTOUhLDI8mZiCHNX1dGEAWrpGnSYX38FTvmaeU")
        runner.setProperty(sslContext, StandardSSLContextService.KEYSTORE_TYPE, KeystoreType.JKS.getType())
        runner.enableControllerService(sslContext)
        runner.enableControllerService(service)

        runner.assertValid()
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
        IndexOperationResponse response = service.bulk([new IndexOperationRequest("nulls", TYPE, "1", doc, IndexOperationRequest.Operation.Index)])
        Assert.assertNotNull(response)
        Assert.assertTrue(response.getTook() > 0)
        Thread.sleep(2000)

        Map<String, Object> result = service.get("nulls", TYPE, "1")
        Assert.assertEquals(doc, result)

        // suppress nulls
        suppressNulls(true)
        response = service.bulk([new IndexOperationRequest("nulls", TYPE, "2", doc, IndexOperationRequest.Operation.Index)])
        Assert.assertNotNull(response)
        Assert.assertTrue(response.getTook() > 0)
        Thread.sleep(2000)

        result = service.get("nulls", TYPE, "2")
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
        IndexOperationResponse response = service.bulk(payload)
        Assert.assertNotNull(response)
        Assert.assertTrue(response.getTook() > 0)
        Thread.sleep(2000)

        /*
         * Now, check to ensure that both indexes got populated appropriately.
         */
        String query = "{ \"query\": { \"match_all\": {}}}"
        Long indexA = service.count(query, "bulk_a", TYPE)
        Long indexB = service.count(query, "bulk_b", TYPE)
        Long indexC = service.count(query, "bulk_c", TYPE)

        Assert.assertNotNull(indexA)
        Assert.assertNotNull(indexB)
        Assert.assertNotNull(indexC)
        Assert.assertEquals(indexA, indexB)
        Assert.assertEquals(10, indexA.intValue())
        Assert.assertEquals(10, indexB.intValue())
        Assert.assertEquals(5, indexC.intValue())

        Long total = service.count(query, "bulk_*", TYPE)
        Assert.assertNotNull(total)
        Assert.assertEquals(25, total.intValue())
    }

    @Test
    void testUpdateAndUpsert() {
        final String TEST_ID = "update-test"
        Map<String, Object> doc = new HashMap<>()
        doc.put("msg", "Buongiorno, mondo")
        service.add(new IndexOperationRequest(INDEX, TYPE, TEST_ID, doc, IndexOperationRequest.Operation.Index))
        Map<String, Object> result = service.get(INDEX, TYPE, TEST_ID)
        Assert.assertEquals("Not the same", doc, result)

        Map<String, Object> updates = new HashMap<>()
        updates.put("from", "john.smith")
        Map<String, Object> merged = new HashMap<>()
        merged.putAll(updates)
        merged.putAll(doc)
        IndexOperationRequest request = new IndexOperationRequest(INDEX, TYPE, TEST_ID, updates, IndexOperationRequest.Operation.Update)
        service.add(request)
        result = service.get(INDEX, TYPE, TEST_ID)
        Assert.assertTrue(result.containsKey("from"))
        Assert.assertTrue(result.containsKey("msg"))
        Assert.assertEquals("Not the same after update.", merged, result)

        final String UPSERTED_ID = "upsert-ftw"
        Map<String, Object> upsertItems = new HashMap<>()
        upsertItems.put("upsert_1", "hello")
        upsertItems.put("upsert_2", 1)
        upsertItems.put("upsert_3", true)
        request = new IndexOperationRequest(INDEX, TYPE, UPSERTED_ID, upsertItems, IndexOperationRequest.Operation.Upsert)
        service.add(request)
        result = service.get(INDEX, TYPE, UPSERTED_ID)
        Assert.assertEquals(upsertItems, result)

        List<IndexOperationRequest> deletes = new ArrayList<>()
        deletes.add(new IndexOperationRequest(INDEX, TYPE, TEST_ID, null, IndexOperationRequest.Operation.Delete))
        deletes.add(new IndexOperationRequest(INDEX, TYPE, UPSERTED_ID, null, IndexOperationRequest.Operation.Delete))
        service.bulk(deletes)
        Assert.assertNull(service.get(INDEX, TYPE, TEST_ID))
        Assert.assertNull(service.get(INDEX, TYPE, UPSERTED_ID))
    }

    @Test
    void testGetBulkResponsesWithErrors() {
        def ops = [
                new IndexOperationRequest(INDEX, TYPE, "1", [ "msg": "one", intField: 1], IndexOperationRequest.Operation.Index), // OK
                new IndexOperationRequest(INDEX, TYPE, "2", [ "msg": "two", intField: 1], IndexOperationRequest.Operation.Create), // already exists
                new IndexOperationRequest(INDEX, TYPE, "1", [ "msg": "one", intField: "notaninteger"], IndexOperationRequest.Operation.Index) // can't parse int field
        ]
        def response = service.bulk(ops)
        assert response.hasErrors()
        assert response.items.findAll {
            def key = it.keySet().stream().findFirst().get()
            it[key].containsKey("error")
        }.size() == 2
    }
}
