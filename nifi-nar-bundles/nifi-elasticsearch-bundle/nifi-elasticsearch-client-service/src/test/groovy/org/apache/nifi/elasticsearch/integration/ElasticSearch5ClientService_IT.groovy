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
import org.apache.nifi.elasticsearch.SearchResponse
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

    static final String INDEX = "messages"
    static final String TYPE  = "message"

    @Before
    void before() throws Exception {
        runner = TestRunners.newTestRunner(TestControllerServiceProcessor.class)
        service = new ElasticSearchClientServiceImpl()
        runner.addControllerService("Client Service", service)
        runner.setProperty(service, ElasticSearchClientService.HTTP_HOSTS, "http://localhost:9400")
        runner.setProperty(service, ElasticSearchClientService.CONNECT_TIMEOUT, "10000")
        runner.setProperty(service, ElasticSearchClientService.SOCKET_TIMEOUT, "60000")
        runner.setProperty(service, ElasticSearchClientService.RETRY_TIMEOUT, "60000")
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
        
        
        SearchResponse response = service.search(query, "messages", "message")
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
        1.upto(15) { index ->
            String id = String.valueOf(index)
            def doc = service.get(INDEX, TYPE, id)
            Assert.assertNotNull("Doc was null", doc)
            Assert.assertNotNull("${doc.toString()}\t${doc.keySet().toString()}", doc.get("msg"))
            old = doc
        }
    }
}
