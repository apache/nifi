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

package org.apache.nifi.elasticsearch.integration;

import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.elasticsearch.ElasticSearchClientServiceImpl;
import org.apache.nifi.elasticsearch.SearchResponse;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticSearch5ClientService_IT {

    private TestRunner runner;
    private ElasticSearchClientServiceImpl service;

    @Before
    public void before() throws Exception {
        runner = TestRunners.newTestRunner(TestControllerServiceProcessor.class);
        service = new ElasticSearchClientServiceImpl();
        runner.addControllerService("Client Service", service);
        runner.setProperty(service, ElasticSearchClientService.HTTP_HOSTS, "http://localhost:9400");
        runner.setProperty(service, ElasticSearchClientService.CONNECT_TIMEOUT, "10000");
        runner.setProperty(service, ElasticSearchClientService.SOCKET_TIMEOUT, "60000");
        runner.setProperty(service, ElasticSearchClientService.RETRY_TIMEOUT, "60000");
        try {
            runner.enableControllerService(service);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @After
    public void after() throws Exception {
        service.onDisabled();
    }

    @Test
    public void testBasicSearch() throws Exception {
        String query = "{\n" +
                "\t\"size\": 10,\n" +
                "\t\"query\": {\n" +
                "\t\t\"match_all\": {}\n" +
                "\t},\n" +
                "\t\"aggs\": {\n" +
                "\t\t\"term_counts\": {\n" +
                "\t\t\t\"terms\": {\n" +
                "\t\t\t\t\"field\": \"msg\",\n" +
                "\t\t\t\t\"size\": 5\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "\t}\n" +
                "}";
        SearchResponse response = service.search(query, "messages", "message");
        Assert.assertNotNull("Response was null", response);

        Assert.assertEquals("Wrong count", response.getNumberOfHits(), 15);
        Assert.assertFalse("Timed out", response.isTimedOut());
        Assert.assertNotNull("Hits was null", response.getHits());
        Assert.assertEquals("Wrong number of hits", 10, response.getHits().size());
        Assert.assertNotNull("Aggregations are missing", response.getAggregations());
        Assert.assertEquals("Aggregation count is wrong", 1, response.getAggregations().size());

        Map<String, Object> termCounts = (Map<String, Object>) response.getAggregations().get("term_counts");
        Assert.assertNotNull("Term counts was missing", termCounts);
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) termCounts.get("buckets");
        Assert.assertNotNull("Buckets branch was empty", buckets);
        Map<String, Integer> expected = new HashMap<>();
        expected.put("one", 1);
        expected.put("two", 2);
        expected.put("three", 3);
        expected.put("four", 4);
        expected.put("five", 5);

        for (Map<String, Object> aggRes : buckets) {
            String key = (String)aggRes.get("key");
            Integer docCount = (Integer)aggRes.get("doc_count");

            Assert.assertEquals(String.format("%s did not match", key), expected.get(key), docCount);
        }
    }
}
