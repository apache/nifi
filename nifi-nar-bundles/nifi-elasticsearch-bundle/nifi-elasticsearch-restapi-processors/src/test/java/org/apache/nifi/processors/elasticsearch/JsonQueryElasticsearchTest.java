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

package org.apache.nifi.processors.elasticsearch;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class JsonQueryElasticsearchTest {
    private static final String INDEX_NAME = "messages";

    public void testCounts(TestRunner runner, int success, int hits, int failure, int aggregations) {
        runner.assertTransferCount(JsonQueryElasticsearch.REL_ORIGINAL, success);
        runner.assertTransferCount(JsonQueryElasticsearch.REL_HITS, hits);
        runner.assertTransferCount(JsonQueryElasticsearch.REL_FAILURE, failure);
        runner.assertTransferCount(JsonQueryElasticsearch.REL_AGGREGATIONS, aggregations);
    }

    @Test
    public void testBasicQuery() throws Exception {

        JsonQueryElasticsearch processor = new JsonQueryElasticsearch();
        TestRunner runner = TestRunners.newTestRunner(processor);
        TestElasticSearchClientService service = new TestElasticSearchClientService(false);
        runner.addControllerService("esService", service);
        runner.enableControllerService(service);
        runner.setProperty(JsonQueryElasticsearch.CLIENT_SERVICE, "esService");
        runner.setProperty(JsonQueryElasticsearch.INDEX, INDEX_NAME);
        runner.setProperty(JsonQueryElasticsearch.TYPE, "message");
        runner.setValidateExpressionUsage(true);
        runner.setProperty(JsonQueryElasticsearch.QUERY, "{ \"query\": { \"match_all\": {} }}");

        runner.enqueue("test");
        runner.run(1, true, true);
        testCounts(runner, 1, 1, 0, 0);

        runner.setProperty(JsonQueryElasticsearch.SPLIT_UP_HITS, JsonQueryElasticsearch.SPLIT_UP_YES);
        runner.clearProvenanceEvents();
        runner.clearTransferState();
        runner.enqueue("test");
        runner.run(1, true, true);
        testCounts(runner, 1, 10, 0, 0);
    }

    @Test
    public void testAggregations() throws Exception {
        String query = "{\n" +
                "\t\"query\": {\n" +
                "\t\t\"match_all\": {}\n" +
                "\t},\n" +
                "\t\"aggs\": {\n" +
                "\t\t\"test_agg\": {\n" +
                "\t\t\t\"terms\": {\n" +
                "\t\t\t\t\"field\": \"msg\"\n" +
                "\t\t\t}\n" +
                "\t\t},\n" +
                "\t\t\"test_agg2\": {\n" +
                "\t\t\t\"terms\": {\n" +
                "\t\t\t\t\"field\": \"msg\"\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "\t}\n" +
                "}";


        JsonQueryElasticsearch processor = new JsonQueryElasticsearch();
        TestRunner runner = TestRunners.newTestRunner(processor);
        TestElasticSearchClientService service = new TestElasticSearchClientService(true);
        runner.addControllerService("esService", service);
        runner.enableControllerService(service);
        runner.setProperty(JsonQueryElasticsearch.CLIENT_SERVICE, "esService");
        runner.setProperty(JsonQueryElasticsearch.INDEX, INDEX_NAME);
        runner.setProperty(JsonQueryElasticsearch.TYPE, "message");
        runner.setValidateExpressionUsage(true);
        runner.setProperty(JsonQueryElasticsearch.QUERY, query);

        runner.enqueue("test");
        runner.run(1, true, true);
        testCounts(runner, 1, 1, 0, 1);

        runner.clearTransferState();

        //Test with the query parameter and no incoming connection
        runner.setIncomingConnection(false);
        runner.run(1, true, true);
        testCounts(runner, 0, 1, 0, 1);
        runner.setIncomingConnection(true);


        runner.clearTransferState();
        runner.clearProvenanceEvents();
        runner.setProperty(JsonQueryElasticsearch.SPLIT_UP_AGGREGATIONS, JsonQueryElasticsearch.SPLIT_UP_YES);
        runner.enqueue("test");
        runner.run(1, true, true);
        testCounts(runner, 1, 1, 0, 2);

        runner.clearProvenanceEvents();
        runner.clearTransferState();

        query = "{\n" +
                "\t\"query\": {\n" +
                "\t\t\"match_all\": {}\n" +
                "\t},\n" +
                "\t\"aggs\": {\n" +
                "\t\t\"test_agg\": {\n" +
                "\t\t\t\"terms\": {\n" +
                "\t\t\t\t\"field\": \"${fieldValue}\"\n" +
                "\t\t\t}\n" +
                "\t\t},\n" +
                "\t\t\"test_agg2\": {\n" +
                "\t\t\t\"terms\": {\n" +
                "\t\t\t\t\"field\": \"${fieldValue}\"\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "\t}\n" +
                "}";
        runner.setVariable("fieldValue", "msg");
        runner.setVariable("es.index", INDEX_NAME);
        runner.setVariable("es.type", "msg");
        runner.setProperty(JsonQueryElasticsearch.QUERY, query);
        runner.setProperty(JsonQueryElasticsearch.INDEX, "${es.index}");
        runner.setProperty(JsonQueryElasticsearch.TYPE, "${es.type}");
        runner.setValidateExpressionUsage(true);
        runner.enqueue("test");
        runner.run(1, true, true);
        testCounts(runner, 1, 1, 0, 2);
    }

    @Test
    public void testErrorDuringSearch() throws Exception {
        String query = "{\n" +
                "\t\"query\": {\n" +
                "\t\t\"match_all\": {}\n" +
                "\t},\n" +
                "\t\"aggs\": {\n" +
                "\t\t\"test_agg\": {\n" +
                "\t\t\t\"terms\": {\n" +
                "\t\t\t\t\"field\": \"msg\"\n" +
                "\t\t\t}\n" +
                "\t\t},\n" +
                "\t\t\"test_agg2\": {\n" +
                "\t\t\t\"terms\": {\n" +
                "\t\t\t\t\"field\": \"msg\"\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "\t}\n" +
                "}";


        JsonQueryElasticsearch processor = new JsonQueryElasticsearch();
        TestRunner runner = TestRunners.newTestRunner(processor);
        TestElasticSearchClientService service = new TestElasticSearchClientService(true);
        service.setThrowErrorInSearch(true);
        runner.addControllerService("esService", service);
        runner.enableControllerService(service);
        runner.setProperty(JsonQueryElasticsearch.CLIENT_SERVICE, "esService");
        runner.setProperty(JsonQueryElasticsearch.INDEX, INDEX_NAME);
        runner.setProperty(JsonQueryElasticsearch.TYPE, "message");
        runner.setValidateExpressionUsage(true);
        runner.setProperty(JsonQueryElasticsearch.QUERY, query);

        runner.enqueue("test");
        runner.run(1, true, true);
        testCounts(runner, 0, 0, 1, 0);
    }

    @Test
    public void testQueryAttribute() throws Exception {
        final String query = "{\n" +
                "\t\"query\": {\n" +
                "\t\t\"match_all\": {}\n" +
                "\t},\n" +
                "\t\"aggs\": {\n" +
                "\t\t\"test_agg\": {\n" +
                "\t\t\t\"terms\": {\n" +
                "\t\t\t\t\"field\": \"msg\"\n" +
                "\t\t\t}\n" +
                "\t\t},\n" +
                "\t\t\"test_agg2\": {\n" +
                "\t\t\t\"terms\": {\n" +
                "\t\t\t\t\"field\": \"msg\"\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "\t}\n" +
                "}";
        final String queryAttr = "es.query";


        JsonQueryElasticsearch processor = new JsonQueryElasticsearch();
        TestRunner runner = TestRunners.newTestRunner(processor);
        TestElasticSearchClientService service = new TestElasticSearchClientService(true);
        runner.addControllerService("esService", service);
        runner.enableControllerService(service);
        runner.setProperty(JsonQueryElasticsearch.CLIENT_SERVICE, "esService");
        runner.setProperty(JsonQueryElasticsearch.INDEX, INDEX_NAME);
        runner.setProperty(JsonQueryElasticsearch.TYPE, "message");
        runner.setValidateExpressionUsage(true);
        runner.setProperty(JsonQueryElasticsearch.QUERY, query);
        runner.setProperty(JsonQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr);

        runner.enqueue("test");
        runner.run(1, true, true);
        testCounts(runner, 1, 1, 0, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(JsonQueryElasticsearch.REL_AGGREGATIONS);
        flowFiles.addAll(runner.getFlowFilesForRelationship(JsonQueryElasticsearch.REL_HITS));

        for (MockFlowFile mockFlowFile : flowFiles) {
            String attr = mockFlowFile.getAttribute(queryAttr);
            Assert.assertNotNull("Missing query attribute", attr);
            Assert.assertEquals("Query had wrong value.", query, attr);
        }
    }
}
